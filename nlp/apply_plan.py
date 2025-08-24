# nlp/apply_plan.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import re, unicodedata
from functools import lru_cache

from config.settings import (
    AUTO_TEXT_LLM, AUTO_TEXT_MAXCHARS, AUTO_ISO_DATES_DEFAULT
)
from nlp.translation_qwen import QwenTranslator
from nlp.ollama_client import OllamaClient

# ========= Helpers básicos =========

def _norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()

def _nkey(s: Any) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(s))
                   if unicodedata.category(c) != "Mn").lower().strip()

def _parse_number(value: Any) -> Optional[float]:
    if value is None: return None
    v = str(value).strip()
    if not v: return None
    if "," in v and "." in v:
        if v.find(",") > v.find("."):
            v = v.replace(".", "").replace(",", ".")
        else:
            v = v.replace(",", "")
    elif "," in v and "." not in v:
        v = v.replace(",", ".")
    v = re.sub(r"[^\d\.\-]+", "", v)
    try:
        return float(v)
    except:
        return None

def _find_keys(obj: Any, target: str):
    matches = []
    nk_target = _nkey(target)
    def _rec(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if _nkey(k) == nk_target:
                    matches.append((o, k))
                _rec(v)
        elif isinstance(o, list):
            for it in o: _rec(it)
    _rec(obj)
    return matches

# ========= Fechas =========

def _format_date(val: Any, input_fmt: str, output_fmt: str) -> Optional[str]:
    s = _norm(val)
    if not s:
        return None
    if input_fmt and input_fmt != "infer":
        try:
            return datetime.strptime(s, input_fmt).strftime(output_fmt)
        except Exception:
            return None
    fmts = ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d",
            "%d.%m.%Y","%d de %B de %Y","%d %B %Y","%d %b %Y")
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None

def _iso_dates_everywhere(obj: Any):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                _iso_dates_everywhere(v)
            elif isinstance(v, str) and "fecha" in _nkey(k):
                newv = _format_date(v, "infer", "%Y-%m-%d")
                if newv:
                    obj[k] = newv
    elif isinstance(obj, list):
        for it in obj:
            _iso_dates_everywhere(it)

def _format_num(num: float) -> str:
    """Formatea a 2 decimales, coma decimal, sin miles."""
    return f"{num:.2f}".replace(".", ",")

def _is_pure_numeric_like(s: str) -> bool:
    """
    True si TODO el string es “numérico” sin letras, sin barras, ni guiones internos.
    Permite signo inicial, espacios, puntos/comas (luego se normalizan), y paréntesis tipo (123).
    Evita tocar códigos como 'D954-2101' o 'G1/2-SS'.
    """
    if s is None:
        return False
    t = s.strip()
    if not t:
        return False
    # Si hay letras, ya no es sólo número
    if re.search(r"[A-Za-z]", t):
        return False
    # Si hay barras o #, lo consideramos código
    if re.search(r"[/:#]", t):
        return False
    # Si hay un guión en medio de caracteres/ dígitos (no como signo inicial) => parece código
    if re.search(r"(?<=\w)-(?=\w)", t):
        return False
    # Quitar espacios y paréntesis negativos (contabilidad)
    u = t.replace(" ", "").strip("()")
    # Quitar signo inicial
    u = re.sub(r"^[\+\-]", "", u)
    # Quitar separadores , .
    u = u.replace(".", "").replace(",", "")
    # Quitar % final si existiera
    u = re.sub(r"%$", "", u)
    # Debe quedar solo dígitos
    return bool(u) and u.isdigit()

def _format_numbers_everywhere(obj: Any):
    """
    Recorre dicts/listas y formatea SOLO strings que sean numéricas “puras”.
    No depende del nombre de las claves.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                _format_numbers_everywhere(v)
            elif isinstance(v, str) and _is_pure_numeric_like(v):
                num = _parse_number(v)
                if num is not None:
                    obj[k] = _format_num(num)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, (dict, list)):
                _format_numbers_everywhere(v)
            elif isinstance(v, str) and _is_pure_numeric_like(v):
                num = _parse_number(v)
                if num is not None:
                    obj[i] = _format_num(num)

# ========= Limpieza determinística y por contenido =========

def _cleanup_spaces(s: Any) -> str:
    if s is None:
        return ""
    txt = str(s)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

_UPPER_LETTERS = "A-ZÁÉÍÓÚÜÑ"
_VOWELS = set("AEIOUÁÉÍÓÚÜ")
def _is_all_caps_letters(token: str) -> bool:
    return bool(re.fullmatch(rf"[{_UPPER_LETTERS}]+", token))

def _split_caps_token(token: str) -> str:
    s = token
    n = len(s)
    if n < 12 or not _is_all_caps_letters(s):
        return token
    parts = []
    start = 0
    i = 1
    while i < n:
        if i - start >= 3:
            prev = s[i - 1]; cur = s[i]
            if (prev in _VOWELS) and (cur not in _VOWELS):
                rem = s[i:]
                if len(rem) >= 3 and any(ch in _VOWELS for ch in rem):
                    parts.append(s[start:i]); start = i
        i += 1
    parts.append(s[start:])
    merged = []
    for p in parts:
        if merged and len(p) < 3:
            merged[-1] = merged[-1] + p
        else:
            merged.append(p)
    return " ".join(merged)

def _split_glued_caps_in_text(txt: str) -> str:
    if not txt:
        return txt
    tokens = txt.split(" ")
    out = []
    for t in tokens:
        if _is_all_caps_letters(t) and len(t) >= 12:
            out.append(_split_caps_token(t))
        else:
            out.append(t)
    return " ".join(out)

_EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+")
_URL_RE = re.compile(r"https?://|www\.", re.I)

def _alpha_ratio(txt: str) -> float:
    if not txt: return 0.0
    letters = sum(ch.isalpha() for ch in txt)
    return letters / max(1, len(txt))

def _looks_like_codeish(txt: str) -> bool:
    tokens = txt.split()
    if not tokens: return False
    upper_short = sum(1 for t in tokens if t.isupper() and len(t) <= 3)
    digits = sum(ch.isdigit() for ch in txt)
    symb = sum(ch in "/\\-_.:#()[]," for ch in txt)
    non_alpha_ratio = (digits + symb) / max(1, len(txt))
    return (upper_short >= max(1, int(len(tokens) * 0.8))) or (non_alpha_ratio >= 0.5)

def _looks_like_textual(txt: str) -> bool:
    if not txt: return False
    if _EMAIL_RE.search(txt) or _URL_RE.search(txt): return False
    if _alpha_ratio(txt) >= 0.6 and (" " in txt): return True
    return False

def _auto_fix_strings(obj: Any, enable_llm: bool = True, maxchars: int = 800):
    def _fix(v: str) -> str:
        base = _cleanup_spaces(v)
        base = _split_glued_caps_in_text(base)  # determinístico
        if not enable_llm:
            return base
        if not _looks_like_textual(base) or _looks_like_codeish(base):
            return base
        return _llm_cleanup_cached(base, maxchars)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                obj[k] = _fix(v)
            elif isinstance(v, (dict, list)):
                _auto_fix_strings(v, enable_llm=enable_llm, maxchars=maxchars)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str):
                obj[i] = _fix(v)
            elif isinstance(v, (dict, list)):
                _auto_fix_strings(v, enable_llm=enable_llm, maxchars=maxchars)

# ========= LLM cleanup infra =========

_OLLAMA_CLIENT: Optional[OllamaClient] = None
def _client() -> OllamaClient:
    global _OLLAMA_CLIENT
    if _OLLAMA_CLIENT is None:
        _OLLAMA_CLIENT = OllamaClient()
    return _OLLAMA_CLIENT

@lru_cache(maxsize=2048)
def _llm_cleanup_cached(base: str, maxchars: int) -> str:
    try:
        system = (
            """
            Sos un corrector de texto técnico.
            Separá palabras pegadas y corregí espacios/ortografía, sin modificar marcas, modelos, ni numeros de parte.
            Devolvé solo el texto corregido, sin explicaciones.
            """
        )
        out = _client().chat_raw(
            system=system,
            user=f"Texto:\n{base[:maxchars]}",
            json_mode=False,
            options={"temperature": 0.2},
        )
        out = (out or "").strip()
        return out or base
    except Exception:
        return base

# ========= Qwen Translator singleton =========

_QWEN_TRANSLATOR: Optional[QwenTranslator] = None
def _get_translator() -> QwenTranslator:
    global _QWEN_TRANSLATOR
    if _QWEN_TRANSLATOR is None:
        _QWEN_TRANSLATOR = QwenTranslator()
    return _QWEN_TRANSLATOR

# ========= Utilidades para fallback de traducción =========

def _iter_items_nodes(doc: Any) -> List[Dict[str, Any]]:
    nodes: List[Dict[str, Any]] = []
    for parent, key in _find_keys(doc, "items"):
        val = parent.get(key)
        if isinstance(val, list):
            for it in val:
                if isinstance(it, dict):
                    nodes.append(it)
    return nodes

def _collect_textual_fields(d: Dict[str, Any]) -> List[Tuple[Dict[str, Any], str]]:
    pairs: List[Tuple[Dict[str, Any], str]] = []
    for k, v in d.items():
        if isinstance(v, str):
            txt = _norm(v)
            if txt and _looks_like_textual(txt) and not _looks_like_codeish(txt):
                pairs.append((d, k))
    return pairs

# ========= Motor principal =========

def execute_plan(doc_or_list: Any, plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]]
    if isinstance(doc_or_list, dict):
        docs = [doc_or_list]
    elif isinstance(doc_or_list, list):
        docs = [d for d in doc_or_list if isinstance(d, dict)]
    else:
        raise TypeError("execute_plan espera un dict o lista de dicts")

    out: List[Dict[str, Any]] = []

    for doc in docs:
        # 1) Limpieza determinística + (opcional) LLM
        _auto_fix_strings(doc, enable_llm=AUTO_TEXT_LLM, maxchars=AUTO_TEXT_MAXCHARS)

        # 2) Fechas ISO por defecto
        if AUTO_ISO_DATES_DEFAULT:
            _iso_dates_everywhere(doc)

        keep = True
        for step in (plan or []):
            op = (step or {}).get("op")

            if op == "rename_columns":
                mapping = step.get("map", {}) or {}
                for old, new in mapping.items():
                    for parent, key in _find_keys(doc, old):
                        parent[new] = parent.pop(key)

            elif op == "format_date":
                col = step.get("column")
                input_fmt = step.get("input_fmt") or "infer"
                output_fmt = step.get("output_fmt") or "%Y-%m-%d"
                for parent, key in _find_keys(doc, col):
                    newv = _format_date(parent[key], input_fmt, output_fmt)
                    if newv is not None:
                        parent[key] = newv

            elif op == "translate_values":
                cols = step.get("columns", []) or []
                target_lang = step.get("target_lang", "en")
                translator = _get_translator()

                translated_any = False

                # 1) Intento directo por columnas del plan
                for c in cols:
                    for parent, key in _find_keys(doc, c):
                        val = parent.get(key)
                        if val is None:
                            continue
                        text = _norm(val)
                        if not text:
                            continue
                        try:
                            parent[key] = translator.translate(text, target_lang)
                            translated_any = True
                        except Exception:
                            pass

                # 2) Fallback por contenido en items[] si el plan no matcheó nada
                if not translated_any:
                    for it in _iter_items_nodes(doc):
                        for parent, key in _collect_textual_fields(it):
                            text = _norm(parent.get(key))
                            if not text:
                                continue
                            try:
                                parent[key] = translator.translate(text, target_lang)
                                translated_any = True
                            except Exception:
                                pass
                # 3) (Opcional) Otro fallback: traducir todo textual del doc si aún no hubo match
                #    Descomentar si lo necesitás:
                if not translated_any:
                     def _translate_textual(o):
                         if isinstance(o, dict):
                             for k, v in o.items():
                                 if isinstance(v, str):
                                     t = _norm(v)
                                     if t and _looks_like_textual(t) and not _looks_like_codeish(t):
                                         try:
                                             o[k] = translator.translate(t, target_lang)
                                         except Exception:
                                             pass
                                 elif isinstance(v, (dict, list)):
                                     _translate_textual(v)
                         elif isinstance(o, list):
                             for el in o: _translate_textual(el)
                     _translate_textual(doc)

            elif op == "convert_units":
                cols = step.get("columns", []) or []
                target = step.get("target_unit") or ""
                for c in cols:
                    for parent, key in _find_keys(doc, c):
                        val = parent[key]
                        if val is not None:
                            parent[key] = f"{val} ({target})"

            elif op == "currency_to":
                cols = step.get("columns", []) or []
                target = (step.get("target") or "USD").upper()
                for c in cols:
                    for parent, key in _find_keys(doc, c):
                        num = _parse_number(parent[key])
                        if num is not None:
                            parent[key] = f"{num:.2f} {target}"
                            parent[key] = f"{(f'{num:.2f}'.replace('.', ','))} {target}"
                for parent, key in _find_keys(doc, "moneda"):
                    parent[key] = target

            elif op == "filter_equals":
                col = step.get("column"); val = step.get("value")
                ok = any(_norm(parent[key]) == _norm(val) for parent, key in _find_keys(doc, col))
                if not ok: keep = False; break

            elif op == "filter_contains":
                col = step.get("column"); val = step.get("value")
                ok = any(_norm(val) in _norm(parent[key]) for parent, key in _find_keys(doc, col))
                if not ok: keep = False; break

            elif op == "filter_compare":
                col = step.get("column"); cmpop = step.get("op"); val = step.get("value")
                ok = False
                for parent, key in _find_keys(doc, col):
                    a, b = _parse_number(parent[key]), _parse_number(val)
                    if a is None or b is None: continue
                    if {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}.get(cmpop, False):
                        ok = True; break
                if not ok: keep = False; break

            elif op == "filter_between":
                col = step.get("column"); rng = step.get("range", [])
                if not (isinstance(rng, list) and len(rng) == 2):
                    keep = False; break
                lo, hi = rng
                ok = False
                for parent, key in _find_keys(doc, col):
                    a = _parse_number(parent[key])
                    al, ah = _parse_number(lo), _parse_number(hi)
                    if None not in (a, al, ah) and al <= a <= ah:
                        ok = True; break
                if not ok: keep = False; break

            elif op == "export":
                pass

        if keep:
            _format_numbers_everywhere(doc)
            out.append(doc)

    return out
