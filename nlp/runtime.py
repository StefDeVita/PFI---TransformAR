# nlp/runtime.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime
from functools import lru_cache
import re, unicodedata

from nlp.translation_qwen import QwenTranslator
from nlp.ollama_client import OllamaClient

# ---------- Normalización básica ----------
def norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()

def nkey(s: Any) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(s))
                   if unicodedata.category(c) != "Mn").lower().strip()

def parse_number(value: Any) -> Optional[float]:
    if value is None: return None
    v = str(value).strip()
    if not v: return None
    # quitar todo excepto dígitos, puntos, comas y signo
    v_clean = re.sub(r"[^\d,.\-]", "", v)
    if "," in v_clean and "." in v_clean:
        if v_clean.find(",") > v_clean.find("."):
            v_clean = v_clean.replace(".", "").replace(",", ".")
        else:
            v_clean = v_clean.replace(",", "")
    elif "," in v_clean and "." not in v_clean:
        v_clean = v_clean.replace(",", ".")
    try:
        return float(v_clean)
    except:
        return None


def find_keys(obj: Any, target: str):
    matches = []
    tgt = nkey(target)
    def _rec(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if nkey(k) == tgt:
                    matches.append((o, k))
                _rec(v)
        elif isinstance(o, list):
            for it in o: _rec(it)
    _rec(obj)
    return matches

# ---------- Fechas ----------
def format_date(val: Any, input_fmt: str, output_fmt: str) -> Optional[str]:
    s = norm(val)
    if not s:
        return None
    out = output_fmt or "%Y-%m-%d"

    # Caso con formato explícito
    if input_fmt and input_fmt != "infer":
        try:
            return datetime.strptime(s, input_fmt).strftime(out)
        except Exception:
            pass

    # Inferencia con formatos numéricos comunes
    m = re.search(r"(\d{1,4}[./\-]\d{1,2}[./\-]\d{2,4})", s)
    s_try = m.group(1) if m else s
    fmts = ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d","%d.%m.%Y",
            "%d/%m/%y","%d-%m-%y","%y-%m-%d","%m/%d/%Y","%m-%d-%Y")
    for f in fmts:
        try:
            return datetime.strptime(s_try, f).strftime(out)
        except Exception:
            pass

    # Inferencia con nombres de mes (es/en). En lugar de construir
    # un string ISO a mano, armamos un datetime y formateamos con `out`.
    months = { ... }  # (dejá tu dict como está)
    st = s.strip().lower()

    rx_es = re.compile(r"\b(?P<d>\d{1,2})\s+de\s+(?P<m>[a-záéíóúüñ]{3,15})\s+de\s+(?P<y>\d{2,4})\b")
    m = rx_es.search(st)
    if m and m.group("m") in months:
        try:
            dt = datetime(_y(m.group("y")), int(months[m.group("m")]), int(m.group("d")))
            return dt.strftime(out)
        except Exception:
            pass

    rx_en = re.compile(r"\b(?P<m>[a-záéíóúüñ]{3,15})\s+(?P<d>\d{1,2})(?:,\s*)?(?P<y>\d{2,4})\b")
    m = rx_en.search(st)
    if m and m.group("m") in months:
        try:
            dt = datetime(_y(m.group("y")), int(months[m.group("m")]), int(m.group("d")))
            return dt.strftime(out)
        except Exception:
            pass

    return None

def iso_dates_everywhere(obj: Any):
    month_words = ("ene","feb","mar","abr","may","jun","jul","ago","sep","sept","oct","nov","dic",
                   "jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec",
                   "enero","febrero","marzo","abril","mayo","junio","julio","agosto",
                   "septiembre","setiembre","octubre","noviembre","diciembre",
                   "january","february","march","april","may","june","july","august",
                   "september","october","november","december")
    def looks_dateish(t: str) -> bool:
        t = (t or "").strip().lower()
        if not (6 <= len(t) <= 40): return False
        if re.search(r"\d{1,4}([./\-])\d{1,2}\1\d{2,4}", t): return True
        return any(w in t for w in month_words)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                iso_dates_everywhere(v)
            elif isinstance(v, str) and looks_dateish(v):
                nv = format_date(v, "infer", "%Y-%m-%d")
                if nv: obj[k] = nv
    elif isinstance(obj, list):
        for it in obj: iso_dates_everywhere(it)

# ---------- Números ----------
def _format_num(num: float) -> str:
    return f"{num:.2f}".replace(".", ",")

def _is_pure_numeric_like(s: str) -> bool:
    if s is None: return False
    t = s.strip()
    if not t: return False
    if re.search(r"[A-Za-z]", t): return False
    if re.search(r"[/:#]", t): return False
    if re.search(r"(?<=\w)-(?=\w)", t): return False
    u = t.replace(" ", "").strip("()")
    u = re.sub(r"^[\+\-]", "", u)
    u = u.replace(".", "").replace(",", "")
    u = re.sub(r"%$", "", u)
    return bool(u) and u.isdigit()

def format_numbers_everywhere(obj: Any):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                format_numbers_everywhere(v)
            elif isinstance(v, str) and _is_pure_numeric_like(v):
                n = parse_number(v)
                if n is not None: obj[k] = _format_num(n)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, (dict, list)):
                format_numbers_everywhere(v)
            elif isinstance(v, str) and _is_pure_numeric_like(v):
                n = parse_number(v)
                if n is not None: obj[i] = _format_num(n)

# ---------- Limpieza de texto ----------
def _cleanup_spaces(s: Any) -> str:
    return re.sub(r"\s+", " ", "" if s is None else str(s)).strip()

_UPPER = "A-ZÁÉÍÓÚÜÑ"; _VOWELS = set("AEIOUÁÉÍÓÚÜ")
def _is_all_caps(tok: str) -> bool:
    return bool(re.fullmatch(rf"[{_UPPER}]+", tok))

def _split_caps_token(tok: str) -> str:
    s = tok; n = len(s)
    if n < 12 or not _is_all_caps(s): return tok
    parts, start = [], 0
    for i in range(1, n):
        if i - start >= 3 and (s[i-1] in _VOWELS) and (s[i] not in _VOWELS):
            rem = s[i:]
            if len(rem) >= 3 and any(ch in _VOWELS for ch in rem):
                parts.append(s[start:i]); start = i
    parts.append(s[start:])
    merged = []
    for p in parts:
        if merged and len(p) < 3: merged[-1] += p
        else: merged.append(p)
    return " ".join(merged)

def _split_glued_caps(txt: str) -> str:
    if not txt: return txt
    out = []
    for t in txt.split(" "):
        out.append(_split_caps_token(t) if _is_all_caps(t) and len(t) >= 12 else t)
    return " ".join(out)

_EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+"); _URL_RE = re.compile(r"https?://|www\.", re.I)
def _alpha_ratio(txt: str) -> float:
    if not txt: return 0.0
    letters = sum(ch.isalpha() for ch in txt); return letters / max(1, len(txt))
def looks_like_codeish(txt: str) -> bool:
    tokens = txt.split(); 
    if not tokens: return False
    upper_short = sum(1 for t in tokens if t.isupper() and len(t) <= 3)
    digits = sum(ch.isdigit() for ch in txt)
    symb = sum(ch in "/\\-_.:#()[]," for ch in txt)
    return (upper_short >= max(1, int(len(tokens)*0.8))) or ((digits+symb)/max(1,len(txt)) >= 0.5)
def looks_like_textual(txt: str) -> bool:
    if not txt or _EMAIL_RE.search(txt) or _URL_RE.search(txt): return False
    return _alpha_ratio(txt) >= 0.6 and (" " in txt)

_OLLAMA: Optional[OllamaClient] = None
def _client() -> OllamaClient:
    global _OLLAMA
    if _OLLAMA is None: _OLLAMA = OllamaClient()
    return _OLLAMA

@lru_cache(maxsize=2048)
def _llm_cleanup_cached(base: str, maxchars: int) -> str:
    try:
        system = ("Sos un corrector de texto técnico.\n"
                  "Separá palabras pegadas y corregí espacios/ortografía, sin modificar marcas/modelos/PN.\n"
                  "Devolvé solo el texto.")
        out = _client().chat_raw(system=system, user=f"Texto:\n{base[:maxchars]}", json_mode=False,
                                 options={"temperature": 0.2})
        return (out or "").strip() or base
    except Exception:
        return base

def auto_fix_strings(obj: Any, enable_llm: bool = True, maxchars: int = 800):
    def _fix(v: str) -> str:
        base = _split_glued_caps(_cleanup_spaces(v))
        if not enable_llm or not looks_like_textual(base) or looks_like_codeish(base):
            return base
        return _llm_cleanup_cached(base, maxchars)
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str):
                obj[k] = _fix(v)
            elif isinstance(v, (dict, list)):
                auto_fix_strings(v, enable_llm, maxchars)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str):
                obj[i] = _fix(v)
            elif isinstance(v, (dict, list)):
                auto_fix_strings(v, enable_llm, maxchars)

# ---------- Estructuras útiles ----------
def iter_items_nodes(doc: Any) -> List[Dict[str, Any]]:
    nodes: List[Dict[str, Any]] = []
    for parent, key in find_keys(doc, "items"):
        val = parent.get(key)
        if isinstance(val, list):
            nodes += [it for it in val if isinstance(it, dict)]
    return nodes

def collect_textual_fields(d: Dict[str, Any]) -> List[Tuple[Dict[str, Any], str]]:
    pairs: List[Tuple[Dict[str, Any], str]] = []
    for k, v in d.items():
        if isinstance(v, str):
            txt = norm(v)
            if txt and looks_like_textual(txt) and not looks_like_codeish(txt):
                pairs.append((d, k))
    return pairs

# ---------- Traductor singleton ----------
_QWEN: Optional[QwenTranslator] = None
def get_translator() -> QwenTranslator:
    global _QWEN
    if _QWEN is None: _QWEN = QwenTranslator()
    return _QWEN
