from __future__ import annotations
from typing import List, Dict, Any, Optional
from datetime import datetime
import re, unicodedata

# ========= Helpers básicos =========

def _norm(s: Any) -> str:
    return ("" if s is None else str(s)).strip()

def _nkey(s: Any) -> str:
    # normaliza claves para matching flexible (sin tildes/case)
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
    """
    Busca claves que coincidan con target (ignorando case/tildes).
    Devuelve lista de referencias (dict, clave).
    """
    matches = []
    nk_target = _nkey(target)

    def _rec(o):
        if isinstance(o, dict):
            for k,v in o.items():
                if _nkey(k) == nk_target:
                    matches.append((o,k))
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
    # inferencia simple → ISO
    fmts = ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d","%d.%m.%Y","%d de %B de %Y","%d %B %Y","%d %b %Y")
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


# ========= Motor principal =========

def execute_plan(doc_or_list: Any, plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aplica el plan directamente sobre el JSON plano extraído por Qwen.
    Soporta operaciones genéricas sobre claves sin depender de un schema fijo.
    """
    docs: List[Dict[str, Any]]
    if isinstance(doc_or_list, dict):
        docs = [doc_or_list]
    elif isinstance(doc_or_list, list):
        docs = [d for d in doc_or_list if isinstance(d, dict)]
    else:
        raise TypeError("execute_plan espera un dict o lista de dicts")

    out: List[Dict[str, Any]] = []

    for doc in docs:
        keep = True
        for step in (plan or []):
            op = (step or {}).get("op")

            # ---------- rename_columns ----------
            if op == "rename_columns":
                mapping = step.get("map", {}) or {}
                for old, new in mapping.items():
                    for parent, key in _find_keys(doc, old):
                        parent[new] = parent.pop(key)

            # ---------- format_date ----------
            elif op == "format_date":
                col = step.get("column")
                input_fmt = step.get("input_fmt") or "infer"
                output_fmt = step.get("output_fmt") or "%Y-%m-%d"
                for parent, key in _find_keys(doc, col):
                    newv = _format_date(parent[key], input_fmt, output_fmt)
                    if newv is not None:
                        parent[key] = newv

            # ---------- translate_values (stub) ----------
            elif op == "translate_values":
                cols = step.get("columns", []) or []
                lang = step.get("target_lang", "EN")
                for c in cols:
                    for parent, key in _find_keys(doc, c):
                        if parent[key]:
                            parent[key] = f"[{lang}]{parent[key]}"

            # ---------- convert_units (stub simple) ----------
            elif op == "convert_units":
                cols = step.get("columns", []) or []
                target = step.get("target_unit") or ""
                for c in cols:
                    for parent, key in _find_keys(doc, c):
                        val = parent[key]
                        if val is not None:
                            parent[key] = f"{val} ({target})"

            # ---------- currency_to (stub) ----------
            elif op == "currency_to":
                cols = step.get("columns", []) or []
                target = (step.get("target") or "USD").upper()
                for c in cols:
                    for parent, key in _find_keys(doc, c):
                        num = _parse_number(parent[key])
                        if num is not None:
                            parent[key] = f"{num:.2f} {target}"
                # cambiar moneda global si existe
                for parent, key in _find_keys(doc, "moneda"):
                    parent[key] = target

            # ---------- filter_equals ----------
            elif op == "filter_equals":
                col = step.get("column"); val = step.get("value")
                ok = any(_norm(parent[key]) == _norm(val) for parent,key in _find_keys(doc,col))
                if not ok: keep=False; break

            # ---------- filter_contains ----------
            elif op == "filter_contains":
                col = step.get("column"); val = step.get("value")
                ok = any(_norm(val) in _norm(parent[key]) for parent,key in _find_keys(doc,col))
                if not ok: keep=False; break

            # ---------- filter_compare ----------
            elif op == "filter_compare":
                col = step.get("column"); cmpop = step.get("op"); val = step.get("value")
                ok=False
                for parent,key in _find_keys(doc,col):
                    a, b = _parse_number(parent[key]), _parse_number(val)
                    if a is None or b is None: continue
                    if {"<":a<b,"<=":a<=b,">":a>b,">=":a>=b}.get(cmpop, False):
                        ok=True; break
                if not ok: keep=False; break

            # ---------- filter_between ----------
            elif op == "filter_between":
                col = step.get("column"); rng = step.get("range",[])
                if not (isinstance(rng,list) and len(rng)==2):
                    keep=False; break
                lo, hi = rng
                ok=False
                for parent,key in _find_keys(doc,col):
                    a = _parse_number(parent[key])
                    al, ah = _parse_number(lo), _parse_number(hi)
                    if None not in (a,al,ah) and al<=a<=ah:
                        ok=True; break
                if not ok: keep=False; break

            # ---------- export ----------
            elif op == "export":
                # No-op aquí; se maneja fuera
                pass

        if keep:
            out.append(doc)

    return out
