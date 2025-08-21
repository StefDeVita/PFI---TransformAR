# apply_plan.py
from __future__ import annotations
from typing import List, Dict, Any, Optional, Tuple
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

# ========= Conversión de unidades =========

_LEN_MM = {"mm":1.0,"cm":10.0,"m":1000.0,"in":25.4,'"':25.4,"inch":25.4,"pulgada":25.4,"pulgadas":25.4}
_W_G   = {"g":1.0,"kg":1000.0,"lb":453.59237,"lbs":453.59237}
_V_ML  = {"ml":1.0,"l":1000.0,"lt":1000.0,"litro":1000.0,"litros":1000.0}

def _parse_val_unit(x: Any) -> Tuple[Optional[float], Optional[str]]:
    if x is None: return None, None
    t = str(x).strip().lower()
    m = re.match(r'^\s*([\d\.,\-]+)\s*["”]\s*$', t)
    if m: return _parse_number(m.group(1)), "in"
    m = re.match(r'^\s*([\d\.,\-]+)\s*([a-záéíóú"”]+)\s*$', t, re.I)
    if m: return _parse_number(m.group(1)), m.group(2).replace("”", '"')
    m = re.match(r'^\s*([a-záéíóú"”]+)\s*([\d\.,\-]+)\s*$', t, re.I)
    if m: return _parse_number(m.group(2)), m.group(1).replace("”", '"')
    v = _parse_number(t)
    return (v, None) if v is not None else (None, None)

def _len_to(value: Any, target: str) -> Optional[str]:
    v,u = _parse_val_unit(value); target=target.lower()
    if v is None: return None
    if u in _LEN_MM: mm = v*_LEN_MM[u]
    elif u is None:  return f"{v:.2f} {target}"
    else:            return f"{v:.2f} {u}"
    if target not in _LEN_MM: return f"{mm:.2f} mm"
    return f"{mm/_LEN_MM[target]:.2f} {target}"

def _w_to(value: Any, target: str) -> Optional[str]:
    v,u = _parse_val_unit(value); target=target.lower()
    if v is None: return None
    if u in _W_G: g = v*_W_G[u]
    elif u is None: return f"{v:.2f} {target}"
    else:           return f"{v:.2f} {u}"
    if target not in _W_G: return f"{g:.2f} g"
    return f"{g/_W_G[target]:.2f} {target}"

def _v_to(value: Any, target: str) -> Optional[str]:
    v,u = _parse_val_unit(value); target=target.lower()
    if v is None: return None
    if u in _V_ML: ml = v*_V_ML[u]
    elif u is None: return f"{v:.2f} {target}"
    else:           return f"{v:.2f} {u}"
    if target not in _V_ML: return f"{ml:.2f} ml"
    return f"{ml/_V_ML[target]:.2f} {target}"

def _convert_units_scalar(value: Any, target: str) -> Optional[str]:
    t = (target or "").lower()
    if t in _LEN_MM: return _len_to(value, t)
    if t in _W_G:   return _w_to(value, t)
    if t in _V_ML:  return _v_to(value, t)
    return f"{value} ({target})" if value is not None else None

# ========= Acceso al schema (tags) =========

# Mapeo flexible de “columnas lógicas” -> rutas del schema de tags
# (podés extenderlo cuando sumes nuevas ops)
_SCHEMA_MAP = {
    "fecha":        ("dates", "issue_date"),
    "date":         ("dates", "issue_date"),
    "fec":          ("dates", "issue_date"),
    "monto":        ("totals", "total"),
    "importe":      ("totals", "total"),
    "total":        ("totals", "total"),
    "amount":       ("totals", "total"),
    "moneda":       ("totals", "currency"),
    "currency":     ("totals", "currency"),
    "divisa":       ("totals", "currency"),
    "descripcion":  ("items", "*", "description"),
    "description":  ("items", "*", "description"),
    "largo":        ("items", "*", "largo"),
    "ancho":        ("items", "*", "ancho"),
    "alto":         ("items", "*", "alto"),
}

def _get_path_for_column(column: str) -> Tuple[str, ...] | None:
    return _SCHEMA_MAP.get(_nkey(column))

def _ensure_obj(d: Dict[str, Any], k: str) -> Dict[str, Any]:
    if k not in d or not isinstance(d[k], dict):
        d[k] = {}
    return d[k]

def _get_value_by_path(tags: Dict[str, Any], path: Tuple[str, ...]) -> Any:
    cur: Any = tags
    for i, key in enumerate(path):
        if key == "*":
            # lista: devolvemos la lista (para que el caller itere)
            return cur if isinstance(cur, list) else None
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur

def _set_value_by_path(tags: Dict[str, Any], path: Tuple[str, ...], value: Any) -> None:
    cur = tags
    for i, key in enumerate(path):
        if key == "*":
            # aplicar en todos los items existentes
            arr = cur if isinstance(cur, list) else None
            if arr is None: 
                return
            last_keys = path[i+1:]
            for j in range(len(arr)):
                if not isinstance(arr[j], dict):
                    continue
                _set_value_by_path(arr[j], tuple(last_keys), value)
            return
        if i == len(path)-1:
            cur[key] = value
            return
        # avanzar creando dicts si faltan
        cur = _ensure_obj(cur, key)

# ========= Ops =========

def _format_date(val: Any, input_fmt: str, output_fmt: str) -> Optional[str]:
    s = _norm(val)
    if not s:
        return None
    if input_fmt and input_fmt != "infer":
        try:
            return datetime.strptime(s, input_fmt).strftime(output_fmt)
        except Exception:
            return None
    # inferencia simple
    fmts = ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d","%d.%m.%Y","%d %B %Y","%d %b %Y")
    for f in fmts:
        try:
            return datetime.strptime(s, f).strftime(output_fmt)
        except Exception:
            continue
    return None

def _filter_eq(val: Any, target: Any) -> bool:
    return _nkey(val) == _nkey(target)

def _filter_contains(val: Any, needle: Any) -> bool:
    return _nkey(needle) in _nkey(val)

def _filter_compare(val: Any, op: str, target: Any) -> bool:
    a, b = _parse_number(val), _parse_number(target)
    if a is None or b is None:
        return False
    return {"<": a<b, "<=": a<=b, ">": a>b, ">=": a>=b}.get(op, False)

def _as_date(s: Any) -> Optional[datetime]:
    if s is None: return None
    for f in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d"):
        try: return datetime.strptime(str(s), f)
        except: pass
    return None

def _filter_between(val: Any, lo: Any, hi: Any) -> bool:
    # fecha
    dv, dl, dh = _as_date(val), _as_date(lo), _as_date(hi)
    if dv and dl and dh:
        return dl <= dv <= dh
    # número
    av, al, ah = _parse_number(val), _parse_number(lo), _parse_number(hi)
    if None not in (av, al, ah):
        return al <= av <= ah
    return False

def _nkey(s: str) -> str:
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFD", str(s))
                   if unicodedata.category(c) != "Mn").lower().strip()

def _rename_keys_recursive(obj, mapping: dict) -> None:
    """
    Renombra claves en todo el árbol (dicts y listas).
    Matching insensible a tildes/mayúsculas.
    mapping: {"description": "descripcion", ...}
    """
    if not isinstance(mapping, dict) or not mapping:
        return
    norm_map = {_nkey(k): v for k, v in mapping.items()}

    if isinstance(obj, dict):
        # renombrar las claves del nivel actual
        for k in list(obj.keys()):
            nk = _nkey(k)
            if nk in norm_map:
                new_k = norm_map[nk]
                if new_k not in obj:
                    obj[new_k] = obj.pop(k)
                else:
                    _ = obj.pop(k)  # si ya existe el destino, priorizamos el destino
        # recorrer valores (ya con posibles claves nuevas)
        for v in obj.values():
            _rename_keys_recursive(v, mapping)

    elif isinstance(obj, list):
        for i in range(len(obj)):
            _rename_keys_recursive(obj[i], mapping)

# ========= Motor principal =========

def execute_plan(tags_or_list: Any, plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aplica el plan directamente sobre el/los JSON de tags (schema Qwen).
    Acepta dict (un documento) o lista de dicts (varios).
    Devuelve siempre una lista (de uno o más JSON transformados).
    """
    print(plan)
    docs: List[Dict[str, Any]]
    if isinstance(tags_or_list, dict):
        docs = [tags_or_list]
    elif isinstance(tags_or_list, list):
        docs = [d for d in tags_or_list if isinstance(d, dict)]
    else:
        raise TypeError("execute_plan espera un dict de tags o lista de tags")

    out: List[Dict[str, Any]] = []

    for doc in docs:
        keep = True
        # Aplicamos cada paso sobre el JSON original
        for step in (plan or []):
            op = (step or {}).get("op")

            # ---------- rename_columns ----------
            if op == "rename_columns":
                mapping = step.get("map", {}) or {}
                # Renombrar claves en TODO el JSON original (incluye items[*].description)
                _rename_keys_recursive(doc, mapping)

            # ---------- format_date ----------
            elif op == "format_date":
                col = step.get("column")
                p = _get_path_for_column(col)
                if not p: 
                    continue
                input_fmt = step.get("input_fmt") or "infer"
                output_fmt = step.get("output_fmt") or "%Y-%m-%d"
                cur = _get_value_by_path(doc, p)
                if isinstance(cur, list) and "*" in p:
                    # no tiene sentido formatear fechas en items[*].description
                    continue
                newv = _format_date(cur, input_fmt, output_fmt)
                if newv is not None:
                    _set_value_by_path(doc, p, newv)

            # ---------- translate_values ----------
            elif op == "translate_values":
                cols = step.get("columns", []) or []
                lang = step.get("target_lang", "EN")
                for c in cols:
                    p = _get_path_for_column(c)
                    if not p: 
                        continue
                    cur = _get_value_by_path(doc, p)
                    if isinstance(cur, list) and "*" in p:
                        # items[*].campo
                        for i, item in enumerate(cur or []):
                            if not isinstance(item, dict): 
                                continue
                            leaf = p[-1]
                            if leaf in item and item[leaf]:
                                item[leaf] = f"[{lang}]{item[leaf]}"
                    else:
                        if cur:
                            _set_value_by_path(doc, p, f"[{lang}]{cur}")

            # ---------- convert_units ----------
            elif op == "convert_units":
                cols = step.get("columns", []) or []
                target = step.get("target_unit")
                for c in cols:
                    p = _get_path_for_column(c)
                    if not p: 
                        continue
                    cur = _get_value_by_path(doc, p)
                    if isinstance(cur, list) and "*" in p:
                        leaf = p[-1]
                        for item in (cur or []):
                            if isinstance(item, dict) and leaf in item and item[leaf]:
                                newv = _convert_units_scalar(item[leaf], target)
                                if newv is not None:
                                    item[leaf] = newv
                    else:
                        if cur is None: 
                            continue
                        newv = _convert_units_scalar(cur, target)
                        if newv is not None:
                            _set_value_by_path(doc, p, newv)

            # ---------- filter_equals ----------
            elif op == "filter_equals":
                col = step.get("column"); val = step.get("value")
                p = _get_path_for_column(col)
                if not p: 
                    keep = False; break
                cur = _get_value_by_path(doc, p)
                if isinstance(cur, list) and "*" in p:
                    # Si el filtro apunta a items[*].x: mantenemos si algún item cumple
                    leaf = p[-1]
                    ok = any(isinstance(it, dict) and _norm(it.get(leaf)) and _norm(it.get(leaf)).lower()==_norm(val).lower()
                             for it in (cur or []))
                    if not ok: keep=False; break
                else:
                    if _norm(cur).lower() != _norm(val).lower():
                        keep=False; break

            # ---------- filter_contains ----------
            elif op == "filter_contains":
                col = step.get("column"); val = step.get("value")
                p = _get_path_for_column(col)
                if not p: keep=False; break
                cur = _get_value_by_path(doc, p)
                if isinstance(cur, list) and "*" in p:
                    leaf = p[-1]
                    ok = any(isinstance(it, dict) and _norm(val).lower() in _norm(it.get(leaf)).lower()
                             for it in (cur or []))
                    if not ok: keep=False; break
                else:
                    if _norm(val).lower() not in _norm(cur).lower():
                        keep=False; break

            # ---------- filter_compare ----------
            elif op == "filter_compare":
                col = step.get("column"); cmpop = step.get("op"); val = step.get("value")
                p = _get_path_for_column(col)
                if not p: keep=False; break
                cur = _get_value_by_path(doc, p)
                a, b = _parse_number(cur), _parse_number(val)
                if a is None or b is None:
                    keep=False; break
                ok = {"<":a<b,"<=":a<=b,">":a>b,">=":a>=b}.get(cmpop, False)
                if not ok: keep=False; break

            # ---------- filter_between ----------
            elif op == "filter_between":
                rng = step.get("range", [])
                col = step.get("column") or "fecha"
                p = _get_path_for_column(col)
                if not p or not isinstance(rng, list) or len(rng)!=2: keep=False; break
                cur = _get_value_by_path(doc, p)

                def _as_date(s):
                    for f in ("%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d"):
                        try: return datetime.strptime(str(s), f)
                        except: pass
                    return None

                if isinstance(cur, list) and "*" in p:
                    # mantener si algún item cae dentro del rango
                    leaf = p[-1]
                    ok = False
                    for it in (cur or []):
                        if not isinstance(it, dict): continue
                        v = it.get(leaf)
                        dv, da, db = _as_date(v), _as_date(rng[0]), _as_date(rng[1])
                        if dv and da and db:
                            ok = da <= dv <= db
                        else:
                            av, al, ah = _parse_number(v), _parse_number(rng[0]), _parse_number(rng[1])
                            ok = (None not in (av,al,ah)) and (al <= av <= ah)
                        if ok: break
                    if not ok: keep=False; break
                else:
                    dv, da, db = _as_date(cur), _as_date(rng[0]), _as_date(rng[1])
                    if dv and da and db:
                        if not (da <= dv <= db): keep=False; break
                    else:
                        av, al, ah = _parse_number(cur), _parse_number(rng[0]), _parse_number(rng[1])
                        if None in (av,al,ah) or not (al <= av <= ah):
                            keep=False; break

            # ---------- currency_to ----------
            elif op == "currency_to":
                cols = step.get("columns", []) or []
                target = (step.get("target") or "USD").upper()
                rate = step.get("rate", 1.0)
                rate_table = step.get("rate_table") or {}
                defaults = {"ARS":0.005,"USD":1.0,"EUR":1.1}

                # moneda actual está en totals.currency
                cur_curr = _get_value_by_path(doc, ("totals","currency"))
                cur_curr = (cur_curr or "ARS").upper()

                if isinstance(rate,(int,float)):
                    factor = float(rate)
                elif rate == "table":
                    factor = float(rate_table.get(cur_curr, defaults.get(cur_curr, 1.0)))
                else:
                    factor = float(defaults.get(cur_curr, 1.0))

                for c in cols:
                    p = _get_path_for_column(c)
                    if not p: continue
                    cur = _get_value_by_path(doc, p)
                    if isinstance(cur, list) and "*" in p:
                        leaf = p[-1]
                        for it in (cur or []):
                            if not isinstance(it, dict): continue
                            num = _parse_number(it.get(leaf))
                            if num is None: continue
                            it[leaf] = f"{num*factor:.2f}"
                    else:
                        num = _parse_number(cur)
                        if num is None: continue
                        _set_value_by_path(doc, p, f"{num*factor:.2f}")

                # setear moneda destino
                _set_value_by_path(doc, ("totals","currency"), target)

            # ---------- export ----------
            elif op == "export":
                # No-op aquí; la exportación se maneja fuera.
                pass

            # ops desconocidas: ignorar sin romper

        if keep:
            out.append(doc)

    return out