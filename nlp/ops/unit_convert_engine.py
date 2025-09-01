# nlp/ops/unit_convert_engine.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Union, Optional
from dataclasses import dataclass
import json, re

from config.settings import OLLAMA_INPUT_LIMIT, OLLAMA_MODEL
from nlp.ollama_client import OllamaClient
from nlp.runtime import collect_textual_fields  # para dar contexto al LLM

# ---- Pint (conversión determinística)
try:
    from pint import UnitRegistry
    _ureg = UnitRegistry(autoconvert_offset_to_baseunit=True)
    _Q_ = _ureg.Quantity
except Exception:
    _ureg = None
    _Q_ = None

JSON = Union[Dict[str, Any], List[Any], int, float, str, bool, None]

# ---- Regex y helpers internos
_UNIT_KEYS  = {
    "unidad","unit","units","uom","measure_unit","unit_of_measure","um",
    "medida_u","u_de_medida","unidad_medida"
}
_NUM_UNIT_RE = re.compile(
    r"^\s*(?P<num>[+-]?(?:\d+(?:[.,]\d+)?|\d{1,3}(?:[.,]\d{3})+(?:[.,]\d+)?))\s*(?P<u>[a-zA-Zµμ%/^\-\._]+)\s*$"
)
_KEY_UNIT_SUFFIX_RE = re.compile(
    r"(?:_|-)(km|m|cm|mm|µm|um|kg|g|mg|lb|lbs|oz|l|lt|L|ml|mL|m3|cm3|mm3)$",
    re.IGNORECASE
)

def _norm_num_locale(s: str) -> float:
    s = s.strip()
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        s = s.replace(",", ".")
    return float(s)

def _to_float(x: Any) -> Optional[float]:
    try:
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            return _norm_num_locale(x)
    except Exception:
        return None
    return None

def _fmt_num(x: float) -> str:
    if x == 0: return "0"
    s = f"{x:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"

def _unit_from_key_name(k: str) -> Optional[str]:
    m = _KEY_UNIT_SUFFIX_RE.search(k or "")
    return m.group(1) if m else None

def _extract_json_from_any(raw: str) -> dict:
    raw_clean = (raw or "").strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw_clean, flags=re.I)
    if m:
        raw_clean = m.group(1).strip()
    try:
        return json.loads(raw_clean)
    except Exception:
        start, end = raw_clean.find("{"), raw_clean.rfind("}")
        if start >= 0 and end > start:
            return json.loads(raw_clean[start:end+1])
        raise ValueError(f"No se pudo parsear JSON: {raw_clean[:200]}")

# ---- Interfaz con LLM para interpretar instrucción de unidades
@dataclass
class ParsedInstruction:
    target_unit: Optional[str]
    category_hint: Optional[str]
    custom_units: Dict[str, Tuple[float, str]]

_MODEL_SYS = (
    "Eres un asistente para extracción estructurada. "
    "Devuelve SOLO JSON válido con este esquema:\n"
    "{"
    "\"target_unit\": \"<unidad destino o null>\", "
    "\"category_hint\": \"masa|longitud|volumen|area|null\", "
    "\"custom_units\": {\"<nombre>\": [factor, \"unidad_base\"]}"
    "}\n"
    "Reglas:\n"
    "- 'target_unit' puede ser estándar (kg, m, L, mm, in, etc.) o personalizada (Ejemplo: camion).\n"
    "- Si el usuario define una unidad personalizada (Ejemplo:\"1 camion = 10 m\" / \"cada camion lleva 10 m\"), inclúyela en custom_units.\n"
    "- 'category_hint' es una pista general (masa/longitud/volumen/area). Si no, null.\n"
)

_MODEL_USER_TMPL = (
    "INSTRUCCIÓN:\n{instr}\n\n"
    "CONTEXT (extractos del documento para orientarte, puede estar vacío):\n{ctx}\n"
)

def _ask_model_for_units(instruction: str, doc: JSON) -> ParsedInstruction:
    client = OllamaClient(model=OLLAMA_MODEL)
    ctx_parts: List[str] = []
    for parent, key in collect_textual_fields(doc):
        val = parent.get(key)
        if isinstance(val, str):
            ctx_parts.append(val)
    ctx = "\n".join(ctx_parts)[:OLLAMA_INPUT_LIMIT]

    user = _MODEL_USER_TMPL.format(instr=instruction or "", ctx=ctx)
    raw = client.chat_json(system=_MODEL_SYS, user=user, options={"top_p": 0.4, "temperature": 0.4})
    out = _extract_json_from_any(raw)

    target_unit = out.get("target_unit") or None
    if isinstance(target_unit, str) and not target_unit.strip():
        target_unit = None
    category_hint = out.get("category_hint") or None
    if isinstance(category_hint, str) and category_hint.lower() == "null":
        category_hint = None

    custom: Dict[str, Tuple[float, str]] = {}
    for k, v in (out.get("custom_units") or {}).items():
        try:
            factor = float(v[0]); base_u = str(v[1])
            custom[k.lower()] = (factor, base_u)
        except Exception:
            continue
    return ParsedInstruction(target_unit=target_unit, category_hint=category_hint, custom_units=custom)

# ---- Utilidades Pint
def _ensure_pint_custom_units(custom: Dict[str, Tuple[float, str]]):
    if _ureg is None:
        return
    for name, (factor, base_u) in custom.items():
        try:
            _ureg.define(f"{name} = {factor} * {base_u}")
        except Exception:
            pass
        # alias singular/plural básico
        try:
            if name.endswith("es"):
                _ureg.define(f"{name[:-2]} = {name}")
            elif name.endswith("s"):
                _ureg.define(f"{name[:-1]} = {name}")
        except Exception:
            pass

def _maybe_quantity(value: Any, unit: Optional[str]) -> Optional[Any]:
    v = _to_float(value)
    if v is None or unit is None:
        return None
    if _ureg is not None:
        try:
            return v * _ureg(unit)
        except Exception:
            return None
    return (v, unit)  # fallback

def _convert_quantity(q: Any, target_unit: str) -> Optional[float]:
    if _ureg is not None:
        try:
            return float(q.to(target_unit).magnitude)
        except Exception:
            return None
    try:
        v, u = q
        if u and u.strip().lower() == target_unit.strip().lower():
            return float(v)
    except Exception:
        pass
    return None

# ---- pistas de unidad en el documento
def _first_sibling_unit(o: Any) -> Optional[str]:
    if isinstance(o, dict):
        for k, v in o.items():
            if isinstance(v, str) and k.lower() in _UNIT_KEYS and v.strip():
                return v.strip()
        for v in o.values():
            r = _first_sibling_unit(v)
            if r: return r
    elif isinstance(o, list):
        for it in o:
            r = _first_sibling_unit(it)
            if r: return r
    return None

# ---- definir unidad custom a partir de conversion_value del step
def _define_custom_unit_from_step(target_unit: str, conversion_value: Any, sibling_hint: Optional[str] = None):
    """
    Acepta conversion_value como:
      - "10m", "12.5 kg", "0,5 L"
      - número → requiere pista de unidad base (sibling_hint) o meter por defecto
    Define en Pint:  <target_unit> = <factor> * <base>
    """
    if _ureg is None or not target_unit:
        return
    try:
        _ = 1 * _ureg(target_unit)
        return  # ya existe
    except Exception:
        pass

    factor = None
    base_u = None

    if isinstance(conversion_value, (int, float)):
        factor = float(conversion_value)
        base_u = sibling_hint or "meter"
    elif isinstance(conversion_value, str):
        s = conversion_value.strip()
        m = re.match(r'^\s*([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-zµμ/]+)\s*$', s)
        if m:
            try:
                factor = _norm_num_locale(m.group(1))
                base_u = m.group(2)
            except Exception:
                factor, base_u = None, None

    # normalización de nombres comunes
    if isinstance(base_u, str):
        bl = base_u.lower()
        if bl in {"meter","meters"}: base_u = "meter"
        if bl in {"liter","liters","lt","l"}: base_u = "liter"
        if bl in {"kilogram","kilograms"}: base_u = "kilogram"

    if factor is not None and base_u:
        try:
            _ureg.define(f"{target_unit} = {factor} * {base_u}")
            if target_unit.endswith("es"):
                try: _ureg.define(f"{target_unit[:-2]} = {target_unit}")
                except Exception: pass
            elif target_unit.endswith("s"):
                try: _ureg.define(f"{target_unit[:-1]} = {target_unit}")
                except Exception: pass
        except Exception:
            pass

# ---- motor principal: convierte y deja auditoría
def apply_convert_units(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    """
    Aplica conversión de unidades al documento (mutación in-place). Devuelve True si no hubo errores críticos.
    step:
      {
        "instruction": "...",           # opcional, para que el LLM infiera target/custom
        "target_unit": "camiones",      # opcional si se infiere por instruction
        "conversion_value": "10m"       # opcional: define 1 camión = 10 m
      }
    """
    instruction = step.get("instruction") or step.get("instr") or ""
    target_unit = (step.get("target_unit") or "").strip()
    conv_value  = step.get("conversion_value")

    parsed = ParsedInstruction(target_unit=None, category_hint=None, custom_units={})
    if instruction:
        try:
            parsed = _ask_model_for_units(instruction, doc)
            if not target_unit:
                target_unit = (parsed.target_unit or "").strip()
        except Exception:
            parsed = ParsedInstruction(target_unit=None, category_hint=None, custom_units={})

    if not target_unit:
        return True

    if parsed.custom_units:
        _ensure_pint_custom_units(parsed.custom_units)

    sibling_hint = _first_sibling_unit(doc)
    if isinstance(sibling_hint, str):
        sl = sibling_hint.lower()
        if sl in {"meter","meters"}: sibling_hint = "meter"
        if sl in {"liter","liters","lt","l"}: sibling_hint = "liter"
        if sl in {"kilogram","kilograms"}: sibling_hint = "kilogram"
    _define_custom_unit_from_step(target_unit, conv_value, sibling_hint)

    changed: List[Dict[str, Any]] = []

    def _walk(node: Any, path: str = "") -> Any:
        if isinstance(node, dict):
            # unidad hermana a nivel de nodo
            sibling_unit_local = None
            for uk in _UNIT_KEYS:
                if uk in node and isinstance(node[uk], str) and node[uk].strip():
                    sibling_unit_local = node[uk].strip()
                    break

            new_node: Dict[str, Any] = {}
            for k, v in node.items():
                child_path = f"{path}.{k}" if path else k
                key_unit = _unit_from_key_name(k)

                # A) numérico puro + unidad por clave/sibling
                if isinstance(v, (int, float)):
                    src_u = key_unit or sibling_unit_local
                    if src_u:
                        q = _maybe_quantity(v, src_u)
                        if q is not None:
                            nv = _convert_quantity(q, target_unit)
                            if nv is not None:
                                new_node[k] = nv
                                for uk in _UNIT_KEYS:
                                    if uk in node:
                                        new_node[uk] = target_unit
                                        break
                                changed.append({"path": child_path, "from": f"{v} {src_u}", "to": f"{_fmt_num(nv)} {target_unit}"})
                                continue

                # B) string "número + unidad" o "número" con unidad derivable
                if isinstance(v, str):
                    m = _NUM_UNIT_RE.match(v.strip())
                    if m:
                        try:
                            num = _norm_num_locale(m.group("num"))
                            u = m.group("u") or key_unit or sibling_unit_local
                        except Exception:
                            num, u = None, None
                        if (num is not None) and u:
                            q = _maybe_quantity(num, u)
                            if q is not None:
                                nv = _convert_quantity(q, target_unit)
                                if nv is not None:
                                    new_node[k] = f"{_fmt_num(nv)} {target_unit}"
                                    for uk in _UNIT_KEYS:
                                        if uk in node:
                                            new_node[uk] = target_unit
                                            break
                                    changed.append({"path": child_path, "from": v, "to": new_node[k]})
                                    continue
                    else:
                        val_num = _to_float(v)
                        src_u = key_unit or sibling_unit_local
                        if (val_num is not None) and src_u:
                            q = _maybe_quantity(val_num, src_u)
                            if q is not None:
                                nv = _convert_quantity(q, target_unit)
                                if nv is not None:
                                    new_node[k] = nv
                                    for uk in _UNIT_KEYS:
                                        if uk in node:
                                            new_node[uk] = target_unit
                                            break
                                    changed.append({"path": child_path, "from": f"{v} {src_u}", "to": f"{_fmt_num(nv)} {target_unit}"})
                                    continue

                # C) recursión
                new_node[k] = _walk(v, child_path)
            return new_node

        elif isinstance(node, list):
            return [_walk(it, f"{path}[{i}]") for i, it in enumerate(node)]
        else:
            return node

    out = _walk(doc, "")
    if isinstance(out, dict):
        doc.clear()
        doc.update(out)

    if changed:
        audit = doc.get("_unit_conversion_audit")
        if not isinstance(audit, list):
            print("_unit_conversion_audit")
            print(changed)
        else:
            audit.extend(changed)

    return True
