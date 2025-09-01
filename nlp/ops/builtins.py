# nlp/ops/builtins.py
from __future__ import annotations
from config.settings import OLLAMA_INPUT_LIMIT, OLLAMA_MODEL
from typing import Dict, Any, List, Tuple, Union
import json, re, math
from nlp.ops.registry import op
from nlp.ollama_client import OllamaClient
from nlp.runtime import (
    norm, parse_number, find_keys, format_date,
    get_translator, iter_items_nodes, collect_textual_fields
)

# --- Pint (rápido/determinístico)
try:
    from pint import UnitRegistry
    _ureg = UnitRegistry(autoconvert_offset_to_baseunit=True)
    _Q_ = _ureg.Quantity
except Exception:
    _ureg = None
    _Q_ = None

# (opcional; no usado acá)
try:
    from input.currency_converter import convert_currency_cached
except ImportError:
    convert_currency_cached = None

_VALUE_KEYS = {
    "valor","value","amount","qty","quantity","medida","measure",
    "size","length","weight","mass","cantidad","longitud"
}
_UNIT_KEYS  = {"unidad","unit","units","uom","measure_unit","unidad_medida"}

# sinónimos/variantes → unidad Pint (ES/EN básicos; el LLM cubrirá el resto)
_UNIT_SYNONYMS = {
    # longitudes
    "meter":"m","meters":"m","metre":"m","metres":"m","metro":"m","metros":"m",
    "centimetro":"cm","centímetros":"cm","centimetros":"cm","centimeter":"cm","centimeters":"cm",
    "milimetro":"mm","milímetros":"mm","milimetros":"mm","millimeter":"mm","millimeters":"mm",
    "pulgada":"inch","pulgadas":"inch","pulg":"inch","inches":"inch","inch":"inch",
    "pie":"ft","pies":"ft","foot":"ft","feet":"ft",
    # masas
    "kilogramo":"kg","kilogramos":"kg","kilo":"kg","kilos":"kg","kilogram":"kg","kilograms":"kg",
    "gramo":"g","gramos":"g","gram":"g","grams":"g",
    "tonelada":"tonne","toneladas":"tonne","tn":"tonne","ton":"tonne","tons":"tonne",
    "libra":"lb","libras":"lb","pound":"lb","pounds":"lb",
    "onza":"oz","onzas":"oz","ounce":"oz","ounces":"oz",
    # volumen
    "litro":"L","litros":"L","lt":"L","liter":"L","liters":"L",
    "mililitro":"mL","mililitros":"mL","milliliter":"mL","milliliters":"mL",
    # presión
    "bares":"bar","bar":"bar","pascal":"Pa","pascales":"Pa",
    # temperatura
    "celsius":"degC","centigrados":"degC","centígrados":"degC","°c":"degC","ºc":"degC","c":"degC",
    "fahrenheit":"degF","°f":"degF","ºf":"degF","f":"degF",
    "kelvin":"K","k":"K",
    # área / otros compactos
    "m2":"m^2","m^2":"m^2","m3":"m^3","m^3":"m^3","km/h":"km/h","kph":"km/h",
}

# Más estricto: la unidad debe empezar con letra o "°" (evita fechas/teléfonos)
_NUM_WITH_UNIT_RE = re.compile(
    r"^\s*(?P<num>[-+]?\d+(?:[.,]\d+)?(?:\s*/\s*\d+(?:[.,]\d+)?)?)\s*(?P<unit>(?:°?[A-Za-z][\w°^/.\-\s]*)?)\s*$",
    re.IGNORECASE
)

def _normalize_unit_text(u: str) -> str:
    if not isinstance(u, str): return str(u)
    t = u.strip().lower().replace("º", "°")
    if t in _UNIT_SYNONYMS: return _UNIT_SYNONYMS[t]
    t2 = re.sub(r"\s+", "", t)
    if t2 in _UNIT_SYNONYMS: return _UNIT_SYNONYMS[t2]
    if t in ("c","°c","degc"): return "degC"
    if t in ("f","°f","degf"): return "degF"
    return u

def _quantity_from(num, unit):
    if _ureg is None or _Q_ is None:
        raise RuntimeError("Pint no disponible")
    if isinstance(num, str) and "/" in num:
        try:
            a, b = re.split(r"/", num)
            num = float(a.replace(",", ".")) / float(b.replace(",", "."))
        except Exception:
            num = float(str(num).replace(",", "."))
    elif isinstance(num, str):
        num = float(num.replace(",", "."))
    return _Q_(num, _normalize_unit_text(str(unit)))

def _try_parse_quantity(text: str):
    if _ureg is None or not isinstance(text, str):
        return None, None
    t = text.strip().replace("º", "°")
    t = re.sub(r"(\d)\s*°?\s*([CF])\b", r"\1 deg\2", t, flags=re.IGNORECASE)  # 21 C → 21 degC
    m = _NUM_WITH_UNIT_RE.match(t)
    if not m: return None, None
    num = m.group("num"); unit = (m.group("unit") or "").strip()
    if not unit: return None, None
    try:
        return _quantity_from(num, unit), unit
    except Exception:
        try:
            return _quantity_from(num, _normalize_unit_text(unit)), unit
        except Exception:
            return None, None

def _dim_of(q) -> str:
    d = str(q.dimensionality)
    if "[temperature]" in d: return "temperature"
    if "[mass]"        in d: return "mass"
    if "[length]"      in d: return "length"
    if "[area]"        in d: return "area"
    if "[volume]"      in d: return "volume"
    if "[pressure]"    in d: return "pressure"
    if "[speed]" in d or "[velocity]" in d: return "speed"
    return d

def _targets_from_step(step: Dict[str, Any]) -> Union[str, Dict[str,str]]:
    """
    - 'to': dict por dimensión o string (target global)
    - 'target_unit': 'imperial' | 'metric' | unidad suelta ('kg','m','L','Zentimeter',...)
    """
    to = step.get("to")
    if to:
        return to
    tu = (step.get("target_unit") or "").strip()
    tul = tu.lower()
    if tul in ("imperial","imperiales","us","usa"):
        return {"length":"ft","mass":"lb","volume":"gal","pressure":"psi","area":"ft^2","temperature":"degF","speed":"mph"}
    if tul in ("metric","si","mks","internacional"):
        return {"length":"m","mass":"kg","volume":"L","pressure":"bar","area":"m^2","temperature":"degC","speed":"km/h"}
    # intentar mapear unidad suelta con Pint (tras normalizar)
    if tu and _ureg is not None:
        try:
            u = _normalize_unit_text(tu)
            dim = _dim_of(_ureg.Quantity(1, u))
            return {dim: u}
        except Exception:
            return {}
    return {}

def _target_for_dim(dim: str, targets: Union[str, Dict[str,str]]) -> Union[str, None]:
    if isinstance(targets, str):
        return targets
    if isinstance(targets, dict):
        return targets.get(dim)
    return None

def _is_compatible(q, tgt_unit: str) -> bool:
    """Evita intentos m→kg, etc. No registra error; simplemente salta."""
    try:
        t = _ureg.Quantity(1, tgt_unit)
        return q.dimensionality == t.dimensionality
    except Exception:
        return False

# ----------------- util: columnas existentes / candidatas -----------------
def _collect_all_keys_lc(node, acc: set):
    if isinstance(node, dict):
        for k, v in node.items():
            acc.add(str(k).lower())
            _collect_all_keys_lc(v, acc)
    elif isinstance(node, list):
        for it in node:
            _collect_all_keys_lc(it, acc)

def _detect_candidates(doc: Any) -> List[Tuple[Tuple[Union[str,int],...], str]]:
    found: List[Tuple[Tuple[Union[str,int],...], str]] = []
    def _walk(node, path):
        if isinstance(node, dict):
            keys = list(node.keys())
            for vk in keys:
                if vk.lower() in _VALUE_KEYS:
                    uk = next((k for k in keys if k.lower() in _UNIT_KEYS), None)
                    if uk:
                        found.append((path + (vk,), "pair"))
            for k, v in node.items():
                if isinstance(v, str):
                    if _NUM_WITH_UNIT_RE.match(v.strip()):
                        found.append((path + (k,), "embedded"))
                elif isinstance(v, (dict, list)):
                    _walk(v, path + (k,))
        elif isinstance(node, list):
            for i, it in enumerate(node):
                _walk(it, path + (i,))
    _walk(doc, tuple())
    return found

# --------------------- LLM helpers ---------------------
def _coerce_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        pass
    if "{" in text and "}" in text:
        try:
            frag = text[text.find("{"): text.rfind("}") + 1]
            return json.loads(frag)
        except Exception:
            return None
    return None

def _llm_guess_targets(human_unit: str) -> Dict[str,str] | None:
    """
    Usa el LLM para mapear 'Zentimeter' → {'length':'cm'}, 'Zoll'→{'length':'inch'}, etc.
    """
    try:
        sys = (
            "Actuás como mapeador de unidades. Dado un nombre humano de unidad en cualquier idioma, "
            "devolvés SOLO JSON con un mapeo por dimensión a un símbolo compatible con Pint. "
            "Si no sabés, devolvé {}."
        )
        user = f"Unidad objetivo humana: {human_unit}\nRespondé SOLO JSON."
        raw = OllamaClient().chat_json(system=sys, user=user, options={"temperature": 0})
        data = _coerce_json(raw) or {}
        if not isinstance(data, dict):
            return None
        # validar que las unidades propuestas existan en Pint
        out = {}
        for dim, unit in data.items():
            try:
                _ureg.Quantity(1, str(unit))
                out[dim] = str(unit)
            except Exception:
                continue
        return out or None
    except Exception:
        return None

def _run_llm_convert(doc, targets_or_human, model_name, temperature, max_chars):
    """
    Fallback general: el LLM detecta columnas convertibles y realiza la conversión.
    'targets_or_human' puede ser:
      - dict por dimensión {"length":"cm", ...}
      - string humano ("Zentimeter", "pies", etc.)
    """
    if OllamaClient is None:
        raise RuntimeError("No hay modelo disponible para fallback.")
    # limit doc size
    doc_json = json.dumps(doc, ensure_ascii=False)
    if len(doc_json) > max_chars:
        doc_json = doc_json[:max_chars]

    # construir prompt
    targets_json = targets_or_human if isinstance(targets_or_human, str) else json.dumps({"to": targets_or_human}, ensure_ascii=False)
    system = (
        "Sos un experto en unidades para documentos JSON. Objetivo: "
        "1) Detectar columnas convertibles (pares valor+unidad y strings 'num unidad'), "
        "2) Detectar su unidad actual (interpretá nombres humanos), "
        "3) Convertir a las UNIDADES TARGET, "
        "4) Actualizar el campo de unidad ('unidad'/'unit') si existe, "
        "5) Redondear magnitudes a 2 decimales, "
        "6) Dejar sin cambios lo no convertible o incompatible, y listar errores en 'report.errors'. "
        "Respondé SOLO JSON con el formato exacto: "
        "{\"doc\":<json_transformado>, \"report\":{\"converted\":[{\"path\":[],\"from\":\"\",\"to\":\"\"}], \"errors\":[], \"columns_detected\":[] } }"
    )
    if isinstance(targets_or_human, str):
        user = f"TARGET (humano, podés interpretarlo): {targets_or_human}\n\nDOC:\n{doc_json}"
    else:
        user = f"TARGETS (por dimensión):\n{targets_json}\n\nDOC:\n{doc_json}"

    # ejemplo
    example = (
        "{\"doc\":{\"cantidad\":328.08,\"unidad\":\"ft\"},"
        "\"report\":{\"converted\":[{\"path\":[\"cantidad\"],\"from\":\"100 m\",\"to\":\"328.08 ft\"}],"
        "\"errors\":[],\"columns_detected\":[\"cantidad\"]}}"
    )

    raw = OllamaClient().chat_json(
        system=system,
        user=user + "\n\n" + "Respetá el formato y devolvé ahora el JSON final.",
        options={"temperature": temperature}
    )
    parsed = _coerce_json(raw)
    if not parsed or "doc" not in parsed or "report" not in parsed:
        raise RuntimeError("LLM no devolvió JSON válido.")
    ndoc = parsed["doc"]; report = parsed["report"]
    if not isinstance(ndoc, dict):
        raise RuntimeError("LLM devolvió 'doc' no dict.")
    report.setdefault("converted", []); report.setdefault("errors", []); report.setdefault("columns_detected", [])
    return ndoc, report

# --------------------- Conversión con Pint ---------------------
def _convert_with_pint(doc: Dict[str,Any], targets, fields: List[str], report: Dict[str,Any]) -> int:
    changed = 0
    fields_lc = {f.lower() for f in fields} if fields else None

    def _pair_allowed(vk: str, uk: str) -> bool:
        if not fields_lc:
            return True
        return (vk.lower() in fields_lc) or (uk and uk.lower() in fields_lc)

    def _walk(node, path):
        nonlocal changed
        if isinstance(node, dict):
            keys = list(node.keys())
            # 1) parejas valor+unidad
            for vk in keys:
                if vk.lower() in _VALUE_KEYS:
                    uk = next((k for k in keys if k.lower() in _UNIT_KEYS), None)
                    if not uk or not _pair_allowed(vk, uk):
                        continue
                    val = node.get(vk); unit = node.get(uk)
                    if unit is None or not isinstance(val, (int, float, str)):
                        continue
                    try:
                        q = _quantity_from(val, unit)
                    except Exception as e:
                        report["errors"].append(f"{'.'.join(map(str, path + (vk,)))}: {e}")
                        continue
                    dim = _dim_of(q); tgt = _target_for_dim(dim, targets)
                    if not tgt or not _is_compatible(q, tgt):
                        continue
                    try:
                        report["_compatible_seen"] = True
                        q2 = q.to(tgt)
                        node[vk] = round(float(q2.magnitude), 2)
                        node[uk] = tgt
                        report["converted"].append({
                            "path": path + (vk,),
                            "from": f"{q.magnitude} {q.units}",
                            "to": f"{q2.magnitude} {tgt}"
                        })
                        report["columns_detected"].add("/".join(map(str, path + (vk,))))
                        changed += 1
                    except Exception as e:
                        report["errors"].append(f"{'.'.join(map(str, path + (vk,)))}: {e}")
            # 2) strings embebidos
            for k, v in list(node.items()):
                if isinstance(v, (dict, list)):
                    _walk(v, path + (k,))
                    continue
                if fields_lc and k.lower() not in fields_lc:
                    continue
                if isinstance(v, str):
                    q, _ = _try_parse_quantity(v)
                    if q is None:
                        continue
                    dim = _dim_of(q); tgt = _target_for_dim(dim, targets)
                    if not tgt or not _is_compatible(q, tgt):
                        continue
                    try:
                        report["_compatible_seen"] = True
                        q2 = q.to(tgt)
                        node[k] = f"{round(float(q2.magnitude), 2):.2f} {tgt}"
                        report["converted"].append({
                            "path": path + (k,),
                            "from": f"{q.magnitude} {q.units}",
                            "to": f"{q2.magnitude} {tgt}"
                        })
                        report["columns_detected"].add("/".join(map(str, path + (k,))))
                        changed += 1
                    except Exception as e:
                        report["errors"].append(f"{'.'.join(map(str, path + (k,)))}: {e}")
        elif isinstance(node, list):
            for i, it in enumerate(node):
                _walk(it, path + (i,))
    _walk(doc, tuple())
    return changed

# ---------------------------- Operación pública ------------------------------
@op("convert_units")
def convert_units(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    """
    Flujo:
      1) Intentar derivar TARGETS (por dimensión) a partir de step (target_unit|to)
      2) Si no hay TARGETS y hay target_unit humano → LLM mapea (p.ej., 'Zentimeter'→{'length':'cm'})
      3) Detectar columnas candidatas
      4) Intentar conversión con Pint
      5) Si Pint no convirtió nada → Fallback LLM de conversión (interpreta unidades y convierte)
      6) Imprimir reporte por consola y NO guardar metadatos en el JSON
    """
    # --- targets iniciales
    targets = _targets_from_step(step) or {}
    human_target = (step.get("target_unit") or "").strip()

    # soportar alias de columnas
    fields = step.get("fields") or step.get("columns")
    if not fields and step.get("column"):
        fields = [step["column"]]
    if isinstance(fields, str):
        fields = [fields]
    fields = fields or []

    # Si el filtro 'fields' no apunta a candidatos cuantitativos, lo ignoramos
    cands = _detect_candidates(doc)
    cand_keys = {str(p[-1]).lower() for (p, _k) in cands}
    if fields and not any(f.lower() in cand_keys for f in fields):
        fields = []

    # --- Reporte interno (solo consola)
    report: Dict[str, Any] = {
        "engine": "pint",
        "targets": targets if targets else ({"human_target": human_target} if human_target else {}),
        "columns_detected": set(),
        "converted": [],
        "errors": [],
        "_compatible_seen": False,
    }
    for path, _k in cands:
        report["columns_detected"].add("/".join(map(str, path)))

    # --- (2) Si no hay targets y tenemos target humano → LLM para mapear
    if not targets and human_target:
        guessed = _llm_guess_targets(human_target)
        if guessed:
            targets = guessed
            report["engine"] = "pint+llm_target"
            report["targets"] = guessed

    # --- (4) Intentar Pint
    changed = 0
    try:
        if _ureg is None or _Q_ is None:
            raise RuntimeError("Pint no disponible.")
        if targets:
            changed = _convert_with_pint(doc, targets, fields, report)
    except Exception as e:
        report["errors"].append(f"pint_error: {e}")

    # Si Pint logró convertir:
    if changed > 0 and not report["errors"]:
        report["columns_detected"] = sorted(list(report["columns_detected"]))
        for it in report["converted"]:
            it["to"] = re.sub(r"([-+]?\d+(\.\d+)?)(?=\s)", lambda m: f"{float(m.group(1)):.2f}", it["to"])
            it["from"] = re.sub(r"([-+]?\d+(\.\d+)?)(?=\s)", lambda m: f"{float(m.group(1)):.2f}", it["from"])
        print(json.dumps({"convert_units_report": report}, ensure_ascii=False, indent=2))
        return True

    # --- (5) FALLBACK LLM DE CONVERSIÓN (si Pint no convirtió nada)
    try:
        # Si no hay targets, pasamos el target humano para que el LLM lo interprete
        targets_or_human = targets if targets else human_target
        model_name = step.get("model") or OLLAMA_MODEL or "qwen2.5:latest"
        temperature = float(step.get("temperature", 0.1))
        max_chars = int(step.get("max_chars", OLLAMA_INPUT_LIMIT if OLLAMA_INPUT_LIMIT else 9000))
        new_doc, llm_rep = _run_llm_convert(doc, targets_or_human, model_name, temperature, max_chars)
        llm_rep["engine"] = f"llm:{model_name}"
        # normalizar col-detected y 2 decimales en report
        prev_cols = sorted(list(report["columns_detected"]))
        def _norm_col(x):
            if isinstance(x, str): return x
            if isinstance(x, (list, tuple)): return "/".join(map(str, x))
            if isinstance(x, dict):
                p = x.get("path") if "path" in x else None
                if isinstance(p, (list, tuple)): return "/".join(map(str, p))
                return json.dumps(x, ensure_ascii=False)
            return str(x)
        llm_cols = {_norm_col(x) for x in (llm_rep.get("columns_detected") or []) if x is not None}
        llm_rep["columns_detected"] = sorted(set(prev_cols) | llm_cols)
        for it in llm_rep.get("converted", []):
            it["to"] = re.sub(r"([-+]?\d+(\.\d+)?)(?=\s)", lambda m: f"{float(m.group(1)):.2f}", it["to"])
            it["from"] = re.sub(r"([-+]?\d+(\.\d+)?)(?=\s)", lambda m: f"{float(m.group(1)):.2f}", it["from"])
        # reemplazar doc (sin _meta)
        doc.clear(); doc.update(new_doc)
        print(json.dumps({"convert_units_report": llm_rep}, ensure_ascii=False, indent=2))
        return True
    except Exception as e:
        report["errors"].append(f"llm_fallback_error: {e}")
        report["columns_detected"] = sorted(list(report["columns_detected"]))
        for it in report["converted"]:
            it["to"] = re.sub(r"([-+]?\d+(\.\d+)?)(?=\s)", lambda m: f"{float(m.group(1)):.2f}", it["to"])
            it["from"] = re.sub(r"([-+]?\d+(\.\d+)?)(?=\s)", lambda m: f"{float(m.group(1)):.2f}", it["from"])
        print(json.dumps({"convert_units_report": report}, ensure_ascii=False, indent=2))
        return True



def _llm_detect_tag(doc: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from nlp.ollama_client import OllamaClient
    except Exception:
        return {"tag": None}

    doc_str = json.dumps(doc, ensure_ascii=False)[:max(3000, min(OLLAMA_INPUT_LIMIT, 9000))]

    system = (
        """
            Devuelve solo un JSON con este formato:

            {"tag":""}

            Reglas:

            tag: nombre exacto de la CLAVE del campo donde encuentres la divisa.

            Nunca inventes claves.
        """)
    user = f"Documento JSON (recortado):\n```\n{doc_str}\n```\nDevolvé tag."

    try:
        raw = OllamaClient().chat_json(system=system, user=user, options={"top_p": 0.7})
        raw = (raw or "").strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            i, j = raw.find("{"), raw.rfind("}")
            if i >= 0 and j > i:
                raw = raw[i:j+1]
        data = json.loads(raw)
        tag = data.get("tag")
        return {"tag": tag}
    except Exception:
        return {"tag": None}
    
def _llm_detect_source(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pide al modelo que devuelva:
      { "columns": ["<nombres de clave monetaria presentes en el doc>"],
        "source": "<moneda_origen_en_ISO_4217_o_null>" }
    - columns: NOMBRES DE CLAVE exactamente como aparecen en el JSON (no rutas).
    - source: 3 letras (USD, EUR, ARS, etc) si puede inferirse del documento
              ya sea por claves como moneda/currency/divisa o por sufijos en valores.
              Si no puede, devolver null.
    """
    try:
        from nlp.ollama_client import OllamaClient
    except Exception:
        return {"columns": [], "source": None}

    doc_str = json.dumps(doc, ensure_ascii=False)[:max(3000, min(OLLAMA_INPUT_LIMIT, 9000))]

    system = (
        """
            Devuelve solo un JSON con este formato:

            {"columns":["..."],"source":""}

            Reglas:

            columns: nombres de claves con montos de dinero.

            source: divisa en formato ISO de 3 caracteres

            Nunca inventes claves ni monedas.
        """)
    user = f"Documento JSON (recortado):\n```\n{doc_str}\n```\nDevolvé columns y source."

    try:
        raw = OllamaClient().chat_json(system=system, user=user, options={"top_p": 0.7})
        raw = (raw or "").strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            i, j = raw.find("{"), raw.rfind("}")
            if i >= 0 and j > i:
                raw = raw[i:j+1]
        data = json.loads(raw)
        cols = [c.strip() for c in (data.get("columns") or []) if isinstance(c, str) and c.strip()]
        src = data.get("source")
        if isinstance(src, str):
            src = src.strip().upper() or None
        else:
            src = None
        return {"columns": cols, "source": src}
    except Exception:
        return {"columns": [], "source": None}
    
def _llm_detect_target(doc: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from nlp.ollama_client import OllamaClient
    except Exception:
        return {"target": None}

    doc_str = json.dumps(doc, ensure_ascii=False)[:max(3000, min(OLLAMA_INPUT_LIMIT, 9000))]
    print(doc_str)
    system = (
        """
            Devuelve solo un JSON con este formato:

            {"target":""}

            Reglas:

            target: divisa en formato ISO 4217.
        """)
    user = f"Documento JSON (recortado):\n```\n{doc_str}\n```\nTransforma la divisa al formato ISO 4217."

    try:
        raw = OllamaClient().chat_json(system=system, user=user, options={"top_p": 0.9})
        raw = (raw or "").strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            i, j = raw.find("{"), raw.rfind("}")
            if i >= 0 and j > i:
                raw = raw[i:j+1]
        data = json.loads(raw)
        target = data.get("target")
        return {"target": target}
    except Exception:
        return {"target": None}
    

@op("rename_columns")
def rename_columns(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    mapping = step.get("map", {}) or {}
    for old, new in mapping.items():
        for parent, key in find_keys(doc, old):
            parent[new] = parent.pop(key)
    return True

@op("format_date")
def format_date_op(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    col = step.get("column")
    input_fmt = step.get("input_fmt") or "infer"
    output_fmt = step.get("output_fmt") or "%Y-%m-%d"
    for parent, key in find_keys(doc, col):
        nv = format_date(parent[key], input_fmt, output_fmt)
        if nv is not None:
            parent[key] = nv
    return True

@op("translate_values")
def translate_values(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    cols = step.get("columns", []) or []
    target = step.get("target_lang", "en")
    tr = get_translator()
    translated = False
    for c in cols:
        for parent, key in find_keys(doc, c):
            txt = norm(parent.get(key))
            if not txt: continue
            try:
                parent[key] = tr.translate(txt, target)
                translated = True
            except Exception:
                pass
    if not translated:
        for it in iter_items_nodes(doc):
            for parent, key in collect_textual_fields(it):
                txt = norm(parent.get(key))
                if not txt: continue
                try:
                    parent[key] = tr.translate(txt, target)
                    translated = True
                except Exception:
                    pass
    return True

@op("currency_to")
def currency_to(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    """
    Convierte columnas monetarias a otra moneda usando input/currency_converter.py.
    - Un único llamado al LLM (_llm_detect_money_and_source) determina columnas y moneda origen.
    - Si step["columns"] o step["source"] vienen explícitos, prevalecen.
    - step["rate"] (si viene) evita consulta de tasas y usa multiplicador fijo.
    - step["date"] puede ser 'latest' o 'YYYY-MM-DD'.
    """
    from input.currency_converter import CurrencyConverter

    targets = _llm_detect_target(step)
    target = (targets.get("target") or "ARS").upper()
    #target = (step.get("target") or "USD").upper()
    override_rate = step.get("rate")
    date = step.get("date") or "latest"

    # Un único llamado al modelo para columns + source
    det = _llm_detect_source(doc)
    tags = _llm_detect_tag(doc)

    cols: List[str] = det.get("columns") or []
    src_llm = det.get("source")
    tag = tags.get("tag")
    source = src_llm or "USD"

    print(tag)
    print(target)

    if not cols:
        # Nada que convertir
        # Normalizar igual la etiqueta visible de moneda si existiera, por consistencia visual
        for parent, key in find_keys(doc, "moneda"):
            parent[key] = target
        for parent, key in find_keys(doc, "currency"):
            parent[key] = target
        for parent, key in find_keys(doc, "divisa"):
            parent[key] = target
        return True

    conv = CurrencyConverter()

    for c in cols:
        for parent, key in find_keys(doc, c):
            raw = parent.get(key)
            num = parse_number(raw)
            if num is None:
                continue
            try:
                if override_rate is not None:
                    out = num * float(override_rate)
                else:
                    out = conv.convert(num, source, target, date=date)
                parent[key + "_orig"] = raw  # auditoría
                parent[key] = f"{float(out):.2f}".replace(".", ",")
            except Exception as e:
                parent[key] = str(e)
                # dejamos el valor original

    # Actualizar etiqueta visible de moneda si existe
    for parent, key in find_keys(doc, tag):
        parent[key] = target

    return True

@op("filter_equals")
def filter_equals(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    col, val = step.get("column"), step.get("value")
    return any(norm(parent[key]) == norm(val) for parent, key in find_keys(doc, col))

@op("filter_contains")
def filter_contains(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    col, val = step.get("column"), step.get("value")
    nv = norm(val)
    return any(nv in norm(parent[key]) for parent, key in find_keys(doc, col))

@op("filter_compare")
def filter_compare(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    col, cmpop, val = step.get("column"), step.get("op"), step.get("value")
    for parent, key in find_keys(doc, col):
        a, b = parse_number(parent.get(key)), parse_number(val)
        if a is None or b is None: 
            continue
        if {"<": a < b, "<=": a <= b, ">": a > b, ">=": a >= b}.get(cmpop, False):
            return True
    return False

@op("filter_between")
def filter_between(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    col, rng = step.get("column"), step.get("range", [])
    if not (isinstance(rng, list) and len(rng) == 2):
        return False
    lo, hi = rng
    for parent, key in find_keys(doc, col):
        a = parse_number(parent.get(key))
        al, ah = parse_number(lo), parse_number(hi)
        if None not in (a, al, ah) and al <= a <= ah:
            return True
    return False

@op("export")
def export_noop(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    # La exportación la maneja la capa superior (CLI/app).
    return True
