# nlp/ops/builtins.py
from config.settings import OLLAMA_INPUT_LIMIT
from __future__ import annotations
from typing import Dict, Any, List
import json
from nlp.ops.registry import op
from nlp.runtime import (
    norm, parse_number, find_keys, format_date,
    get_translator, iter_items_nodes, collect_textual_fields
)
try:
    from input.currency_converter import convert_currency_cached
except ImportError:
    convert_currency_cached = None


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

@op("convert_units")
def convert_units(doc: Dict[str, Any], step: Dict[str, Any]) -> bool:
    cols = step.get("columns", []) or []
    target = step.get("target_unit") or ""
    for c in cols:
        for parent, key in find_keys(doc, c):
            val = parent.get(key)
            if val is not None:
                parent[key] = f"{val} ({target})"
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
