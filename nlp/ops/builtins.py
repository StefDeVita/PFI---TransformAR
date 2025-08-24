# nlp/ops/builtins.py
from __future__ import annotations
from typing import Dict, Any
from nlp.ops.registry import op
from nlp.runtime import (
    norm, parse_number, find_keys, format_date,
    get_translator, iter_items_nodes, collect_textual_fields
)

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
    cols = step.get("columns", []) or []
    target = (step.get("target") or "USD").upper()
    for c in cols:
        for parent, key in find_keys(doc, c):
            num = parse_number(parent.get(key))
            if num is not None:
                parent[key] = f"{num:.2f}".replace(".", ",") + f" {target}"
    for parent, key in find_keys(doc, "moneda"):
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
