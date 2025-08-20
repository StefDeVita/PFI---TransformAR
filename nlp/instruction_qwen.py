# nlp/instruction_qwen.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple
import json
import re

from config.settings import OLLAMA_INPUT_LIMIT
from nlp.ollama_client import OllamaClient

# === Esquema de plan que generará Qwen ===
# ops posibles: rename_columns, format_date, translate_values, convert_units,
#               filter_equals, filter_contains, filter_compare, filter_between,
#               currency_to, export
#
# Ejemplo:
# {
#   "plan": [
#     {"op": "rename_columns", "map": {"Descripción":"descripcion","Fecha":"fecha"}},
#     {"op": "format_date", "column":"fecha", "input_fmt":"infer", "output_fmt":"%Y-%m-%d"},
#     {"op": "translate_values", "columns":["descripcion"], "target_lang":"EN"},
#     {"op": "convert_units", "columns":["largo","ancho","alto"], "target_unit":"mm"},
#     {"op": "filter_equals", "column":"cliente", "value":"Acme"},
#     {"op": "currency_to", "columns":["monto","total"], "target":"USD", "rate":"ask_user|table"},
#     {"op": "export", "format":"xlsx", "path":"output/resultado.xlsx"}
#   ],
#   "report": {"decisions":[{"op":"...","why":"...","confidence":0.8}]}
# }
"""
SYSTEM_PROMPT = Eres un asistente de orquestación de transformaciones de datos.
Recibirás una INSTRUCCIÓN EN ESPAÑOL escrita por un usuario no técnico que describe
cómo transformar/filtrar/renombrar/convertir/exportar datos de un documento tabular
o de un conjunto extraído por OCR.

Debes devolver EXCLUSIVAMENTE un JSON VÁLIDO con dos claves:
- "plan": lista de operaciones (ver catálogo de ops más abajo)
- "report": objeto con decisiones y confianzas

CATÁLOGO DE OPS (elige solo las necesarias; respeta las claves):
- {"op":"rename_columns", "map":{ "<col_origen>":"<col_destino>", ... }}
- {"op":"format_date", "column":"<col>", "input_fmt":"infer", "output_fmt":"%Y-%m-%d"}
- {"op":"translate_values", "columns":["<col1>","<col2>",...], "target_lang":"EN"}
- {"op":"convert_units", "columns":["<col1>",...], "target_unit":"mm|cm|m|in|kg|g|lb|l|ml"}
- {"op":"filter_equals", "column":"<col>", "value":"<texto|número>"}
- {"op":"filter_contains", "column":"<col>", "value":"<substring>"}
- {"op":"filter_compare", "column":"<col>", "op":"<|<=|>|>=", "value":"<número>"}
- {"op":"filter_between", "range":["<desde>","<hasta>"]}
- {"op":"currency_to", "columns":["<col1>",...], "target":"USD|EUR|ARS", "rate":"ask_user|table"}
- {"op":"export", "format":"csv|xlsx", "path":"output/resultado.<ext>"}

REGLAS:
- No inventes columnas. Si el usuario nombra columnas entre comillas, úsalas.

Devuelve SOLO el JSON.

"""

SYSTEM_PROMPT = """ Devuelve SOLO un JSON. """

USER_PROMPT_TEMPLATE = """INSTRUCCIÓN DEL USUARIO (ES):
\"\"\"{text}\"\"\""""

def _iso_lang_from_spanish_name(name: str) -> str | None:
    name = name.lower().strip()
    mapping = {
        "ingles":"EN", "inglés":"EN", "en":"EN",
        "aleman":"DE", "alemán":"DE", "de":"DE",
        "italiano":"IT", "it":"IT",
        "portugues":"PT", "portugués":"PT", "pt":"PT",
        "español":"ES", "es":"ES",
    }
    return mapping.get(name)

def _postprocess(result: Dict[str, Any]) -> Tuple[List[Dict], Dict]:
    """Valida estructura básica y normaliza pequeños detalles."""
    plan = result.get("plan") or []
    report = result.get("report") or {"decisions": []}

    # Normaliza target_lang si viene como nombre
    for step in plan:
        if step.get("op") == "translate_values":
            lang = step.get("target_lang")
            if lang and len(lang) > 2:
                iso = _iso_lang_from_spanish_name(lang)
                if iso:
                    step["target_lang"] = iso
    return plan, report

def interpret_with_qwen(text: str) -> Tuple[List[Dict], Dict]:
    """
    Reemplazo de interpret_with_spacy: usa Qwen (Ollama) para convertir
    la instrucción en un plan de transformación.
    """
    text = (text or "").strip()
    if not text:
        return [], {"decisions":[{"op":"none","why":"texto vacío","confidence":0.0}]}

    clipped = text[:OLLAMA_INPUT_LIMIT]
    client = OllamaClient()
    user_prompt = USER_PROMPT_TEMPLATE.format(text=clipped)

    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.9})

    # Acepta JSON puro o envuelto en bloque ```json
    raw_clean = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw_clean, flags=re.I)
    if m:
        raw_clean = m.group(1).strip()

    try:
        parsed = json.loads(raw_clean)
    except Exception as e:
        # fallback: intenta encontrar { ... } balanceado
        start = raw_clean.find("{")
        if start < 0:
            raise ValueError(f"Respuesta no JSON de Qwen: {raw_clean[:300]}")
        depth = 0
        end_idx = None
        for i, ch in enumerate(raw_clean[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break
        if end_idx is None:
            raise ValueError(f"No se pudo balancear JSON de Qwen: {raw_clean[:300]}")
        parsed = json.loads(raw_clean[start:end_idx])

    plan, report = _postprocess(parsed)
    return plan, report
