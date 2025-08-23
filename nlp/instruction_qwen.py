# nlp/instruction_qwen.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple
import json, re
from config.settings import OLLAMA_INPUT_LIMIT
from nlp.ollama_client import OllamaClient

SYSTEM_PROMPT = """
Sos un planificador de transformaciones de datos.
Recibirás una INSTRUCCIÓN breve en español y deberás devolver SOLO un JSON válido con un plan segun lo que consideres que desea el usuario.
el json debera tener el formato {plan:[]} donde el array son las operaciones a realizar
Operaciones posibles:
- renombrar campos: {"op":"rename_columns","map":{"A":"B",...}}
- formatear fecha: {"op":"format_date","column":"fecha","input_fmt":"infer","output_fmt":"%Y/%m/%d"}
- traducir valores: {"op":"translate_values","columns":["col1"],"target_lang":"EN"}
- conversion de unidades de medida fisicas no monetarias (ej: peso, largo, etc) : {"op":"convert_units","columns":["col1"],"target_unit":"cm|m|kg|g"}
- filtrar que sea igual: {"op":"filter_equals","column":"col","value":"..."}
- filtrar que contenga: {"op":"filter_contains","column":"col","value":"..."}
- filter_compare: {"op":"filter_compare","column":"col","op":"<|<=|>|>=","value":"..."}
- filtrar entre: {"op":"filter_between","column":"col","range":["a","b"]}
- conversion SOLO de moneda(pesos,dolares,euros,etc): {"op":"currency_to","columns":["precio","total"],"target":"ARS","rate":"0.5"}
- exportar a otro formato: {"op":"export","format":"csv|xlsx","path":"output/resultado.ext"}

Reglas:
- No expliques nada.
- No inventes columnas que no existan.
- Devuelve SOLO JSON.
- **No** inventes campos ni valores.
- Si un campo no existe, **omitilo** (no lo inventes).
- Separa la unidad/moneda del valor en si en los items y totales (es decir un campo sera el precio y otro campo la unidad/moneda)
- Si un valor no está en el documento, NO inventes nada.
- La fecha pasala por defecto a dd/mm/yyyy.
- Sé robusto: los documentos pueden estar incompletos, desordenados o en lenguaje coloquial.
- Cuando extraigas valores numéricos de documentos:
    **Elimina los separadores de miles**
    Usa siempre el punto (.) como separador decimal.
    Devuelve los números en formato float.
    No cambies el valor, solo el formato.
"""

USER_PROMPT_TEMPLATE = """INSTRUCCIÓN:
\"\"\"{text}\"\"\""""

def interpret_with_qwen(text: str) -> Tuple[List[Dict], Dict]:
    text = (text or "").strip()
    if not text:
        return [], {"decisions":[{"op":"none","why":"texto vacío","confidence":0.0}]}

    clipped = text[:OLLAMA_INPUT_LIMIT]
    client = OllamaClient()
    user_prompt = USER_PROMPT_TEMPLATE.format(text=clipped)

    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.9})
    raw_clean = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw_clean, flags=re.I)
    if m:
        raw_clean = m.group(1).strip()

    parsed = json.loads(raw_clean)
    plan = parsed.get("plan", [])
    return plan, parsed.get("report", {})

