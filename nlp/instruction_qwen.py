# nlp/instruction_qwen.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple
import json, re
from config.settings import OLLAMA_INPUT_LIMIT
from nlp.ollama_client import OllamaClient

SYSTEM_PROMPT = """
Sos un planificador de transformaciones de datos.
Recibirás una INSTRUCCIÓN breve en español y deberás devolver SOLO un JSON válido con un plan segun lo que consideres que desea el usuario.
El json SIEMPRE debera tener el formato {plan:[]} donde el array son la o las operaciones a realizar.
Debe usar el formato {plan:[]} incluso si hay una sola operacion.

Operaciones posibles:
- renombrar campos: {"op":"rename_columns","map":{"A":"B",...}}
- formatear fecha: {"op":"format_date","column":"fecha","input_fmt":"infer","output_fmt":"%Y/%m/%d"}
- traducir valores: {"op":"translate_values","columns":["col1"],"target_lang":"EN"}
- conversion de unidades de medida no monetarias (ej: peso, largo, etc) : {'op': 'convert_units', 'target_unit': '', 'conversion_value': ''}
- filtrar que sea igual: {"op":"filter_equals","column":"col","value":"..."}
- filtrar que contenga: {"op":"filter_contains","column":"col","value":"..."}
- filter_compare: {"op":"filter_compare","column":"col","op":"<|<=|>|>=","value":"..."}
- filtrar entre: {"op":"filter_between","column":"col","range":["a","b"]}
- conversion SOLO de moneda(pesos,dolares,euros,etc): {"op":"currency_to","target":""}
- exportar a otro formato: {"op":"export","format":"csv|xlsx","path":"output/resultado.ext"}
- normalizar texto (determinístico): {"op":"normalize_text","columns":["col1"],"options":{"strip_accents":true,"collapse_spaces":true,"trim":true,"uppercase":false,"lowercase":false}}
- limpieza con LLM (espaciado/ortografía): {"op":"cleanup_text_llm","columns":["col1"],"instruction":"..."}

Reglas:
- Asegurate que lo que devuelvas tenga sentido en su contexto.
- Siempre devolve la divisa detectada en formato ISO 4217.
- Incluye solamente operaciones posibles del listado.
- No expliques nada.
- Devuelve SOLO JSON con el formato {plan:[]} donde el array son las operaciones a realizar, incluso si hay una sola operacion.
- **No** inventes campos ni valores.
- Si un campo no existe, **omitilo** (no lo inventes).
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

    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.2,"temperature":0.2})
    raw_clean = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw_clean, flags=re.I)
    if m:
        raw_clean = m.group(1).strip()

    parsed = json.loads(raw_clean)
    plan = parsed.get("plan", [])
    print(plan)
    return plan, parsed.get("report", {})

# --- Injected: translate intent detection ---
def _infer_target_lang_from_text(text: str) -> str:
    # very simple heuristic; you can expand with langcodes or regex
    # patterns like: "a ingles/en inglés", "to english", "-> en"
    t = text.lower()
    if "alemán" in t or "aleman" in t or "german" in t or "deutsch" in t:
        return "de"
    if "italiano" in t or "italian" in t or "italiano" in t:
        return "it"
    if "portugués" in t or "portugues" in t or "portuguese" in t:
        return "pt"
    if "francés" in t or "frances" in t or "french" in t:
        return "fr"
    if "inglés" in t or "ingles" in t or "english" in t:
        return "en"
    if "español" in t or "espanol" in t or "spanish" in t:
        return "es"
    return "en"

def build_translate_step(user_instruction: str, columns=None, text_key="text"):
    return {
        "op": "translate",
        "args": {
            "target_lang": _infer_target_lang_from_text(user_instruction),
            **({"columns": columns} if columns else {}),
            "text_key": text_key,
        }
    }