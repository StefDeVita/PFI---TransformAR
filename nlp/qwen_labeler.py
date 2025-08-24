# nlp/qwen_labeler.py
from datetime import datetime
from typing import Any, Dict
import json, re
from nlp.ollama_client import OllamaClient


SYSTEM_PROMPT = """
Sos un extractor de información de documentos empresariales.

Tu tarea:
- Extraer **exactamente** los campos que pide el usuario, con **nombres descriptivos y formatos tal como los solicita**.
- Devolver SIEMPRE un JSON plano, sin explicaciones ni texto adicional.

Reglas:
- Agregar espacios entre palabras que se encuentren o quitalos si hay mas de uno. La informacion debe quedar lo mas limpia posible
- **No** inventes campos ni valores.
- Si un campo no existe, **omitilo** (no lo inventes).
- Separa la moneda del valor en si en los items y totales (es decir un campo sera el precio y otro campo la moneda)
- Las fechas pasalas por defecto a dd/mm/yyyy.
- Cuando extraigas valores numéricos nunca usar separador de miles, siempre poner dos decimales y usar coma como separador decimal
"""

def _extract_json_from_any(raw: str) -> Dict[str, Any]:
    raw_clean = raw.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw_clean, flags=re.I)
    if m:
        raw_clean = m.group(1).strip()
    try:
        return json.loads(raw_clean)
    except:
        start = raw_clean.find("{")
        end = raw_clean.rfind("}")
        if start >= 0 and end > start:
            return json.loads(raw_clean[start:end+1])
        raise ValueError(f"No se pudo parsear JSON de Qwen: {raw[:300]}")
import re

def extract_with_qwen(doc_text: str, extract_instr: str) -> Dict[str, Any]:
    user_prompt = f"""EXTRAE lo siguiente **exactamente** como se pide:
\"\"\"{extract_instr.strip()}\"\"\" 

Documento:
\"\"\"{doc_text.strip()[:8000]}\"\"\""""

    client = OllamaClient()
    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.9})
    parsed = _extract_json_from_any(raw)
    return parsed