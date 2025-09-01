# nlp/qwen_labeler.py
from datetime import datetime
from typing import Any, Dict
import json, re
from nlp.ollama_client import OllamaClient


SYSTEM_PROMPT = """
Sos un extractor de información de documentos empresariales.

Tu tarea:
- Extraer **exactamente** los campos que pide el usuario, con **nombres descriptivos y formatos tal como los solicita** revisa 3 veces la respuesta asegurando que el resultado tenga sentido en su contexto.
- Devolver SIEMPRE un JSON plano, sin explicaciones ni texto adicional.

Reglas:
- Siempre inclui la moneda como el **código ISO de 3 letras** correspondiente.
- Agregar espacios entre palabras que se encuentren o quitalos si hay mas de uno. La informacion debe quedar lo mas limpia posible
- Asegurate que lo que estes escribiendo tenga sentido en su contexto
- **No** inventes valores, coloca "null" de ser necesario.
- Si un campo no existe, **omitilo** (no lo inventes).
- **No inventes valores.** Si un campo no existe o no puede inferirse con alta confianza, **omitilo** (no lo agregues).
- Siempre separa las unidades del valor en otro campo
- convertí las fechas a **dd/mm/yyyy** cuando el día/mes/año se puedan determinar con claridad. Si es ambiguo, **omití** el campo o dejá el valor original solo si es inequívoco.
- Asegurate de que las **claves** del JSON coincidan exactamente con las pedidas por el usuario.

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
    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.7,"temperature": 0.7})
    parsed = _extract_json_from_any(raw)
    return parsed