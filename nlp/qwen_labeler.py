# nlp/qwen_labeler.py
from datetime import datetime
from typing import Any, Dict
import json, re
from nlp.ollama_client import OllamaClient


SYSTEM_PROMPT = """
Sos un extractor de información de documentos empresariales.

Podés recibir como entrada:
- Documentos corporativos (facturas, remitos, órdenes de compra, presupuestos, ofertas, contratos).
- Mensajes informales (WhatsApp, emails, notas de pedido).

Tu tarea:
- Extraer **exactamente** los campos que pide el usuario, con **nombres descriptivos y formatos tal como los solicita**.
- Devolver SIEMPRE un JSON plano, sin explicaciones ni texto adicional.

Reglas:
- **No** inventes campos ni valores.
- Si un campo no existe, **omitilo** (no lo inventes).
- Separa la moneda del valor en si en los items y totales (es decir un campo sera el precio y otro campo la moneda)
- Si un valor no está en el documento, NO inventes nada.
- La fecha pasala por defecto a dd/mm/yyyy.
- Sé robusto: los documentos pueden estar incompletos, desordenados o en lenguaje coloquial.
- Cuando extraigas valores numéricos de documentos:
    **Elimina los separadores de miles**
    Usa siempre el punto (.) como separador decimal.
    Devuelve los números en formato float o string numérica entendible por cualquier máquina.
    No cambies el valor, solo el formato.
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