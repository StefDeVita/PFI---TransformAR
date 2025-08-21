# nlp/qwen_labeler.py
from typing import Any, Dict
import json, re
from nlp.ollama_client import OllamaClient


SYSTEM_PROMPT = """
Sos un extractor de información de documentos empresariales.

Podés recibir como entrada:
- Documentos corporativos (facturas, remitos, órdenes de compra, presupuestos, ofertas, contratos).
- Mensajes informales (WhatsApp, emails, notas de pedido).

Tu tarea:
- Extraer la información pedida por el usuario.
- Devolver SIEMPRE un JSON plano, sin explicaciones ni texto adicional.

Estructura sugerida (usar solo lo que corresponda al caso):
{
  "fecha": "...",
  "cliente": "...",
  "proveedor": "...",
  "condiciones": "...",
  "items": [
    {
      "descripcion": "...",
      "cantidad": "...",
      "precio_unitario": "...",
      "total": "..."
    }
  ],
  "totales": {
    "subtotal": "...",
    "impuestos": "...",
    "total_general": "...",
    "moneda": "..."
  },
  "otros": {...}
}

Reglas:
- Si un campo no existe, omítelo.
- Si un valor no está en el documento, NO inventes nada.
- Sé robusto: los documentos pueden estar incompletos, desordenados o en lenguaje coloquial.
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

def extract_with_qwen(doc_text: str, extract_instr: str) -> Dict[str, Any]:
    """
    Usa Qwen (Ollama) para extraer la info pedida por el usuario
    y devolverla en un JSON simple.
    """
    user_prompt = f"""EXTRAE lo siguiente:
\"\"\"{extract_instr.strip()}\"\"\"

Documento:
\"\"\"{doc_text.strip()[:8000]}\"\"\""""

    client = OllamaClient()
    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.9})
    return _extract_json_from_any(raw)
