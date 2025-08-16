from __future__ import annotations
import json, logging, re
from typing import Any, Dict

from config.settings import DEFAULT_TAG_SCHEMA, OLLAMA_INPUT_LIMIT
from nlp.ollama_client import OllamaClient

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """Eres un asistente experto en documentos comerciales en español.
Recibirás contenido en TEXTO/Markdown (salida de OCR/Docling).
Debes extraer etiquetas estructuradas de facturas, ofertas, pedidos y cotizaciones.
Responde EXCLUSIVAMENTE con un JSON VÁLIDO, sin texto adicional.
Si un campo no aparece, usa string vacío, 0 o lista vacía según corresponda. No inventes.
"""

def _schema_to_string(schema: Dict[str, Any]) -> str:
    def fmt(v):
        if isinstance(v, dict):
            return "{ " + ", ".join(f"\"{k}\": {fmt(vv)}" for k, vv in v.items()) + " }"
        if isinstance(v, list) and v:
            return "[ " + fmt(v[0]) + " ]"
        if isinstance(v, str):
            return v
        return str(v)
    return fmt(schema)

def _build_user_prompt(md_text: str, schema: Dict[str, Any]) -> str:
    schema_str = _schema_to_string(schema)
    # ¡OJO! Todo adentro usa md_text, nunca 'text'
    return f"""El siguiente contenido está en Markdown simple. Ignora #, **, tablas ASCII o '=== Página N ===';
concéntrate solo en el contenido comercial y devuelve UN ÚNICO JSON válido con el siguiente esquema:

{schema_str}

DOCUMENTO (Markdown extraído):
\"\"\"{md_text}\"\"\"
"""

def _extract_json_from_any(s: str) -> Dict[str, Any]:
    """
    Acepta JSON puro o JSON envuelto en ```json ...``` o texto extra.
    Intenta localizar el primer objeto { ... } balanceado.
    """
    s = s.strip()

    # Bloque de código markdown
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", s, re.IGNORECASE)
    if m:
        s = m.group(1).strip()

    # Si ya es JSON directo
    try:
        return json.loads(s)
    except Exception:
        pass

    # Buscar primer objeto { ... } balanceado
    start = s.find("{")
    if start != -1:
        depth = 0
        for i, ch in enumerate(s[start:], start):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = s[start:i+1]
                    try:
                        return json.loads(candidate)
                    except Exception:
                        break

    # Último intento: limpiar líneas tipo "Respuesta:" u otras y reintentar
    cleaned = re.sub(r"^[^\{\[]+", "", s).strip()
    return json.loads(cleaned)

def tag_text_with_qwen(extracted_text: str, schema: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Recibe el texto Markdown de Docling y devuelve tags estructurados usando Qwen vía Ollama.
    """
    if not extracted_text or not extracted_text.strip():
        return {}

    # recortar si es muy largo (evitar desbordar el contexto del modelo)
    md_text = extracted_text[:OLLAMA_INPUT_LIMIT]

    schema = schema or DEFAULT_TAG_SCHEMA
    user_prompt = _build_user_prompt(md_text, schema)
    client = OllamaClient()

    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.9})

    try:
        result = _extract_json_from_any(raw)
    except Exception as e:
        # Log para diagnóstico; reenviamos excepción con el texto recibido
        log.warning("Respuesta no-JSON del modelo (primeros 500 chars): %s", raw[:500])
        raise ValueError(f"No pude parsear JSON desde la respuesta del modelo: {e}")

    # Normalizaciones suaves
    if "_confidence" in result and isinstance(result["_confidence"], str):
        try:
            result["_confidence"] = float(result["_confidence"])
        except Exception:
            result["_confidence"] = 0.0
    result.setdefault("_confidence", 0.0)

    return result
