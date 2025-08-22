from typing import Dict, Any
import json, re
from nlp.ollama_client import OllamaClient
from nlp.qwen_labeler import _extract_json_from_any  # ya lo tenías

def qwen_build_extraction_schema(extract_instr: str) -> Dict[str, Any]:
    """
    Interpreta la instrucción de extracción -> JSON schema simple
    Ej: "Extrae cliente, fecha, moneda y monto total"
    -> { "cliente": "string", "fecha": "string", "monto": "number", "moneda": "string" }
    """
    system = (
        "Sos un generador de schema de extracción. "
        "Dada una instrucción en español, devolvés SOLO un JSON simple con claves = campos pedidos. "
        "Ejemplo: {\"cliente\":\"string\",\"fecha\":\"string\",\"monto\":\"number\",\"moneda\":\"string\"},no uses un campo 'otros'"
    )
    user = f"EXTRACCIÓN:\n\"\"\"{extract_instr}\"\"\""
    raw = OllamaClient().chat_json(system=system, user=user)
    return _extract_json_from_any(raw)

def tag_text_with_qwen(md_text: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Rellena el schema con valores extraídos del documento.
    """
    system = (
        "Sos un extractor de información. "
        "Dado un documento y un SCHEMA JSON, devolvés SOLO un JSON con esos campos completos. "
        "Respetá las claves EXACTAS del schema. "
        "Si no encontrás un campo, poné null. En el caso de incluir la fecha, hacelo en formato ISO de ser posible"
    )
    user = f"SCHEMA:\n{json.dumps(schema, ensure_ascii=False)}\n\nDOCUMENTO:\n{md_text[:6000]}"
    raw = OllamaClient().chat_json(system=system, user=user)
    return _extract_json_from_any(raw)
