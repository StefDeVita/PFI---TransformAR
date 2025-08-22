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
      "total": "...",
      "moneda":"..."
    }
  ],
  "totales": {
    "subtotal": "...",
    "impuestos": "...",
    "total_general": "...",
    "moneda": "..."
  }
}

Reglas:
- Si un campo no existe, omítelo.
- Separa la moneda del valor en si en los items y totales (es decir un campo sera el precio y el otro moneda, ej 20000 y luego moneda:USD), que el valor quede como un numero parseable sin comas (Ej: 50.000 debe quedar como 50000)
- Si un valor no está en el documento, NO inventes nada.
- Nunca mezcles valores numéricos y moneda en un mismo campo: separalos siempre.
- La fecha pasala por defecto a dd/mm/yyyy.
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
import re

def _split_value_and_currency(value: str, default_currency: str = None):
    """
    Separa un string en número y moneda.
    Ej:
      "35000.00 USD" -> ("35000.00", "USD")
      "20.000"       -> ("20000.00", default_currency)
      "$10,500"      -> ("10500.00", default_currency)
    """
    if not value:
        return None, default_currency

    text = str(value).strip()

    # buscar patrón número + moneda
    m = re.match(r"^\s*([\d\.,]+)\s*([A-Za-z$€¥₽]+)?\s*$", text)
    if m:
        num_raw, cur_raw = m.groups()
        # limpiar separadores de miles
        num_clean = num_raw.replace(".", "").replace(",", ".")
        try:
            num = f"{float(num_clean):.2f}"
        except:
            num = num_raw
        cur = cur_raw.strip() if cur_raw else default_currency
        return num, cur

    # si no hay match, intentar extraer moneda al final
    parts = text.split()
    if len(parts) > 1:
        try:
            num = f"{float(parts[0].replace('.', '').replace(',', '.')):.2f}"
            cur = parts[1]
            return num, cur
        except:
            pass

    # fallback: solo número
    try:
        num = f"{float(text.replace('.', '').replace(',', '.')):.2f}"
    except:
        num = text
    return num, default_currency


# --- Helpers para normalización ---
def _parse_date(txt: str) -> str | None:
    if not txt:
        return None
    txt = txt.strip()
    fmts = [
        "%Y-%m-%d","%d/%m/%Y","%d-%m-%Y","%Y/%m/%d",
        "%d.%m.%Y","%d %B %Y","%d %b %Y"
    ]
    for f in fmts:
        try:
            return datetime.strptime(txt, f).strftime("%Y-%m-%d")
        except:
            continue
    # intentamos con '2 de Mayo de 2030' → borramos los ' de '
    txt2 = re.sub(r"\s+de\s+", " ", txt, flags=re.I)
    for f in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(txt2, f).strftime("%Y-%m-%d")
        except:
            continue
    return txt


def normalize_extraction(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recibe el JSON crudo de Qwen y lo lleva a un schema fijo y limpio.
    """
    out: Dict[str, Any] = {
        "fecha": None,
        "cliente": {"nombre": None, "direccion": None, "telefono": None, "email": None},
        "proveedor": {"nombre": None, "direccion": None, "telefono": None, "email": None},
        "condiciones_entrega": None,
        "forma_pago": None,
        "items": [],
        "totales": {"subtotal": None, "impuestos": None, "total_general": None, "moneda": None},
    }

    out["fecha"] = _parse_date(raw.get("fecha") or raw.get("date"))

    cliente = raw.get("cliente")
    if isinstance(cliente, dict):
        out["cliente"]["nombre"] = cliente.get("nombre") or cliente.get("name")
        out["cliente"]["direccion"] = cliente.get("direccion") or cliente.get("calle")
        out["cliente"]["telefono"] = cliente.get("telefono")
        out["cliente"]["email"] = cliente.get("email")
    elif isinstance(cliente, str):
        out["cliente"]["nombre"] = cliente

    proveedor = raw.get("proveedor")
    if isinstance(proveedor, dict):
        out["proveedor"]["nombre"] = proveedor.get("nombre")
        out["proveedor"]["direccion"] = proveedor.get("direccion")
    elif isinstance(proveedor, str):
        out["proveedor"]["nombre"] = proveedor

    out["condiciones_entrega"] = raw.get("condiciones_entrega") or raw.get("condiciones")
    out["forma_pago"] = raw.get("forma_pago") or raw.get("pago")

    # normalizar items
    normalized_items = []
    for item in raw.get("items", []):
        new_item = dict(item)  # copia para no pisar el original
        for field in ["precio_unitario", "total"]:
            if field in new_item and new_item[field]:
                num, cur = _split_value_and_currency(new_item[field], raw.get("totales", {}).get("moneda"))
                if num is not None:
                    new_item[field] = num
                if cur and "moneda" not in new_item:
                    new_item["moneda"] = cur
        normalized_items.append(new_item)

    out["items"] = normalized_items
    totales = raw.get("totales") or {}
    if isinstance(totales, dict):
        out["totales"]["subtotal"] = totales.get("subtotal")
        out["totales"]["impuestos"] = totales.get("impuestos")
        out["totales"]["total_general"] = totales.get("total_general")
        out["totales"]["moneda"] = totales.get("moneda")
    print(out["items"])
    return out

def extract_with_qwen(doc_text: str, extract_instr: str) -> Dict[str, Any]:
    user_prompt = f"""EXTRAE lo siguiente:
\"\"\"{extract_instr.strip()}\"\"\"

Documento:
\"\"\"{doc_text.strip()[:8000]}\"\"\""""

    client = OllamaClient()
    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.9})
    parsed = _extract_json_from_any(raw)
    return normalize_extraction(parsed)