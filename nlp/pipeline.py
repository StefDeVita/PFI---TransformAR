# nlp/pipeline.py
import re
from datetime import datetime
from typing import Optional, Tuple

import spacy
from nlp.patterns import add_custom_patterns
from nlp.instruction_spacy import build_nlp, interpret_with_spacy

from nlp.qwen_labeler import tag_text_with_qwen


_SPACY_MODELS = {}

def get_nlp(model_name: str):
    if model_name not in _SPACY_MODELS:
        nlp = spacy.load(model_name)
        add_custom_patterns(nlp)
        _SPACY_MODELS[model_name] = nlp
    return _SPACY_MODELS[model_name]

# -----------------------
# Helpers de extracción
# -----------------------
LABELS = {
    "cliente": [r"cliente", r"destinatario", r"atenci[oó]n", r"contacto"],
    "company": [r"proveedor", r"empresa", r"raz[oó]n\s+social", r"emisor", r"remitente"],
    "fecha":   [r"fecha"],
    "monto":   [r"monto(?:\s+total)?", r"total(?:\s+general)?", r"importe"],
    "moneda":  [r"moneda", r"divisa"],
    "descripcion": [r"descripci[oó]n", r"concepto", r"detalle"]
}

DATE_REGEX = r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}|\d{1,2}\s+\w+\s+\d{4}|\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2})"

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _find_label_value(text: str, variants) -> str | None:
    # Busca línea a línea: label: valor
    for v in variants:
        pat = re.compile(rf"(?im)^\s*(?:{v})\s*[:\-]\s*(.+)$")
        m = pat.search(text)
        if m:
            return _norm(m.group(1))
    return None

def _find_date_anywhere(text: str) -> str | None:
    m = re.search(DATE_REGEX, text)
    return _norm(m.group(1)) if m else None
def _validate_date(value: str) -> str | None:
    """Intenta validar fechas comunes."""
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def _validate_amount(value: str) -> Optional[Tuple[str, str]]:
    """Busca números y moneda."""
    import re
    m = re.search(r"([\d.,]+)\s*([A-Z]{2,4}|USD|EUR|ARS)", value)
    if m:
        return m.group(1), m.group(2)
    return None
    return None, None
def _find_amount(text):
    """
    Devuelve (monto_str, moneda_str).
    Normaliza separadores de miles y decimales.
    """
    import re

    monto = None
    div_line = None

    # Regex flexible: captura número y divisa opcional con espacios
    m = re.search(
        r"(?im)^\s*(?:monto(?:\s+total)?|total|importe|precio(?:\s+total)?)\s*[:\-]?\s*([\d\.,]+)\s+?([A-Z]{2,3}|USD|EUR|ARS|MXN|\$|€)?\s*$",
        text
    )

    if m:
        raw_amount = m.group(1)
        raw_amount = raw_amount.replace(".", "").replace(",", ".")
        try:
            monto = str(float(raw_amount))
        except:
            monto = raw_amount
        div_line = m.group(2).upper() if m.group(2) else None

    # Moneda en línea separada
    mon = _find_label_value(text, LABELS["moneda"])
    mon = mon.upper() if mon else None

    # Map símbolos
    code_map = {"$": "ARS", "€": "EUR", "USD": "USD"}
    if div_line in code_map:
        div_line = code_map[div_line]

    moneda = mon or div_line or "ARS"  # default ARS
    return monto, moneda


def extract_dimensions_from_text(text):
    """Extrae dimensiones y peso del texto."""
    dims = {}
    patterns = {
        "largo": r"\bLargo\s*[:\-]?\s*([\d.,]+\s?(mm|cm|m|in))",
        "ancho": r"\bAncho\s*[:\-]?\s*([\d.,]+\s?(mm|cm|m|in))",
        "alto": r"\bAlto\s*[:\-]?\s*([\d.,]+\s?(mm|cm|m|in))",
        "peso": r"\bPeso\s*[:\-]?\s*([\d.,]+\s?(kg|g|lb))"
    }
    for k, p in patterns.items():
        m = re.search(p, text, flags=re.I)
        if m:
            dims[k] = _norm(m.group(1))
    return dims

# -----------------------
# Pipeline principal
# -----------------------
def process_text(text: str, model_name: str):
    """Procesa texto con spaCy + reglas adicionales para structured."""
    nlp = spacy.load(model_name)
    add_custom_patterns(nlp)
    doc = nlp(text)

    entities = [{"texto": ent.text, "etiqueta": ent.label_} for ent in doc.ents]

    structured = {
        "company": None,
        "cliente": None,
        "person": None,
        "fecha": None,
        "monto": None,
        "moneda": None,
        "descripcion": None,
    }

    for ent in doc.ents:
        txt = ent.text.strip()

        if ent.label_ in ["ORG", "LOC"] and not structured["company"]:
            structured["company"] = txt

        elif ent.label_ in ["PER", "CLIENTE"] and not structured["cliente"]:
            # Evitar que quede "Cliente" solo
            if txt.lower() != "cliente":
                structured["cliente"] = txt
                structured["person"] = txt

        elif ent.label_ == "FECHA" and not structured["fecha"]:
            val = _validate_date(txt)
            if val:
                structured["fecha"] = val


        elif ent.label_ == "MONTO" and not structured["monto"]:
            result = _validate_amount(txt)
            if result is not None:
                num, cur = result
                structured["monto"] = num
                structured["moneda"] = cur

        elif ent.label_ == "PRODUCTO" and not structured["descripcion"]:
            if txt.lower() not in ["descripcion", "descripción"]:
                structured["descripcion"] = txt

    return {"structured": structured, "entities": entities}

def interpret_instructions(text: str, model_name: str = "es_core_news_md"):
    nlp = build_nlp(model_name)
    return interpret_with_spacy(text, nlp)

def process_pdf_text_to_tags(extracted_text: str) -> dict:
    """
    Toma el texto (ya extraído por Docling) y devuelve etiquetas estructuradas
    usando Qwen vía Ollama.
    """
    return tag_text_with_qwen(extracted_text)