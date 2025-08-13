# nlp/pipeline.py
import re
import spacy
from nlp.patterns import add_custom_patterns
from nlp.instruction_spacy import build_nlp, interpret_with_spacy

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
    """
    1) Extrae campos por cabeceras (robusto y general).
    2) Completa con NER solo si faltan datos.
    3) Extrae dimensiones como apoyo.
    """
    nlp = get_nlp(model_name)
    doc = nlp(text)

    structured = {
        "company": None,      # Proveedor/Empresa/Razón social/Emisor
        "cliente": None,      # Cliente/Destinatario
        "person": None,       # Persona (si aparece una PER y 'cliente' no está)
        "fecha": None,
        "monto": None,
        "moneda": None,
        "descripcion": None
    }

    # 1) Cabeceras primero (alta precisión)
    structured["cliente"] = _find_label_value(text, LABELS["cliente"])
    structured["company"] = _find_label_value(text, LABELS["company"])
    structured["descripcion"] = _find_label_value(text, LABELS["descripcion"])

    # Fecha por label o cualquier fecha en el texto
    f = _find_label_value(text, LABELS["fecha"])
    structured["fecha"] = f or _find_date_anywhere(text)

    # Monto + Moneda
    monto, moneda = _find_amount(text)
    structured["monto"] = monto
    structured["moneda"] = moneda

    # 2) Dimensiones (apoyo)
    structured.update(extract_dimensions_from_text(text))

    # 3) Relleno con NER solo si faltan campos
    if not structured["company"] or structured["company"] == structured["cliente"]:
        orgs = [ent.text for ent in doc.ents if ent.label_ == "ORG"]
        orgs = sorted(orgs, key=lambda s: len(s), reverse=True)
        for o in orgs:
            norm_o = _norm(o)
            if norm_o != structured["cliente"]:
                structured["company"] = norm_o
                break

    if not structured["cliente"]:
        # si hay PER y parece nombre de persona, usarlo (último recurso)
        pers = [ent.text for ent in doc.ents if ent.label_ == "PER"]
        if pers:
            structured["cliente"] = _norm(pers[0])
            structured["person"] = structured["cliente"]

    # 4) Armar listado de entidades para inspección
    ents = [{"texto": ent.text, "etiqueta": ent.label_} for ent in doc.ents]

    return {
        "structured": structured,
        "entities": ents
    }

def interpret_instructions(text: str, model_name: str = "es_core_news_md"):
    nlp = build_nlp(model_name)
    return interpret_with_spacy(text, nlp)
