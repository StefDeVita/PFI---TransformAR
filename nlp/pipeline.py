# nlp/pipeline.py
import re
import spacy
from nlp.patterns import add_custom_patterns
from nlp.instruction_spacy import build_nlp, interpret_with_spacy

# Inicializamos solo una vez
_SPACY_MODELS = {}

def get_nlp(model_name: str):
    if model_name not in _SPACY_MODELS:
        nlp = spacy.load(model_name)
        add_custom_patterns(nlp)
        _SPACY_MODELS[model_name] = nlp
    return _SPACY_MODELS[model_name]


def extract_dimensions_from_text(text):
    """Extrae ancho y largo del texto."""
    dims = {}

    # Buscar largo
    largo_match = re.search(r"(?i)\bLargo\s*[:\-]?\s*([\d.,]+\s?(mm|cm|m))", text)
    if largo_match:
        dims["largo"] = largo_match.group(1).strip()

    # Buscar ancho
    ancho_match = re.search(r"(?i)\bAncho\s*[:\-]?\s*([\d.,]+\s?(mm|cm|m))", text)
    if ancho_match:
        dims["ancho"] = ancho_match.group(1).strip()

    return dims



def process_text(text: str, model_name: str):
    """Extrae entidades del documento y mapea campos clave usando spaCy + fallback regex."""
    nlp = get_nlp(model_name)
    doc = nlp(text)

    # --- Diccionario de mapeo spaCy -> campos ---
    FIELD_MAPPING = {
        "ORG": "company",
        "PER": "cliente",       # Persona física como cliente
        "MONEY": "monto",
        "DATE": "fecha",
        "PRODUCT": "descripcion"  # Si agregamos esta etiqueta en patrones
    }

    # --- Estructura inicial ---
    structured = {
        "factura": None,
        "cliente": None,
        "company": None,
        "fecha": None,
        "monto": None,
        "descripcion": None
    }

    # --- Detectar dimensiones ---
    dims = extract_dimensions_from_text(text)
    structured.update(dims)

    # --- 1) Poblar desde spaCy ---
    for ent in doc.ents:
        key = FIELD_MAPPING.get(ent.label_)
        if key and not structured.get(key):
            structured[key] = ent.text.strip()

    # --- 2) Fallback regex para los campos que falten ---
    if not structured["cliente"]:
        cliente = re.search(r"Cliente:\s*([^\—\n]+)", text)
        if cliente:
            structured["cliente"] = cliente.group(1).strip()

    if not structured["company"]:
        prov = re.search(r"(Proveedor|Empresa|Razón Social):\s*([^\—\n]+)", text, re.IGNORECASE)
        if prov:
            structured["company"] = prov.group(2).strip()

    if not structured["fecha"]:
        fecha_match = re.search(
            r"Fecha:\s*(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4}|\d{1,2}\s+\w+\s+\d{4})",
            text,
            re.IGNORECASE
        )
        if fecha_match:
            fecha_valor = fecha_match.group(1).strip()
            # Normalizamos a DD/MM/YYYY si es numérica
            num_fecha = re.match(r"(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})", fecha_valor)
            if num_fecha:
                d, m, y = num_fecha.groups()
                fecha_valor = f"{d.zfill(2)}/{m.zfill(2)}/{y}"
            structured["fecha"] = fecha_valor

    if not structured["monto"]:
        monto = re.search(r"Monto:\s*([\d.,]+\s?(USD|ARS|€)?)", text)
        if monto:
            structured["monto"] = monto.group(1).strip()

    if not structured["descripcion"]:
        descripcion_match = re.search(r"Descripción:\s*(.+)$", text)
        if descripcion_match:
            desc = descripcion_match.group(1).strip()
            desc = re.sub(
                r"[\—\-]?\s*(Largo|Ancho)\s*[:\-]?\s*[\d.,]+\s?(mm|cm|m)",
                "",
                desc,
                flags=re.IGNORECASE
            )
            desc = re.sub(r"\s{2,}", " ", desc).strip(" —-")
            structured["descripcion"] = desc

    # --- Número de factura u orden (puede ser la primera secuencia numérica significativa) ---
    structured["factura"] = next((t.text for t in doc if t.like_num), None)

    # --- Entidades detectadas ---
    ents = [{"texto": ent.text, "etiqueta": ent.label_} for ent in doc.ents]

    return {
        "structured": structured,
        "entities": ents
    }


def interpret_instructions(text: str, model_name: str = "es_core_news_md"):
    """Interpreta instrucciones de usuario → plan de transformación."""
    nlp = build_nlp(model_name)
    return interpret_with_spacy(text, nlp)
