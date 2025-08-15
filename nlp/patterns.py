# nlp/patterns.py
from spacy.pipeline import EntityRuler


def add_custom_patterns(nlp):
    """Agrega patrones para detectar entidades específicas."""
    ruler = nlp.add_pipe("entity_ruler", before="ner", name="custom_patterns")

    patterns = [
        # Cliente
        {"label": "CLIENTE", "pattern": [{"LOWER": "cliente"}, {"IS_PUNCT": True, "OP": "?"}, {"IS_TITLE": True, "OP": "+"}]},
        {"label": "CLIENTE", "pattern": [{"LOWER": "proveedor"}, {"IS_PUNCT": True, "OP": "?"}, {"IS_TITLE": True, "OP": "+"}]},

        # Fechas comunes
        {"label": "FECHA", "pattern": [{"SHAPE": "dd/dd/dddd"}]},
        {"label": "FECHA", "pattern": [{"SHAPE": "dd-dd-dddd"}]},
        {"label": "FECHA", "pattern": [{"SHAPE": "dddd-dd-dd"}]},

        # Montos
        {"label": "MONTO", "pattern": [{"LOWER": {"IN": ["total", "importe", "monto", "valor"]}}, {"IS_PUNCT": True, "OP": "?"}, {"LIKE_NUM": True}, {"IS_ALPHA": True, "OP": "?"}]},

        # Producto / Descripción
        {"label": "PRODUCTO", "pattern": [{"LOWER": {"IN": ["descripcion", "descripción", "producto", "detalle"]}}, {"IS_PUNCT": True, "OP": "?"}, {"IS_TITLE": True, "OP": "+"}]},
    ]

    ruler.add_patterns(patterns)
    return ruler

