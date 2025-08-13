from spacy.pipeline import EntityRuler

def add_custom_patterns(nlp):
    ruler = nlp.add_pipe("entity_ruler", before="ner")
    patterns = [
        {"label": "CLIENTE", "pattern": [{"LOWER": "cliente"}]},
        {"label": "CLIENTE", "pattern": [{"LOWER": "proveedor"}]},
        {"label": "CLIENTE", "pattern": [{"LOWER": "empresa"}]},
        {"label": "MONTO", "pattern": [{"LOWER": {"IN": ["monto", "importe", "total"]}}, {"IS_PUNCT": True, "OP": "?"}, {"LIKE_NUM": True}]},
        {"label": "MONTO", "pattern": [{"LIKE_NUM": True}, {"LOWER": {"IN": ["usd", "ars", "eur", "€", "$"]}}]},
        {"label": "FECHA", "pattern": [{"LOWER": "fecha"}]},
        {"label": "FECHA", "pattern": [{"SHAPE": "dd/dd/dddd"}]},
        {"label": "FECHA", "pattern": [{"SHAPE": "dd-dd-dddd"}]},
        {"label": "PRODUCTO", "pattern": [{"LOWER": {"IN": ["producto", "descripción", "descripcion", "detalle", "item"]}}]},
        # Nuevo patrón para Orden de compra
        {"label": "ORDEN_COMPRA", "pattern": [
            {"LOWER": "orden"},
            {"LOWER": "de"},
            {"LOWER": "compra"},
            {"IS_PUNCT": True, "OP": "?"},
            {"LIKE_NUM": True}
        ]}
    ]
    ruler.add_patterns(patterns)
    return ruler
