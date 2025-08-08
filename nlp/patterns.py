from spacy.pipeline import EntityRuler

def add_custom_patterns(nlp):
    ruler = nlp.add_pipe("entity_ruler", before="ner")
    patterns = [
        {"label": "CLIENTE", "pattern": [{"LOWER": "cliente"}]},
        {"label": "MONTO", "pattern": [{"LIKE_NUM": True}, {"LOWER": "usd"}]},
    ]
    ruler.add_patterns(patterns)
    return ruler
