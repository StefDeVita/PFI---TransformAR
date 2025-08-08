import spacy
from spacy_layout import Language
from nlp.patterns import add_custom_patterns


def process_text(text, model_name):
    nlp = spacy.load(model_name)
    nlp = Language.factory("spacy_layout")(nlp)  # Integrar layout
    ruler = add_custom_patterns(nlp)

    doc = nlp(text)

    extracted = []
    for ent in doc.ents:
        extracted.append({"texto": ent.text, "etiqueta": ent.label_})

    return extracted

