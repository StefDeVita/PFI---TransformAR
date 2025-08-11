# nlp/pipeline.py
import spacy
from nlp.patterns import add_custom_patterns
from nlp.instruction_spacy import build_nlp, interpret_with_spacy

def process_text(text: str, model_name: str):
    """NLP básico para entidades del documento (no confundir con instrucciones)."""
    nlp = spacy.load(model_name)
    add_custom_patterns(nlp)
    doc = nlp(text)
    return [{"texto": ent.text, "etiqueta": ent.label_} for ent in doc.ents]

def interpret_instructions(text: str, model_name: str = "es_core_news_md"):
    """Intérprete de instrucciones → (plan, report)."""
    nlp = build_nlp(model_name)
    return interpret_with_spacy(text, nlp)
