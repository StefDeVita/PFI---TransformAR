# config/settings.py
SPACY_MODEL = "es_core_news_md"

# Docling + Tesseract
DOCLING_DO_OCR = True
DOCLING_FORCE_FULL_PAGE_OCR = True
# "auto" = Docling pide a Tesseract detectar idioma automáticamente
DOCLING_OCR_LANGS = ["auto"]   # o por ejemplo ["spa", "eng"] si querés forzar
