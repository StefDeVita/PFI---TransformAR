# config/settings.py
SPACY_MODEL = "es_core_news_md"

# Docling + Tesseract
DOCLING_DO_OCR = True
DOCLING_FORCE_FULL_PAGE_OCR = True
# "auto" = Docling pide a Tesseract detectar idioma automáticamente
DOCLING_OCR_LANGS = ["auto"]

import os

# === OLLAMA / Qwen ===
OLLAMA_HOST: str = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "qwen2.5vl:3b")
OLLAMA_TEMPERATURE: float = float(os.getenv("OLLAMA_TEMPERATURE", "0"))
OLLAMA_MAX_TOKENS: int = int(os.getenv("OLLAMA_MAX_TOKENS", "2048"))  # respuesta
OLLAMA_INPUT_LIMIT: int = int(os.getenv("OLLAMA_INPUT_LIMIT", "12000"))  # chars de texto

# === Limpieza automática / defaults ===
# Habilitar reparación con LLM (separar palabras pegadas, ortografía leve)
AUTO_TEXT_LLM: bool = bool(int(os.getenv("AUTO_TEXT_LLM", "1")))
# Límite de chars por string a enviar al LLM
AUTO_TEXT_MAXCHARS: int = int(os.getenv("AUTO_TEXT_MAXCHARS", "800"))
# Normalizar a ISO (YYYY-MM-DD) cualquier clave que contenga 'fecha'
AUTO_ISO_DATES_DEFAULT: bool = bool(int(os.getenv("AUTO_ISO_DATES_DEFAULT", "0")))
