# input/docling_reader.py
from __future__ import annotations
import os, json
from pathlib import Path
from typing import Optional, List
from datetime import datetime

from config.settings import (
    DOCLING_DO_OCR,
    DOCLING_FORCE_FULL_PAGE_OCR,
    DOCLING_OCR_LANGS,
)

# 📦 Ruta de caché
CACHE_FILE = Path("cache/docling_last.json")


def _ensure_cache_dir():
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)


def save_docling_cache(md_text: str, meta: Optional[dict] = None):
    """Guarda texto y metadatos en cache."""
    _ensure_cache_dir()
    payload = {
        "text": md_text,
        "meta": meta or {},
        "saved_at": datetime.utcnow().isoformat() + "Z",
    }
    CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_docling_cache() -> dict:
    """Carga cache en formato dict {text, meta, saved_at}."""
    if not CACHE_FILE.exists():
        raise FileNotFoundError("No hay cache previo de Docling. Ejecutá primero sin --default.")
    return json.loads(CACHE_FILE.read_text(encoding="utf-8"))


def extract_text_with_layout(file_path: str) -> str:
    """
    Extrae texto de PDF usando Docling con OCR de Tesseract.
    Exporta a Markdown y lo guarda en cache.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    try:
        from pathlib import Path as P
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions,
            TesseractCliOcrOptions,
        )
        from docling.document_converter import DocumentConverter, PdfFormatOption
    except Exception as e:
        raise ImportError(
            "Docling no está instalado o su API no está disponible. "
            "Instalá con: pip install docling"
        ) from e

    # Configurar OCR
    ocr_options = TesseractCliOcrOptions(lang=DOCLING_OCR_LANGS)
    pipeline_options = PdfPipelineOptions(
        do_ocr=DOCLING_DO_OCR,
        force_full_page_ocr=DOCLING_FORCE_FULL_PAGE_OCR,
        ocr_options=ocr_options,
    )

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pipeline_options,
            )
        }
    )

    result = converter.convert(P(file_path))
    doc = getattr(result, "document", None) or result

    try:
        md = doc.export_to_markdown()
    except Exception:
        # fallback a bloques de texto
        pages = getattr(doc, "pages", None)
        parts: List[str] = []
        if pages:
            for i, page in enumerate(pages, start=1):
                blocks = getattr(page, "blocks", None) or getattr(page, "elements", []) or []
                lines = []
                for b in blocks:
                    t = getattr(b, "text", "") or getattr(b, "content", "") or ""
                    t = (t or "").strip()
                    if t:
                        lines.append(t)
                if lines:
                    parts.append(f"=== Página {i} ===\n" + "\n".join(lines))
            if parts:
                md = "\n\n".join(parts)
        md = md or getattr(doc, "text", "") or getattr(doc, "content", "")

    md = (md or "").strip()

    # Guardar en cache
    meta = {"source": file_path, "ext": Path(file_path).suffix.lower()}
    save_docling_cache(md, meta)

    return md
