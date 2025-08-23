# input/docling_reader.py
from __future__ import annotations
import os
from typing import Optional, List

from config.settings import (
    DOCLING_DO_OCR,
    DOCLING_FORCE_FULL_PAGE_OCR,
    DOCLING_OCR_LANGS,
)

def extract_text_with_layout(file_path: str) -> str:
    """
    Extrae texto de PDF usando Docling con OCR de Tesseract.
    - Activa OCR y detección automática de idioma (lang=["auto"]) o lista fija.
    - Usa force_full_page_ocr=True para PDFs escaneados/mixtos.
    - Exporta a Markdown.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    try:
        from pathlib import Path
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

    # Configurar OCR de Tesseract con detección automática o lista fija de idiomas
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

    # Convertir y exportar a Markdown
    result = converter.convert(Path(file_path))
    # Según la API actual, el documento está en .document
    doc = getattr(result, "document", None) or result
    try:
        md = doc.export_to_markdown()
    except Exception:
        # Fallback: intentar páginas -> blocks/elements -> text
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
                return "\n\n".join(parts).strip()

        # Último intento: atributos de texto planos
        md = getattr(doc, "text", "") or getattr(doc, "content", "") or ""

    md = (md or "").strip()
    return md
