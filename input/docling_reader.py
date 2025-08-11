# input/docling_reader.py

from docling.document_converter import DocumentConverter
import os

def extract_text_with_layout(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    converter = DocumentConverter()
    doc = converter.convert(file_path)  # sin type-hint de Document

    # Algunas versiones devuelven un objeto con .pages, otras con .document.pages
    pages = getattr(doc, "pages", None)
    if pages is None and hasattr(doc, "document"):
        pages = getattr(doc.document, "pages", None)
    if pages is None:
        raise RuntimeError("Docling: no se encontraron páginas en el objeto convertido.")

    text_blocks = []
    for page in pages:
        # idem: diferentes builds pueden tener .blocks o .elements
        blocks = getattr(page, "blocks", None) or getattr(page, "elements", [])
        for block in blocks:
            text = getattr(block, "text", "") or getattr(block, "content", "")
            text = (text or "").strip()
            if text:
                text_blocks.append(text)

    return "\n".join(text_blocks)
