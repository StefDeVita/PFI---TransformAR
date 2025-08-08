from docling.document_converter import DocumentConverter
from docling.models import Document
import os

def extract_text_with_layout(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    converter = DocumentConverter()
    doc: Document = converter.convert(file_path)

    # Extraer el texto preservando layout básico
    text_blocks = []
    for page in doc.pages:
        for block in page.blocks:
            text_blocks.append(block.text.strip())

    return "\n".join(text_blocks)
