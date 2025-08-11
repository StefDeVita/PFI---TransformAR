from config.settings import SPACY_MODEL
from input.docling_reader import extract_text_with_layout
from nlp.pipeline import process_text
from nlp.pipeline import interpret_instructions



def main():
    # === 1. Fuente de entrada ===
    file_path = "docs/ejemplo_factura.pdf"
    print(f"📄 Procesando archivo: {file_path}")

    try:
        raw_text = extract_text_with_layout(file_path)
    except Exception as e:
        print(f"⚠️ Docling falló: {e}")
        raw_text = "Factura 001 — Cliente: YPF — Fecha: 06/08/2025 — Monto: 1.250,00 USD — Descripción: Bomba centrífuga"

    # === 2. Procesamiento NLP ===
    extracted_data = process_text(raw_text, SPACY_MODEL)
    print("\n🔍 Datos extraídos (NLP):")
    print(extracted_data)

    print("\n🧠 Demo intérprete de instrucciones (spaCy)")
    instrucciones = [
        'Unificá largo y ancho a mm y exportá a CSV.',
        'Poné la fecha en formato DD/MM/AAAA y traducí "Descripción" al inglés.',
        'Renombrá "Descripción" a "description" y "Cliente" a "customer".',
        'Filtrá donde cliente = "YPF" y pasá el importe a USD.',
        'descripciones de los items en aleman'
    ]
    for i, txt in enumerate(instrucciones, 1):
        plan, report = interpret_instructions(txt, SPACY_MODEL)
        print(f"\n#{i} Instrucción: {txt}")
        print("Plan:", plan)
        print("Reporte:", report)


if __name__ == "__main__":
    main()
