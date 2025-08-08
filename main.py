from config.settings import SPACY_MODEL
from input.docling_reader import extract_text_with_layout
from nlp.pipeline import process_text
from transform.transformations import apply_transformations
from output.export import export_results


def main():
    # === 1. Fuente de entrada ===
    file_path = "docs/ejemplo_factura.pdf"
    print(f"📄 Procesando archivo: {file_path}")

    raw_text = extract_text_with_layout(file_path)
    print("\n📝 Texto extraído:")
    print(raw_text)

    # === 2. Procesamiento NLP ===
    extracted_data = process_text(raw_text, SPACY_MODEL)
    print("\n🔍 Datos extraídos (NLP):")
    print(extracted_data)

    # === 3. Transformaciones ===
    transformed_data = apply_transformations(extracted_data)
    print("\n⚙️ Datos transformados:")
    print(transformed_data)

    # === 4. Exportación ===
    export_results(transformed_data, output_format="csv", output_path="output/resultados.csv")
    print("\n✅ Proceso completado.")


if __name__ == "__main__":
    main()
