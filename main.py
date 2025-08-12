from config.settings import SPACY_MODEL
from input.docling_reader import extract_text_with_layout
from nlp.pipeline import process_text, interpret_instructions
from nlp.apply_plan import execute_plan


def main():
    # === 1. Fuente de entrada ===
    file_path = "docs/ejemplo_factura.pdf"
    print(f"📄 Procesando archivo: {file_path}")

    try:
        raw_text = extract_text_with_layout(file_path)
    except Exception as e:
        print(f"⚠️ Docling falló: {e}")
        raw_text = """
        Orden de compra N° 4589
        Fecha: 14/07/2025

        Proveedor: Industrias Metalúrgicas Delta S.A.
        Dirección: Av. San Martín 2450, Córdoba, Argentina

        Descripción: Bomba centrífuga de acero inoxidable para uso industrial
        Largo: 2.5 m
        Ancho: 60 cm

        Monto total: 25.400,75
        Moneda: ARS
        """
    # === 2. Procesamiento NLP para extraer datos ===
    current_data = process_text(raw_text, SPACY_MODEL)
    print("\n🔍 Datos extraídos (NLP) iniciales:")
    print(current_data)

    # === 3. Procesamiento de instrucciones ===
    print("\n🧠 Aplicando instrucciones en cadena (spaCy)")
    instrucciones = [
        "Poné la fecha en formato AAAA/MM/DD.",
        "Convertí el largo y el ancho a milímetros.",
        "Traducí la descripción al inglés.",
        "Convertí el monto a USD.",
        "Renombrá 'Proveedor' como 'company' y 'Descripción' como 'product_description'.",
        "Filtrá para que solo pasen los registros donde la moneda sea ARS."
    ]

    for i, txt in enumerate(instrucciones, 1):
        plan, report = interpret_instructions(txt, SPACY_MODEL)
        print(f"\n#{i} Instrucción: {txt}")
        print("Plan:", plan)
        print("Reporte:", report)

        # === 4. Aplicar el plan sobre el resultado actual ===
        current_data = execute_plan(current_data, plan)
        print("✅ Resultado después de aplicar esta instrucción:")
        print(current_data)

    # === 5. Resultado final después de todas las instrucciones ===
    print("\n🎯 Resultado final después de aplicar todas las instrucciones:")
    print(current_data)


if __name__ == "__main__":
    main()
