# main.py
from config.settings import SPACY_MODEL
from input.docling_reader import extract_text_with_layout
from nlp.pipeline import process_text, interpret_instructions
from nlp.apply_plan import execute_plan


def main():
    file_path = "docs/ejemplo_factura.pdf"
    print(f"📄 Procesando archivo: {file_path}")

    try:
        raw_text = extract_text_with_layout(file_path)
    except Exception as e:
        print(f"⚠️ Docling falló: {e}")
        raw_text = """
        Factura N° 1023
        Fecha emisión: 8/8/2025
        Cliente: Compañía TecnoNova S.A.
        Descripción: Máquina cortadora de precisión con accesorios incluidos
        Largo: 1.2 m
        Ancho: 80 cm
        Alto: 50 cm
        Peso: 150 kg
        Precio total: 12.500,00 USD
        """

    # Procesamiento NLP
    current_data = process_text(raw_text, SPACY_MODEL)
    print("\n🔍 Datos extraídos (NLP) iniciales:")
    print(current_data)

    # Instrucciones
    instrucciones = [
        "Poné la fecha en formato dd/mm/aaaa.",
        "Convertí largo, ancho y alto a pulgadas.",
        "Traducí la descripción al portugues",
        "Renombrá 'cliente' como 'customer'",
        "Filtrá donde customer = \"Compania tecnonov\" y convertí el peso a lb.",
        "Exportá a excel"
    ]

    # Aplicar instrucciones sobre el dict completo
    for i, txt in enumerate(instrucciones, 1):
        plan, report = interpret_instructions(txt, SPACY_MODEL)
        print(f"\n#{i} Instrucción: {txt}")
        print("Plan:", plan)
        print("Reporte:", report)

        current_data = execute_plan(current_data, plan)
        print("✅ Resultado después de aplicar esta instrucción:")
        print(current_data)

    print("\n🎯 Resultado final después de aplicar todas las instrucciones:")
    print(current_data)


if __name__ == "__main__":
    main()
