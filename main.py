# main.py
import os
from config.settings import SPACY_MODEL
from input.docling_reader import extract_text_with_layout
from nlp.pipeline import process_text, interpret_instructions
from nlp.apply_plan import execute_plan

def main():
    # --- Entrada del usuario ---
    file_path = input("📄 Ruta del PDF a procesar: ").strip()
    if not file_path:
        print("⚠️ No se ingresó ninguna ruta. Saliendo...")
        return

    if not os.path.exists(file_path):
        print(f"⚠️ El archivo no existe: {file_path}")
        return

    # --- Extracción con Docling ---
    print(f"\n📄 Procesando archivo: {file_path}")
    try:
        raw_text = extract_text_with_layout(file_path)
    except Exception as e:
        print(f"⚠️ Docling falló: {e}")
        return

    if not raw_text.strip():
        print("⚠️ No se pudo extraer texto del PDF.")
        return

    # --- Vista previa del texto ---
    preview = raw_text[:800].replace("\n", " ")
    print(f"\n📝 Preview del texto extraído (800 chars):\n{preview}")

    # --- Procesamiento NLP ---
    current_data = process_text(raw_text, SPACY_MODEL)
    print("\n🔍 Datos extraídos (NLP):")
    print(current_data)

    # --- Ingreso de instrucciones ---
    while True:
        txt = input("\n✏️ Ingrese una instrucción (o 'salir' para terminar): ").strip()
        if txt.lower() == "salir":
            break

        plan, report = interpret_instructions(txt, SPACY_MODEL)
        print("\n📋 Plan detectado:", plan)
        print("🗒 Reporte:", report)

        current_data = execute_plan(current_data, plan)
        print("\n✅ Resultado después de aplicar la instrucción:")
        print(current_data)

    print("\n🎯 Resultado final después de aplicar todas las instrucciones:")
    print(current_data)


if __name__ == "__main__":
    main()
