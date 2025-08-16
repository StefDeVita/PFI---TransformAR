import argparse, json, sys, pathlib
from input.docling_reader import extract_text_with_layout  # asumiendo esta función
from nlp.pipeline import process_pdf_text_to_tags

def main():
    parser = argparse.ArgumentParser(description="PFI TransformAR")
    parser.add_argument("--pdf", required=True, help="Ruta al PDF a procesar")
    parser.add_argument("--label", action="store_true",
                        help="Etiquetar con Ollama Qwen2.5-VL (texto, sin imágenes)")
    parser.add_argument("--out", default="", help="Archivo de salida JSON con tags")
    args = parser.parse_args()

    pdf_path = pathlib.Path(args.pdf)
    print(f"📄 Procesando archivo: {pdf_path}")

    # 1) Extraer texto con Docling (como ya haces)
    try:
        text = extract_text_with_layout(str(pdf_path))
    except Exception as e:
        print(f"⚠️ No se pudo extraer texto del PDF: {e}")
        sys.exit(1)

    if not args.label:
        # comportamiento anterior
        print("✅ Texto extraído (etiquetado deshabilitado).")
        print(text[:500] + ("..." if len(text) > 500 else ""))
        return

    # 2) Etiquetar con Qwen (vía Ollama)
    try:
        tags = process_pdf_text_to_tags(text)
    except Exception as e:
        print(f"⚠️ Falló el etiquetado con Qwen/Ollama: {e}")
        sys.exit(2)

    # 3) Persistir salida (opcional)
    if args.out:
        out_path = pathlib.Path(args.out)
        out_path.write_text(json.dumps(tags, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"🧾 Tags guardados en: {out_path}")
    else:
        print("🧾 Tags:")
        print(json.dumps(tags, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
