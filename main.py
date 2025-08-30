# main.py
import argparse, json, sys, pathlib

# Extracción PDF -> texto
from input.docling_reader import extract_text_with_layout
# Extracción con Qwen
from nlp.qwen_labeler import extract_with_qwen
# Interpretación de instrucciones
from nlp.instruction_qwen import interpret_with_qwen
# Aplicación de plan
from nlp.apply_plan import execute_plan
from input.gmail_reader import authenticate_gmail, list_messages, get_message_content
from input.outlook_reader import authenticate_outlook, get_token, list_messages_outlook, get_message_body, get_attachments


CACHE_FILE = pathlib.Path("cache/docling_last.md")


def load_cached_text() -> str:
    if CACHE_FILE.exists():
        return CACHE_FILE.read_text(encoding="utf-8")
    raise FileNotFoundError("⚠️ No existe cache previo (cache/docling_last.md).")


def save_cache(md_text: str):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(md_text, encoding="utf-8")


def process_file(pdf_path: pathlib.Path, extract_instr: str, transform_instr: str, use_cache: bool = False):
    """Pipeline para archivos PDF locales"""
    if use_cache:
        md = load_cached_text()
    else:
        md = extract_text_with_layout(str(pdf_path))
        save_cache(md)
    extracted = extract_with_qwen(md, extract_instr)
    plan, _ = interpret_with_qwen(transform_instr)
    return execute_plan(extracted, plan)


def process_text(doc_text: str, extract_instr: str, transform_instr: str):
    """Pipeline para texto plano (ej: correo sin adjunto)"""
    extracted = extract_with_qwen(doc_text, extract_instr)
    plan, _ = interpret_with_qwen(transform_instr)
    return execute_plan(extracted, plan)


def main():
    parser = argparse.ArgumentParser(description="PFI TransformAR — Extracción y Transformación con Qwen")
    parser.add_argument("--outlook", action="store_true", help="Usar Outlook/Office365 en vez de PDFs/Gmail")
    parser.add_argument("--files", nargs="+", help="Ruta(s) de PDF(s) a procesar")
    parser.add_argument("--gmail", action="store_true", help="Usar Gmail como fuente de entrada")
    parser.add_argument("--default", action="store_true", help="Usar texto cacheado previo en vez de analizar archivo nuevo")
    parser.add_argument("--extract", required=True, help="Instrucción sobre qué campos extraer")
    parser.add_argument("--instr", required=True, help="Instrucción de transformación sobre lo extraído")
    parser.add_argument("--outdir", default="output", help="Carpeta para guardar resultados JSON")
    args = parser.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    any_error = False
    results = []

    if args.default:
        # 🔹 Ejecutar pipeline con cache
        try:
            md = load_cached_text()
        except FileNotFoundError as e:
            print(str(e))
            sys.exit(1)
        extracted = extract_with_qwen(md, args.extract)
        print(extracted)
        plan, _ = interpret_with_qwen(args.instr)
        results.append(execute_plan(extracted, plan))

    elif args.outlook:
        app = authenticate_outlook()
        token = get_token(app)
        mails = list_messages_outlook(token, top=10)

        print("📧 Seleccioná un correo:")
        for i, m in enumerate(mails):
            sender = m.get("sender", {}).get("emailAddress", {}).get("address")
            subject = m.get("subject")
            print(f"{i + 1}. {sender} — {subject}")

        choice = int(input("Número: ")) - 1
        msg_id = mails[choice]["id"]

        if mails[choice].get("hasAttachments"):
            mode = input("¿Usar [T]exto del correo o [A]djuntos? ").lower()
            if mode == "a":
                files = get_attachments(token, msg_id)
                if not files:
                    print("⚠️ No se encontraron adjuntos, usando cuerpo del correo")
                    md = get_message_body(token, msg_id)
                else:
                    pdf_path = files[0]
                    md = extract_text_with_layout(pdf_path)
                    save_cache(md)
            else:
                md = get_message_body(token, msg_id)
        else:
            md = get_message_body(token, msg_id)

        extracted = extract_with_qwen(md, args.extract)
        plan, _ = interpret_with_qwen(args.instr)
        results.append(execute_plan(extracted, plan))

    elif args.gmail:
        service = authenticate_gmail()
        mails = list_messages(service, max_results=10)
        print("\n📬 Correos disponibles:")
        for i, m in enumerate(mails, 1):
            print(f"{i}. {m['from']} - {m['subject']}")

        choice = int(input("👉 Elegí un correo (número): ")) - 1
        msg_id = mails[choice]["id"]

        content = get_message_content(service, msg_id)

        if content["attachments"]:
            print("\nEl correo tiene adjuntos:")
            for i, att in enumerate(content["attachments"], 1):
                print(f"{i}. {att}")
            use_text = input("¿Querés usar el TEXTO del correo en lugar del adjunto? (s/n): ").strip().lower()
            if use_text == "s":
                transformed = process_text(content["text"], args.extract, args.instr)
            else:
                if len(content["attachments"]) > 1:
                    att_choice = int(input("👉 Elegí un adjunto (número): ")) - 1
                    pdf_path = pathlib.Path(content["attachments"][att_choice])
                else:
                    pdf_path = pathlib.Path(content["attachments"][0])
                transformed = process_file(pdf_path, args.extract, args.instr)
            results.append(transformed)
        else:
            print("⚠️ No hay adjuntos, se usará el texto del correo.")
            transformed = process_text(content["text"], args.extract, args.instr)
            results.append(transformed)

    elif args.files:
        for file_str in args.files:
            pdf_path = pathlib.Path(file_str)
            if not pdf_path.exists():
                print(f"⚠️ No existe: {pdf_path}")
                any_error = True
                continue

            print(f"📄 Procesando: {pdf_path.name}")
            try:
                transformed = process_file(pdf_path, args.extract, args.instr, use_cache=args.default)
                results.append(transformed)
            except Exception as e:
                print(f"❌ Error procesando {pdf_path.name}: {e}")
                any_error = True
                continue
    else:
        print("❌ Debés indicar --files o --gmail o --outlook o --default")
        sys.exit(2)

    for i, res in enumerate(results):
        out_path = outdir / f"resultado_{i+1}.json"
        out_path.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ Resultado guardado en: {out_path}")

    if any_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
