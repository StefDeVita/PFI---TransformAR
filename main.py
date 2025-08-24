# main.py
import argparse, json, sys, pathlib
from typing import Dict, Any, List

# Extracción PDF -> texto
from input.docling_reader import extract_text_with_layout
# Etiquetado de documentos con Qwen (Ollama)
from nlp.qwen_labeler import extract_with_qwen
# Aplicación del plan sobre los datos estructurados
from nlp.apply_plan import execute_plan
from nlp.instruction_qwen import interpret_with_qwen

def process_file(pdf_path: pathlib.Path, extract_instr: str, transform_instr: str) -> List[Dict[str, Any]]:
    md = extract_text_with_layout(str(pdf_path))
    extracted = extract_with_qwen(md, extract_instr)
    plan = []
    report = {}
    if transform_instr.strip():
        plan, report = interpret_with_qwen(transform_instr)
    if plan is None:
        plan = []
    return execute_plan(extracted, plan), plan, report

def main():
    parser = argparse.ArgumentParser(description="PFI TransformAR — Extracción y Transformación de documentos con Qwen")
    parser.add_argument("--files", nargs="+", required=True, help="Ruta(s) de PDF(s) o textos a procesar")
    parser.add_argument("--extract", required=True, help="Instrucción del usuario sobre qué campos extraer")
    parser.add_argument("--instr", default="", help="Instrucción de transformación sobre lo ya extraído (opcional)")
    parser.add_argument("--outdir", default="output", help="Carpeta para guardar resultados JSON")
    parser.add_argument("--print-plan", action="store_true", help="Imprimir el plan interpretado")
    args = parser.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    any_error = False
    for file_str in args.files:
        pdf_path = pathlib.Path(file_str)
        if not pdf_path.exists():
            print(f"⚠️ No existe: {pdf_path}")
            any_error = True
            continue

        print(f"📄 Procesando: {pdf_path.name}")
        try:
            transformed, plan, report = process_file(pdf_path, args.extract, args.instr)
            if args.print_plan and plan:
                print("🧭 Plan interpretado:")
                print(json.dumps({"plan": plan, **({"report": report} if report else {})}, ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"❌ Error procesando {pdf_path.name}: {e}")
            any_error = True
            continue

        out_path = outdir / f"{pdf_path.stem}.result.json"
        out_path.write_text(json.dumps(transformed, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ Resultado guardado en: {out_path}")

    if any_error:
        sys.exit(1)

if __name__ == "__main__":
    main()
