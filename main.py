# main.py
import argparse, json, sys, pathlib
from typing import Dict, Any, List

# Extracción PDF -> texto
from input.docling_reader import extract_text_with_layout
# Etiquetado de documentos con Qwen (Ollama)
from nlp.qwen_labeler import extract_with_qwen
# Cliente Ollama para interpretar instrucciones con Qwen
from nlp.ollama_client import OllamaClient
# Aplicación del plan sobre los datos estructurados
from nlp.apply_plan import execute_plan
from nlp.instruction_qwen import interpret_with_qwen


def process_file(pdf_path: pathlib.Path, extract_instr: str, transform_instr: str) -> List[Dict[str, Any]]:
    # 1) PDF -> Texto plano/markdown
    md = extract_text_with_layout(str(pdf_path))

    # 2) Extraer datos relevantes con Qwen
    extracted = extract_with_qwen(md, extract_instr)

    # 3) Interpretar instrucción de transformación -> PLAN
    plan, _ = interpret_with_qwen(transform_instr)

    # 4) Aplicar plan sobre lo extraído
    return execute_plan(extracted, plan)


def main():
    parser = argparse.ArgumentParser(description="PFI TransformAR — Extracción y Transformación de documentos con Qwen")
    parser.add_argument("--files", nargs="+", required=True, help="Ruta(s) de PDF(s) o textos a procesar")
    parser.add_argument("--extract", required=True, help="Instrucción del usuario sobre qué campos extraer")
    parser.add_argument("--instr", required=True, help="Instrucción de transformación sobre lo ya extraído")
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
            transformed = process_file(pdf_path, args.extract, args.instr)
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
