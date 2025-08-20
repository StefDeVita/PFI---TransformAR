# main.py
import argparse, json, sys, pathlib
from typing import Dict, Any, List

# Extracción PDF -> Markdown
from input.docling_reader import extract_text_with_layout
# Etiquetado de documentos comerciales con Qwen (Ollama)
from nlp.qwen_labeler import tag_text_with_qwen, _extract_json_from_any  # reutilizamos el parser robusto
# Cliente Ollama para interpretar instrucciones con Qwen
from nlp.ollama_client import OllamaClient
# Aplicación del plan sobre los datos estructurados
from nlp.apply_plan import execute_plan

def qwen_interpret_instructions(instr_text: str) -> List[Dict[str, Any]]:
    """
    Usa Qwen (vía Ollama) para convertir una instrucción de usuario
    en un PLAN JSON compatible con apply_plan.execute_plan.
    """
    system = (
        "Sos un planificador de transformaciones de datos. "
        "Dada una INSTRUCCIÓN breve en español, devolvés SOLO un JSON con un array 'plan' "
        "de pasos atómicos para aplicar sobre datos tabulares/estructurados. "
        "Operaciones válidas (usa solo las necesarias):\n"
        "- rename_columns: {\"op\":\"rename_columns\",\"map\":{ \"A\":\"B\", ... }}\n"
        "- format_date: {\"op\":\"format_date\",\"column\":\"fecha\",\"input_fmt\":\"infer\",\"output_fmt\":\"%Y-%m-%d\"}\n"
        "- translate_values: {\"op\":\"translate_values\",\"columns\":[...],\"target_lang\":\"EN\"}\n"
        "- convert_units: {\"op\":\"convert_units\",\"columns\":[...],\"target_unit\":\"mm|cm|m|in|kg|g|lb|l|ml\"}\n"
        "- filter_equals: {\"op\":\"filter_equals\",\"column\":\"...\",\"value\":\"...\"}\n"
        "- filter_contains: {\"op\":\"filter_contains\",\"column\":\"...\",\"value\":\"...\"}\n"
        "- filter_compare: {\"op\":\"filter_compare\",\"column\":\"...\",\"op\":\"<|<=|>|>=\",\"value\":\"...\"}\n"
        "- filter_between: {\"op\":\"filter_between\",\"range\":[\"a\",\"b\"]}\n"
        "- currency_to: {\"op\":\"currency_to\",\"columns\":[...],\"target\":\"USD\",\"rate\":\"ask_user|table\"}\n"
        "- export: {\"op\":\"export\",\"format\":\"csv|xlsx\",\"path\":\"output/resultado.ext\"}\n"
        "No expliques nada, no agregues texto. Solo JSON válido."
    )

    user = (
        "INSTRUCCIÓN:\n"
        f"\"\"\"{instr_text.strip()}\"\"\"\n\n"
        "Devolvé JSON con esta forma mínima:\n"
        "{ \"plan\": [ /* pasos */ ] }"
    )

    client = OllamaClient()
    raw = client.chat_json(system=system, user=user, options={"top_p": 0.9})
    obj = _extract_json_from_any(raw)
    plan = obj.get("plan")
    if not isinstance(plan, list):
        raise ValueError("La respuesta del modelo no contiene 'plan' como lista.")
    return plan

def process_file(pdf_path: pathlib.Path, plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 1) PDF -> Markdown
    md = extract_text_with_layout(str(pdf_path))
    # 2) Markdown -> tags (Qwen)
    tags = tag_text_with_qwen(md)
    # 3) aplicar plan DIRECTAMENTE sobre el JSON original (tags)
    #    execute_plan actualiza in-place donde corresponde y devuelve una lista para consistencia
    return execute_plan(tags, plan)

def main():
    parser = argparse.ArgumentParser(description="PFI TransformAR — aplicar instrucciones a archivos (Qwen)")
    parser.add_argument("--files", nargs="+", required=True, help="Ruta(s) de PDF(s) a procesar")
    parser.add_argument("--instr", required=True, help="Texto de la instrucción del usuario")
    parser.add_argument("--outdir", default="output", help="Carpeta para guardar resultados JSON")
    parser.add_argument("--print-plan", action="store_true", help="Imprimir el plan interpretado")
    args = parser.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # A) Interpretar instrucción con Qwen -> PLAN
    try:
        plan = qwen_interpret_instructions(args.instr)
    except Exception as e:
        print(f"❌ No pude interpretar la instrucción con Qwen: {e}")
        sys.exit(2)

    if args.print_plan:
        print("📋 PLAN:")
        print(json.dumps(plan, ensure_ascii=False, indent=2))

    # B) Procesar cada archivo y aplicar el plan
    any_error = False
    for file_str in args.files:
        pdf_path = pathlib.Path(file_str)
        if not pdf_path.exists():
            print(f"⚠️ No existe: {pdf_path}")
            any_error = True
            continue

        print(f"📄 Procesando: {pdf_path.name}")
        try:
            transformed = process_file(pdf_path, plan)
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
