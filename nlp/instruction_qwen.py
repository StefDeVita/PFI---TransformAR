# nlp/instruction_qwen.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
import re, unicodedata
import json, re

from config.settings import OLLAMA_INPUT_LIMIT
from nlp.ollama_client import OllamaClient

SYSTEM_PROMPT = """
Sos un planificador de transformaciones de datos.
Recibirás una INSTRUCCIÓN breve en español y deberás devolver SOLO un JSON válido con un plan segun lo que consideres que desea el usuario.
El json SIEMPRE debera tener el formato { "plan": [] } donde el array son la o las operaciones a realizar.
Debe usar el formato { "plan": [] } incluso si hay una sola operacion.

Operaciones posibles:
- renombrar campos: {"op":"rename_columns","map":{"A":"B",...}}
- formatear fecha: {"op":"format_date","column":"fecha","input_fmt":"infer","output_fmt":"%Y/%m/%d"}
- traducir valores: {"op":"translate_values","columns":["col1"],"target_lang":"EN"}
- conversion de unidades de medida no monetarias (ej: peso, largo, etc) : {"op":"convert_units","target_unit":"","conversion_value":""}
- filtrar que sea igual: {"op":"filter_equals","column":"col","value":"..."}
- filtrar que contenga: {"op":"filter_contains","column":"col","value":"..."}
- filter_compare: {"op":"filter_compare","column":"col","op":"<|<=|>|>=","value":"..."}
- filtrar entre: {"op":"filter_between","column":"col","range":["a","b"]}
- conversion SOLO de moneda(pesos,dolares,euros,etc): {"op":"currency_to","target":""}
- exportar a otro formato: {"op":"export","format":"csv|xlsx","path":"output/resultado.ext"}
- normalizar texto (determinístico): {"op":"normalize_text","columns":["col1"],"options":{"strip_accents":true,"collapse_spaces":true,"trim":true,"uppercase":false,"lowercase":false}}
- limpieza con LLM (espaciado/ortografía): {"op":"cleanup_text_llm","columns":["col1"],"instruction":"..."}

Reglas:
- Asegurate que lo que devuelvas tenga sentido en su contexto.
- Incluye solamente operaciones posibles del listado.
- No expliques nada.
- Devuelve SOLO JSON con el formato { "plan": [] }.
- **No** inventes campos ni valores.
- Si un campo no existe, **omitilo** (no lo inventes).
"""

USER_PROMPT_TEMPLATE = """INSTRUCCIÓN:
\"\"\"{text}\"\"\""""

# ============================================================
# Utilidades robustas para parsear el JSON del modelo
# ============================================================

def _extract_json_from_any(raw: str) -> dict:
    """
    Extrae un objeto JSON desde:
    - bloque ```json ... ```
    - o el primer {...} balanceado dentro del texto
    - repara casos comunes (comillas simples, espacios, saltos)
    Lanza ValueError si no puede.
    """
    if not raw:
        raise ValueError("Respuesta vacía del modelo.")
    s = raw.strip()

    # 1) triple backticks ```json ... ```
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", s, flags=re.I)
    if m:
        s = m.group(1).strip()

    # 2) aislar el primer bloque {...}
    if not (s.startswith("{") and s.endswith("}")):
        i, j = s.find("{"), s.rfind("}")
        if i != -1 and j != -1 and j > i:
            s = s[i:j+1]

    # 3) reparar comillas simples si parece JSON con ' en vez de "
    if "'" in s and '"' not in s:
        s = s.replace("'", '"')

    # 4) limpiar saltos
    s = s.replace("\r\n", "\n").replace("\r", "\n").strip()

    return json.loads(s)

# ============================================================
# Heurísticas deterministas (fallback si el LLM falla)
# ============================================================

_LANG_MAP = {
    "aleman": "DE", "alemán": "DE", "alemana": "DE", "german": "DE", "deutsch": "DE",
    "ingles": "EN", "inglés": "EN", "english": "EN",
    "espanol": "ES", "español": "ES", "spanish": "ES",
    "italiano": "IT", "italian": "IT",
    "portugues": "PT", "portugués": "PT", "portuguese": "PT",
    "frances": "FR", "francés": "FR", "french": "FR",
}

def _infer_target_lang_from_text(text: str) -> str:
    t = text.lower()
    for key, code in _LANG_MAP.items():
        if re.search(fr"\b{re.escape(key)}\b", t):
            return code
    return "EN"

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def _to_singular(noun: str) -> str:
    """Singularización súper simple para comparar (auto/autos, camion/camiones)."""
    n = _strip_accents(noun.lower().strip())
    # primero 'es' (camiones -> camion)
    if len(n) > 3 and n.endswith("es"):
        return n[:-2]
    # luego 's' (autos -> auto)
    if len(n) > 2 and n.endswith("s"):
        return n[:-1]
    return n

def _find_convert_target_and_custom(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (target_unit, conversion_value) si encuentra frases del tipo:
      - "expresa las unidades en kg"
      - "convertí a cm"
      - "expresá en camiones sabiendo que cada camión lleva 10 m"
      - "expresá en autos; 10 m por auto"
      - "convierte a pallets; cada pallet transporta 800 kg"
      - "expresá en auto = 10 m"
    Donde conversion_value es un string como "10m", "12.5 kg", etc.
    """
    # normalización básica
    t = " ".join((text or "").lower().split())

    # 1) target después de "a" o "en" (admite acentos/dígitos/guiones/underscore/°)
    #    ej: "expresa en autos", "convierte a camiones"
    m_target = re.search(
        r"(?:expres[aeá]|expresá|convierte|convertí|convertir)\s+(?:las\s+)?(?:unidades|unidad|medidas)?\s*(?:a|en)\s+([a-z0-9_./°µμáéíóúñ\-]+)",
        t
    )
    target = m_target.group(1) if m_target else None

    # 2) equivalencias (varias formas)
    #    A) "cada|por|x <sustantivo> (verbo) <num><unit>"
    #       verbos comunes: lleva/transporta/carga/contiene/equivale a/tiene capacidad de/soporta/admite/entra
    pat_a = re.compile(
        r"(?:cada|por|x)\s+(?P<noun>[a-záéíóúñ\-]+)\s+(?:"
        r"lleva|transporta|carga|contiene|equivale(?:n)?\s*a|tiene\s+capacidad\s+de|soporta|admite|entra"
        r")\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>[a-zA-Zµμ°º²³/]+)"
    )
    #    B) "<num><unit> por|x <sustantivo>"
    pat_b = re.compile(
        r"(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>[a-zA-Zµμ°º²³/]+)\s*(?:por|x)\s*(?P<noun>[a-záéíóúñ\-]+)"
    )
    #    C) "<sustantivo> = <num><unit>"
    pat_c = re.compile(
        r"(?P<noun>[a-záéíóúñ\-]+)\s*=\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<unit>[a-zA-Zµμ°º²³/]+)"
    )

    conv_value = None
    found_noun = None

    m2 = pat_a.search(t) or pat_b.search(t) or pat_c.search(t)
    if m2:
        found_noun = m2.group("noun")
        num = m2.group("num")
        unit = m2.group("unit")
        conv_value = f"{num}{unit}"

    # 3) coherencia target ↔ noun: si ambos existen, deben coincidir (singularizados)
    if target and found_noun:
        if _to_singular(target) != _to_singular(found_noun):
            # si no coinciden, preferimos el target explícito; mantenemos conv_value
            # pero OJO: si conv_value se refiere a otro noun, igual sirve porque define la unidad base
            pass
    # 4) si no había target pero sí noun, usamos el noun como destino
    if not target and found_noun:
        target = found_noun

    return (target, conv_value)

def _heuristic_plan(natural_instruction: str) -> List[Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    t = natural_instruction.lower()

    # traducir "descripcion" si pide traducir
    if re.search(r"\btraduc", t) or "translate" in t:
        lang = _infer_target_lang_from_text(natural_instruction)
        ops.append({"op": "translate_values", "columns": ["descripcion"], "target_lang": lang})

    # conversión de unidades
    target, conv = _find_convert_target_and_custom(natural_instruction)
    if target:
        step = {"op": "convert_units", "target_unit": target}
        if conv:
            step["conversion_value"] = conv
        ops.append(step)

    return ops

# ============================================================
# Planner con LLM (si falla, cae a heurística)
# ============================================================

def interpret_with_qwen(text: str) -> Tuple[List[Dict], Dict]:
    """
    Devuelve (plan, meta). El plan es una lista de pasos con las ops soportadas por tu runtime.
    - Si el LLM devuelve JSON inválido, se usa una heurística determinista para NO romper el pipeline.
    """
    text = (text or "").strip()
    if not text:
        return [], {"decisions":[{"op":"none","why":"texto vacío","confidence":0.0}]}

    clipped = text[:OLLAMA_INPUT_LIMIT]
    client = OllamaClient()
    user_prompt = USER_PROMPT_TEMPLATE.format(text=clipped)

    # LLM primero
    plan_llm: List[Dict[str, Any]] = []
    raw = client.chat_json(system=SYSTEM_PROMPT, user=user_prompt, options={"top_p": 0.2, "temperature": 0.2})
    try:
        parsed = _extract_json_from_any(raw)
        plan = parsed.get("plan", [])
        # filtro/normalización mínima para asegurar solo ops soportadas
        safe_ops = {"rename_columns","format_date","translate_values","convert_units","filter_equals",
                    "filter_contains","filter_compare","filter_between","currency_to","export",
                    "normalize_text","cleanup_text_llm"}
        plan_llm = [s for s in plan if isinstance(s, dict) and s.get("op") in safe_ops]
    except Exception:
        plan_llm = []

    # Heurística de respaldo
    plan_h = _heuristic_plan(text)

    # Merge simple: preferir LLM si trajo algo; si no, usar heurística
    final_plan: List[Dict[str, Any]] = plan_llm or plan_h

    # DEBUG opcional (vos tenías un print)
    try:
        print(final_plan)
    except Exception:
        pass

    meta = {
        "source": "llm" if plan_llm else "heuristic",
        "raw_ok": bool(plan_llm),
    }
    return final_plan, meta
