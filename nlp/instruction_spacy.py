# nlp/instruction_spacy.py
from __future__ import annotations
from typing import List, Dict, Tuple, Any, Optional
import re, json, os
import spacy
from spacy.matcher import Matcher, PhraseMatcher
from spacy.pipeline import EntityRuler

# ====== Constantes & léxico ======
LANG_MAP = {
    "inglés": "EN", "ingles": "EN", "en": "EN",
    "español": "ES", "es": "ES",
    "alemán": "DE", "aleman": "DE", "de": "DE",
    "italiano": "IT", "it": "IT",
    "portugués": "PT", "portugues": "PT", "pt": "PT",
}

UNIT_CANON = {
    "mm": "mm", "milimetro": "mm", "milímetros": "mm",
    "cm": "cm", "m": "m", "metro": "m",
    "in": "in", "inch": "in", "pulgada": "in", "pulgadas": "in",
    "ft": "ft",
    "kg": "kg", "g": "g", "gramo": "g", "gramos": "g",
    "lb": "lb", "libra": "lb", "libras": "lb",
    "l": "l", "litro": "l", "litros": "l",
    "ml": "ml",
}

DATEFMTS = [
    ("DATEFMT", "dd/mm/aaaa", "%d/%m/%Y"),
    ("DATEFMT", "dd/mm/yyyy", "%d/%m/%Y"),
    ("DATEFMT", "mm/dd/aaaa", "%m/%d/%Y"),
    ("DATEFMT", "mm/dd/yyyy", "%m/%d/%Y"),
    ("DATEFMT", "iso", "%Y-%m-%d"),
]

# patrones genéricos para detectar número+unidad y destino de conversión
NUM_UNIT_PATTERNS = [
    r"\b\d+[.,]?\d*\s*(mm|cm|m|in|inch|pulgadas?|ft|kg|g|lb|l|ml)\b",
    r"\b(mm|cm|m|in|inch|pulgadas?|ft|kg|g|lb|l|ml)\s*\d+[.,]?\d*\b",
    r"\d\s*[\"”]"  # 3" o 3 ”
]
DESTINATION_PATTERNS = [
    r"\b(a|en|a\s+los|a\s+las)\s+(mm|cm|m|in|inch|pulgadas?|ft|kg|g|lb|l|ml)\b"
]


def _load_lexicon() -> Dict[str, Any]:
    """Carga config/lexicon.json si existe; caso contrario usa defaults."""
    base = {"intents": {}, "nominals": {}}
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "lexicon.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # fallback mínimo
    base["intents"] = {
        "RENAME": ["renombrar", "llamar", "etiquetar"],
        "FORMAT": ["formatear", "formato", "poné la fecha", "pone la fecha"],
        "TRANSLATE": ["traducir", "traducí", "traduce"],
        "CONVERT": ["convertir", "convertí", "unificar", "unificá", "normalizar", "normalizá", "pasar", "pasá", "pasa"],
        "FILTER": ["filtrar", "filtrá", "mostrar", "mostrá"],
        "EXPORT": ["exportar", "exportá", "guardar", "guardá", "descargar", "descargá", "csv", "xlsx", "excel"],
    }
    base["nominals"] = {
        "DATE": ["fecha", "fechas", "fec", "date"],
        "MEASURE": ["largo", "ancho", "alto", "medidas", "dimensiones", "longitud", "anchura", "altura"],
        "DESC": ["descripcion", "descripción", "detalle", "detalles", "desc", "concepto", "item", "items", "ítem", "ítems"],
        "MONEY": ["monto", "importe", "total", "precio", "coste", "costo"],
        "EXPORT": ["salida", "formato", "xlsx", "excel", "csv"],
    }
    return base


LEXICON = _load_lexicon()

# ====== Construcción NLP ======
def build_nlp(model: str = "es_core_news_md"):
    """Crea el pipeline spaCy con un EntityRuler para LANG/UNIT/DATEFMT/EXPORTFMT."""
    nlp = spacy.load(model)
    er: EntityRuler = nlp.add_pipe("entity_ruler", name="instruction_ruler", before="ner")  # type: ignore
    patterns = []

    # idiomas
    for k in LANG_MAP.keys():
        patterns.append({"label": "LANG", "pattern": k})

    # unidades
    for k in UNIT_CANON.keys():
        patterns.append({"label": "UNIT", "pattern": k})

    # formatos de fecha
    for _, key, _fmt in DATEFMTS:
        patterns.append({"label": "DATEFMT", "pattern": key})

    # formatos export
    patterns += [
        {"label": "EXPORTFMT", "pattern": "csv"},
        {"label": "EXPORTFMT", "pattern": "xlsx"},
        {"label": "EXPORTFMT", "pattern": "excel"},
    ]
    er.add_patterns(patterns)
    return nlp


def _build_matchers(nlp):
    """
    Construye (Matcher, PhraseMatcher) usando el vocab de `nlp`.
    - Usa el léxico (palabras exactas) de LEXICON["intents"].
    - Agrega REGEX por intent para tolerar voseo, imperativo y variaciones.
    """
    vocab = nlp.vocab
    matcher = Matcher(vocab)

    # Diccionario de regex por intent (minimiza overfitting)
    intent_regex = {
        "RENAME": [
            r"renombr[aá]\w*",     # renombrar, renombrá, renombra...
            r"llam[aá]\w*",        # llamar, llamá, llama...
            r"etiquet[aá]\w*",     # etiquetar, etiquetá, etiqueta...
        ],
        "FORMAT": [
            r"format\w*",                      # formatear, formateo...
            r"pon[ée]\s+la\s+fecha",           # poné/ pone la fecha
            r"poner\s+formato",                # poner formato
        ],
        "TRANSLATE": [
            r"traduc[eií]\w*",     # traducir, traducí, traduce...
        ],
        "CONVERT": [
            r"(convert|pas|unific|normaliz)[aáeéií]\w*",  # convertí, pasar/pasá, unificar/unificá, normalizar/normalizá...
        ],
        "FILTER": [
            r"(filtr|mostr)[aáeéií]\w*",       # filtrar/filtrá/filtra, mostrar/mostrá/mostra...
            r"contiene",                       # patrón común para filtros por substring
        ],
        "EXPORT": [
            r"(export|guard|descarg)[aáeéií]\w*",  # exportar/exportá, guardar/guardá, descargar/descargá...
            r"csv", r"xlsx", r"excel"              # keywords de formato
        ],
    }

    # 1) Patrones por léxico exacto (del JSON o fallback)
    for label, words in LEXICON["intents"].items():
        patterns = [[{"LOWER": w.lower()}] for w in words]

        # 2) Patrones REGEX por intent (robustos a conjugaciones/acentos)
        for rx in intent_regex.get(label, []):
            patterns.append([{"LOWER": {"REGEX": rx}}])

        matcher.add(label, patterns)

    # 3) Frases multi-token útiles (phrase matcher)
    pmatcher = PhraseMatcher(vocab, attr="LOWER")
    pmatcher.add("EXPORT", [nlp.make_doc("exportar a csv"), nlp.make_doc("exportar a excel")])

    return matcher, pmatcher


# ====== Helpers comunes ======
def _extract_columns(raw: str) -> List[str]:
    """Detecta columnas por comillas y por patrón 'columna(s) A, B, C', excluyendo valores de filtro."""
    m = re.search(
        r'(?:donde|dónde)\s+([\wÁÉÍÓÚÜÑáéíóúüñ]+)\s*=\s*[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]',
        raw, flags=re.IGNORECASE
    )
    filter_value = m.group(2).strip() if m else None

    quoted = re.findall(r'[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]', raw)
    if filter_value:
        quoted = [q for q in quoted if q.strip().lower() != filter_value.lower()]

    csv_like = re.findall(r'(?:columna|columnas?)\s+([\wÁÉÍÓÚÜÑáéíóúüñ ,/.-]+)', raw, flags=re.IGNORECASE)
    cols = [c.strip() for c in quoted]
    if csv_like:
        cols += [c.strip() for c in re.split(r'[ ,/]+', csv_like[0]) if c.strip()]

    seen, out = set(), []
    for c in cols:
        k = c.lower()
        if k not in seen:
            out.append(c)
            seen.add(k)
    return out


def _text_has_num_unit(text: str) -> bool:
    t = text.lower()
    return any(re.search(p, t) for p in NUM_UNIT_PATTERNS)


def _find_destination_unit(text: str) -> Optional[str]:
    t = text.lower()
    for p in DESTINATION_PATTERNS:
        m = re.search(p, t)
        if m:
            u = m.group(2)
            return UNIT_CANON.get(u, None)
    return None


def _contains_any(text: str, words: List[str]) -> bool:
    t = text.lower()
    return any(w.lower() in t for w in words)


# ====== 1) Detect INTENTS ======
def detect_intents(doc, nlp) -> Tuple[set, Dict]:
    """Detecta intents con Matcher/PhraseMatcher construidos sobre `nlp`."""
    matcher, pmatcher = _build_matchers(nlp)

    intents = set([doc.vocab.strings[m_id] for m_id, _, _ in matcher(doc)])
    intents.update([doc.vocab.strings[m_id] for m_id, _, _ in pmatcher(doc)])

    return intents, {
        "why": "matcher/phrase-matcher con lexicon",
        "confidence": 0.8 if intents else 0.0,
    }


# ====== 2) Extract SLOTS ======
def extract_slots(doc, raw_text: str) -> Dict[str, Any]:
    entities = {"LANG": [], "UNIT": [], "DATEFMT": [], "EXPORTFMT": []}
    for ent in doc.ents:
        if ent.label_ in entities:
            entities[ent.label_].append(ent.text.lower())

    slots: Dict[str, Any] = {"columns": _extract_columns(raw_text)}

    # idioma bruto (antes de normalizar)
    slots["lang_raw"] = entities["LANG"][0] if entities["LANG"] else None

    # unidad destino por construcción "a/en <unidad>"
    slots["unit_dest"] = _find_destination_unit(raw_text)

    # si no hubo construcción destino, considerar entidades UNIT con reglas robustas
    if not slots["unit_dest"]:
        for u in entities["UNIT"]:
            if u == '"' and _text_has_num_unit(raw_text):
                slots["unit_dest"] = "in"
                break
            if u in UNIT_CANON:
                slots["unit_dest"] = UNIT_CANON[u]
                break

    # formatos de fecha (por entidad o heurística)
    date_fmt = None
    for _, key, fmt in DATEFMTS:
        if key in entities["DATEFMT"]:
            date_fmt = fmt
            break
    if not date_fmt:
        t = raw_text.lower()
        if "dd/mm/aaaa" in t or "dd/mm/yyyy" in t:
            date_fmt = "%d/%m/%Y"
        elif "mm/dd/aaaa" in t or "mm/dd/yyyy" in t:
            date_fmt = "%m/%d/%Y"
        elif "iso" in t:
            date_fmt = "%Y-%m-%d"
    slots["date_fmt"] = date_fmt

    # formato de export
    export_fmt = None
    ef = entities["EXPORTFMT"]
    if ef:
        export_fmt = "xlsx" if ("xlsx" in ef or "excel" in ef) else "csv"
    slots["export_fmt"] = export_fmt

    # patrones de filtro (eq / contains / comparadores / between)
    m_eq = re.search(
        r'(?:donde|dónde)?\s*([\wáéíóúüñ]+)\s*=\s*[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]',
        raw_text, flags=re.IGNORECASE
    )
    m_contains = re.search(
        r'(?:donde|dónde|que)?\s*([\wáéíóúüñ]+)\s+contiene\s*[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]',
        raw_text, flags=re.IGNORECASE
    )
    m_cmp = re.search(
        r'(?:donde|dónde)?\s*([\wáéíóúüñ]+)\s*(<=|>=|<|>)\s*([0-9][0-9.,]*)',
        raw_text, flags=re.IGNORECASE
    )
    m_between = re.search(
        r'(?:donde|dónde)?\s*(?:entre)\s+([0-9/.\-]+)\s+y\s+([0-9/.\-]+)',
        raw_text, flags=re.IGNORECASE
    )


    slots["filter"] = None
    if m_eq:
        slots["filter"] = {"type": "eq", "column": m_eq.group(1), "value": m_eq.group(2)}
    elif m_contains:
        slots["filter"] = {"type": "contains", "column": m_contains.group(1), "value": m_contains.group(2)}
    elif m_cmp:
        slots["filter"] = {"type": "cmp", "column": m_cmp.group(1), "op": m_cmp.group(2), "value": m_cmp.group(3)}
    elif m_between:
        slots["filter"] = {"type": "between", "range": [m_between.group(1), m_between.group(2)]}

    # banderas nominales (sin verbo explícito)
    slots["mention_date"] = _contains_any(raw_text, LEXICON["nominals"]["DATE"])
    slots["mention_measure"] = _contains_any(raw_text, LEXICON["nominals"]["MEASURE"])
    slots["mention_desc"] = _contains_any(raw_text, LEXICON["nominals"]["DESC"])
    slots["mention_money"] = _contains_any(raw_text, LEXICON["nominals"]["MONEY"])
    slots["mention_export"] = _contains_any(raw_text, LEXICON["nominals"]["EXPORT"])
    return slots


# ====== 3) Normalize SLOTS ======
def normalize_slots(slots: Dict[str, Any]) -> Dict[str, Any]:
    norm = dict(slots)
    # idioma destino normalizado
    norm["target_lang"] = LANG_MAP.get((slots.get("lang_raw") or "").lower(), None)

    # columnas genéricas por dominio si no hay comillas
    if not norm["columns"]:
        if slots.get("mention_desc"):
            norm["columns"] = ["descripcion"]
        elif slots.get("mention_date"):
            norm["columns"] = ["fecha"]
        elif slots.get("mention_money"):
            norm["columns"] = ["monto"]

    return norm


# ====== 4) Build PLAN ======
def build_plan(intents: set, slots: Dict[str, Any], raw_text: str) -> Tuple[List[Dict], Dict]:
    plan: List[Dict] = []
    report: Dict[str, Any] = {"decisions": []}
    t = raw_text.lower()
    cols = slots["columns"]

    # Inferir intents por nominalizaciones (sin verbo)
    infer_translate = bool(slots.get("target_lang")) and slots.get("mention_desc") and "TRANSLATE" not in intents
    infer_format = bool(slots.get("date_fmt")) and slots.get("mention_date") and "FORMAT" not in intents
    infer_convert = bool(slots.get("unit_dest")) and slots.get("mention_measure") and "CONVERT" not in intents
    infer_export = bool(slots.get("export_fmt")) and slots.get("mention_export") and "EXPORT" not in intents

    # --- RENAME ---
    if "RENAME" in intents and len(cols) >= 2:
        mapping = {a: b for a, b in zip(cols[0::2], cols[1::2])}
        plan.append({"op": "rename_columns", "map": mapping})
        report["decisions"].append({"op": "rename_columns", "why": "RENAME + pares de columnas detectadas", "confidence": 0.85})

    # --- FORMAT DATE ---
    if "FORMAT" in intents or infer_format or slots.get("date_fmt"):
        out_fmt = slots.get("date_fmt") or "%Y-%m-%d"
        target_col = "fecha" if "fecha" in t else next((c for c in cols if c.lower() in ["fecha", "fec", "date"]), "fecha")
        plan.append({"op": "format_date", "column": target_col, "input_fmt": "infer", "output_fmt": out_fmt})
        report["decisions"].append({"op": "format_date", "why": "FORMAT o nominal + DATEFMT", "confidence": 0.8 if slots.get("date_fmt") else 0.65})

    # --- TRANSLATE ---
    if "TRANSLATE" in intents or infer_translate:
        lang = slots.get("target_lang") or "EN"
        tr_cols = [c for c in cols if c.lower() not in ["fecha", "fec", "date"]] or ["descripcion"]
        plan.append({"op": "translate_values", "columns": tr_cols, "target_lang": lang})
        report["decisions"].append({"op": "translate_values", "why": "TRANSLATE o nominal + idioma", "confidence": 0.75 if slots.get("target_lang") else 0.7})

    # --- CONVERT ---
    if "CONVERT" in intents or infer_convert:
        unit = slots.get("unit_dest")
        if unit:
            cu_cols = cols or ["largo", "ancho", "alto"]
            plan.append({"op": "convert_units", "columns": cu_cols, "target_unit": unit})
            report["decisions"].append({"op": "convert_units", "why": "CONVERT/nominal + unidad destino inequívoca", "confidence": 0.8})
        else:
            report["decisions"].append({"op": "convert_units", "why": "CONVERT sin unidad destino inequívoca (no se aplica)", "confidence": 0.3})

    # --- FILTER ---
    if "FILTER" in intents or slots.get("filter"):
        f = slots.get("filter")
        if f:
            if f["type"] == "eq":
                plan.append({"op": "filter_equals", "column": f["column"], "value": f["value"]})
            elif f["type"] == "contains":
                plan.append({"op": "filter_contains", "column": f["column"], "value": f["value"]})
            elif f["type"] == "cmp":
                plan.append({"op": "filter_compare", "column": f["column"], "op": f["op"], "value": f["value"]})
            elif f["type"] == "between":
                plan.append({"op": "filter_between", "range": f["range"]})
            report["decisions"].append({"op": "filter", "why": "patrón de filtro reconocido", "confidence": 0.9})
        else:
            report["decisions"].append({"op": "filter", "why": "intención FILTER sin patrón claro", "confidence": 0.4})

    # --- CURRENCY (simple USD nominal) ---
    if any(k in t for k in ["usd", "dólar", "dolar"]) and "currency_to" not in [p["op"] for p in plan]:
        money_cols = [c for c in cols if c.lower() not in ["fecha", "fec", "date"]] or ["monto", "importe", "total"]
        plan.append({"op": "currency_to", "columns": money_cols, "target": "USD", "rate": "ask_user|table"})
        report["decisions"].append({"op": "currency_to", "why": "mención de USD/dólar", "confidence": 0.6})

    # --- EXPORT ---
    if "EXPORT" in intents or infer_export or slots.get("export_fmt"):
        fmt = slots.get("export_fmt") or ("xlsx" if "excel" in t else "csv")
        plan.append({"op": "export", "format": fmt, "path": f"output/resultado.{fmt}"})
        report["decisions"].append({"op": "export", "why": "EXPORT o nominal + formato", "confidence": 0.95 if slots.get("export_fmt") else 0.7})

    return plan, report


# ====== Función pública ======
def interpret_with_spacy(text: str, nlp=None) -> Tuple[List[Dict], Dict]:
    """Punto de entrada: texto -> (plan, reporte)."""
    nlp = nlp or build_nlp()
    doc = nlp(text)
    intents, _ = detect_intents(doc, nlp)
    slots = extract_slots(doc, text)
    slots = normalize_slots(slots)
    return build_plan(intents, slots, text)
