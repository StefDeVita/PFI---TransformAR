# nlp/instruction_spacy.py
from typing import List, Dict, Tuple
import re
import spacy
from spacy.matcher import Matcher, PhraseMatcher
from spacy.pipeline import EntityRuler

LANG_MAP = {
    "inglés":"EN","ingles":"EN","en":"EN",
    "español":"ES","es":"ES",
    "alemán":"DE","aleman":"DE","de":"DE",
    "italiano":"IT","it":"IT",
    "portugués":"PT","portugues":"PT","pt":"PT"
}
UNIT_MAP = {
    "mm":"mm","milimetro":"mm","milímetros":"mm",
    "cm":"cm","m":"m","metro":"m",
    "in":"in","inch":"in","\"":"in","pulgada":"in","pulgadas":"in",
    "ft":"ft",
    "kg":"kg","g":"g","gramo":"g","gramos":"g",
    "lb":"lb","libra":"lb","libras":"lb"
}
DATEFMTS = [
    ("DATEFMT","dd/mm/aaaa","%d/%m/%Y"),
    ("DATEFMT","dd/mm/yyyy","%d/%m/%Y"),
    ("DATEFMT","mm/dd/aaaa","%m/%d/%Y"),
    ("DATEFMT","mm/dd/yyyy","%m/%d/%Y"),
    ("DATEFMT","iso","%Y-%m-%d")
]
NUM_UNIT_PATTERNS = [
    r"\b\d+[.,]?\d*\s*(mm|cm|m|in|inch|pulgadas?|ft|kg|g|lb)\b",
    r"\b(mm|cm|m|in|inch|pulgadas?|ft|kg|g|lb)\s*\d+[.,]?\d*\b",
    r"\d\s*[\"”]"  # 3" o 3 ”
]

DESTINATION_PATTERNS = [
    r"\b(a|en|a\s+los|a\s+las)\s+(mm|cm|m|in|inch|pulgadas?|ft|kg|g|lb)\b"
]
DESC_SYNONYMS = ["descripcion","descripción","detalle","detalles","desc","concepto","item","items","ítem","ítems"]

def _find_desc_columns_in_text(text: str) -> list[str]:
    t = text.lower()
    hits = []
    for w in DESC_SYNONYMS:
        if w in t:
            hits.append("descripcion")  # usamos un alias genérico; luego se hará grounding al documento
            break
    return hits

def _text_has_num_unit(text: str) -> bool:
    import re
    t = text.lower()
    return any(re.search(p, t) for p in NUM_UNIT_PATTERNS)

def _find_destination_unit(text: str) -> str | None:
    """Busca 'a mm', 'en cm', 'a pulgadas', etc. Devuelve unidad canónica si aparece."""
    import re
    t = text.lower()
    for p in DESTINATION_PATTERNS:
        m = re.search(p, t)
        if m:
            u = m.group(2)
            return {
                "mm":"mm","cm":"cm","m":"m",
                "in":"in","inch":"in","pulgada":"in","pulgadas":"in",
                "ft":"ft","kg":"kg","g":"g","lb":"lb"
            }.get(u, None)
    return None


def build_nlp(model: str = "es_core_news_md"):
    """Crea el nlp con EntityRuler para LANG, UNIT, DATEFMT, EXPORTFMT."""
    nlp = spacy.load(model)
    er = nlp.add_pipe("entity_ruler", name="instruction_ruler", before="ner")
    patterns = []
    # idiomas
    for k in LANG_MAP.keys():
        patterns.append({"label":"LANG", "pattern": k})
    # unidades
    for k in UNIT_MAP.keys():
        patterns.append({"label":"UNIT", "pattern": k})
    # formatos de fecha
    for _, key, _fmt in DATEFMTS:
        patterns.append({"label":"DATEFMT", "pattern": key})
    # formatos export
    patterns += [
        {"label":"EXPORTFMT","pattern":"csv"},
        {"label":"EXPORTFMT","pattern":"xlsx"},
        {"label":"EXPORTFMT","pattern":"excel"},
    ]
    er.add_patterns(patterns)
    return nlp

def _build_matchers(nlp):
    from spacy.matcher import Matcher, PhraseMatcher
    matcher = Matcher(nlp.vocab)

    # --- rename_columns ---
    matcher.add("RENAME", [
        [{"LEMMA": {"IN": ["renombrar","llamar","etiquetar"]}}],
        [{"LOWER": {"IN": ["renombrá","renombra"]}}],
        [{"TEXT": {"REGEX": "(?i)renombr.*"}}],
    ])

    # --- format_date ---
    matcher.add("FORMAT", [
        [{"LEMMA": {"IN": ["formatear"]}}],
        [{"LOWER": "formato"}],
        # voseo: "poné la fecha ..."
        [{"LOWER": {"IN": ["poné","pone"]}}, {"LOWER": "la"}, {"LOWER": "fecha"}],
    ])

    # --- translate_values ---
    matcher.add("TRANSLATE", [
        [{"LEMMA": {"IN": ["traducir"]}}],
        [{"LOWER": {"IN": ["traducí","traduce"]}}],
    ])

    # --- convert_units ---
    matcher.add("CONVERT", [
        [{"LEMMA": {"IN": ["convertir","unificar","normalizar","pasar"]}}],
        [{"LOWER": {"IN": ["convertí","unificá","normalizá","pasá","pasa"]}}],
    ])

    # --- filter_equals ---
    matcher.add("FILTER", [
        [{"LEMMA": {"IN": ["filtrar","mostrar"]}}],
        [{"LOWER": {"IN": ["filtrá","mostrá","filtra","mostra"]}}],
    ])

    # --- export ---
    matcher.add("EXPORT", [
        [{"LEMMA": {"IN": ["exportar","guardar","descargar"]}}],
        [{"LOWER": {"IN": ["exportá","guardá","descargá","csv","xlsx","excel"]}}],
    ])

    pmatcher = PhraseMatcher(nlp.vocab, attr="LOWER")
    pmatcher.add("EXPORT", [nlp.make_doc("exportar a csv"), nlp.make_doc("exportar a excel")])
    return matcher, pmatcher

def _extract_columns(text: str) -> List[str]:
    import re
    # valor de filtro para excluirlo de columnas citadas
    m = re.search(r'donde\s+([\wÁÉÍÓÚÜÑáéíóúüñ]+)\s*=\s*[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]', text, flags=re.IGNORECASE)
    filter_value = m.group(2).strip() if m else None

    quoted = re.findall(r'[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]', text)
    if filter_value:
        quoted = [q for q in quoted if q.strip().lower() != filter_value.lower()]

    csv_like = re.findall(r'(?:columna|columnas?)\s+([\wÁÉÍÓÚÜÑáéíóúüñ ,/.-]+)', text, flags=re.IGNORECASE)
    cols = [c.strip() for c in quoted]
    if csv_like:
        cols += [c.strip() for c in re.split(r'[ ,/]+', csv_like[0]) if c.strip()]

    seen, out = set(), []
    for c in cols:
        k = c.lower()
        if k not in seen:
            out.append(c); seen.add(k)
    return out


def interpret_with_spacy(text: str, nlp=None):
    nlp = nlp or build_nlp()
    matcher, pmatcher = _build_matchers(nlp)
    doc = nlp(text)
    t = text.lower()

    # intents
    intents = set([nlp.vocab.strings[m_id] for m_id, _, _ in matcher(doc)])
    intents.update([nlp.vocab.strings[m_id] for m_id, _, _ in pmatcher(doc)])

    # entities (slots)
    entities = {"LANG":[], "UNIT":[], "DATEFMT":[], "EXPORTFMT":[]}
    for ent in doc.ents:
        if ent.label_ in entities:
            entities[ent.label_].append(ent.text.lower())

    # normalizaciones de slots
    target_lang = None
    if entities["LANG"]:
        target_lang = LANG_MAP.get(entities["LANG"][0], None)

    # --- Unidad destino robusta ---
    # 1) Priorizar construcciones destino: "a mm", "en cm", "a pulgadas"
    target_unit = _find_destination_unit(text)

    # 2) Si no hubo construcción de destino, considerar entidades UNIT:
    #    - Aceptar comillas como pulgadas SOLO si hay número cercano (3")
    #    - Aceptar unidades alfabéticas (mm, cm, inch, etc.) aunque no haya número,
    #      pero SOLO si también existe un verbo de conversión en intents (CONVERT).
    if target_unit is None:
        for u in entities["UNIT"]:
            if u == '"':
                if _text_has_num_unit(text):
                    target_unit = "in"
                    break
                else:
                    continue
            if u in UNIT_MAP:
                # dejas esto como candidato si luego hay intención de convertir
                candidate = UNIT_MAP[u]
                # si no hay patrón número+unidad en el texto, igual vale como destino
                # siempre que exista intención CONVERT (lo chequeamos más abajo)
                target_unit = candidate
                break

    # Fallback para formatos de fecha en mayúsculas (DD/MM/AAAA)
    date_fmt = None
    for _, key, fmt in DATEFMTS:
        if key in entities["DATEFMT"]:
            date_fmt = fmt; break
    if not date_fmt:
        if "dd/mm/aaaa" in t or "dd/mm/yyyy" in t: date_fmt = "%d/%m/%Y"
        elif "mm/dd/aaaa" in t or "mm/dd/yyyy" in t: date_fmt = "%m/%d/%Y"
        elif "iso" in t: date_fmt = "%Y-%m-%d"

    export_fmt = None
    if entities["EXPORTFMT"]:
        export_fmt = "xlsx" if ("xlsx" in entities["EXPORTFMT"] or "excel" in entities["EXPORTFMT"]) else "csv"

    columns = _extract_columns(text)
    # si no hay columnas citadas, pero habla de "descripción/detalle/ítems", proponer columna genérica
    if not columns:
        desc_cols = _find_desc_columns_in_text(text)
        if desc_cols:
            columns = desc_cols


    # Si pide conversión de unidades y no hay columnas citadas, inferir clásico: largo/ancho/alto
    measure_words = [w for w in ["largo","ancho","alto","longitud","anchura","altura"] if w in t]
    if "CONVERT" in intents and not columns and measure_words:
        columns = measure_words

    plan, report = [], {"decisions":[]}

    # --- RENAME ---
    if "RENAME" in intents and len(columns) >= 2:
        mapping = {a:b for a,b in zip(columns[0::2], columns[1::2])}
        plan.append({"op":"rename_columns","map":mapping})
        report["decisions"].append({"op":"rename_columns","why":"RENAME por lemmas/voseo + columnas detectadas","confidence":0.85})

    # --- FORMAT DATE ---
    if "FORMAT" in intents or date_fmt:
        out_fmt = date_fmt or "%Y-%m-%d"
        # preferí 'fecha' si aparece en el texto
        if "fecha" in t:
            target_col = "fecha"
        else:
            # si entre comillas aparece explícita, la tomamos; si no, default
            target_col = next((c for c in columns if c.lower() in ["fecha","fec","date"]), "fecha")
        plan.append({"op":"format_date","column":target_col,"input_fmt":"infer","output_fmt":out_fmt})
        report["decisions"].append({"op":"format_date","why":"FORMAT/DATEFMT + preferencia por 'fecha'","confidence":0.8 if date_fmt else 0.65})

    # --- Inferir intención de traducción sin verbo ---
    infer_translate = False
    if target_lang and "TRANSLATE" not in intents:
        # si hay idioma y se mencionan descripciones/ítems/detalle, interpretar como traducción de valores
        if any(w in text.lower() for w in DESC_SYNONYMS):
            infer_translate = True

    # --- TRANSLATE VALUES ---
    if "TRANSLATE" in intents or infer_translate:
        lang = target_lang or "EN"
        tr_cols = [c for c in columns if c.lower() not in ["fecha","fec","date"]] or ["descripcion"]
        plan.append({"op":"translate_values","columns":tr_cols,"target_lang":lang})
        report["decisions"].append({
            "op":"translate_values",
            "why":"idioma detectado + referencia a descripciones/ítems" if infer_translate else "TRANSLATE + idioma (si hay)",
            "confidence": 0.7 if infer_translate else (0.75 if target_lang else 0.6)
        })


    # --- CONVERT UNITS ---
    # Reglas:
    # - Debe existir intención CONVERT.
    # - Debe haber target_unit válido.
    # - Si target_unit vino solo por comillas sin número, no se acepta.
    if "CONVERT" in intents and target_unit:
        cu_cols = columns or ["largo","ancho","alto"]
        plan.append({"op":"convert_units","columns":cu_cols,"target_unit":target_unit})
        report["decisions"].append({"op":"convert_units","why":"CONVERT + unidad destino válida (construcción 'a/en <unidad>' o unidad inequívoca)", "confidence":0.8})
    elif "CONVERT" in intents:
        report["decisions"].append({"op":"convert_units","why":"CONVERT sin unidad destino inequívoca (no se aplica)", "confidence":0.3})


    # --- FILTER EQUALS ---
    m = re.search(r'donde\s+([\wáéíóúüñ]+)\s*=\s*[\"“”\'‘’]([^\"“”\'‘’]+)[\"“”\'‘’]', text, flags=re.IGNORECASE)
    if "FILTER" in intents or m:
        if m:
            plan.append({"op":"filter_equals","column":m.group(1), "value":m.group(2)})
            report["decisions"].append({"op":"filter_equals","why":"FILTER + patrón 'donde col = \"valor\"'","confidence":0.9})
        else:
            report["decisions"].append({"op":"filter_equals","why":"FILTER sin patrón claro","confidence":0.4})

    # --- CURRENCY (USD simple) ---
    if "usd" in t or "dólar" in t or "dolar" in t:
        money_cols = [c for c in columns if c.lower() not in [m.group(2).lower() if m else ""]] or ["monto","importe","total"]
        plan.append({"op":"currency_to","columns":money_cols,"target":"USD","rate":"ask_user|table"})
        report["decisions"].append({"op":"currency_to","why":"mención de USD/dólar","confidence":0.6})

    # --- EXPORT ---
    if "EXPORT" in intents or export_fmt:
        fmt = export_fmt or "csv"
        plan.append({"op":"export","format":fmt,"path":f"output/resultado.{fmt}"})
        report["decisions"].append({"op":"export","why":"EXPORT o formato detectado","confidence":0.95 if export_fmt else 0.7})

    return plan, report
