# nlp/apply_plan.py
from typing import List, Dict, Any
from datetime import datetime
import unicodedata
import re

def normalize_key(key: str) -> str:
    """Convierte claves a minúsculas y sin tildes para comparación insensible."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', key)
        if unicodedata.category(c) != 'Mn'
    ).lower()

def parse_number(value: str) -> float:
    """Convierte string numérico en float, soportando formato europeo y anglosajón."""
    if value is None:
        return None
    v = str(value).strip()
    # Quitar separadores de miles
    if ',' in v and '.' in v:
        if v.find(',') > v.find('.'):
            # 1.234,56 -> 1234.56
            v = v.replace('.', '').replace(',', '.')
        else:
            # 1,234.56 -> 1234.56
            v = v.replace(',', '')
    elif ',' in v and not '.' in v:
        # 1234,56 -> 1234.56
        v = v.replace(',', '.')
    try:
        return float(v)
    except ValueError:
        return None

def convert_to_mm(value: float, unit: str) -> float:
    """Convierte un valor a milímetros."""
    unit = unit.lower()
    if unit.startswith('m') and not unit.startswith('mm'):
        return value * 1000
    if unit.startswith('cm'):
        return value * 10
    if unit.startswith('mm'):
        return value
    if unit.startswith('in'):
        return value * 25.4
    return value

def extract_fields_from_description(record: Dict[str, Any]) -> None:
    """Extrae ancho y largo desde la descripción y los agrega a structured."""
    desc = None
    if isinstance(record.get("structured"), dict):
        desc = record["structured"].get("descripcion") or record["structured"].get("description")
    if not desc:
        desc = record.get("descripcion") or record.get("description")
    if not isinstance(desc, str):
        return

    patterns = {
        "ancho": r"ancho\s*[:=]?\s*([\d\.,]+)\s*(cm|mm|m|in)\b",
        "largo": r"largo\s*[:=]?\s*([\d\.,]+)\s*(cm|mm|m|in)\b",
    }

    if "structured" not in record or not isinstance(record["structured"], dict):
        record["structured"] = {}

    for field, pat in patterns.items():
        match = re.search(pat, desc, re.IGNORECASE)
        if match:
            num_str, unit = match.groups()
            num_val = parse_number(num_str)
            if num_val is not None:
                mm_val = convert_to_mm(num_val, unit)
                record["structured"][field] = f"{mm_val:.2f} mm"

def _rename_columns_in_dict(d: Dict[str, Any], mapping: Dict[str, str], renamed_map: Dict[str, str]) -> None:
    """Renombra claves en el dict `d` usando `mapping` con comparación insensible."""
    if not isinstance(d, dict):
        return
    norm_map = {normalize_key(k): v for k, v in mapping.items()}
    keys = list(d.keys())
    for k in keys:
        nk = normalize_key(k)
        if nk in norm_map:
            new_k = norm_map[nk]
            d[new_k] = d.pop(k)
            renamed_map[nk] = new_k  # Persistir el renombre

def execute_plan(data: Any, plan: List[Dict]) -> List[Dict]:
    """Aplica las operaciones del plan a los datos extraídos."""
    if isinstance(data, dict):
        data = [data]

    transformed_data = []
    renamed_map: Dict[str, str] = {}

    for record in data:
        transformed = record.copy()

        # Preprocesamiento: extraer campos desde la descripción
        extract_fields_from_description(transformed)

        for step in plan:
            op = step["op"]
            if op == "rename_columns":
                mapping = step.get("map", {})
                _rename_columns_in_dict(transformed, mapping, renamed_map)
                if "structured" in transformed and isinstance(transformed["structured"], dict):
                    _rename_columns_in_dict(transformed["structured"], mapping, renamed_map)
            elif op == "format_date":
                col = step.get("column")
                # Buscar la fecha tanto en el nivel raíz como en structured
                targets = []
                if col in transformed:
                    targets.append(("root", col))
                if "structured" in transformed and isinstance(transformed["structured"], dict) and col in transformed[
                    "structured"]:
                    targets.append(("structured", col))

                for scope, colname in targets:
                    raw_date = transformed[colname] if scope == "root" else transformed["structured"][colname]
                    if not raw_date:
                        continue
                    raw_date = raw_date.strip()
                    try:
                        input_fmt = step.get("input_fmt") or step.get("input_format")
                        if input_fmt and input_fmt != "infer":
                            dt = datetime.strptime(raw_date, input_fmt)
                        else:
                            posibles_formatos = [
                                "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d", "%Y-%m-%d",
                                "%d.%m.%Y", "%d %B %Y", "%d %b %Y"
                            ]
                            dt = None
                            for fmt in posibles_formatos:
                                try:
                                    dt = datetime.strptime(raw_date, fmt)
                                    break
                                except ValueError:
                                    continue
                            if not dt:
                                raise ValueError(f"No se pudo inferir formato de fecha: {raw_date}")

                        output_fmt = step.get("output_fmt") or step.get("output_format")
                        new_value = dt.strftime(output_fmt)
                        if scope == "root":
                            transformed[colname] = new_value
                        else:
                            transformed["structured"][colname] = new_value

                    except Exception as e:
                        print(f"[WARN] No se pudo formatear la fecha '{raw_date}': {e}")

            elif op == "translate_values":
                cols = step.get("columns", [])
                lang = step.get("target_lang", "EN")
                for c in cols:
                    c_norm = renamed_map.get(normalize_key(c), c)
                    if c_norm in transformed and transformed[c_norm]:
                        transformed[c_norm] = f"[{lang}]{transformed[c_norm]}"
                    if "structured" in transformed and isinstance(transformed["structured"], dict):
                        if c_norm in transformed["structured"] and transformed["structured"][c_norm]:
                            transformed["structured"][c_norm] = f"[{lang}]{transformed['structured'][c_norm]}"

            elif op == "convert_units":
                cols = step.get("columns", [])
                target_unit = step.get("target_unit")
                for c in cols:
                    c_norm = renamed_map.get(normalize_key(c), c)
                    if c_norm in transformed and transformed[c_norm]:
                        transformed[c_norm] = f"{transformed[c_norm]} ({target_unit})"
                    if "structured" in transformed and isinstance(transformed["structured"], dict):
                        if c_norm in transformed["structured"] and transformed["structured"][c_norm]:
                            transformed["structured"][c_norm] = f"{transformed['structured'][c_norm]} ({target_unit})"

            elif op == "filter_equals":
                col = step.get("column")
                val = step.get("value")
                if col and val is not None:
                    ncol = normalize_key(col)
                    nval = normalize_key(str(val).strip())
                    found_val = None

                    for k, v in transformed.items():
                        if normalize_key(k) == ncol:
                            found_val = v
                            break

                    if found_val is None and isinstance(transformed.get("structured"), dict):
                        for k, v in transformed["structured"].items():
                            if normalize_key(k) == ncol:
                                found_val = v
                                break

                    if found_val is None:
                        all_keys = list(transformed.keys())
                        if isinstance(transformed.get("structured"), dict):
                            all_keys += list(transformed["structured"].keys())
                        for k in all_keys:
                            if normalize_key(k) == ncol:
                                found_val = (
                                    transformed.get(k) or
                                    transformed.get("structured", {}).get(k)
                                )
                                break

                    if isinstance(found_val, (list, tuple)) and found_val:
                        found_val = found_val[0]

                    if found_val is None or normalize_key(str(found_val)) != nval:
                        transformed = None
                        break

            elif op == "currency_to":
                cols = step.get("columns", [])
                target_currency = step.get("target", "USD")
                for c in cols:
                    c_norm = renamed_map.get(normalize_key(c), c)
                    if c_norm in transformed and transformed[c_norm]:
                        transformed[c_norm] = f"{transformed[c_norm]} {target_currency}"
                    if "structured" in transformed and isinstance(transformed["structured"], dict):
                        if c_norm in transformed["structured"] and transformed["structured"][c_norm]:
                            transformed["structured"][c_norm] = f"{transformed['structured'][c_norm]} {target_currency}"

            elif op == "export":
                pass

        if transformed is not None:
            transformed_data.append(transformed)

    return transformed_data

