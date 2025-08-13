import os, sys, yaml, json
import importlib
import re

# Asegurar import del proyecto (raíz)
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from nlp.pipeline import interpret_instructions
from config.settings import SPACY_MODEL

def load_cases():
    path = os.path.join(ROOT, "tests", "instructions.yml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def find_op(plan, op_name):
    return [p for p in plan if p.get("op") == op_name]

def subset_dict(small, big):
    """Devuelve True si todos los pares clave:valor de small están en big (comparación literal)."""
    for k, v in small.items():
        if k not in big:
            return False
        if big[k] != v:
            return False
    return True

def check_expected_op(exp, plan):
    """Chequear una operación esperada contra el plan."""
    candidates = find_op(plan, exp["op"])
    assert candidates, f"No se encontró op '{exp['op']}' en el plan: {plan}"

    def ok_candidate(c):
        # checks exactos simples
        for k in ["target_unit", "format", "output_fmt", "column", "value", "op", "target"]:
            if k in exp and c.get(k) != exp[k]:
                return False

        # columns_contains: cada item debe estar en c["columns"]
        if "columns_contains" in exp:
            cols = c.get("columns", [])
            for want in exp["columns_contains"]:
                if want not in cols:
                    return False

        # columns_any_of: al menos uno coincida
        if "columns_any_of" in exp:
            cols = c.get("columns", [])
            if not any(w in cols for w in exp["columns_any_of"]):
                return False

        # column_any_of: columna única debe ser una del set
        if "column_any_of" in exp:
            if c.get("column") not in exp["column_any_of"]:
                return False

        # map_contains: subset
        if "map_contains" in exp:
            cmap = c.get("map", {})
            for k, v in exp["map_contains"].items():
                if cmap.get(k) != v:
                    return False

        # value_like_number: valor debe parecer número (para comparadores)
        if exp.get("value_like_number"):
            val = c.get("value")
            if val is None or not re.search(r"^\s*\d+[.,]?\d*\s*$", str(val)):
                return False

        # range_len: longitud mínima del rango
        if "range_len" in exp:
            r = c.get("range")
            if not isinstance(r, list) or len(r) < exp["range_len"]:
                return False

        return True

    # validar que al menos un candidato matchee
    assert any(ok_candidate(c) for c in candidates), f"No hay candidato válido para {exp} en {candidates}"

def test_instructions_suite():
    cases = load_cases()
    failures = []
    for case in cases:
        txt = case["input"]
        plan, report = interpret_instructions(txt, SPACY_MODEL)

        # prohibidos (p. ej., evitar overfitting)
        for op in case.get("forbid_ops", []):
            assert all(p.get("op") != op for p in plan), f"Op '{op}' no debería aparecer en plan: {plan}"

        for exp in case["expect"]["ops"]:
            check_expected_op(exp, plan)
