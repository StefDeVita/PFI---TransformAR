# nlp/apply_plan.py
from __future__ import annotations
from typing import List, Dict, Any

from config.settings import AUTO_TEXT_LLM, AUTO_TEXT_MAXCHARS, AUTO_ISO_DATES_DEFAULT
from nlp.runtime import (
    auto_fix_strings,
    iso_dates_everywhere,
    format_numbers_everywhere,
)
from nlp.ops.registry import get_op

def _pre(doc: Dict[str, Any]) -> None:
    # Limpieza determinística + (opt) LLM
    auto_fix_strings(doc, enable_llm=AUTO_TEXT_LLM, maxchars=AUTO_TEXT_MAXCHARS)
    # Normalizar fechas “date-like” a ISO si está habilitado
    if AUTO_ISO_DATES_DEFAULT:
        iso_dates_everywhere(doc)

def _post(doc: Dict[str, Any]) -> None:
    # Unificar estilo de numéricos (solo strings numéricos puros)
    format_numbers_everywhere(doc)

def execute_plan(doc_or_list: Any, plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Motor de plan simple:
      - Preprocesa doc
      - Ejecuta cada step llamando a la operación registrada por nombre
      - Si un filtro falla => descarta el doc
      - Postprocesa y devuelve
    """
    docs: List[Dict[str, Any]]
    if isinstance(doc_or_list, dict):
        docs = [doc_or_list]
    elif isinstance(doc_or_list, list):
        docs = [d for d in doc_or_list if isinstance(d, dict)]
    else:
        raise TypeError("execute_plan espera un dict o lista de dicts")

    out: List[Dict[str, Any]] = []
    for doc in docs:
        _pre(doc)
        keep = True
        for step in plan or []:
            op_name = (step or {}).get("op", "")
            op = get_op(op_name)
            if not op:
                # Operación desconocida: seguir (diseño tolerante)
                continue
            keep = bool(op(doc, step))
            if not keep:
                break
        if keep:
            _post(doc)
            out.append(doc)
    print(out)
    return out

# --- registrar operaciones builtin por side-effect ---
import nlp.ops.builtins  # noqa: F401
