# nlp/ops/registry.py
from __future__ import annotations
from typing import Callable, Dict, Any, Optional

# Firma: (doc, step) -> bool
OperationFn = Callable[[Dict[str, Any], Dict[str, Any]], bool]

_REGISTRY: Dict[str, OperationFn] = {}

def op(name: str):
    """Decorador para registrar operaciones por nombre."""
    def _wrap(fn: OperationFn) -> OperationFn:
        _REGISTRY[name] = fn
        return fn
    return _wrap

def get_op(name: str) -> Optional[OperationFn]:
    return _REGISTRY.get(name)
