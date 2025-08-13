import pytest

def pytest_assertrepr_compare(op, left, right):
    # mejora mensajes en asserts si quisieras comparar dicts complejos (opcional)
    return None
