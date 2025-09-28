# currency_converter.py
from __future__ import annotations
import os, json, time, math
from typing import Dict
import requests

JSDELIVR = "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{date}/v1/currencies/{base}.json"
CLOUDFLARE = "https://{date}.currency-api.pages.dev/v1/currencies/{base}.json"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache_currency")
TTL_SECONDS = 12 * 60 * 60  # 12 horas

class CurrencyConverter:
    def __init__(self, ttl_seconds: int = TTL_SECONDS):
        self.ttl = ttl_seconds
        os.makedirs(CACHE_DIR, exist_ok=True)

    def _cache_path(self, base: str, date: str) -> str:
        return os.path.join(CACHE_DIR, f"rates_{base.lower()}_{date}.json")

    def _is_fresh(self, path: str) -> bool:
        return os.path.exists(path) and (time.time() - os.path.getmtime(path) < self.ttl)

    def _load_cache(self, path: str) -> Dict:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_cache(self, path: str, data: Dict) -> None:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, path)

    def _fetch_rates(self, base: str, date: str) -> Dict:
        urls = [
            JSDELIVR.format(date=date, base=base.lower()),
            CLOUDFLARE.format(date=date, base=base.lower()),
        ]
        last_error = None
        for url in urls:
            try:
                r = requests.get(url, timeout=15)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                last_error = e
        raise RuntimeError(f"No se pudieron obtener tasas para {base} @ {date}: {last_error}")

    def get_rates(self, base: str, date: str = "latest") -> Dict[str, float]:
        """Devuelve dict con tasas respecto a 'base'. Ej.: rates['eur']['ars']"""
        path = self._cache_path(base, date)
        if self._is_fresh(path):
            return self._load_cache(path)

        data = self._fetch_rates(base, date)
        # Estructura esperada: {"<base>": {"usd": 1, "ars": 900.0, ...}}
        if base.lower() not in data:
            raise ValueError(f"Respuesta inválida: no contiene clave '{base.lower()}'")
        self._save_cache(path, data)
        return data

    def convert(self, amount: float, from_: str, to: str, date: str = "latest") -> float:
        """
        Convierte monto de 'from_' a 'to' usando tasas del día 'date'.
        Si la base pedida no coincide con 'from_', se usa triangulación.
        """
        from_ = from_.lower()
        to = to.lower()
        if from_ == to:
            return float(amount)

        # Intento 1: usar base = from_
        try:
            data = self.get_rates(base=from_, date=date)
            rates = data[from_]
            if to in rates:
                return float(amount) * float(rates[to])
        except Exception:
            pass

        # Intento 2: usar base = to (y revertir)
        data = self.get_rates(base=to, date=date)
        rates = data[to]
        if from_ not in rates:
            raise ValueError(f"No hay cruce disponible {from_.upper()}→{to.upper()} para {date}")
        inv = 1.0 / float(rates[from_])
        return float(amount) * inv

# Uso rápido:
# conv = CurrencyConverter()
# print(conv.convert(100, "USD", "ARS"))  # 100 USD -> ARS
