from __future__ import annotations
import requests
from typing import Any, Dict, List, Optional

from config.settings import (
    OLLAMA_HOST, OLLAMA_MODEL, OLLAMA_TEMPERATURE, OLLAMA_MAX_TOKENS
)

class OllamaClient:
    def __init__(self, host: str = OLLAMA_HOST, model: str = OLLAMA_MODEL):
        self.host = host.rstrip("/")
        self.model = model

    def chat_raw(
        self,
        system: str,
        user: str,
        json_mode: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Llama a /api/chat de Ollama y devuelve el contenido crudo de la respuesta.
        Si json_mode=True, intenta forzar salida JSON (algunos modelos lo soportan).
        """
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
            "options": {
                "temperature": OLLAMA_TEMPERATURE,
                "num_predict": OLLAMA_MAX_TOKENS,
                **(options or {})
            },
        }
        if json_mode:
            payload["format"] = "json"  # si el modelo lo soporta, saldrá JSON puro

        url = f"{self.host}/api/chat"
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return (data.get("message") or {}).get("content", "")  # texto (a veces JSON, a veces markdown)

    def chat_json(
        self,
        system: str,
        user: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Igual que chat_raw pero dejando json_mode=True por defecto.
        Devuelve el contenido crudo (string); el parseo a dict lo hace el caller.
        """
        return self.chat_raw(system=system, user=user, json_mode=True, options=options)
