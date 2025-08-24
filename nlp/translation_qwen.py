from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List, Iterable, Any
from config.settings import (
    OLLAMA_MODEL, OLLAMA_TEMPERATURE, OLLAMA_MAX_TOKENS
)

# --- Imports robustos del cliente ---
try:
    from .ollama_client import OllamaClient
except Exception:
    try:
        from ollama_client import OllamaClient  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "OllamaClient not found. Asegurate de que 'ollama_client.py' "
            "esté en 'nlp/' o en la raíz del proyecto, o ajustá este import."
        ) from e

LANG_ALIASES: Dict[str, str] = {
    "es": "Spanish",
    "en": "English",
    "de": "German",
    "it": "Italian",
    "pt": "Portuguese",
    "fr": "French",
    "zh": "Chinese",
    "zh-cn": "Chinese (Simplified)",
    "zh-tw": "Chinese (Traditional)",
}

SYSTEM_TRANSLATOR = (
    """
    Eres un traductor y editor técnico profesional
    Siempre produce un texto natural, técnico y bien formado en el idioma de destino
    Si la entrada está en un idioma mixto, REESCRÍBELA en el idioma de destino
    Traduce los sustantivos comunes incluso si están en MAYÚSCULAS; conserva exactamente los nombres de marca, códigos de modelo y números de parte
    No agregues explicaciones ni notas. Devuelve solo la traducción.
    """
)


def _normalize_lang(lang: str) -> str:
    key = (lang or "").strip().lower()
    return LANG_ALIASES.get(key, lang)

@dataclass
class QwenTranslator:
    client: Optional[OllamaClient] = None
    model: str = OLLAMA_MODEL
    temperature: float = OLLAMA_TEMPERATURE
    max_tokens: int = OLLAMA_MAX_TOKENS

    def __post_init__(self):
        if self.client is None:
            self.client = OllamaClient()

    def _prompt_messages(self, text: str, target_lang: str) -> List[Dict[str, str]]:
        target = _normalize_lang(target_lang or "en")
        user = (
            f"Target language: {target}\n\n"
            "Text to translate:\n"
            f"{text}\n"
            "Return only the translation."
        )
        return [
            {"role": "system", "content": SYSTEM_TRANSLATOR},
            {"role": "user", "content": user},
        ]

    def _prompt_plain(self, text: str, target_lang: str) -> str:
        target = _normalize_lang(target_lang or "en")
        return (
            f"{SYSTEM_TRANSLATOR}\n\n"
            f"Target language: {target}\n\n"
            "Text to translate:\n"
            f"{text}\n"
            "Return only the translation."
        )

    def translate(self, text: Any, target_lang: str) -> str:
        raw = "" if text is None else str(text)

        # 1) chat_raw
        try:
            if hasattr(self.client, "chat_raw"):
                return (self.client.chat_raw(
                    system=SYSTEM_TRANSLATOR,
                    user=self._prompt_messages(raw, target_lang)[1]["content"],
                    json_mode=False,
                    options={"temperature": self.temperature, "num_predict": self.max_tokens},
                    model=self.model,
                ) or "").strip()
        except Exception:
            pass

        # 2) chat
        try:
            if hasattr(self.client, "chat"):
                resp = self.client.chat(
                    model=self.model,
                    messages=self._prompt_messages(raw, target_lang),
                    options={"temperature": self.temperature, "num_predict": self.max_tokens},
                )
                if isinstance(resp, dict):
                    if "message" in resp and isinstance(resp["message"], dict):
                        return (resp["message"].get("content", "") or "").strip()
                    if "choices" in resp and resp["choices"]:
                        return (resp["choices"][0]["message"]["content"] or "").strip()
                if isinstance(resp, str):
                    return resp.strip()
        except Exception:
            pass

        # 3) generate
        try:
            if hasattr(self.client, "generate"):
                out = self.client.generate(
                    model=self.model,
                    prompt=self._prompt_plain(raw, target_lang),
                    options={"temperature": self.temperature, "num_predict": self.max_tokens},
                )
                if isinstance(out, dict):
                    return (out.get("response", "") or out.get("text", "") or "").strip()
                if isinstance(out, str):
                    return out.strip()
        except Exception:
            pass

        raise RuntimeError("No pude llamar al modelo: ninguna de las rutas (chat_raw/chat/generate) funcionó.")

    def batch_translate(self, texts: Iterable[Any], target_lang: str):
        return [self.translate(t, target_lang) for t in texts]
