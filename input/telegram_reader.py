# input/telegram_reader.py
"""
Integración con Telegram Bot API
Sigue el mismo patrón que gmail_reader.py y outlook_reader.py
"""

import os
import requests
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

load_dotenv()


class TelegramClient:
    """Cliente para Telegram Bot API"""

    BASE_URL = "https://api.telegram.org"

    def __init__(self, credentials_dict: Optional[Dict[str, Any]] = None):
        """
        Inicializa el cliente de Telegram.

        Args:
            credentials_dict: Credenciales desde Firestore (recomendado).
                              Debe contener: {"bot_token": "..."}
                              Si no se proporciona, usa variables de entorno (DEPRECATED).
        """
        if credentials_dict:
            # Usar credenciales desde Firestore (multi-tenant)
            self.bot_token = credentials_dict.get("bot_token")
        else:
            # DEPRECATED: Usar variables de entorno (modo legacy)
            self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")

        if not self.bot_token:
            raise ValueError("Falta bot_token de Telegram en credenciales o variables de entorno")

        self.api_url = f"{self.BASE_URL}/bot{self.bot_token}"
        self.offset = None  # Para tracking de updates en polling

    def get_updates(self, limit: int = 10, timeout: int = 30) -> Dict[str, Any]:
        """
        Obtiene nuevas actualizaciones usando long polling

        Este método es útil para:
        - Testing y desarrollo
        - Fallback si webhook no está disponible
        - Aplicaciones que no requieren respuestas en tiempo real

        Args:
            limit: Máximo número de updates a obtener (1-100)
            timeout: Tiempo de espera del long polling en segundos

        Returns:
            Respuesta de la API con estructura:
            {
                "ok": true,
                "result": [
                    {
                        "update_id": 123456,
                        "message": {
                            "message_id": 1,
                            "from": {"id": 111, "username": "user"},
                            "chat": {"id": 987654321, "type": "private"},
                            "date": 1234567890,
                            "text": "Hola"
                        }
                    }
                ]
            }
        """
        params = {"limit": limit, "timeout": timeout}
        if self.offset:
            params["offset"] = self.offset

        try:
            resp = requests.get(f"{self.api_url}/getUpdates", params=params, timeout=timeout + 5)
            resp.raise_for_status()
            data = resp.json()

            # Actualizar offset para próximas llamadas
            if data.get("ok") and data.get("result"):
                last_update = data["result"][-1]
                self.offset = last_update["update_id"] + 1

            return data
        except Exception as e:
            print(f"[Telegram] Error obteniendo updates: {e}")
            return {"ok": False, "result": []}

    def get_file_info(self, file_id: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información de un archivo para descargarlo

        Args:
            file_id: ID único del archivo en Telegram

        Returns:
            Diccionario con información del archivo:
            {
                "file_id": "...",
                "file_unique_id": "...",
                "file_size": 12345,
                "file_path": "documents/file_123.pdf"
            }
        """
        try:
            resp = requests.get(f"{self.api_url}/getFile", params={"file_id": file_id}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            return data.get("result") if data.get("ok") else None
        except Exception as e:
            print(f"[Telegram] Error obteniendo info del archivo {file_id}: {e}")
            return None

    def download_file(self, file_path: str) -> Optional[bytes]:
        """
        Descarga un archivo desde Telegram

        Args:
            file_path: Ruta del archivo obtenida de get_file_info()

        Returns:
            Contenido binario del archivo
        """
        try:
            url = f"{self.BASE_URL}/file/bot{self.bot_token}/{file_path}"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            print(f"[Telegram] Error descargando archivo desde {file_path}: {e}")
            return None

    def send_message(self, chat_id: int, text: str, parse_mode: str = "HTML") -> bool:
        """
        Envía mensaje de texto

        Args:
            chat_id: ID del chat/usuario
            text: Texto del mensaje (soporta HTML o Markdown según parse_mode)
            parse_mode: "HTML" o "Markdown" para formateo

        Returns:
            True si el mensaje fue enviado exitosamente
        """
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }

        try:
            resp = requests.post(f"{self.api_url}/sendMessage", json=payload, timeout=10)
            resp.raise_for_status()
            print(f"[Telegram] Mensaje enviado a chat {chat_id}")
            return True
        except Exception as e:
            print(f"[Telegram] Error enviando mensaje: {e}")
            return False

    def send_document(self, chat_id: int, file_path: str,
                      caption: Optional[str] = None) -> bool:
        """
        Envía un documento

        Args:
            chat_id: ID del chat/usuario
            file_path: Ruta local del archivo a enviar
            caption: Texto descriptivo opcional

        Returns:
            True si el documento fue enviado exitosamente
        """
        try:
            with open(file_path, 'rb') as f:
                files = {'document': f}
                data = {'chat_id': chat_id}
                if caption:
                    data['caption'] = caption

                resp = requests.post(
                    f"{self.api_url}/sendDocument",
                    data=data,
                    files=files,
                    timeout=60
                )
                resp.raise_for_status()
                print(f"[Telegram] Documento enviado a chat {chat_id}")
                return True
        except Exception as e:
            print(f"[Telegram] Error enviando documento: {e}")
            return False


# =============== FUNCIONES ESTÁNDAR (Interfaz compatible con Gmail/Outlook) ===============

def download_file_from_credentials(
    credentials_dict: Dict[str, Any],
    file_id: str
) -> Optional[bytes]:
    """
    Descarga un archivo de Telegram usando credenciales desde Firestore.

    Args:
        credentials_dict: Credenciales del usuario desde Firestore
                         {
                             "bot_token": "..."
                         }
        file_id: ID del archivo en Telegram

    Returns:
        Contenido binario del archivo, o None si falla la descarga
    """
    try:
        client = TelegramClient(credentials_dict)
        file_info = client.get_file_info(file_id)

        if not file_info or "file_path" not in file_info:
            print(f"[Telegram] No se pudo obtener info del archivo {file_id}")
            return None

        return client.download_file(file_info["file_path"])
    except Exception as e:
        print(f"[Telegram] Error descargando archivo {file_id}: {e}")
        return None


def authenticate_telegram(credentials_dict: Optional[Dict[str, Any]] = None) -> TelegramClient:
    """
    Inicializa cliente Telegram

    A diferencia de Gmail/Outlook que usan OAuth, Telegram usa un token fijo
    obtenido desde @BotFather.

    Args:
        credentials_dict: Credenciales desde Firestore (recomendado).
                          Debe contener: {"bot_token": "..."}
                          Si no se proporciona, usa variables de entorno (DEPRECATED).

    Returns:
        TelegramClient configurado

    Raises:
        ValueError: Si falta bot_token en credenciales o variables de entorno
    """
    return TelegramClient(credentials_dict)


def list_messages_telegram(client: TelegramClient, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Lista mensajes recientes usando polling

    NOTA: Para producción, se recomienda usar webhook en lugar de polling.
    Esta función es útil para desarrollo y testing.

    Args:
        client: Cliente Telegram autenticado
        limit: Máximo número de mensajes a devolver

    Returns:
        Lista de diccionarios con estructura:
        [
            {
                "id": "123",
                "chat_id": 987654321,
                "from": {"id": 111, "username": "user", "first_name": "John"},
                "text": "Hola, adjunto mi documento",
                "type": "text|document|photo|video|audio",
                "date": 1234567890
            }
        ]
    """
    updates = client.get_updates(limit=limit)
    messages = []

    if not updates.get("ok"):
        return messages

    for update in updates.get("result", []):
        if "message" not in update:
            continue

        msg = update["message"]
        msg_type = _get_message_type(msg)

        # Extraer texto según el tipo
        text = ""
        if "text" in msg:
            text = msg["text"]
        elif "caption" in msg:
            text = msg["caption"]

        messages.append({
            "id": str(msg["message_id"]),
            "chat_id": msg["chat"]["id"],
            "from": msg.get("from", {}),
            "text": text,
            "type": msg_type,
            "date": msg.get("date", 0)
        })

    return messages


def get_message_content(client: TelegramClient, message_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Obtiene contenido completo del mensaje + descarga adjuntos

    Esta función es llamada cuando el usuario selecciona un mensaje para procesar.
    Descarga archivos adjuntos a la carpeta 'attachments/'.

    Args:
        client: Cliente Telegram autenticado
        message_data: Diccionario con datos del mensaje (de list_messages o webhook)
                      Debe incluir al menos: {"id": "...", "chat_id": ..., ...}

    Returns:
        Diccionario con estructura:
        {
            "text": "Contenido del mensaje si es texto",
            "attachments": [
                {
                    "type": "document|photo|video|audio",
                    "path": "attachments/archivo.pdf",
                    "file_id": "BQACAgEAAxkB...",
                    "filename": "documento.pdf",
                    "file_size": 12345
                }
            ]
        }
    """
    out = {"text": "", "attachments": []}

    try:
        # Extraer texto
        if "text" in message_data:
            out["text"] = message_data["text"]
        elif "caption" in message_data:
            out["text"] = message_data["caption"]

        msg_type = _get_message_type(message_data)

        # Descargar documento
        if msg_type == "document" and "document" in message_data:
            doc = message_data["document"]
            file_id = doc["file_id"]
            filename = doc.get("file_name", f"document_{file_id}.pdf")
            file_size = doc.get("file_size", 0)

            file_info = client.get_file_info(file_id)
            if file_info and "file_path" in file_info:
                file_data = client.download_file(file_info["file_path"])
                if file_data:
                    os.makedirs("attachments", exist_ok=True)
                    save_path = f"attachments/{filename}"

                    with open(save_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "document",
                        "path": save_path,
                        "file_id": file_id,
                        "filename": filename,
                        "file_size": file_size
                    })
                    print(f"[Telegram] Documento descargado: {save_path}")

        # Descargar foto (usar la de mayor resolución)
        elif msg_type == "photo" and "photo" in message_data:
            photos = message_data["photo"]
            if isinstance(photos, list) and photos:
                # Telegram envía múltiples resoluciones, tomamos la última (mayor)
                best_photo = photos[-1]
                file_id = best_photo["file_id"]
                file_size = best_photo.get("file_size", 0)

                file_info = client.get_file_info(file_id)
                if file_info and "file_path" in file_info:
                    file_data = client.download_file(file_info["file_path"])
                    if file_data:
                        filename = f"photo_{file_id}.jpg"
                        os.makedirs("attachments", exist_ok=True)
                        save_path = f"attachments/{filename}"

                        with open(save_path, "wb") as f:
                            f.write(file_data)

                        out["attachments"].append({
                            "type": "photo",
                            "path": save_path,
                            "file_id": file_id,
                            "filename": filename,
                            "file_size": file_size
                        })
                        print(f"[Telegram] Foto descargada: {save_path}")

        # Descargar video
        elif msg_type == "video" and "video" in message_data:
            video = message_data["video"]
            file_id = video["file_id"]
            file_size = video.get("file_size", 0)

            file_info = client.get_file_info(file_id)
            if file_info and "file_path" in file_info:
                file_data = client.download_file(file_info["file_path"])
                if file_data:
                    filename = f"video_{file_id}.mp4"
                    os.makedirs("attachments", exist_ok=True)
                    save_path = f"attachments/{filename}"

                    with open(save_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "video",
                        "path": save_path,
                        "file_id": file_id,
                        "filename": filename,
                        "file_size": file_size
                    })
                    print(f"[Telegram] Video descargado: {save_path}")

        # Descargar audio
        elif msg_type == "audio" and "audio" in message_data:
            audio = message_data["audio"]
            file_id = audio["file_id"]
            file_size = audio.get("file_size", 0)
            filename = audio.get("file_name", f"audio_{file_id}.mp3")

            file_info = client.get_file_info(file_id)
            if file_info and "file_path" in file_info:
                file_data = client.download_file(file_info["file_path"])
                if file_data:
                    os.makedirs("attachments", exist_ok=True)
                    save_path = f"attachments/{filename}"

                    with open(save_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "audio",
                        "path": save_path,
                        "file_id": file_id,
                        "filename": filename,
                        "file_size": file_size
                    })
                    print(f"[Telegram] Audio descargado: {save_path}")

        return out

    except Exception as e:
        print(f"[Telegram] Error obteniendo contenido del mensaje: {e}")
        return out


def _get_message_type(msg: Dict[str, Any]) -> str:
    """
    Detecta el tipo de mensaje basado en los campos presentes

    Args:
        msg: Diccionario con datos del mensaje

    Returns:
        Tipo de mensaje: "text", "document", "photo", "video", "audio", "voice", "sticker", "other"
    """
    if "text" in msg:
        return "text"
    elif "document" in msg:
        return "document"
    elif "photo" in msg:
        return "photo"
    elif "video" in msg:
        return "video"
    elif "audio" in msg:
        return "audio"
    elif "voice" in msg:
        return "voice"
    elif "sticker" in msg:
        return "sticker"
    elif "location" in msg:
        return "location"
    elif "contact" in msg:
        return "contact"
    else:
        return "other"


if __name__ == "__main__":
    # Test de configuración y polling básico
    try:
        client = authenticate_telegram()
        print("✓ Cliente Telegram inicializado correctamente")
        print(f"  API URL: {client.api_url[:50]}...")

        # Obtener últimos mensajes
        print("\nObteniendo últimos mensajes...")
        messages = list_messages_telegram(client, limit=5)

        if messages:
            print(f"  Se encontraron {len(messages)} mensajes:")
            for msg in messages:
                print(f"  - [{msg['type']}] De: {msg['from'].get('username', 'N/A')}: {msg['text'][:50]}")
        else:
            print("  No hay mensajes nuevos (o el bot no ha recibido mensajes aún)")

    except Exception as e:
        print(f"✗ Error inicializando cliente: {e}")
        print("\nAsegúrate de configurar la variable de entorno:")
        print("  - TELEGRAM_BOT_TOKEN")
        print("\nPara obtener un token:")
        print("  1. Habla con @BotFather en Telegram")
        print("  2. Usa el comando /newbot")
        print("  3. Copia el token generado")
