# input/whatsapp_reader.py
"""
Integración con WhatsApp Business Cloud API
Sigue el mismo patrón que gmail_reader.py y outlook_reader.py
"""

import os
import hashlib
import hmac
import requests
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

load_dotenv()


class WhatsAppClient:
    """Cliente para WhatsApp Business Cloud API"""

    BASE_URL = "https://graph.facebook.com/v18.0"

    def __init__(self):
        self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        self.access_token = os.getenv("WHATSAPP_ACCESS_TOKEN")
        self.webhook_token = os.getenv("WHATSAPP_WEBHOOK_TOKEN", "default_webhook_token")

        if not self.phone_number_id or not self.access_token:
            raise ValueError(
                "Faltan variables de entorno: WHATSAPP_PHONE_NUMBER_ID y WHATSAPP_ACCESS_TOKEN son requeridas"
            )

    def _headers(self) -> Dict[str, str]:
        """Headers estándar para requests"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def get_media_url(self, media_id: str) -> Optional[str]:
        """Obtiene URL temporal de descarga de un archivo multimedia"""
        url = f"{self.BASE_URL}/{media_id}"

        try:
            resp = requests.get(url, headers=self._headers(), timeout=10)
            resp.raise_for_status()
            data = resp.json()
            return data.get("url")
        except Exception as e:
            print(f"[WhatsApp] Error obteniendo URL de media {media_id}: {e}")
            return None

    def download_media(self, media_id: str) -> Optional[bytes]:
        """Descarga archivo multimedia desde WhatsApp"""
        media_url = self.get_media_url(media_id)
        if not media_url:
            return None

        try:
            # El download requiere el mismo token de autorización
            resp = requests.get(media_url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            print(f"[WhatsApp] Error descargando media desde {media_url}: {e}")
            return None

    def send_text_message(self, to_phone: str, message: str) -> bool:
        """
        Envía mensaje de texto a un número de WhatsApp

        Args:
            to_phone: Número con código de país (ej: "5491112345678")
            message: Texto del mensaje
        """
        url = f"{self.BASE_URL}/{self.phone_number_id}/messages"

        payload = {
            "messaging_product": "whatsapp",
            "to": to_phone,
            "type": "text",
            "text": {"body": message}
        }

        try:
            resp = requests.post(url, headers=self._headers(), json=payload, timeout=10)
            resp.raise_for_status()
            print(f"[WhatsApp] Mensaje enviado a {to_phone}")
            return True
        except Exception as e:
            print(f"[WhatsApp] Error enviando mensaje: {e}")
            return False

    def validate_webhook_signature(self, body: bytes, signature: str) -> bool:
        """
        Valida la firma HMAC-SHA256 del webhook para seguridad

        Args:
            body: Cuerpo raw del request
            signature: Valor del header X-Hub-Signature-256
        """
        if not signature.startswith("sha256="):
            return False

        expected_signature = "sha256=" + hmac.new(
            self.webhook_token.encode(),
            body,
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected_signature)


# =============== FUNCIONES ESTÁNDAR (Interfaz compatible con Gmail/Outlook) ===============

def authenticate_whatsapp() -> WhatsAppClient:
    """
    Inicializa cliente WhatsApp

    A diferencia de Gmail/Outlook que usan OAuth, WhatsApp usa un token de larga duración
    obtenido desde Meta for Developers.

    Returns:
        WhatsAppClient configurado

    Raises:
        ValueError: Si faltan variables de entorno requeridas
    """
    return WhatsAppClient()


def list_messages_whatsapp(client: WhatsAppClient, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Lista mensajes recibidos

    NOTA: WhatsApp no provee un endpoint para listar mensajes históricos.
    Los mensajes llegan exclusivamente por webhook y deben ser almacenados
    en una base de datos por la aplicación.

    Esta función devuelve una lista vacía como placeholder.
    En producción, deberías implementar:
    1. Webhook endpoint que capture mensajes entrantes
    2. Almacenarlos en Firestore/DB
    3. Esta función consulta esa DB

    Args:
        client: Cliente WhatsApp autenticado
        limit: Máximo número de mensajes a devolver

    Returns:
        Lista de diccionarios con estructura:
        [
            {
                "id": "wamid.xxx",
                "from": "+5491112345678",
                "timestamp": "2024-01-15T10:30:00Z",
                "text": "Hola, adjunto mi documento",
                "type": "text|document|image|video|audio"
            }
        ]
    """
    # TODO: Implementar consulta a base de datos donde se guardan mensajes del webhook
    print("[WhatsApp] list_messages: Esta función requiere implementar webhook + DB")
    return []


def get_message_content(client: WhatsAppClient, message_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Obtiene contenido completo del mensaje + descarga adjuntos

    Esta función es llamada cuando el usuario selecciona un mensaje para procesar.
    Descarga archivos adjuntos a la carpeta 'attachments/'.

    Args:
        client: Cliente WhatsApp autenticado
        message_data: Diccionario con datos del mensaje (del webhook)
                      Debe incluir al menos: {"type": "...", ...}

    Returns:
        Diccionario con estructura:
        {
            "text": "Contenido del mensaje si es texto",
            "attachments": [
                {
                    "type": "document|image|video|audio",
                    "path": "attachments/archivo.pdf",
                    "media_id": "123456",
                    "filename": "documento.pdf"
                }
            ]
        }
    """
    out = {"text": "", "attachments": []}

    try:
        msg_type = message_data.get("type", "")

        # Mensaje de texto
        if msg_type == "text":
            text_body = message_data.get("text", {})
            out["text"] = text_body.get("body", "") if isinstance(text_body, dict) else str(text_body)

        # Documento (PDF, Word, etc.)
        elif msg_type == "document":
            doc_data = message_data.get("document", {})
            media_id = doc_data.get("id")
            filename = doc_data.get("filename", f"document_{media_id}.pdf")

            if media_id:
                file_data = client.download_media(media_id)
                if file_data:
                    os.makedirs("attachments", exist_ok=True)
                    file_path = f"attachments/{filename}"

                    with open(file_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "document",
                        "path": file_path,
                        "media_id": media_id,
                        "filename": filename
                    })
                    print(f"[WhatsApp] Documento descargado: {file_path}")

        # Imagen
        elif msg_type == "image":
            img_data = message_data.get("image", {})
            media_id = img_data.get("id")
            caption = img_data.get("caption", "")

            if media_id:
                file_data = client.download_media(media_id)
                if file_data:
                    filename = f"image_{media_id}.jpg"
                    os.makedirs("attachments", exist_ok=True)
                    file_path = f"attachments/{filename}"

                    with open(file_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "image",
                        "path": file_path,
                        "media_id": media_id,
                        "filename": filename
                    })
                    out["text"] = caption  # Caption como texto
                    print(f"[WhatsApp] Imagen descargada: {file_path}")

        # Video
        elif msg_type == "video":
            video_data = message_data.get("video", {})
            media_id = video_data.get("id")

            if media_id:
                file_data = client.download_media(media_id)
                if file_data:
                    filename = f"video_{media_id}.mp4"
                    os.makedirs("attachments", exist_ok=True)
                    file_path = f"attachments/{filename}"

                    with open(file_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "video",
                        "path": file_path,
                        "media_id": media_id,
                        "filename": filename
                    })
                    print(f"[WhatsApp] Video descargado: {file_path}")

        # Audio
        elif msg_type == "audio":
            audio_data = message_data.get("audio", {})
            media_id = audio_data.get("id")

            if media_id:
                file_data = client.download_media(media_id)
                if file_data:
                    filename = f"audio_{media_id}.ogg"
                    os.makedirs("attachments", exist_ok=True)
                    file_path = f"attachments/{filename}"

                    with open(file_path, "wb") as f:
                        f.write(file_data)

                    out["attachments"].append({
                        "type": "audio",
                        "path": file_path,
                        "media_id": media_id,
                        "filename": filename
                    })
                    print(f"[WhatsApp] Audio descargado: {file_path}")

        return out

    except Exception as e:
        print(f"[WhatsApp] Error obteniendo contenido del mensaje: {e}")
        return out


if __name__ == "__main__":
    # Test de configuración
    try:
        client = authenticate_whatsapp()
        print("✓ Cliente WhatsApp inicializado correctamente")
        print(f"  Phone Number ID: {client.phone_number_id}")
        print(f"  Access Token: {client.access_token[:20]}...")
    except Exception as e:
        print(f"✗ Error inicializando cliente: {e}")
        print("\nAsegúrate de configurar las variables de entorno:")
        print("  - WHATSAPP_PHONE_NUMBER_ID")
        print("  - WHATSAPP_ACCESS_TOKEN")
