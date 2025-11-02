"""
Gestión de mensajes de WhatsApp en Firestore
Guarda hasta 10 mensajes por usuario
"""

import re
from typing import Optional, Dict, Any, List
from datetime import datetime
from firebase_admin import firestore

from auth import get_db, get_user_document, get_users_collection

def _get_db():
    """Obtiene la instancia de Firestore con lazy initialization"""
    return get_db()


async def save_whatsapp_message(
    user_id: str,
    message_data: Dict[str, Any],
    max_messages: int = 10
) -> bool:
    """
    Guarda un mensaje de WhatsApp en Firestore para un usuario
    Mantiene solo los últimos max_messages mensajes

    Args:
        user_id: ID del usuario que RECIBE el mensaje
        message_data: Datos completos del mensaje del webhook
        max_messages: Número máximo de mensajes a mantener (default: 10)

    Returns:
        True si se guardó exitosamente

    Estructura del mensaje en Firestore:
    organization/{organizationId}/users/{user_id}/whatsapp_messages/{message_id}
    {
        "message_id": "wamid.xxx",
        "sender": {
            "phone": "+1234567890",
            "name": "John Doe"  # si está disponible
        },
        "timestamp": "2024-01-01T12:00:00Z",
        "received_at": (SERVER_TIMESTAMP),
        "type": "text|image|document|audio|video|sticker|location|contacts",
        "content": {
            "text": "contenido del mensaje",  # para mensajes de texto
            "caption": "descripción"  # para media con caption
        },
        "attachment": {  # si hay adjunto
            "type": "image|video|document|audio|sticker|location|contacts",
            "mime_type": "image/jpeg",
            "url": "https://...",  # URL para descargar
            "id": "media_id",
            "filename": "archivo.pdf",  # para documentos
            "latitude": -34.xxx,  # para ubicaciones
            "longitude": -58.xxx,
            ...
        },
        "raw_data": {...}  # datos completos del webhook para debug
    }
    """
    try:
        db = _get_db()
        if not db:
            print("[WhatsApp Messages] Error: Firestore no está inicializado")
            return False

        # Extraer información básica del mensaje
        msg_id = message_data.get("id")
        from_number = message_data.get("from")
        timestamp = message_data.get("timestamp")
        msg_type = message_data.get("type", "text")

        if not msg_id or not from_number:
            print("[WhatsApp Messages] Error: Mensaje sin id o from")
            return False

        # Referencia a la colección de mensajes del usuario dentro de la organización
        user_doc = get_user_document(user_id, db)
        if user_doc is None:
            print(f"[WhatsApp Messages] Error: Usuario {user_id} no encontrado en la organización configurada")
            return False
        messages_ref = user_doc.collection("whatsapp_messages")

        # Preparar datos básicos del mensaje
        message_doc = {
            "message_id": msg_id,
            "sender": {
                "phone": from_number,
                "name": message_data.get("profile", {}).get("name", "")  # nombre del contacto si está disponible
            },
            "timestamp": timestamp,
            "received_at": firestore.SERVER_TIMESTAMP,
            "type": msg_type,
            "raw_data": message_data
        }

        # Inicializar content y attachment
        content = {}
        attachment = None

        # Extraer contenido según el tipo de mensaje
        if msg_type == "text" and "text" in message_data:
            content["text"] = message_data["text"].get("body", "")

        elif msg_type == "image" and "image" in message_data:
            image_data = message_data["image"]
            attachment = {
                "type": "image",
                "mime_type": image_data.get("mime_type", "image/jpeg"),
                "id": image_data.get("id"),
                "sha256": image_data.get("sha256")
            }
            # Caption opcional
            if image_data.get("caption"):
                content["caption"] = image_data["caption"]

        elif msg_type == "video" and "video" in message_data:
            video_data = message_data["video"]
            attachment = {
                "type": "video",
                "mime_type": video_data.get("mime_type", "video/mp4"),
                "id": video_data.get("id"),
                "sha256": video_data.get("sha256")
            }
            if video_data.get("caption"):
                content["caption"] = video_data["caption"]

        elif msg_type == "document" and "document" in message_data:
            doc_data = message_data["document"]
            attachment = {
                "type": "document",
                "mime_type": doc_data.get("mime_type", "application/octet-stream"),
                "id": doc_data.get("id"),
                "filename": doc_data.get("filename", "documento"),
                "sha256": doc_data.get("sha256")
            }
            if doc_data.get("caption"):
                content["caption"] = doc_data["caption"]

        elif msg_type == "audio" and "audio" in message_data:
            audio_data = message_data["audio"]
            attachment = {
                "type": "audio",
                "mime_type": audio_data.get("mime_type", "audio/ogg"),
                "id": audio_data.get("id"),
                "voice": audio_data.get("voice", False),  # True si es nota de voz
                "sha256": audio_data.get("sha256")
            }

        elif msg_type == "sticker" and "sticker" in message_data:
            sticker_data = message_data["sticker"]
            attachment = {
                "type": "sticker",
                "mime_type": sticker_data.get("mime_type", "image/webp"),
                "id": sticker_data.get("id"),
                "animated": sticker_data.get("animated", False),
                "sha256": sticker_data.get("sha256")
            }

        elif msg_type == "location" and "location" in message_data:
            loc_data = message_data["location"]
            attachment = {
                "type": "location",
                "latitude": loc_data.get("latitude"),
                "longitude": loc_data.get("longitude"),
                "name": loc_data.get("name", ""),
                "address": loc_data.get("address", "")
            }

        elif msg_type == "contacts" and "contacts" in message_data:
            contacts_data = message_data["contacts"]
            attachment = {
                "type": "contacts",
                "contacts": contacts_data  # Lista de contactos
            }

        # Agregar content y attachment al documento si existen
        if content:
            message_doc["content"] = content
        if attachment:
            message_doc["attachment"] = attachment

        # Guardar el mensaje
        messages_ref.document(msg_id).set(message_doc)
        print(f"[WhatsApp Messages] Mensaje guardado para usuario {user_id}: {msg_id} (tipo: {msg_type}, remitente: {from_number})")

        # Mantener solo los últimos max_messages
        await cleanup_old_messages(user_id, max_messages)

        return True

    except Exception as e:
        print(f"[WhatsApp Messages] Error guardando mensaje: {e}")
        import traceback
        traceback.print_exc()
        return False


async def cleanup_old_messages(user_id: str, max_messages: int = 10):
    """
    Elimina mensajes antiguos manteniendo solo los últimos max_messages

    Args:
        user_id: ID del usuario
        max_messages: Número máximo de mensajes a mantener
    """
    try:
        db = _get_db()
        if not db:
            return

        user_doc = get_user_document(user_id, db)
        if user_doc is None:
            return
        messages_ref = user_doc.collection("whatsapp_messages")

        # Obtener todos los mensajes ordenados por timestamp descendente
        docs = messages_ref.order_by("timestamp", direction=firestore.Query.DESCENDING).stream()

        # Convertir a lista para poder contar
        all_docs = list(docs)

        # Si hay más de max_messages, eliminar los más antiguos
        if len(all_docs) > max_messages:
            docs_to_delete = all_docs[max_messages:]
            for doc in docs_to_delete:
                doc.reference.delete()
                print(f"[WhatsApp Messages] Mensaje antiguo eliminado: {doc.id}")

    except Exception as e:
        print(f"[WhatsApp Messages] Error limpiando mensajes antiguos: {e}")


async def get_whatsapp_messages(
    user_id: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Obtiene los últimos mensajes de WhatsApp de un usuario desde Firestore

    Args:
        user_id: ID del usuario que recibe los mensajes
        limit: Número máximo de mensajes a devolver

    Returns:
        Lista de mensajes con estructura completa:
        [
            {
                "id": "wamid.xxx",
                "sender": {
                    "phone": "+1234567890",
                    "name": "John Doe"
                },
                "timestamp": "1234567890",
                "received_at": "2024-01-01T12:00:00Z",
                "type": "text|image|video|document|audio|sticker|location|contacts",
                "content": {
                    "text": "mensaje de texto",
                    "caption": "descripción del adjunto"
                },
                "attachment": {
                    "type": "image",
                    "mime_type": "image/jpeg",
                    "id": "media_id",
                    ...
                }
            }
        ]
    """
    try:
        db = _get_db()
        if not db:
            print("[WhatsApp Messages] Error: Firestore no está inicializado")
            return []

        user_doc = get_user_document(user_id, db)
        if user_doc is None:
            return []
        messages_ref = user_doc.collection("whatsapp_messages")
        
        # Obtener mensajes ordenados por timestamp descendente
        docs = messages_ref.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()

        messages = []
        for doc in docs:
            data = doc.to_dict()

            # Formato estructurado completo
            message = {
                "id": data.get("message_id"),
                "sender": data.get("sender", {
                    "phone": data.get("from", ""),  # fallback para mensajes antiguos
                    "name": ""
                }),
                "timestamp": data.get("timestamp"),
                "received_at": data.get("received_at"),
                "type": data.get("type", "text")
            }

            # Agregar content si existe
            if "content" in data:
                message["content"] = data["content"]
            else:
                # Fallback para mensajes antiguos que solo tienen "text"
                if data.get("text"):
                    message["content"] = {"text": data["text"]}

            # Agregar attachment si existe
            if "attachment" in data:
                message["attachment"] = data["attachment"]
            elif "media" in data:
                # Fallback para mensajes antiguos que solo tienen "media"
                message["attachment"] = data["media"]

            messages.append(message)

        return messages

    except Exception as e:
        print(f"[WhatsApp Messages] Error obteniendo mensajes: {e}")
        import traceback
        traceback.print_exc()
        return []


async def find_user_by_whatsapp_number(phone_number: str) -> Optional[str]:
    """
    Busca el user_id que tiene conectado un número de WhatsApp Business específico.

    Este método busca el usuario que configuró el número de WhatsApp Business
    (el número que RECIBE los mensajes), no el remitente.

    Args:
        phone_number: Número de WhatsApp Business (puede incluir código de país).
                     Ejemplos: "1234567890", "+1234567890", "521234567890"

    Returns:
        user_id si se encuentra, None en caso contrario
    """
    try:
        db = _get_db()
        if not db:
            return None

        # Normalizar el número (remover espacios, guiones, signos +)
        normalized_input = re.sub(r'[^\d]', '', phone_number)

        # Buscar en todas las credenciales de WhatsApp
        users_ref = get_users_collection(db)
        if users_ref is None:
            return None
        users = users_ref.stream()

        for user_doc in users:
            user_id = user_doc.id

            # Verificar si tiene credenciales de WhatsApp
            whatsapp_cred_ref = user_doc.reference.collection("external_credentials").document("whatsapp")
            whatsapp_cred = whatsapp_cred_ref.get()

            if whatsapp_cred.exists:
                cred_data = whatsapp_cred.to_dict()
                metadata = cred_data.get("metadata", {})

                # Obtener el número almacenado y normalizarlo
                stored_number = metadata.get("phone_number", "")
                normalized_stored = re.sub(r'[^\d]', '', stored_number)

                # Comparar números normalizados
                # También intentar comparar sin el código de país inicial
                if normalized_stored == normalized_input:
                    return user_id

                # Intentar match sin código de país (últimos 10 dígitos)
                if len(normalized_stored) >= 10 and len(normalized_input) >= 10:
                    if normalized_stored[-10:] == normalized_input[-10:]:
                        return user_id

        print(f"[WhatsApp Messages] No se encontró usuario con número: {phone_number}")
        return None

    except Exception as e:
        print(f"[WhatsApp Messages] Error buscando usuario por número: {e}")
        import traceback
        traceback.print_exc()
        return None
