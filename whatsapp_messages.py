"""
Gestión de mensajes de WhatsApp en Firestore
Guarda hasta 10 mensajes por usuario
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from firebase_admin import firestore


def _get_db():
    """Obtiene la instancia de Firestore con lazy initialization"""
    from auth import get_db
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
        user_id: ID del usuario
        message_data: Datos completos del mensaje del webhook
        max_messages: Número máximo de mensajes a mantener (default: 10)

    Returns:
        True si se guardó exitosamente

    Estructura del mensaje en Firestore:
    users/{user_id}/whatsapp_messages/{message_id}
    {
        "message_id": "wamid.xxx",
        "from": "+1234567890",
        "timestamp": "2024-01-01T12:00:00Z",
        "type": "text|image|document|audio|video",
        "text": "contenido del mensaje",
        "media": {...},  # si aplica
        "raw_data": {...}  # datos completos del webhook
    }
    """
    try:
        db = _get_db()
        if not db:
            print("[WhatsApp Messages] Error: Firestore no está inicializado")
            return False

        # Extraer información del mensaje
        msg_id = message_data.get("id")
        from_number = message_data.get("from")
        timestamp = message_data.get("timestamp")
        msg_type = message_data.get("type", "text")

        if not msg_id or not from_number:
            print("[WhatsApp Messages] Error: Mensaje sin id o from")
            return False

        # Referencia a la colección de mensajes del usuario
        messages_ref = db.collection("users").document(user_id).collection("whatsapp_messages")

        # Preparar datos del mensaje
        message_doc = {
            "message_id": msg_id,
            "from": from_number,
            "timestamp": timestamp,
            "received_at": firestore.SERVER_TIMESTAMP,
            "type": msg_type,
            "raw_data": message_data
        }

        # Extraer texto si existe
        if msg_type == "text" and "text" in message_data:
            message_doc["text"] = message_data["text"].get("body", "")

        # Extraer información de media si existe
        if msg_type in ["image", "document", "audio", "video"] and msg_type in message_data:
            message_doc["media"] = message_data[msg_type]

        # Guardar el mensaje
        messages_ref.document(msg_id).set(message_doc)
        print(f"[WhatsApp Messages] Mensaje guardado para usuario {user_id}: {msg_id}")

        # Mantener solo los últimos max_messages
        await cleanup_old_messages(user_id, max_messages)

        return True

    except Exception as e:
        print(f"[WhatsApp Messages] Error guardando mensaje: {e}")
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

        messages_ref = db.collection("users").document(user_id).collection("whatsapp_messages")

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
        user_id: ID del usuario
        limit: Número máximo de mensajes a devolver

    Returns:
        Lista de mensajes en formato compatible con la API
    """
    try:
        db = _get_db()
        if not db:
            print("[WhatsApp Messages] Error: Firestore no está inicializado")
            return []

        messages_ref = db.collection("users").document(user_id).collection("whatsapp_messages")

        # Obtener mensajes ordenados por timestamp descendente
        docs = messages_ref.order_by("timestamp", direction=firestore.Query.DESCENDING).limit(limit).stream()

        messages = []
        for doc in docs:
            data = doc.to_dict()

            # Formato compatible con la API
            message = {
                "id": data.get("message_id"),
                "from": data.get("from"),
                "timestamp": data.get("timestamp"),
                "type": data.get("type", "text"),
                "text": data.get("text", ""),
                "raw_data": data.get("raw_data", {})
            }

            if "media" in data:
                message["media"] = data["media"]

            messages.append(message)

        return messages

    except Exception as e:
        print(f"[WhatsApp Messages] Error obteniendo mensajes: {e}")
        return []


async def find_user_by_whatsapp_number(phone_number: str) -> Optional[str]:
    """
    Busca el user_id que tiene conectado un número de WhatsApp específico

    Args:
        phone_number: Número de teléfono de WhatsApp

    Returns:
        user_id si se encuentra, None en caso contrario
    """
    try:
        db = _get_db()
        if not db:
            return None

        # Buscar en todas las credenciales de WhatsApp
        users_ref = db.collection("users")
        users = users_ref.stream()

        for user_doc in users:
            user_id = user_doc.id

            # Verificar si tiene credenciales de WhatsApp
            whatsapp_cred_ref = user_doc.reference.collection("external_credentials").document("whatsapp")
            whatsapp_cred = whatsapp_cred_ref.get()

            if whatsapp_cred.exists:
                cred_data = whatsapp_cred.to_dict()
                metadata = cred_data.get("metadata", {})

                # Verificar si el número coincide
                if metadata.get("phone_number") == phone_number:
                    return user_id

        return None

    except Exception as e:
        print(f"[WhatsApp Messages] Error buscando usuario por número: {e}")
        return None
