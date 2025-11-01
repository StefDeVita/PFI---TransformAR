"""
Servicio para gestionar credenciales externas de usuarios
Soporta: Gmail, Outlook, WhatsApp, Telegram
"""

from typing import Optional, Dict, Any, Literal
from datetime import datetime
import firebase_admin
from firebase_admin import firestore
from pydantic import BaseModel
import os


# No inicializar al importar, usar lazy initialization
def _get_db():
    """Obtiene la instancia de Firestore con lazy initialization"""
    from auth import get_db
    return get_db()


ServiceType = Literal["gmail", "outlook", "whatsapp", "telegram"]


class ExternalCredential(BaseModel):
    """Modelo de credencial externa"""
    service: ServiceType
    connected_at: datetime
    credentials: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = {}


class ExternalCredentialsManager:
    """Gestión de credenciales externas en Firestore"""

    COLLECTION = "external_credentials"

    @staticmethod
    def _get_credential_ref(user_id: str, service: ServiceType):
        """Obtiene referencia al documento de credencial"""
        return _get_db().collection("users").document(user_id).collection(
            ExternalCredentialsManager.COLLECTION
        ).document(service)

    @staticmethod
    async def save_credential(
        user_id: str,
        service: ServiceType,
        credentials: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Guarda o actualiza credencial de un servicio

        Args:
            user_id: ID del usuario
            service: Servicio (gmail, outlook, whatsapp, telegram)
            credentials: Datos de autenticación (tokens, etc.)
            metadata: Información adicional (email, username, etc.)

        Returns:
            True si se guardó exitosamente
        """
        try:
            ref = ExternalCredentialsManager._get_credential_ref(user_id, service)

            data = {
                "service": service,
                "connected_at": firestore.SERVER_TIMESTAMP,
                "credentials": credentials,
                "metadata": metadata or {},
                "updated_at": firestore.SERVER_TIMESTAMP
            }

            ref.set(data)
            print(f"[ExternalCredentials] Credencial {service} guardada para usuario {user_id}")
            return True

        except Exception as e:
            print(f"[ExternalCredentials] Error guardando credencial {service}: {e}")
            return False

    @staticmethod
    async def get_credential(
        user_id: str,
        service: ServiceType
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene credencial de un servicio

        Args:
            user_id: ID del usuario
            service: Servicio a obtener

        Returns:
            Diccionario con credenciales o None si no existe
        """
        try:
            ref = ExternalCredentialsManager._get_credential_ref(user_id, service)
            doc = ref.get()

            if doc.exists:
                data = doc.to_dict()
                # Devolver solo el campo 'credentials', no todo el documento
                return data.get("credentials")
            return None

        except Exception as e:
            print(f"[ExternalCredentials] Error obteniendo credencial {service}: {e}")
            return None

    @staticmethod
    async def delete_credential(
        user_id: str,
        service: ServiceType
    ) -> bool:
        """
        Elimina credencial de un servicio

        Args:
            user_id: ID del usuario
            service: Servicio a desconectar

        Returns:
            True si se eliminó exitosamente
        """
        try:
            ref = ExternalCredentialsManager._get_credential_ref(user_id, service)
            ref.delete()
            print(f"[ExternalCredentials] Credencial {service} eliminada para usuario {user_id}")
            return True

        except Exception as e:
            print(f"[ExternalCredentials] Error eliminando credencial {service}: {e}")
            return False

    @staticmethod
    async def list_credentials(user_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Lista todas las credenciales conectadas de un usuario

        Args:
            user_id: ID del usuario

        Returns:
            Diccionario con {service: credential_data}
        """
        try:
            collection_ref = _get_db().collection("users").document(user_id).collection(
                ExternalCredentialsManager.COLLECTION
            )

            docs = collection_ref.stream()

            credentials = {}
            for doc in docs:
                data = doc.to_dict()
                # No incluir las credenciales completas, solo metadata
                credentials[doc.id] = {
                    "service": data.get("service"),
                    "connected_at": data.get("connected_at"),
                    "metadata": data.get("metadata", {})
                }

            return credentials

        except Exception as e:
            print(f"[ExternalCredentials] Error listando credenciales: {e}")
            return {}

    @staticmethod
    async def is_service_connected(user_id: str, service: ServiceType) -> bool:
        """
        Verifica si un servicio está conectado

        Args:
            user_id: ID del usuario
            service: Servicio a verificar

        Returns:
            True si está conectado
        """
        credential = await ExternalCredentialsManager.get_credential(user_id, service)
        return credential is not None


# ============ Funciones de ayuda específicas por servicio ============

class GmailCredentials(BaseModel):
    """Credenciales de Gmail OAuth"""
    token: str
    refresh_token: Optional[str] = None
    token_uri: str
    client_id: str
    client_secret: str
    scopes: list


class OutlookCredentials(BaseModel):
    """Credenciales de Outlook OAuth"""
    access_token: str
    refresh_token: Optional[str] = None
    expires_at: Optional[int] = None


class WhatsAppCredentials(BaseModel):
    """Credenciales de WhatsApp Business API"""
    phone_number_id: str
    access_token: str
    business_account_id: Optional[str] = None


class TelegramCredentials(BaseModel):
    """Credenciales de Telegram Bot"""
    bot_token: str
    bot_username: Optional[str] = None


async def save_gmail_credentials(
    user_id: str,
    token_data: Dict[str, Any],
    email: str
) -> bool:
    """Guarda credenciales de Gmail"""
    credentials = {
        "token": token_data.get("token"),
        "refresh_token": token_data.get("refresh_token"),
        "token_uri": token_data.get("token_uri"),
        "client_id": token_data.get("client_id"),
        "client_secret": token_data.get("client_secret"),
        "scopes": token_data.get("scopes", [])
    }

    metadata = {
        "email": email,
        "provider": "gmail"
    }

    return await ExternalCredentialsManager.save_credential(
        user_id, "gmail", credentials, metadata
    )


async def save_outlook_credentials(
    user_id: str,
    access_token: str,
    refresh_token: Optional[str],
    expires_at: Optional[int],
    email: str
) -> bool:
    """Guarda credenciales de Outlook"""
    credentials = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at
    }

    metadata = {
        "email": email,
        "provider": "outlook"
    }

    return await ExternalCredentialsManager.save_credential(
        user_id, "outlook", credentials, metadata
    )


async def save_whatsapp_credentials(
    user_id: str,
    phone_number_id: str,
    access_token: str,
    business_account_id: Optional[str] = None,
    phone_number: Optional[str] = None
) -> bool:
    """Guarda credenciales de WhatsApp"""
    credentials = {
        "phone_number_id": phone_number_id,
        "access_token": access_token,
        "business_account_id": business_account_id
    }

    metadata = {
        "phone_number": phone_number,
        "provider": "whatsapp"
    }

    return await ExternalCredentialsManager.save_credential(
        user_id, "whatsapp", credentials, metadata
    )


async def save_telegram_credentials(
    user_id: str,
    bot_token: str,
    bot_username: Optional[str] = None,
    bot_name: Optional[str] = None
) -> bool:
    """Guarda credenciales de Telegram"""
    credentials = {
        "bot_token": bot_token,
        "bot_username": bot_username
    }

    metadata = {
        "bot_name": bot_name,
        "bot_username": bot_username,
        "provider": "telegram"
    }

    return await ExternalCredentialsManager.save_credential(
        user_id, "telegram", credentials, metadata
    )


if __name__ == "__main__":
    # Test de funciones
    import asyncio

    async def test():
        test_user_id = "test_user_123"

        # Probar guardar credencial de Telegram
        print("Guardando credencial de Telegram...")
        await save_telegram_credentials(
            test_user_id,
            "123456:ABC-DEF",
            "my_test_bot"
        )

        # Listar credenciales
        print("\nCredenciales del usuario:")
        creds = await ExternalCredentialsManager.list_credentials(test_user_id)
        for service, data in creds.items():
            print(f"  - {service}: {data}")

        # Verificar conexión
        print(f"\n¿Telegram conectado? {await ExternalCredentialsManager.is_service_connected(test_user_id, 'telegram')}")

        # Eliminar credencial
        print("\nEliminando credencial...")
        await ExternalCredentialsManager.delete_credential(test_user_id, "telegram")

        print(f"¿Telegram conectado? {await ExternalCredentialsManager.is_service_connected(test_user_id, 'telegram')}")

    asyncio.run(test())
