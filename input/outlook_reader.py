# input/outlook_reader.py
import base64
import os
import requests
import msal
import time
from dotenv import load_dotenv
from typing import Any, Dict, List
load_dotenv()
CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID", os.getenv('OUTLOOK_CLIENT_ID'))
AUTHORITY = "https://login.microsoftonline.com/common"
SCOPES = ["Mail.Read","User.Read"]

TOKEN_CACHE = "outlook_token.json"  # DEPRECATED: Solo para compatibilidad


# ========== NUEVAS FUNCIONES: USO CON CREDENCIALES DE FIRESTORE ==========

def refresh_outlook_token(credentials_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Refresca el access_token de Outlook usando el refresh_token.

    Args:
        credentials_dict: Credenciales actuales del usuario desde Firestore
                         {
                             "access_token": "...",
                             "refresh_token": "...",
                             "expires_at": timestamp
                         }

    Returns:
        Nuevo diccionario de credenciales con access_token actualizado
        {
            "access_token": "nuevo_token",
            "refresh_token": "nuevo_refresh_token",  # puede cambiar
            "expires_at": nuevo_timestamp
        }

    Raises:
        ValueError: Si no hay refresh_token o falla el refresh
    """
    refresh_token = credentials_dict.get("refresh_token")

    if not refresh_token:
        raise ValueError("No hay refresh_token disponible. Por favor reconecta Outlook.")

    client_id = os.getenv("OUTLOOK_CLIENT_ID")
    client_secret = os.getenv("OUTLOOK_CLIENT_SECRET")  # puede ser None para clientes públicos

    if not client_id:
        raise ValueError("OUTLOOK_CLIENT_ID no está configurado")

    # Preparar datos para el refresh
    token_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"

    data = {
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "scope": " ".join(SCOPES)
    }

    # Agregar client_secret solo si está disponible (cliente confidencial)
    if client_secret:
        data["client_secret"] = client_secret

    try:
        resp = requests.post(token_url, data=data)
        resp.raise_for_status()
        token_data = resp.json()

        # Calcular expires_at
        expires_in = token_data.get("expires_in", 3600)
        expires_at = time.time() + expires_in

        # Preparar nuevas credenciales
        new_credentials = {
            "access_token": token_data["access_token"],
            "refresh_token": token_data.get("refresh_token", refresh_token),  # a veces no cambia
            "expires_at": expires_at,
            "token_type": token_data.get("token_type", "Bearer")
        }

        print(f"[Outlook] Token refrescado exitosamente. Expira en {expires_in} segundos")
        return new_credentials

    except requests.exceptions.HTTPError as e:
        error_msg = f"Error refrescando token de Outlook: {e}"
        if e.response is not None:
            error_details = e.response.json() if e.response.content else {}
            error_msg += f" - {error_details}"
        print(f"[Outlook] {error_msg}")
        raise ValueError(error_msg)


def _ensure_valid_token(credentials_dict: Dict[str, Any], user_id: str = None) -> Dict[str, Any]:
    """
    Asegura que el token sea válido, refrescándolo si es necesario.

    Args:
        credentials_dict: Credenciales actuales
        user_id: ID del usuario (opcional, para actualizar Firestore)

    Returns:
        Credenciales con token válido (puede ser el mismo o uno nuevo)
    """
    expires_at = credentials_dict.get("expires_at")

    # Verificar si el token está expirado o por expirar (margen de 5 minutos)
    if expires_at and time.time() > (expires_at - 300):
        print("[Outlook] Token expirado o por expirar, refrescando...")

        try:
            new_credentials = refresh_outlook_token(credentials_dict)

            # Si se proporcionó user_id, actualizar en Firestore
            if user_id:
                from external_credentials import ExternalCredentialsManager
                manager = ExternalCredentialsManager()
                # Obtener credenciales actuales para preservar metadata
                current_cred = manager.get_credential(user_id, "outlook")
                if current_cred:
                    # Actualizar solo los campos del token
                    import asyncio
                    asyncio.create_task(
                        manager.save_credential(user_id, "outlook", new_credentials)
                    )
                    print(f"[Outlook] Token refrescado y guardado en Firestore para usuario {user_id}")

            return new_credentials

        except Exception as e:
            print(f"[Outlook] Error refrescando token: {e}")
            raise ValueError(f"El token de Outlook ha expirado y no se pudo refrescar: {e}")

    return credentials_dict


def list_messages_outlook_from_credentials(credentials_dict: Dict[str, Any], top: int = 10, user_id: str = None) -> List[Dict[str, Any]]:
    """
    Lista mensajes de Outlook usando credenciales desde Firestore.

    Refresca automáticamente el token si está expirado o por expirar.

    Args:
        credentials_dict: Credenciales del usuario desde Firestore
                         {
                             "access_token": "...",
                             "refresh_token": "...",
                             "expires_at": timestamp
                         }
        top: Número máximo de mensajes a devolver
        user_id: ID del usuario (opcional, para guardar token refrescado en Firestore)

    Returns:
        Lista de mensajes
    """
    # Asegurar que el token es válido (refresca automáticamente si es necesario)
    credentials_dict = _ensure_valid_token(credentials_dict, user_id)

    access_token = credentials_dict.get("access_token")
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://graph.microsoft.com/v1.0/me/messages?$top={top}&$select=sender,subject,receivedDateTime,hasAttachments"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value", [])


def get_message_body_from_credentials(credentials_dict: Dict[str, Any], msg_id: str, user_id: str = None) -> str:
    """
    Obtiene el cuerpo de un mensaje usando credenciales desde Firestore.

    Refresca automáticamente el token si está expirado o por expirar.

    Args:
        credentials_dict: Credenciales del usuario desde Firestore
        msg_id: ID del mensaje
        user_id: ID del usuario (opcional, para guardar token refrescado en Firestore)

    Returns:
        Contenido del mensaje (HTML o texto)
    """
    # Asegurar que el token es válido (refresca automáticamente si es necesario)
    credentials_dict = _ensure_valid_token(credentials_dict, user_id)

    access_token = credentials_dict.get("access_token")
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}?$select=body"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    body = resp.json().get("body", {}).get("content", "")
    return body


def get_attachments_from_credentials(
    credentials_dict: Dict[str, Any],
    msg_id: str,
    outdir="attachments",
    user_id: str = None
) -> List[str]:
    """
    Descarga adjuntos de un mensaje usando credenciales desde Firestore.

    Refresca automáticamente el token si está expirado o por expirar.

    Args:
        credentials_dict: Credenciales del usuario desde Firestore
        msg_id: ID del mensaje
        outdir: Carpeta donde guardar los adjuntos
        user_id: ID del usuario (opcional, para guardar token refrescado en Firestore)

    Returns:
        Lista de rutas de archivos descargados
    """
    # Asegurar que el token es válido (refresca automáticamente si es necesario)
    credentials_dict = _ensure_valid_token(credentials_dict, user_id)

    access_token = credentials_dict.get("access_token")
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}/attachments"

    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    attachments = resp.json().get("value", [])

    os.makedirs(outdir, exist_ok=True)
    saved_files = []

    for att in attachments:
        if att["@odata.type"] == "#microsoft.graph.fileAttachment":
            fname = os.path.join(outdir, att["name"])
            with open(fname, "wb") as f:
                f.write(base64.b64decode(att["contentBytes"]))
            saved_files.append(fname)

    return saved_files


# ========== FUNCIONES ANTIGUAS (DEPRECATED - Solo para compatibilidad) ==========


def authenticate_outlook() -> msal.PublicClientApplication:
    return msal.PublicClientApplication(CLIENT_ID, authority=AUTHORITY)


def get_token(app: msal.PublicClientApplication) -> Dict[str, Any]:
    cache = msal.SerializableTokenCache()
    if os.path.exists(TOKEN_CACHE):
        cache.deserialize(open(TOKEN_CACHE, "r").read())
    app.token_cache = cache
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
        if result:
            return result
    flow = app.initiate_device_flow(scopes=SCOPES)
    print(flow)
    if "user_code" not in flow:
        raise Exception("No se pudo iniciar flujo de dispositivo")

    print(f"🔑 Abrí {flow['verification_uri']} e ingresá el código: {flow['user_code']}")
    result = app.acquire_token_by_device_flow(flow)  # bloquea hasta que completes login
    if "access_token" in result:
        # save the token cache to disk
        with open(TOKEN_CACHE, "w") as f:
            f.write(app.token_cache.serialize())
    if "access_token" not in result:
        raise Exception(f"Error autenticando: {result}")
    return result


def list_messages_outlook(token: Dict[str, Any], top: int = 10) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    url = f"https://graph.microsoft.com/v1.0/me/messages?$top={top}&$select=sender,subject,receivedDateTime,hasAttachments"
    print(headers)
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    return data.get("value", [])


def get_message_body(token: Dict[str, Any], msg_id: str) -> str:
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    url = f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}?$select=body"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    body = resp.json().get("body", {}).get("content", "")
    return body


def get_attachments(token: Dict[str, Any], msg_id: str, outdir="attachments") -> List[str]:
    headers = {"Authorization": f"Bearer {token['access_token']}"}
    url = f"https://graph.microsoft.com/v1.0/me/messages/{msg_id}/attachments"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    attachments = resp.json().get("value", [])
    os.makedirs(outdir, exist_ok=True)
    saved_files = []
    for att in attachments:
        if att["@odata.type"] == "#microsoft.graph.fileAttachment":
            fname = os.path.join(outdir, att["name"])
            with open(fname, "wb") as f:
                # Decode the base64 string and write it as binary
                f.write(base64.b64decode(att["contentBytes"]))
            saved_files.append(fname)
    return saved_files
