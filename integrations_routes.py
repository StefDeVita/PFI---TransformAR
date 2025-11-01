"""
Endpoints para conectar/desconectar servicios externos por usuario
Soporta: Gmail, Outlook, WhatsApp, Telegram
"""

from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import os
import secrets
from urllib.parse import urlencode

from auth import decode_jwt_token
from external_credentials import (
    ExternalCredentialsManager,
    save_gmail_credentials,
    save_outlook_credentials,
    save_whatsapp_credentials,
    save_telegram_credentials
)

# Importar funciones de autenticación OAuth
from google_auth_oauthlib.flow import Flow
from msal import ConfidentialClientApplication
import requests

router = APIRouter(prefix="/integration", tags=["integrations"])


# ==================== Dependency ====================

async def get_current_user(authorization: str = Header(None)) -> str:
    """
    Obtiene el ID del usuario actual desde el token JWT

    Args:
        authorization: Header de autorización con formato "Bearer {token}"

    Returns:
        User ID

    Raises:
        HTTPException: Si el token es inválido o falta
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="No se proporcionó token de autenticación")

    parts = authorization.split(" ")
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Formato de token inválido. Use: Bearer {token}")

    token = parts[1]
    user_id = decode_jwt_token(token)

    if not user_id:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")

    return user_id


# ==================== Modelos ====================

class ConnectGmailRequest(BaseModel):
    """Request para conectar Gmail vía OAuth"""
    # El flujo OAuth se manejará con redirect, este endpoint inicia el proceso
    pass


class ConnectOutlookRequest(BaseModel):
    """Request para conectar Outlook vía OAuth"""
    pass


class ConnectWhatsAppRequest(BaseModel):
    """Request para conectar WhatsApp Business API"""
    phone_number_id: str = Field(..., description="ID del número de WhatsApp Business")
    access_token: str = Field(..., description="Token de acceso permanente de Meta")
    business_account_id: Optional[str] = Field(None, description="ID de la cuenta de negocio (opcional)")
    phone_number: Optional[str] = Field(None, description="Número de teléfono (opcional)")


class ConnectTelegramRequest(BaseModel):
    """Request para conectar Telegram Bot"""
    bot_token: str = Field(..., description="Token del bot obtenido de @BotFather")


class IntegrationResponse(BaseModel):
    """Respuesta de conexión/desconexión"""
    success: bool
    message: str
    service: str
    metadata: Optional[Dict[str, Any]] = None


class IntegrationListResponse(BaseModel):
    """Lista de integraciones conectadas"""
    integrations: Dict[str, Dict[str, Any]]


class IntegrationStatusResponse(BaseModel):
    """Estado de una integración específica"""
    connected: bool
    service: str
    metadata: Optional[Dict[str, Any]] = None


# ==================== Listar Integraciones ====================

@router.get("/", response_model=IntegrationListResponse)
async def list_integrations(user_id: str = Depends(get_current_user)):
    """
    Lista todas las integraciones conectadas del usuario actual

    Retorna metadata de cada integración (sin credenciales sensibles)
    """
    credentials = await ExternalCredentialsManager.list_credentials(user_id)

    return IntegrationListResponse(integrations=credentials)


# ==================== GMAIL ====================

# Variables globales para el flujo OAuth
gmail_oauth_states = {}  # {state: user_id}


@router.post("/gmail/connect")
async def connect_gmail_start(
    user_id: str = Depends(get_current_user)
):
    """
    Inicia el flujo OAuth2 para conectar Gmail

    Retorna la URL a la que el usuario debe ser redirigido
    """
    # Ruta al archivo de credenciales OAuth de Google
    credentials_path = os.path.join("input", "credentials.json")

    if not os.path.exists(credentials_path):
        raise HTTPException(
            status_code=500,
            detail="No se encontró el archivo de credenciales de Gmail. "
                   "Descárgalo desde Google Cloud Console y guárdalo en input/credentials.json"
        )

    # Configurar flujo OAuth
    scopes = ['https://www.googleapis.com/auth/gmail.readonly']
    redirect_uri = os.getenv("GMAIL_REDIRECT_URI", "http://localhost:8000/integration/gmail/callback")

    try:
        flow = Flow.from_client_secrets_file(
            credentials_path,
            scopes=scopes,
            redirect_uri=redirect_uri
        )

        # Generar state único para prevenir CSRF
        state = secrets.token_urlsafe(32)
        gmail_oauth_states[state] = user_id

        authorization_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            state=state
        )

        return {
            "authorization_url": authorization_url,
            "message": "Redirige al usuario a esta URL para autorizar el acceso"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error iniciando OAuth: {str(e)}")


@router.get("/gmail/callback")
async def connect_gmail_callback(
    code: str,
    state: str
):
    """
    Callback de OAuth2 de Gmail

    Este endpoint es llamado por Google después de que el usuario autoriza
    """
    # Verificar state
    if state not in gmail_oauth_states:
        raise HTTPException(status_code=400, detail="State inválido o expirado")

    user_id = gmail_oauth_states.pop(state)

    credentials_path = os.path.join("input", "credentials.json")
    redirect_uri = os.getenv("GMAIL_REDIRECT_URI", "http://localhost:8000/integration/gmail/callback")

    try:
        scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        flow = Flow.from_client_secrets_file(
            credentials_path,
            scopes=scopes,
            redirect_uri=redirect_uri
        )

        # Intercambiar código por token
        flow.fetch_token(code=code)
        credentials = flow.credentials

        # Obtener información del usuario
        from googleapiclient.discovery import build
        service = build('gmail', 'v1', credentials=credentials)
        profile = service.users().getProfile(userId='me').execute()
        email = profile.get('emailAddress')

        # Guardar credenciales en Firestore
        token_data = {
            "token": credentials.token,
            "refresh_token": credentials.refresh_token,
            "token_uri": credentials.token_uri,
            "client_id": credentials.client_id,
            "client_secret": credentials.client_secret,
            "scopes": credentials.scopes
        }

        success = await save_gmail_credentials(user_id, token_data, email)

        if success:
            return {
                "success": True,
                "message": f"Gmail conectado exitosamente: {email}",
                "redirect": os.getenv("FRONTEND_URL", "http://localhost:3000") + "/integrations?success=gmail"
            }
        else:
            raise HTTPException(status_code=500, detail="Error guardando credenciales")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en OAuth callback: {str(e)}")


@router.delete("/gmail/disconnect", response_model=IntegrationResponse)
async def disconnect_gmail(user_id: str = Depends(get_current_user)):
    """Desconecta Gmail del usuario actual"""
    success = await ExternalCredentialsManager.delete_credential(user_id, "gmail")

    if success:
        return IntegrationResponse(
            success=True,
            message="Gmail desconectado exitosamente",
            service="gmail"
        )
    else:
        raise HTTPException(status_code=500, detail="Error desconectando Gmail")


@router.get("/gmail/status", response_model=IntegrationStatusResponse)
async def gmail_status(user_id: str = Depends(get_current_user)):
    """
    Verifica si Gmail está conectado para el usuario actual

    Retorna el estado de conexión y metadata si está conectado
    """
    credential = await ExternalCredentialsManager.get_credential(user_id, "gmail")

    if credential:
        return IntegrationStatusResponse(
            connected=True,
            service="gmail",
            metadata=credential.get("metadata", {})
        )
    else:
        return IntegrationStatusResponse(
            connected=False,
            service="gmail",
            metadata=None
        )


# ==================== OUTLOOK ====================

outlook_oauth_states = {}  # {state: user_id}


@router.post("/outlook/connect")
async def connect_outlook_start(
    user_id: str = Depends(get_current_user)
):
    """
    Inicia el flujo OAuth2 para conectar Outlook

    Retorna la URL a la que el usuario debe ser redirigido
    """
    client_id = os.getenv("OUTLOOK_CLIENT_ID")
    client_secret = os.getenv("OUTLOOK_CLIENT_SECRET")
    redirect_uri = os.getenv("OUTLOOK_REDIRECT_URI", "http://localhost:8000/integration/outlook/callback")

    if not client_id or not client_secret:
        raise HTTPException(
            status_code=500,
            detail="Faltan credenciales de Outlook. Configura OUTLOOK_CLIENT_ID y OUTLOOK_CLIENT_SECRET"
        )

    # Generar state único
    state = secrets.token_urlsafe(32)
    outlook_oauth_states[state] = user_id

    # Construir URL de autorización
    authority = "https://login.microsoftonline.com/common"
    scopes = ["https://graph.microsoft.com/Mail.Read", "https://graph.microsoft.com/User.Read"]

    app = ConfidentialClientApplication(
        client_id,
        client_credential=client_secret,
        authority=authority
    )

    auth_url = app.get_authorization_request_url(
        scopes=scopes,
        state=state,
        redirect_uri=redirect_uri
    )

    return {
        "authorization_url": auth_url,
        "message": "Redirige al usuario a esta URL para autorizar el acceso"
    }


@router.get("/outlook/callback")
async def connect_outlook_callback(
    code: str,
    state: str
):
    """
    Callback de OAuth2 de Outlook

    Este endpoint es llamado por Microsoft después de que el usuario autoriza
    """
    # Verificar state
    if state not in outlook_oauth_states:
        raise HTTPException(status_code=400, detail="State inválido o expirado")

    user_id = outlook_oauth_states.pop(state)

    client_id = os.getenv("OUTLOOK_CLIENT_ID")
    client_secret = os.getenv("OUTLOOK_CLIENT_SECRET")
    redirect_uri = os.getenv("OUTLOOK_REDIRECT_URI", "http://localhost:8000/integration/outlook/callback")

    try:
        authority = "https://login.microsoftonline.com/common"
        scopes = ["https://graph.microsoft.com/Mail.Read", "https://graph.microsoft.com/User.Read"]

        app = ConfidentialClientApplication(
            client_id,
            client_credential=client_secret,
            authority=authority
        )

        # Intercambiar código por token
        result = app.acquire_token_by_authorization_code(
            code,
            scopes=scopes,
            redirect_uri=redirect_uri
        )

        if "access_token" not in result:
            raise HTTPException(status_code=400, detail=f"Error obteniendo token: {result.get('error_description')}")

        access_token = result["access_token"]
        refresh_token = result.get("refresh_token")
        expires_in = result.get("expires_in")

        # Obtener email del usuario
        headers = {"Authorization": f"Bearer {access_token}"}
        user_response = requests.get("https://graph.microsoft.com/v1.0/me", headers=headers)
        user_data = user_response.json()
        email = user_data.get("mail") or user_data.get("userPrincipalName")

        # Guardar credenciales
        import time
        expires_at = int(time.time()) + expires_in if expires_in else None

        success = await save_outlook_credentials(
            user_id,
            access_token,
            refresh_token,
            expires_at,
            email
        )

        if success:
            return {
                "success": True,
                "message": f"Outlook conectado exitosamente: {email}",
                "redirect": os.getenv("FRONTEND_URL", "http://localhost:3000") + "/integrations?success=outlook"
            }
        else:
            raise HTTPException(status_code=500, detail="Error guardando credenciales")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en OAuth callback: {str(e)}")


@router.delete("/outlook/disconnect", response_model=IntegrationResponse)
async def disconnect_outlook(user_id: str = Depends(get_current_user)):
    """Desconecta Outlook del usuario actual"""
    success = await ExternalCredentialsManager.delete_credential(user_id, "outlook")

    if success:
        return IntegrationResponse(
            success=True,
            message="Outlook desconectado exitosamente",
            service="outlook"
        )
    else:
        raise HTTPException(status_code=500, detail="Error desconectando Outlook")


@router.get("/outlook/status", response_model=IntegrationStatusResponse)
async def outlook_status(user_id: str = Depends(get_current_user)):
    """
    Verifica si Outlook está conectado para el usuario actual

    Retorna el estado de conexión y metadata si está conectado
    """
    credential = await ExternalCredentialsManager.get_credential(user_id, "outlook")

    if credential:
        return IntegrationStatusResponse(
            connected=True,
            service="outlook",
            metadata=credential.get("metadata", {})
        )
    else:
        return IntegrationStatusResponse(
            connected=False,
            service="outlook",
            metadata=None
        )


# ==================== WHATSAPP ====================

@router.post("/whatsapp/connect", response_model=IntegrationResponse)
async def connect_whatsapp(
    request: ConnectWhatsAppRequest,
    user_id: str = Depends(get_current_user)
):
    """
    Conecta WhatsApp Business API

    Requiere:
    - phone_number_id: ID del número de WhatsApp Business
    - access_token: Token de acceso permanente de Meta
    - business_account_id: (opcional) ID de la cuenta de negocio
    - phone_number: (opcional) Número de teléfono

    Para obtener estos datos:
    1. Ve a https://business.facebook.com
    2. Crea/selecciona tu workspace
    3. Agrega aplicación WhatsApp
    4. Copia los datos de Settings > API Setup
    """
    try:
        # Validar que las credenciales funcionen
        headers = {"Authorization": f"Bearer {request.access_token}"}
        test_url = f"https://graph.facebook.com/v18.0/{request.phone_number_id}"

        response = requests.get(test_url, headers=headers, timeout=10)

        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Credenciales inválidas de WhatsApp: {response.text}"
            )

        # Guardar credenciales
        success = await save_whatsapp_credentials(
            user_id,
            request.phone_number_id,
            request.access_token,
            request.business_account_id,
            request.phone_number
        )

        if success:
            return IntegrationResponse(
                success=True,
                message="WhatsApp conectado exitosamente",
                service="whatsapp",
                metadata={"phone_number": request.phone_number}
            )
        else:
            raise HTTPException(status_code=500, detail="Error guardando credenciales")

    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Error validando credenciales: {str(e)}")


@router.delete("/whatsapp/disconnect", response_model=IntegrationResponse)
async def disconnect_whatsapp(user_id: str = Depends(get_current_user)):
    """Desconecta WhatsApp del usuario actual"""
    success = await ExternalCredentialsManager.delete_credential(user_id, "whatsapp")

    if success:
        return IntegrationResponse(
            success=True,
            message="WhatsApp desconectado exitosamente",
            service="whatsapp"
        )
    else:
        raise HTTPException(status_code=500, detail="Error desconectando WhatsApp")


@router.get("/whatsapp/status", response_model=IntegrationStatusResponse)
async def whatsapp_status(user_id: str = Depends(get_current_user)):
    """
    Verifica si WhatsApp está conectado para el usuario actual

    Retorna el estado de conexión y metadata si está conectado
    """
    credential = await ExternalCredentialsManager.get_credential(user_id, "whatsapp")

    if credential:
        return IntegrationStatusResponse(
            connected=True,
            service="whatsapp",
            metadata=credential.get("metadata", {})
        )
    else:
        return IntegrationStatusResponse(
            connected=False,
            service="whatsapp",
            metadata=None
        )


# ==================== TELEGRAM ====================

@router.post("/telegram/connect", response_model=IntegrationResponse)
async def connect_telegram(
    request: ConnectTelegramRequest,
    user_id: str = Depends(get_current_user)
):
    """
    Conecta Telegram Bot

    Requiere:
    - bot_token: Token obtenido de @BotFather

    Para obtener el token:
    1. Abre Telegram
    2. Busca @BotFather
    3. Envía /newbot
    4. Sigue las instrucciones
    5. Copia el token generado
    """
    try:
        # Validar que el token funcione
        api_url = f"https://api.telegram.org/bot{request.bot_token}/getMe"
        response = requests.get(api_url, timeout=10)

        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail="Token de Telegram inválido"
            )

        bot_data = response.json()
        if not bot_data.get("ok"):
            raise HTTPException(
                status_code=400,
                detail="Token de Telegram inválido"
            )

        bot_info = bot_data.get("result", {})
        bot_username = bot_info.get("username")
        bot_name = bot_info.get("first_name")

        # Guardar credenciales
        success = await save_telegram_credentials(
            user_id,
            request.bot_token,
            bot_username,
            bot_name
        )

        if success:
            return IntegrationResponse(
                success=True,
                message=f"Telegram conectado exitosamente: @{bot_username}",
                service="telegram",
                metadata={
                    "bot_username": bot_username,
                    "bot_name": bot_name
                }
            )
        else:
            raise HTTPException(status_code=500, detail="Error guardando credenciales")

    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Error validando token: {str(e)}")


@router.delete("/telegram/disconnect", response_model=IntegrationResponse)
async def disconnect_telegram(user_id: str = Depends(get_current_user)):
    """Desconecta Telegram del usuario actual"""
    success = await ExternalCredentialsManager.delete_credential(user_id, "telegram")

    if success:
        return IntegrationResponse(
            success=True,
            message="Telegram desconectado exitosamente",
            service="telegram"
        )
    else:
        raise HTTPException(status_code=500, detail="Error desconectando Telegram")


@router.get("/telegram/status", response_model=IntegrationStatusResponse)
async def telegram_status(user_id: str = Depends(get_current_user)):
    """
    Verifica si Telegram está conectado para el usuario actual

    Retorna el estado de conexión y metadata si está conectado
    """
    credential = await ExternalCredentialsManager.get_credential(user_id, "telegram")

    if credential:
        return IntegrationStatusResponse(
            connected=True,
            service="telegram",
            metadata=credential.get("metadata", {})
        )
    else:
        return IntegrationStatusResponse(
            connected=False,
            service="telegram",
            metadata=None
        )
