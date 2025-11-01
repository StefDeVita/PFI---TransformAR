# api.py (grid-templates aligned)
from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Form, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic.functional_validators import field_validator
from typing import List, Optional, Literal, Dict, Any
import pathlib, os, json, re, unicodedata
import tempfile

# --- Importar pipeline y fuentes existentes ---
from input.docling_reader import extract_text_with_layout
from nlp.qwen_labeler import extract_with_qwen
from nlp.instruction_qwen import interpret_with_qwen
from nlp.apply_plan import execute_plan
from input.gmail_reader import (
    authenticate_gmail, list_messages as gmail_list, get_message_content as gmail_get,
    authenticate_gmail_from_credentials, list_messages_from_credentials as gmail_list_from_creds,
    get_message_content_from_credentials as gmail_get_from_creds
)
from input.outlook_reader import (
    authenticate_outlook, get_token, list_messages_outlook, get_message_body, get_attachments,
    list_messages_outlook_from_credentials as outlook_list_from_creds,
    get_message_body_from_credentials as outlook_get_body_from_creds,
    get_attachments_from_credentials as outlook_get_attachments_from_creds
)
from input.whatsapp_reader import authenticate_whatsapp, list_messages_whatsapp, get_message_content as whatsapp_get
from input.telegram_reader import authenticate_telegram, list_messages_telegram, get_message_content as telegram_get
from auth import authenticate_user, create_access_token, create_password_reset_token, send_password_reset_email, decode_jwt_token
from integrations_routes import router as integrations_router
from external_credentials import ExternalCredentialsManager

UPLOAD_DIR = pathlib.Path("uploads")
TEMPLATES_DIR = pathlib.Path("templates")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="TransformAR API", version="0.2.0")

ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:4200",
    # "https://dominio.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir router de integraciones
app.include_router(integrations_router)


# --------- Grid Template Model (alineado al front) ---------

class GridColumn(BaseModel):
    col: str = Field(..., description="Letra de columna (A,B,C,...)")
    title: str = Field(..., description="Descripción del dato a extraer (fila 1)")
    example: Optional[str] = Field(
        "",
        description=(
            "Instrucción de transformación que se debe aplicar al dato extraído (fila 2)"
        ),
    )


class GridTemplate(BaseModel):
    id: str
    name: str
    description: Optional[str] = ""
    columns: List[GridColumn] = Field(..., description="Definición de columnas en orden visual")

    @field_validator('columns')
    @classmethod
    def _order_cols(cls, v):
        # A -> 1, B -> 2, ..., Z -> 26, AA -> 27, etc.
        def col_to_num(col) -> int:
            s = str(col).strip().upper()
            acc = 0
            for ch in s:
                if 'A' <= ch <= 'Z':
                    acc = acc * 26 + (ord(ch) - 64)
            return acc or 10 ** 9

        def key(item):
            if isinstance(item, dict):
                return col_to_num(item.get('col', ''))
            return col_to_num(getattr(item, 'col', ''))

        return sorted(v, key=key)


class TemplateMeta(BaseModel):
    id: str
    name: str
    description: Optional[str] = None


# DTO de proceso
class GmailSelection(BaseModel):
    message_id: str
    use_text: bool = False
    attachment_index: Optional[int] = None


class OutlookSelection(BaseModel):
    message_id: str
    use_text: bool = False
    attachment_index: Optional[int] = None


class ManualDocSelection(BaseModel):
    file_id: str


class WhatsAppSelection(BaseModel):
    message_data: Dict[str, Any]  # Datos completos del mensaje (del webhook)
    use_text: bool = False
    attachment_index: Optional[int] = None


class TelegramSelection(BaseModel):
    message_data: Dict[str, Any]  # Datos completos del mensaje (de list_messages o webhook)
    use_text: bool = False
    attachment_index: Optional[int] = None


Method = Literal["document", "gmail", "outlook", "text", "whatsapp", "telegram"]


class ProcessRequest(BaseModel):
    method: Method
    template_id: str
    manual: Optional[ManualDocSelection] = None
    gmail: Optional[GmailSelection] = None
    outlook: Optional[OutlookSelection] = None
    whatsapp: Optional[WhatsAppSelection] = None
    telegram: Optional[TelegramSelection] = None
    text: Optional[str] = None


# --------- Authentication Models ---------

class LoginRequest(BaseModel):
    email: str = Field(..., description="User's email address")
    password: str = Field(..., description="User's password")


class LoginResponse(BaseModel):
    authtoken: str = Field(..., description="JWT authentication token")
    user: Dict[str, Any] = Field(..., description="User data (without password)")


class RecoverPasswordRequest(BaseModel):
    email: str = Field(..., description="Email address to send recovery link to")


# --------- Helpers ---------

async def get_current_user_optional(authorization: str = Header(None)) -> Optional[str]:
    """
    Extrae el user_id del JWT token si está presente.
    Retorna None si no hay token (para endpoints públicos).
    """
    if not authorization or not authorization.startswith("Bearer "):
        return None

    try:
        token = authorization.split(" ")[1]
        user_id = decode_jwt_token(token)
        return user_id
    except Exception:
        return None


async def get_current_user(authorization: str = Header(None)) -> str:
    """
    Extrae el user_id del JWT token.
    Lanza HTTPException si el token es inválido o no está presente.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="No se proporcionó token de autenticación")

    try:
        token = authorization.split(" ")[1]
        user_id = decode_jwt_token(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Token inválido")
        return user_id
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Error decodificando token: {str(e)}")


def _slug(s: str) -> str:
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r'[^a-zA-Z0-9]+', '_', s).strip('_').lower()
    return s or 'col'


def _compile_grid_to_instructions(gt: GridTemplate) -> Dict[str, str]:
    """
    Dado un grid del front, genera las instrucciones necesarias para Qwen:
    - extract_instr: qué campos extraer y bajo qué clave devolverlos.
    - transform_instr: transformaciones a aplicar sobre cada campo extraído.
    """

    fields = []
    transforms = []

    for col in gt.columns:
        key = _slug(col.title)
        title = (col.title or "").strip()
        transform_text = (col.example or "").strip()

        if title:
            fields.append(f"- Clave '{key}': {title}")
        else:
            fields.append(f"- Clave '{key}': dato sin descripción especificada")

        if transform_text:
            transforms.append(
                transform_text
            )

    extract_instr = "Extrae un JSON con las siguientes claves y valores:\n" + "\n".join(fields)
    transform_instr = (
        " ".join(transforms)
        if transforms
        else "No apliques transformaciones adicionales; deja los valores tal como fueron extraídos."
    )

    return {"extract_instr": extract_instr, "transform_instr": transform_instr}


def _save_template(gt: GridTemplate):
    path = TEMPLATES_DIR / f"{gt.id}.grid.json"
    # v2: serializamos así (soporta acentos con ensure_ascii=False)
    payload = json.dumps(gt.model_dump(), ensure_ascii=False, indent=2)
    path.write_text(payload, encoding="utf-8")


def _load_template_grid(tid: str) -> GridTemplate:
    path = TEMPLATES_DIR / f"{tid}.grid.json"
    if not path.exists():
        raise HTTPException(404, f"Plantilla '{tid}' no encontrada")
    data = json.loads(path.read_text(encoding="utf-8"))
    return GridTemplate(**data)


def _list_template_meta() -> List[TemplateMeta]:
    metas = []
    for p in TEMPLATES_DIR.glob("*.grid.json"):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            metas.append(TemplateMeta(id=d["id"], name=d["name"], description=d.get("description", "")))
        except Exception:
            continue
    return metas


def _pipeline_from_text(text: str, extract_instr: str, transform_instr: str) -> List[Dict[str, Any]]:
    extracted = extract_with_qwen(text, extract_instr)
    plan, _ = interpret_with_qwen(transform_instr)
    return execute_plan(extracted, plan)


def _pipeline_from_file(path: pathlib.Path, extract_instr: str, transform_instr: str) -> List[Dict[str, Any]]:
    md = extract_text_with_layout(str(path))
    return _pipeline_from_text(md, extract_instr, transform_instr)


# --------- Endpoints ---------

@app.get("/health")
def health():
    return {"ok": True}


# --------- Authentication Endpoints ---------

@app.post("/auth/login", response_model=LoginResponse, summary="Authenticate user and return auth token")
async def login(credentials: LoginRequest):
    """
    Authenticate user with email and password.
    Returns a JWT token to be set in a cookie on the frontend.
    """
    user = await authenticate_user(credentials.email, credentials.password)

    if not user:
        raise HTTPException(
            status_code=401,
            detail="Credenciales inválidas"
        )

    # Create JWT token
    token_data = {
        "sub": user.get("id"),
        "email": user.get("email")
    }
    access_token = create_access_token(token_data)

    return LoginResponse(
        authtoken=access_token,
        user=user
    )


@app.post("/auth/recover-password", summary="Send password recovery email")
async def recover_password(request: RecoverPasswordRequest):
    """
    Send password recovery email to the specified address.
    Returns success even if user doesn't exist (security best practice).
    """
    # Create reset token (returns None if user doesn't exist)
    reset_token = await create_password_reset_token(request.email)

    # Only send email if user exists
    if reset_token:
        await send_password_reset_email(request.email, reset_token)

    # Always return success to prevent email enumeration attacks
    return {
        "success": True,
        "message": "If the email exists in our system, a recovery link has been sent."
    }


# Subida de documento manual
@app.post("/process/document", summary="Sube un archivo, lo procesa con una plantilla y lo descarta")
async def process_document_with_template(
        template_id: str = Form(...),
        file: UploadFile = File(...)
):
    # 1) leer el archivo en un tmp file
    content = await file.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file.filename}") as tmp:
        tmp.write(content)
        tmp_path = tmp.name

    try:
        # 2) cargar plantilla y compilar instrucciones
        gtpl = _load_template_grid(template_id)
        compiled = _compile_grid_to_instructions(gtpl)
        extract_instr = compiled["extract_instr"]
        transform_instr = compiled["transform_instr"]
        # 3) ejecutar pipeline sobre el tmp file
        result = _pipeline_from_file(pathlib.Path(tmp_path), extract_instr, transform_instr)
        return {
            "template_id": template_id,
            "compiled": compiled,
            "result": result
        }
    finally:
        # 4) borrar archivo temporal
        try:
            pathlib.Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


@app.post("/input/document")
async def upload_document(file: UploadFile = File(...)):
    safe = (file.filename or "upload.bin").replace("/", "_").replace("\\", "_")
    dest = UPLOAD_DIR / safe
    with open(dest, "wb") as f:
        f.write(await file.read())
    return {"file_id": safe, "path": str(dest)}


# Gmail
@app.get("/input/gmail/messages")
async def gmail_messages(limit: int = Query(10, ge=1, le=50), user_id: str = Depends(get_current_user)):
    """Lista mensajes de Gmail usando las credenciales del usuario autenticado."""
    cred_manager = ExternalCredentialsManager()
    gmail_creds = await cred_manager.get_credential(user_id, "gmail")

    if not gmail_creds:
        raise HTTPException(
            status_code=400,
            detail="No has conectado tu cuenta de Gmail. Usa POST /integrations/gmail/connect primero."
        )

    mails = gmail_list_from_creds(gmail_creds, max_results=limit)
    return {"messages": [{"id": m["id"], "from": m.get("from"), "subject": m.get("subject")} for m in mails]}


@app.get("/input/gmail/messages/{msg_id}")
async def gmail_message_detail(msg_id: str, user_id: str = Depends(get_current_user)):
    """Obtiene detalles de un mensaje de Gmail usando las credenciales del usuario autenticado."""
    cred_manager = ExternalCredentialsManager()
    gmail_creds = await cred_manager.get_credential(user_id, "gmail")

    if not gmail_creds:
        raise HTTPException(
            status_code=400,
            detail="No has conectado tu cuenta de Gmail. Usa POST /integrations/gmail/connect primero."
        )

    content = gmail_get_from_creds(gmail_creds, msg_id)
    return {"text": content.get("text", ""), "attachments": content.get("attachments", [])}


# Outlook
@app.get("/input/outlook/messages")
async def outlook_messages(limit: int = Query(10, ge=1, le=50), user_id: str = Depends(get_current_user)):
    """Lista mensajes de Outlook usando las credenciales del usuario autenticado."""
    cred_manager = ExternalCredentialsManager()
    outlook_creds = await cred_manager.get_credential(user_id, "outlook")

    if not outlook_creds:
        raise HTTPException(
            status_code=400,
            detail="No has conectado tu cuenta de Outlook. Usa POST /integrations/outlook/connect primero."
        )

    mails = outlook_list_from_creds(outlook_creds, top=limit)
    out = []
    for m in mails:
        sender = (m.get("sender") or {}).get("emailAddress", {}).get("address")
        out.append({"id": m.get("id"), "from": sender, "subject": m.get("subject"),
                    "hasAttachments": m.get("hasAttachments", False)})
    return {"messages": out}


@app.get("/input/outlook/messages/{msg_id}")
async def outlook_message_detail(msg_id: str, user_id: str = Depends(get_current_user)):
    """Obtiene detalles de un mensaje de Outlook usando las credenciales del usuario autenticado."""
    cred_manager = ExternalCredentialsManager()
    outlook_creds = await cred_manager.get_credential(user_id, "outlook")

    if not outlook_creds:
        raise HTTPException(
            status_code=400,
            detail="No has conectado tu cuenta de Outlook. Usa POST /integrations/outlook/connect primero."
        )

    text = outlook_get_body_from_creds(outlook_creds, msg_id)
    atts = outlook_get_attachments_from_creds(outlook_creds, msg_id)
    return {"text": text or "", "attachments": atts or []}


# WhatsApp
@app.get("/input/whatsapp/messages")
async def whatsapp_messages(limit: int = Query(10, ge=1, le=50), user_id: str = Depends(get_current_user)):
    """
    Lista mensajes de WhatsApp usando las credenciales del usuario autenticado.

    NOTA: WhatsApp no provee un endpoint para listar mensajes históricos.
    Los mensajes deben ser capturados por webhook y almacenados en una DB.
    Esta función devuelve una lista vacía como placeholder.
    """
    try:
        cred_manager = ExternalCredentialsManager()
        whatsapp_creds = await cred_manager.get_credential(user_id, "whatsapp")

        if not whatsapp_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de WhatsApp. Usa POST /integrations/whatsapp/connect primero."
            )

        client = authenticate_whatsapp(whatsapp_creds)
        messages = list_messages_whatsapp(client, limit=limit)
        return {"messages": messages}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(500, f"Error de configuración: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Error obteniendo mensajes de WhatsApp: {str(e)}")


@app.post("/input/whatsapp/content")
async def whatsapp_content(message_data: Dict[str, Any], user_id: str = Depends(get_current_user)):
    """
    Obtiene el contenido completo de un mensaje de WhatsApp.

    El message_data debe contener la estructura del mensaje del webhook.
    """
    try:
        cred_manager = ExternalCredentialsManager()
        whatsapp_creds = await cred_manager.get_credential(user_id, "whatsapp")

        if not whatsapp_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de WhatsApp. Usa POST /integrations/whatsapp/connect primero."
            )

        client = authenticate_whatsapp(whatsapp_creds)
        content = whatsapp_get(client, message_data)
        return {"text": content.get("text", ""), "attachments": content.get("attachments", [])}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(500, f"Error de configuración: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Error obteniendo contenido: {str(e)}")


@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    """
    Webhook para recibir mensajes de WhatsApp en tiempo real.

    Este endpoint debe ser configurado en Meta for Developers.
    Recibe notificaciones cuando llegan nuevos mensajes.
    """
    try:
        data = await request.json()
        client = authenticate_whatsapp()

        # Procesar entrada de webhook
        for entry in data.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})

                # Procesar mensajes
                for message in value.get("messages", []):
                    msg_id = message.get("id")
                    from_number = message.get("from")

                    # Aquí deberías guardar el mensaje en una base de datos
                    # para luego poder listarlo con list_messages_whatsapp
                    print(f"[WhatsApp Webhook] Mensaje recibido de {from_number}: {msg_id}")

                    # Opcional: Enviar confirmación automática
                    # client.send_text_message(from_number, "Mensaje recibido ✓")

        return {"status": "ok"}
    except Exception as e:
        print(f"[WhatsApp Webhook] Error: {e}")
        raise HTTPException(500, str(e))


@app.get("/webhook/whatsapp")
async def whatsapp_webhook_verify(
    hub_mode: str = Query(alias="hub.mode"),
    hub_verify_token: str = Query(alias="hub.verify_token"),
    hub_challenge: str = Query(alias="hub.challenge")
):
    """
    Verificación inicial del webhook de WhatsApp.

    Meta llama a este endpoint para verificar que controlas el servidor.
    """
    expected_token = os.getenv("WHATSAPP_WEBHOOK_TOKEN", "default_webhook_token")

    if hub_mode == "subscribe" and hub_verify_token == expected_token:
        return int(hub_challenge)
    else:
        raise HTTPException(403, "Token de verificación inválido")


# Telegram
@app.get("/input/telegram/messages")
async def telegram_messages(limit: int = Query(10, ge=1, le=50), user_id: str = Depends(get_current_user)):
    """
    Lista últimos mensajes de Telegram usando polling y las credenciales del usuario autenticado.

    Para producción se recomienda usar webhook en lugar de polling.
    """
    try:
        cred_manager = ExternalCredentialsManager()
        telegram_creds = await cred_manager.get_credential(user_id, "telegram")

        if not telegram_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de Telegram. Usa POST /integrations/telegram/connect primero."
            )

        client = authenticate_telegram(telegram_creds)
        messages = list_messages_telegram(client, limit=limit)
        return {"messages": messages}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(500, f"Error de configuración: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Error obteniendo mensajes de Telegram: {str(e)}")


@app.post("/input/telegram/content")
async def telegram_content(message_data: Dict[str, Any], user_id: str = Depends(get_current_user)):
    """
    Obtiene el contenido completo de un mensaje de Telegram.

    El message_data debe contener la estructura completa del mensaje.
    """
    try:
        cred_manager = ExternalCredentialsManager()
        telegram_creds = await cred_manager.get_credential(user_id, "telegram")

        if not telegram_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de Telegram. Usa POST /integrations/telegram/connect primero."
            )

        client = authenticate_telegram(telegram_creds)
        content = telegram_get(client, message_data)
        return {"text": content.get("text", ""), "attachments": content.get("attachments", [])}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(500, f"Error de configuración: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Error obteniendo contenido: {str(e)}")


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """
    Webhook para recibir mensajes de Telegram en tiempo real.

    Este endpoint debe ser configurado usando setWebhook de la API de Telegram.
    """
    try:
        data = await request.json()
        client = authenticate_telegram()

        if "message" in data:
            message = data["message"]
            chat_id = message["chat"]["id"]
            msg_id = message["message_id"]

            # Aquí deberías guardar el mensaje en una base de datos
            print(f"[Telegram Webhook] Mensaje recibido en chat {chat_id}: {msg_id}")

            # Opcional: Enviar confirmación automática
            # client.send_message(chat_id, "Mensaje recibido ✓")

        return {"ok": True}
    except Exception as e:
        print(f"[Telegram Webhook] Error: {e}")
        raise HTTPException(500, str(e))


# Plantillas (grid)
@app.get("/templates", response_model=List[TemplateMeta])
def list_templates():
    return _list_template_meta()


@app.get("/templates/{tid}", response_model=GridTemplate)
def get_template(tid: str):
    return _load_template_grid(tid)


@app.post("/templates", response_model=GridTemplate, summary="Crear/Actualizar plantilla desde el front (grid)")
def upsert_template(gt: GridTemplate):
    _save_template(gt)
    return gt


# Proceso
@app.post("/process")
async def process(req: ProcessRequest, user_id: str = Depends(get_current_user)):
    gtpl = _load_template_grid(req.template_id)
    compiled = _compile_grid_to_instructions(gtpl)
    extract_instr = compiled["extract_instr"]
    transform_instr = compiled["transform_instr"]

    text: Optional[str] = None

    # Inicializar credential manager
    cred_manager = ExternalCredentialsManager()

    if req.method == "text":
        if not req.text: raise HTTPException(400, "Falta 'text'")
        text = req.text

    elif req.method == "document":
        if not req.manual: raise HTTPException(400, "Falta 'manual'")
        path = UPLOAD_DIR / req.manual.file_id
        if not path.exists(): raise HTTPException(404, "Archivo no encontrado")
        result = _pipeline_from_file(path, extract_instr, transform_instr)
        print(result)
        return {"result": result, "compiled": compiled}

    elif req.method == "gmail":
        if not req.gmail: raise HTTPException(400, "Falta 'gmail'")

        # Obtener credenciales del usuario desde Firestore
        gmail_creds = await cred_manager.get_credential(user_id, "gmail")
        if not gmail_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de Gmail. Usa POST /integrations/gmail/connect primero."
            )

        # Usar funciones que aceptan credenciales desde Firestore
        content = gmail_get_from_creds(gmail_creds, req.gmail.message_id)
        if req.gmail.use_text or not content.get("attachments"):
            text = content.get("text", "")
            if not text: raise HTTPException(400, "El correo no tiene texto utilizable.")
        else:
            atts = content.get("attachments") or []
            idx = req.gmail.attachment_index or 0
            if idx < 0 or idx >= len(atts): raise HTTPException(400, "attachment_index inválido")
            result = _pipeline_from_file(pathlib.Path(atts[idx]), extract_instr, transform_instr)
            return {"result": result, "compiled": compiled}

    elif req.method == "outlook":
        if not req.outlook: raise HTTPException(400, "Falta 'outlook'")

        # Obtener credenciales del usuario desde Firestore
        outlook_creds = await cred_manager.get_credential(user_id, "outlook")
        if not outlook_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de Outlook. Usa POST /integrations/outlook/connect primero."
            )

        # Usar funciones que aceptan credenciales desde Firestore
        if req.outlook.use_text:
            text = outlook_get_body_from_creds(outlook_creds, req.outlook.message_id)
            if not text: raise HTTPException(400, "No se pudo obtener texto del correo.")
        else:
            files = outlook_get_attachments_from_creds(outlook_creds, req.outlook.message_id)
            if files:
                idx = req.outlook.attachment_index or 0
                if idx < 0 or idx >= len(files): raise HTTPException(400, "attachment_index inválido")
                result = _pipeline_from_file(pathlib.Path(files[idx]), extract_instr, transform_instr)
                return {"result": result, "compiled": compiled}
            else:
                text = outlook_get_body_from_creds(outlook_creds, req.outlook.message_id)

    elif req.method == "whatsapp":
        if not req.whatsapp: raise HTTPException(400, "Falta 'whatsapp'")

        # Obtener credenciales del usuario desde Firestore
        whatsapp_creds = await cred_manager.get_credential(user_id, "whatsapp")
        if not whatsapp_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de WhatsApp. Usa POST /integrations/whatsapp/connect primero."
            )

        # Crear cliente con credenciales desde Firestore
        client = authenticate_whatsapp(whatsapp_creds)
        content = whatsapp_get(client, req.whatsapp.message_data)
        if req.whatsapp.use_text or not content.get("attachments"):
            text = content.get("text", "")
            if not text: raise HTTPException(400, "El mensaje no tiene texto utilizable.")
        else:
            atts = content.get("attachments") or []
            idx = req.whatsapp.attachment_index or 0
            if idx < 0 or idx >= len(atts): raise HTTPException(400, "attachment_index inválido")
            result = _pipeline_from_file(pathlib.Path(atts[idx]["path"]), extract_instr, transform_instr)
            return {"result": result, "compiled": compiled}

    elif req.method == "telegram":
        if not req.telegram: raise HTTPException(400, "Falta 'telegram'")

        # Obtener credenciales del usuario desde Firestore
        telegram_creds = await cred_manager.get_credential(user_id, "telegram")
        if not telegram_creds:
            raise HTTPException(
                status_code=400,
                detail="No has conectado tu cuenta de Telegram. Usa POST /integrations/telegram/connect primero."
            )

        # Crear cliente con credenciales desde Firestore
        client = authenticate_telegram(telegram_creds)
        content = telegram_get(client, req.telegram.message_data)
        if req.telegram.use_text or not content.get("attachments"):
            text = content.get("text", "")
            if not text: raise HTTPException(400, "El mensaje no tiene texto utilizable.")
        else:
            atts = content.get("attachments") or []
            idx = req.telegram.attachment_index or 0
            if idx < 0 or idx >= len(atts): raise HTTPException(400, "attachment_index inválido")
            result = _pipeline_from_file(pathlib.Path(atts[idx]["path"]), extract_instr, transform_instr)
            return {"result": result, "compiled": compiled}

    else:
        raise HTTPException(400, "Método inválido")

    result = _pipeline_from_text(text, extract_instr, transform_instr)
    return {"result": result, "compiled": compiled}
