# gmail_reader.py
from __future__ import annotations
import os, base64, re, pathlib
from typing import List, Dict, Any, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pathlib
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent
CREDENTIALS_FILE = BASE_DIR / "input" / "credentials.json"

# Gmail necesita este scope para leer mails y adjuntos
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


def authenticate_gmail() -> Any:
    """
    Maneja el flujo OAuth para conectarse a la API de Gmail.
    Guarda y reutiliza token.json para no pedir login cada vez.
    """
    creds: Optional[Credentials] = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    # Si no hay credenciales válidas, pedimos login al usuario
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_FILE), SCOPES)
            creds = flow.run_local_server(port=0)

        # Guardamos el token para reuso
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("gmail", "v1", credentials=creds)
    return service


def list_messages(service, max_results=10) -> List[Dict[str, Any]]:
    """
    Lista últimos correos recibidos con emisor y asunto.
    """
    results = service.users().messages().list(userId="me", maxResults=max_results).execute()
    messages = results.get("messages", [])

    mails: List[Dict[str, Any]] = []
    for msg in messages:
        mdata = service.users().messages().get(userId="me", id=msg["id"], format="metadata", metadataHeaders=["From", "Subject"]).execute()
        headers = {h["name"]: h["value"] for h in mdata.get("payload", {}).get("headers", [])}
        mails.append({
            "id": msg["id"],
            "from": headers.get("From", ""),
            "subject": headers.get("Subject", ""),
        })
    return mails


def get_message_content(service, msg_id: str) -> Dict[str, Any]:
    """
    Devuelve el contenido del correo:
    - texto plano
    - lista de adjuntos descargados (PDFs, etc.)
    """
    message = service.users().messages().get(userId="me", id=msg_id).execute()
    payload = message.get("payload", {})

    out: Dict[str, Any] = {"text": "", "attachments": []}

    def _walk_parts(part, msg_id):
        mime_type = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data")

        # Texto simple
        if mime_type == "text/plain" and data:
            text = base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
            out["text"] += text

        # Archivos adjuntos
        if "attachmentId" in body:
            att_id = body["attachmentId"]
            att = service.users().messages().attachments().get(userId="me", messageId=msg_id, id=att_id).execute()
            file_data = base64.urlsafe_b64decode(att["data"])
            filename = part.get("filename") or f"adjunto_{att_id}"
            path = pathlib.Path("downloads") / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(file_data)
            out["attachments"].append(str(path))

        # Si tiene subpartes
        for sub in part.get("parts", []):
            _walk_parts(sub, msg_id)

    _walk_parts(payload, msg_id)
    return out


if __name__ == "__main__":
    # 🔹 Ejemplo: autenticación + lista mails + elegir uno + leer contenido
    service = authenticate_gmail()
    mails = list_messages(service, max_results=5)

    print("Correos disponibles:")
    for i, m in enumerate(mails, 1):
        print(f"{i}. {m['from']} — {m['subject']}")

    choice = int(input("Elegí un correo: ")) - 1
    msg_id = mails[choice]["id"]

    content = get_message_content(service, msg_id)
    print("\n=== TEXTO ===")
    print(content["text"][:1000])
    print("\n=== ADJUNTOS ===")
    print(content["attachments"])
