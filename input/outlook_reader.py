# input/outlook_reader.py
import base64
import os
import requests
import msal
from dotenv import load_dotenv
from typing import Any, Dict, List
load_dotenv()
CLIENT_ID = os.getenv("OUTLOOK_CLIENT_ID", os.getenv('OUTLOOK_CLIENT_ID'))
AUTHORITY = "https://login.microsoftonline.com/common"
SCOPES = ["Mail.Read","User.Read"]

TOKEN_CACHE = "outlook_token.json"


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
