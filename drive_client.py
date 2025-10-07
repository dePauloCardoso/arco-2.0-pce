import io
import json
import mimetypes
import os
from typing import Optional

from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


def authenticate_google_drive(client_secret_file: str, scopes: list[str], token_file: Optional[str] = None) -> any:
    creds = None

    client_secret_data: dict | None = None
    if client_secret_file and os.path.exists(client_secret_file):
        try:
            with open(client_secret_file, "r", encoding="utf-8") as f:
                client_secret_data = json.load(f)
        except Exception:
            client_secret_data = None

    # Tenta conta de serviço se o arquivo corresponder
    if isinstance(client_secret_data, dict) and client_secret_data.get("type") == "service_account":
        creds = ServiceAccountCredentials.from_service_account_file(client_secret_file, scopes=scopes)

    # Senão, tenta OAuth com token.json. Se faltar campos, tenta montar manualmente usando client_secret.json (web/installed)
    if creds is None and token_file and os.path.exists(token_file):
        # Primeiro, trate token.json como possível chave de conta de serviço (como em drive_api.py)
        try:
            with open(token_file, "r", encoding="utf-8") as f:
                token_data = json.load(f)
        except Exception:
            token_data = {}

        if isinstance(token_data, dict) and token_data.get("type") == "service_account":
            creds = ServiceAccountCredentials.from_service_account_file(token_file, scopes=scopes)
        else:
            # Tenta formato OAuth padrão
            try:
                creds = UserCredentials.from_authorized_user_file(token_file, scopes=scopes)
            except Exception:
                # Monta manualmente a partir de token.json + client_secret.json (installed/web)
                client_id = None
                client_secret = None
                token_uri = None
                if isinstance(client_secret_data, dict):
                    if isinstance(client_secret_data.get("installed"), dict):
                        info = client_secret_data["installed"]
                        client_id = info.get("client_id")
                        client_secret = info.get("client_secret")
                        token_uri = info.get("token_uri")
                    elif isinstance(client_secret_data.get("web"), dict):
                        info = client_secret_data["web"]
                        client_id = info.get("client_id")
                        client_secret = info.get("client_secret")
                        token_uri = info.get("token_uri")

                access_token = token_data.get("access_token") or token_data.get("token")
                refresh_token = token_data.get("refresh_token")
                creds = UserCredentials(
                    token=access_token,
                    refresh_token=refresh_token,
                    token_uri=token_uri,
                    client_id=client_id,
                    client_secret=client_secret,
                    scopes=scopes,
                )
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())

    if creds is None:
        raise ValueError(
            "Credenciais do Google não encontradas ou inválidas. Forneça um client_secret.json (service account ou OAuth) e/ou token.json válidos."
        )

    service = build("drive", "v3", credentials=creds)
    return service


def _find_file_in_folder(service: any, folder_id: str, file_name: str, shared_drive_id: Optional[str]) -> Optional[str]:
    query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
    kwargs = {
        "q": query,
        "spaces": "drive",
        "fields": "files(id, name)",
        "supportsAllDrives": True,
    }
    if shared_drive_id:
        kwargs.update({
            "corpora": "drive",
            "driveId": shared_drive_id,
            "includeItemsFromAllDrives": True,
        })
    response = service.files().list(**kwargs).execute()
    files = response.get("files", [])
    if files:
        return files[0].get("id")
    return None


def upload_or_update_bytes(
    service: any,
    folder_id: str,
    shared_drive_id: Optional[str],
    file_name: str,
    content_bytes: bytes,
    mime_type: Optional[str] = None,
) -> str:
    if not mime_type:
        guessed, _ = mimetypes.guess_type(file_name)
        mime_type = guessed or "application/octet-stream"

    file_id = _find_file_in_folder(service, folder_id, file_name, shared_drive_id)

    media = MediaIoBaseUpload(io.BytesIO(content_bytes), mimetype=mime_type, resumable=True)

    if file_id:
        request = service.files().update(
            fileId=file_id,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )
    else:
        metadata = {"name": file_name, "parents": [folder_id]}
        request = service.files().create(
            body=metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )

    response_upload = None
    while response_upload is None:
        status, response_upload = request.next_chunk()
        # status may be None near completion; no need to print here
    return response_upload.get("id")
