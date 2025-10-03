
# api.py (grid-templates aligned)
from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Form
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
from input.gmail_reader import authenticate_gmail, list_messages as gmail_list, get_message_content as gmail_get
from input.outlook_reader import authenticate_outlook, get_token, list_messages_outlook, get_message_body, get_attachments

UPLOAD_DIR = pathlib.Path("uploads")
TEMPLATES_DIR = pathlib.Path("templates")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="TransformAR API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------- Grid Template Model (alineado al front) ---------

class GridColumn(BaseModel):
    col: str = Field(..., description="Letra de columna (A,B,C,...)")
    title: str = Field(..., description="Encabezado (fila 1)")
    example: Optional[str] = Field(
        "",
        description=(
            "Transformación que debe aplicarse al campo indicado por 'title' (fila 2)"
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
            return acc or 10**9

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

Method = Literal["document", "gmail", "outlook", "text"]

class ProcessRequest(BaseModel):
    method: Method
    template_id: str
    manual: Optional[ManualDocSelection] = None
    gmail: Optional[GmailSelection] = None
    outlook: Optional[OutlookSelection] = None
    text: Optional[str] = None

# --------- Helpers ---------

def _slug(s: str) -> str:
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r'[^a-zA-Z0-9]+', '_', s).strip('_').lower()
    return s or 'col'

def _compile_grid_to_instructions(gt: GridTemplate) -> Dict[str, str]:
    """
    Dado un grid del front, genera las instrucciones necesarias para Qwen:
    - extract_instr: qué campos buscar y de qué tipo son.
    - transform_instr: plan de transformaciones a ejecutar con interpret_with_qwen.
    """

    def _infer_type(title: str, transform_text: str) -> str:
        def _normalize(text: str) -> str:
            normalized = unicodedata.normalize('NFKD', text.lower())
            return ''.join(c for c in normalized if not unicodedata.combining(c))

        title_norm = _normalize(title)
        transform_norm = _normalize(transform_text)

        date_kw = ["fecha", "date", "dia"]
        money_kw = ["monto", "importe", "total", "precio", "amount", "valor"]
        number_kw = ["cantidad", "cant", "qty", "numero", "unidad", "units"]

        if any(kw in title_norm for kw in date_kw) or re.search(r"\b(aaaa|yyyy)\b", transform_norm):
            return "fecha"
        if any(kw in title_norm for kw in money_kw) or re.search(r"\b(usd|eur|ars|\$)\b", transform_text.lower()):
            return "monto"
        if any(kw in title_norm for kw in number_kw) or "numero" in transform_norm:
            return "numero"
        return "texto"

    fields = []
    transforms = []

    for col in gt.columns:
        key = _slug(col.title)
        transform_text = (col.example or "").strip()
        field_type = _infer_type(col.title, transform_text)
        fields.append(f"{key}: {field_type}")

        if transform_text:
            transforms.append(
                f"Aplicá {transform_text} al campo {key} ({col.title})"
            )

    extract_instr = "Extrae: " + ", ".join(fields)
    transform_instr = (
        ". ".join(transforms)
        if transforms
        else "No apliques transformaciones adicionales."
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
            metas.append(TemplateMeta(id=d["id"], name=d["name"], description=d.get("description","")))
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
def gmail_messages(limit: int = Query(10, ge=1, le=50)):
    service = authenticate_gmail()
    mails = gmail_list(service, max_results=limit)
    return {"messages": [{"id": m["id"], "from": m.get("from"), "subject": m.get("subject") } for m in mails]}

@app.get("/input/gmail/messages/{msg_id}")
def gmail_message_detail(msg_id: str):
    service = authenticate_gmail()
    content = gmail_get(service, msg_id)
    return {"text": content.get("text",""), "attachments": content.get("attachments",[])}

# Outlook
@app.get("/input/outlook/messages")
def outlook_messages(limit: int = Query(10, ge=1, le=50)):
    app_o = authenticate_outlook()
    token = get_token(app_o)
    mails = list_messages_outlook(token, top=limit)
    out = []
    for m in mails:
        sender = (m.get("sender") or {}).get("emailAddress", {}).get("address")
        out.append({"id": m.get("id"), "from": sender, "subject": m.get("subject"), "hasAttachments": m.get("hasAttachments", False)})
    return {"messages": out}

@app.get("/input/outlook/messages/{msg_id}")
def outlook_message_detail(msg_id: str):
    app_o = authenticate_outlook()
    token = get_token(app_o)
    text = get_message_body(token, msg_id)
    atts = get_attachments(token, msg_id)
    return {"text": text or "", "attachments": atts or []}

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
def process(req: ProcessRequest):
    gtpl = _load_template_grid(req.template_id)
    compiled = _compile_grid_to_instructions(gtpl)
    extract_instr = compiled["extract_instr"]
    transform_instr = compiled["transform_instr"]

    text: Optional[str] = None

    if req.method == "text":
        if not req.text: raise HTTPException(400, "Falta 'text'")
        text = req.text

    elif req.method == "document":
        if not req.manual: raise HTTPException(400, "Falta 'manual'")
        path = UPLOAD_DIR / req.manual.file_id
        if not path.exists(): raise HTTPException(404, "Archivo no encontrado")
        result = _pipeline_from_file(path, extract_instr, transform_instr)
        return {"result": result, "compiled": compiled}

    elif req.method == "gmail":
        if not req.gmail: raise HTTPException(400, "Falta 'gmail'")
        service = authenticate_gmail()
        content = gmail_get(service, req.gmail.message_id)
        if req.gmail.use_text or not content.get("attachments"):
            text = content.get("text","")
            if not text: raise HTTPException(400, "El correo no tiene texto utilizable.")
        else:
            atts = content.get("attachments") or []
            idx = req.gmail.attachment_index or 0
            if idx < 0 or idx >= len(atts): raise HTTPException(400, "attachment_index inválido")
            result = _pipeline_from_file(pathlib.Path(atts[idx]), extract_instr, transform_instr)
            return {"result": result, "compiled": compiled}

    elif req.method == "outlook":
        if not req.outlook: raise HTTPException(400, "Falta 'outlook'")
        app_o = authenticate_outlook()
        token = get_token(app_o)
        if req.outlook.use_text:
            text = get_message_body(token, req.outlook.message_id)
            if not text: raise HTTPException(400, "No se pudo obtener texto del correo.")
        else:
            files = get_attachments(token, req.outlook.message_id)
            if files:
                idx = req.outlook.attachment_index or 0
                if idx < 0 or idx >= len(files): raise HTTPException(400, "attachment_index inválido")
                result = _pipeline_from_file(pathlib.Path(files[idx]), extract_instr, transform_instr)
                return {"result": result, "compiled": compiled}
            else:
                text = get_message_body(token, req.outlook.message_id)

    else:
        raise HTTPException(400, "Método inválido")

    result = _pipeline_from_text(text, extract_instr, transform_instr)
    return {"result": result, "compiled": compiled}
