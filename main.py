# main.py
import os, io, re, time, asyncio
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urlparse, parse_qs, unquote, quote, urljoin
from datetime import datetime, timedelta, date

import requests
import httpx
import pandas as pd
import pdfplumber
from fastapi import FastAPI, UploadFile, File, Form, Header, HTTPException, Depends
from fastapi.responses import JSONResponse
# --- ADICIONADO IMPORT FALTANTE ---
from fastapi.middleware.cors import CORSMiddleware
# --- FIM DA ADIÇÃO ---
from pydantic import BaseModel, Field

# Vertex AI (Imports corretos)
from vertexai import init as vertex_init
from vertexai.generative_models import (
    GenerativeModel,
    Tool,
    FunctionDeclaration,
    Part,
    Content,
)

# Mongo
from pymongo import MongoClient, ReturnDocument
from bson import ObjectId, DBRef

# XLSX
from openpyxl import load_workbook
from openpyxl.utils import range_boundaries

from dotenv import load_dotenv
load_dotenv()

class MessageContent(BaseModel):
    tipo_resposta: str
    conteudo_texto: str
    dados: Optional[List[Any]] = None

class HistoryMessage(BaseModel):
    sender: str # 'user', 'ai', ou 'system'
    content: MessageContent

class ChatRequest(BaseModel):
    pergunta: str
    history: Optional[List[HistoryMessage]] = None
    nome_usuario: Optional[str] = None
    email_usuario: Optional[str] = None
    id_usuario: Optional[str] = None

# =========================
# Config
# =========================
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
APPLICATION_NAME = os.getenv("GOOGLE_CLOUD_APLICATION", "ai-service")
GEMINI_MODEL_ID = os.getenv("GEMINI_MODEL_ID", "gemini-2.0-flash")

API_KEY = os.getenv("API_KEY") 
# --- REMOVIDO: NÃO VAMOS MAIS USAR MONGO DIRETAMENTE ---
# MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/acheflow")
# COLL_PROJETOS = os.getenv("MONGO_COLL_PROJETOS", "projetos")
# COLL_TAREFAS = os.getenv("MONGO_COLL_TAREFAS", "tarefas")
# COLL_FUNCIONARIOS = os.getenv("MONGO_COLL_FUNCIONARIOS", "funcionarios")
# --- FIM DA REMOÇÃO ---

TASKS_API_BASE           = os.getenv("TASKS_API_BASE", "https://ache-flow-back.onrender.com").rstrip("/")
TASKS_API_PROJECTS_PATH  = os.getenv("TASKS_API_PROJECTS_PATH", "/projetos")
TASKS_API_TASKS_PATH     = os.getenv("TASKS_API_TASKS_PATH", "/tarefas")
TASKS_API_TOKEN_PATH     = os.getenv("TASKS_API_TOKEN_PATH", "/token")
TASKS_API_USERNAME       = os.getenv("TASKS_API_USERNAME")
TASKS_API_PASSWORD       = os.getenv("TASKS_API_PASSWORD")
ACHEFLOW_MAIN_API_TOKEN = os.getenv("ACHEFLOW_API_TOKEN") 

TIMEOUT_S = int(os.getenv("TIMEOUT_S", "90"))
GENERIC_USER_AGENT = os.getenv("GENERIC_USER_AGENT", "ache-flow-ia/1.0 (+https://tistto.com.br)")
PDF_USER_AGENT     = os.getenv("PDF_USER_AGENT", GENERIC_USER_AGENT)

MAX_TOOL_STEPS = 6
DEFAULT_TOP_K = 8

# =========================
# FastAPI App (Único)
# =========================
app = FastAPI(title=f"{APPLICATION_NAME} (Serviço Unificado de IA e Importação)", version="2.0.4") # Versão

# === ADICIONADO BLOCO CORS ===
# Lista de domínios que podem acessar sua API
origins = [
    "http://localhost:5173", # Para desenvolvimento local
    "http://localhost:5174", # Outra porta local comum
    "https://acheflow.web.app", # Exemplo de site no ar
    "https://acheflow.firebaseapp.com" # Exemplo de site no ar
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       # Permite origens específicas
    allow_credentials=True,
    allow_methods=["*"],         # Permite todos os métodos (GET, POST, etc.)
    allow_headers=["*"],         # Permite todos os cabeçalhos (x-api-key, etc.)
)
# ===============================

# =========================
# Segurança
# =========================
def require_api_key(x_api_key: Optional[str] = Header(None)):
    """Dependência do FastAPI para proteger rotas"""
    if API_KEY and (x_api_key or "") != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")

# =========================
# Handler de Erro
# =========================
@app.exception_handler(Exception)
async def all_exception_handler(request, exc):
    import traceback
    tb = traceback.format_exc()
    detail = str(exc)
    status_code = 500
    
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            detail = exc.response.json().get("detail", str(exc))
            status_code = exc.response.status_code
        except Exception:
            detail = exc.response.text
    elif isinstance(exc, HTTPException):
        detail = exc.detail
        status_code = exc.status_code

    trace_info = tb[-4000:] if "localhost" in str(request.url) or os.getenv("ENV_MODE") == "debug" else None
    return JSONResponse(
        status_code=status_code,
        content={"erro": "internal_error", "detail": detail, "trace": trace_info},
    )

# =========================
# Helpers (Datas, Mongo, etc)
# =========================
def today() -> datetime: return datetime.utcnow()
def iso_date(d: datetime) -> str: return d.date().isoformat()

# --- FUNÇÃO QUE ESTAVA FALTANDO ---
def month_bounds(d: datetime) -> Tuple[str, str]:
    first = d.replace(day=1).date().isoformat()
    if d.month == 12:
        nxt = d.replace(year=d.year + 1, month=1, day=1)
    else:
        nxt = d.replace(month=d.month + 1, day=1)
    last = (nxt - timedelta(days=1)).date().isoformat()
    return first, last
# --- FIM DA ADIÇÃO ---

def to_oid(id_str: str) -> ObjectId:
    try: return ObjectId(id_str)
    except Exception: return id_str

def pick(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys if k in d}

# main.py - Linha 160 (VERSÃO NOVA E RECURSIVA)
def sanitize_doc(data: Any) -> Any:
    # Se for um datetime ou date, converte para string
    if isinstance(data, (datetime, date)):
        return data.isoformat()
    
    # Se for um ObjectId, converte para string
    if isinstance(data, ObjectId):
        return str(data)
    
    # Se for um DBRef, converte para string (apenas o ID)
    if isinstance(data, DBRef):
        return str(data.id)
    
    # --- NOVO: Se for um dicionário (dict), chama a função recursivamente para cada valor ---
    if isinstance(data, dict):
        return {k: sanitize_doc(v) for k, v in data.items()}
    
    # --- NOVO: Se for uma lista (list), chama a função recursivamente para cada item ---
    if isinstance(data, list):
        return [sanitize_doc(item) for item in data]
    
    # Se for qualquer outro tipo (int, str, bool, None), retorna como está
    return data

# --- REMOVIDA: NÃO VAMOS MAIS USAR MONGO DIRETAMENTE ---
# def mongo():
# ...
# --- FIM DA REMOÇÃO ---

# === INÍCIO DAS NOVAS FUNÇÕES HELPER ===

async def _get_employee_map(client: httpx.AsyncClient) -> Dict[str, str]:
    """
    Helper para buscar todos os funcionários PELA API e criar um mapa de 
    { "id_do_funcionario": "Nome Sobrenome" }.
    """
    try:
        employees = await list_funcionarios(client) # Chama a nova função de API
        
        return {
            str(emp.get("_id")): f"{emp.get('nome', '')} {emp.get('sobrenome', '')}".strip()
            for emp in employees
            if emp.get("_id")
        }
    except Exception as e:
        print(f"Erro ao buscar mapa de funcionários pela API: {e}")
        return {}

def _enrich_doc_with_responsavel(doc: Dict[str, Any], employee_map: Dict[str, str]) -> Dict[str, Any]:
    """
    Substitui 'responsavel' (objeto) por 'responsavel_nome' (string).
    """
    # --- CORREÇÃO: Lendo o ID de dentro do objeto 'responsavel' ---
    resp_obj = doc.get("responsavel", {})
    if isinstance(resp_obj, dict):
        resp_id = str(resp_obj.get("id"))
    else:
        resp_id = None
    # --- FIM DA CORREÇÃO ---
    
    if resp_id and resp_id != "None":
        if resp_id in employee_map:
            # Sucesso: Encontrou o nome
            doc["responsavel_nome"] = employee_map[resp_id]
        else:
            # Falha: O ID existe mas não foi encontrado no mapa
            doc["responsavel_nome"] = f"(ID não encontrado: {resp_id})"
    else:
        # O projeto não tem responsável
        doc["responsavel_nome"] = "(Nenhum responsável)"

    # Remove o objeto original para não confundir a IA
    if "responsavel" in doc:
        del doc["responsavel"]
    if "responsavel_id" in doc: # Limpa o antigo também, por via das dúvidas
        del doc["responsavel_id"]
        
    return doc

# === FIM DAS NOVAS FUNÇÕES HELPER ===

# =========================
# Helpers de Download (PDF/XLSX)
# (Omitidos por brevidade)
# =========================
def fetch_bytes(url: str) -> bytes:
    if not url: raise ValueError("URL ausente")
    u = url.strip()
    if not u.lower().startswith(("http://", "https://")): raise ValueError("URL inválida")
    r = requests.get(u, timeout=TIMEOUT_S, allow_redirects=True, headers={"User-Agent": GENERIC_USER_AGENT})
    r.raise_for_status()
    return r.content
def _try_download_traced(url: str, timeout: int, user_agent: str):
    headers = {"User-Agent": user_agent, "Accept": "application/pdf, */*"}
    with requests.get(url, timeout=timeout, allow_redirects=True, headers=headers, stream=False) as r:
        status, ctype, final_url, content = r.status_code, (r.headers.get("Content-Type") or "").lower(), r.url, r.content or b""
        return {"status": status, "content_type": ctype, "final_url": final_url, "content": content, "is_pdf_signature": content[:4] == b"%PDF", "size": len(content)}
def normalize_sharepoint_pdf_url(u: str) -> str:
    try:
        pu = urlparse(u)
        if pu.netloc and "sharepoint.com" in pu.netloc and pu.path.endswith("/onedrive.aspx"):
            qs = parse_qs(pu.query or ""); idp = qs.get("id", [None])[0]
            if idp:
                raw_path = unquote(idp);
                if not raw_path.startswith("/"): raw_path = "/" + raw_path
                encoded_path = quote(raw_path, safe="/-_.()~")
                return f"{pu.scheme}://{pu.netloc}{encoded_path}"
    except Exception: pass
    return u
def fetch_pdf_bytes(url: str):
    if not url: raise ValueError("URL ausente para PDF")
    u = url.strip()
    if not u.lower().startswith(("http://", "https://")): raise ValueError("URL inválida para PDF")
    u_norm = normalize_sharepoint_pdf_url(u)
    def _is_pdf(ctype: str, content: bytes) -> bool: return ("pdf" in ctype) or (content[:4] == b"%PDF")
    variants = [("direct", u_norm)]
    if "download=1" not in u_norm.lower():
        sep = "&" if "?" in u_norm else "?"; variants.append(("download=1", u_norm + f"{sep}download=1"))
    if "sharepoint.com" in u_norm.lower():
        pu = urlparse(u_norm); base = f"{pu.scheme}://{pu.netloc}"; src = quote(u_norm, safe=""); variants.append(("download.aspx", f"{base}/_layouts/15/download.aspx?SourceUrl={src}"))
    last = None
    for label, cand in variants:
        r = _try_download_traced(cand, TIMEOUT_S, PDF_USER_AGENT); last = r
        if r["status"] == 200 and _is_pdf(r["content_type"], r["content"]): return r["content"]
    raise ValueError(f"Não foi possível obter PDF (último status={last['status'] if last else None}, content-type={last['content_type'] if last else None}).")
def clean_pdf_text(s: str) -> str:
    if not s: return s
    s = re.sub(r"[ \t]*\n[ \t]*", " ", s); s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"\s+([,;\.\!\?\:\)])", r"\1", s); s = re.sub(r"([,;\.\!\?\:])([^\s])", r"\1 \2", s)
    return s.strip()
def _anchor_regex_flex(label: str) -> re.Pattern:
    m = re.search(r"(?i)texto\.?(\d+)", label or "");
    if not m: return re.compile(r"(?!)")
    num = m.group(1); return re.compile(rf"(?i)\bTexto\.?{re.escape(num)}\.?\b[:\-]?\s*")
def extract_after_anchor_from_pdf(pdf_bytes: bytes, anchor_label: str, max_chars: int = 4000) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf: text_all = "\n".join((page.extract_text() or "") for page in pdf.pages)
    if not text_all.strip(): return ""
    rx = _anchor_regex_flex(anchor_label); m = rx.search(text_all)
    if not m: return ""
    start = m.end(); next_m = re.search(r"(?i)\bTexto\.?\d+\.?\b", text_all[start:])
    end = start + next_m.start() if next_m else len(text_all)
    return text_all[start:end].strip()[:max_chars].strip()
def xlsx_bytes_to_dataframe_preserving_hyperlinks(xlsx_bytes: bytes) -> pd.DataFrame:
    wb = load_workbook(io.BytesIO(xlsx_bytes), data_only=True, read_only=False); ws = wb.active
    headers: List[str] = [str(cell.value).strip() if cell.value is not None else "" for cell in next(ws.iter_rows(min_row=1, max_row=1, values_only=False))]
    hyperlink_map: Dict[str, str] = {}
    for hl in getattr(ws, "_hyperlinks", []) or []:
        try:
            ref = getattr(hl, "ref", None); target = getattr(hl, "target", None) or getattr(hl, "location", None)
            if not ref or not target: continue
            if ":" in ref:
                min_col, min_row, max_col, max_row = range_boundaries(ref)
                for r in range(min_row, max_row + 1):
                    for c in range(min_col, max_col + 1): hyperlink_map[ws.cell(row=r, column=c).coordinate] = target
            else: hyperlink_map[ref] = target
        except Exception: continue
    rows: List[Dict[str, Any]] = []
    col_idx_doc = headers.index("Documento Referência") if "Documento Referência" in headers else -1
    for row in ws.iter_rows(min_row=2, values_only=False):
        if all((c.value is None or str(c.value).strip() == "") for c in row): continue
        record: Dict[str, Any] = {}
        for i, cell in enumerate(row):
            header = headers[i] if i < len(headers) else f"col{i+1}"; val = cell.value
            if i == col_idx_doc:
                url = (getattr(cell.hyperlink, "target", None) if getattr(cell, "hyperlink", None) else None) or hyperlink_map.get(cell.coordinate)
                if not url and isinstance(val, str) and val.strip().lower().startswith(("http://", "https"://")): url = val.strip()
                record[header] = url or (val if val is not None else "")
            else: record[header] = val if val is not None else ""
        rows.append(record)
    return pd.DataFrame(rows)

# =========================
# Auth (Falar com API Render)
# (Omitido por brevidade, está correto)
# =========================
_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": 0, "user_id": None}
async def get_auth_header(client: httpx.AsyncClient) -> Dict[str, str]:
    if ACHEFLOW_MAIN_API_TOKEN:
        return {"Authorization": f"Bearer {ACHEFLOW_MAIN_API_TOKEN}"}
    now = time.time()
    if _token_cache.get("access_token") and now < _token_cache.get("expires_at", 0) - 30:
        return {"Authorization": f"Bearer {_token_cache['access_token']}"}
    if not TASKS_API_USERNAME or not TASKS_API_PASSWORD:
        raise HTTPException(status_code=401, detail="Nenhum token (ACHEFLOW_MAIN_API_TOKEN) ou credenciais (TASKS_API_USERNAME/PASSWORD) fornecidos para a API Principal.")
    token_url = urljoin(TASKS_API_BASE + "/", TASKS_API_TOKEN_PATH.lstrip("/"))
    data = {"username": TASKS_API_USERNAME, "password": TASKS_API_PASSWORD}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        resp = await client.post(token_url, data=data, headers=headers, timeout=TIMEOUT_S)
        resp.raise_for_status()
        payload = resp.json()
        access_token = payload.get("access_token")
        expires_in  = int(payload.get("expires_in") or 3600)
        _token_cache["user_id"] = payload.get("id") or _token_cache.get("user_id")
        if not access_token: raise RuntimeError(f"Resposta de token sem access_token")
        _token_cache["access_token"] = access_token
        _token_cache["expires_at"] = time.time() + expires_in
        return {"Authorization": f"Bearer {access_token}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao autenticar com a API Principal em {token_url}: {str(e)}")
async def get_api_auth_headers(client: httpx.AsyncClient, use_json: bool = True) -> Dict[str, str]:
    auth_header = await get_auth_header(client)
    if use_json:
        auth_header["Content-Type"] = "application/json"
    return auth_header

# =========================
# Lógica de Importação (do ai_api.py)
# (Funções de API e lógica de importação)
# =========================
class CreateTaskItem(BaseModel):
    titulo: str; descricao: Optional[str] = None; responsavel: Optional[str] = None
    deadline: Optional[str] = None; doc_ref: Optional[str] = None; prazo_data: Optional[str] = None

# --- ESSAS FUNÇÕES JÁ ESTAVAM CORRETAS ---
async def create_project_api(client: httpx.AsyncClient, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{TASKS_API_BASE}{TASKS_API_PROJECTS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    payload = pick(data, ["nome", "responsavel_id", "situacao", "prazo", "descricao", "categoria"])
    r = await client.post(url, json=payload, headers=auth_headers, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()
async def create_task_api(client: httpx.AsyncClient, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{TASKS_API_BASE}{TASKS_API_TASKS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    payload = pick(data, ["nome", "projeto_id", "responsavel_id", "descricao", "prioridade", "status", "data_inicio", "data_fim", "documento_referencia", "concluido"])
    payload = {k: v for k, v in payload.items() if v is not None}
    r = await client.post(url, json=payload, headers=auth_headers, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()
# --- FIM DAS FUNÇÕES CORRETAS ---


# --- INÍCIO DA REESCRITA DAS FUNÇÕES (AGORA USAM API) ---
async def find_project_id_by_name(client: httpx.AsyncClient, projeto_nome: str) -> Optional[str]:
    """Busca o ID de um projeto pelo nome, usando a API."""
    url = f"{TASKS_API_BASE}{TASKS_API_PROJECTS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    r = await client.get(url, headers=auth_headers, timeout=TIMEOUT_S)
    if r.status_code != 200: 
        return None
    try:
        items = r.json()
        if isinstance(items, list):
            hit = next((p for p in items if str(p.get("nome")).strip().lower() == projeto_nome.strip().lower()), None)
            return (hit or {}).get("_id") if hit else None
    except Exception: 
        return None
    return None

async def list_funcionarios(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Lista todos os funcionários, usando a API."""
    url = f"{TASKS_API_BASE}/funcionarios"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    r = await client.get(url, headers=auth_headers, timeout=TIMEOUT_S)
    if r.status_code != 200: 
        return []
    try: 
        return r.json() if isinstance(r.json(), list) else []
    except Exception: 
        return []
# --- FIM DA REESCRITA ---

async def resolve_responsavel_id(client: httpx.AsyncClient, nome_ou_email: Optional[str]) -> Optional[str]:
    nome_ou_email = (nome_ou_email or "").strip()
    # Se o nome for "eu", "eu mesmo", "para mim", etc., usa o ID do cache (usuário logado)
    if not nome_ou_email or nome_ou_email.lower() in ["eu", "eu mesmo", "me", "para mim", "eu sou responsável", "sou eu"]: 
        return _token_cache.get("user_id")
    
    # --- CORREÇÃO: AGORA CHAMA A FUNÇÃO DE API ---
    pessoas = await list_funcionarios(client)
    # --- FIM DA CORREÇÃO ---
    
    key = nome_ou_email.lower()
    if len(key) == 24 and all(c in '0123456789abcdef' for c in key):
        if any(p.get("_id") == key for p in pessoas): return key
    for p in pessoas:
        if str(p.get("email") or "").lower() == key: return p.get("_id")
    for p in pessoas:
        full = f"{str(p.get('nome') or '').lower()} {str(p.get('sobrenome') or '').lower()}".strip()
        if full == key or str(p.get('nome') or '').lower() == key: return p.get("_id")
    
    # Fallback: Se não achou NINGUÉM, retorna o ID do usuário logado
    return _token_cache.get("user_id")

# --- LÓGICA DE IMPORTAÇÃO (Omitida por brevidade, está correta) ---
def duration_to_date(duracao: Optional[str]) -> str:
    base = datetime.utcnow().date()
    try:
        s = (duracao or "").strip().lower(); m = re.search(r"(\d+)", s)
        n = int(m.group(1)) if m else 7
    except Exception: n = 7
    return (base + timedelta(days=n)).isoformat()
def resolve_descricao_pdf(row) -> str:
    como, docrf = str(row.get("Como Fazer") or "").strip(), str(row.get("Documento Referência") or "").strip()
    if not como or not docrf or not re.search(r"(?i)\b((?:Doc\.?\s*)?(Texto\.?\d+))\b\.?", como): return como
    try: 
        pdf_bytes = fetch_pdf_bytes(docrf)
    except Exception: 
        return como
    def _repl(m: re.Match) -> str:
        full_token, anchor = m.group(1), m.group(2)
        extracted = clean_pdf_text(extract_after_anchor_from_pdf(pdf_bytes, anchor))
        return extracted if extracted else full_token
    return re.sub(r"(?i)\b((?:Doc\.?\s*)?(Texto\.?\d+))\b\.?", _repl, como)
async def tasks_from_xlsx_logic(
    projeto_id: Optional[str],
    projeto_nome: Optional[str],
    create_project_flag: int,
    projeto_situacao: Optional[str],
    projeto_prazo: Optional[str],
    projeto_responsavel: Optional[str],
    projeto_descricao: Optional[str],
    projeto_categoria: Optional[str],
    xlsx_url: Optional[str],
    file_bytes: Optional[bytes]
) -> Dict[str, Any]:
    if xlsx_url:
        xbytes = fetch_bytes(xlsx_url)
        df = xlsx_bytes_to_dataframe_preserving_hyperlinks(xbytes)
    elif file_bytes:
        df = xlsx_bytes_to_dataframe_preserving_hyperlinks(file_bytes)
    else:
        raise HTTPException(status_code=400, detail={"erro": "Forneça 'xlsx_url' ou envie um 'file'."})
    required = {"Nome", "Como Fazer", "Documento Referência"}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise HTTPException(status_code=400, detail={"erro": f"Colunas faltando: {', '.join(missing)}"})
    df["descricao_final"] = df.apply(resolve_descricao_pdf, axis=1)
    preview: List[Dict[str, Any]] = []
    latest_task_date: Optional[datetime.date] = None
    today_date = datetime.utcnow().date()
    for _, row in df.iterrows():
        prazo_col = str(row.get("Prazo") or "").strip()
        task_date_obj: Optional[datetime.date] = None
        if prazo_col:
            try:
                if re.match(r"^\d{2}/\d{2}/\d{4}$", prazo_col): d, m, y = prazo_col.split("/"); prazo_col = f"{y}-{m}-{d}"
                task_date_obj = datetime.strptime(prazo_col, "%Y-%m-%d").date()
            except Exception: prazo_col = None
        if not task_date_obj:
            duracao_txt = str(row.get("Duração") or "")
            try:
                s = (duracao_txt or "").strip().lower(); m = re.search(r"(\d+)", s)
                n = int(m.group(1)) if m else 7; task_date_obj = today_date + timedelta(days=n)
            except Exception: task_date_obj = today_date + timedelta(days=7)
            prazo_col = task_date_obj.isoformat()
        if latest_task_date is None or task_date_obj > latest_task_date:
            latest_task_date = task_date_obj
        preview.append({
            "titulo": str(row["Nome"]), "descricao": str(row.get("descricao_final") or ""),
            "responsavel": str(row.get("Responsavel") or ""), "doc_ref": str(row.get("Documento Referência") or "").strip(),
            "prazo": prazo_col,
        })
    if not projeto_id and not projeto_nome:
        raise HTTPException(status_code=400, detail={"erro": "Para importar, forneça 'projeto_id' ou 'projeto_nome'."})
    async with httpx.AsyncClient() as client:
        resolved_project_id: Optional[str] = projeto_id
        if not resolved_project_id and projeto_nome:
            resolved_project_id = await find_project_id_by_name(client, projeto_nome)
        if not resolved_project_id:
            if create_project_flag and projeto_nome:
                proj_resp_id = await resolve_responsavel_id(client, projeto_responsavel)
                proj_prazo = (projeto_prazo or "").strip()
                if not proj_prazo:
                    proj_prazo = (latest_task_date or (today_date + timedelta(days=30))).isoformat()
                proj = await create_project_api(client, {
                    "nome": projeto_nome, "responsavel_id": proj_resp_id,
                    "situacao": (projeto_situacao or "Em planejamento").strip(),
                    "prazo": proj_prazo, "descricao": projeto_descricao, "categoria": projeto_categoria
                })
                resolved_project_id = proj.get("_id") or proj.get("id")
            else:
                raise HTTPException(status_code=404, detail={"erro": f"Projeto '{projeto_nome}' não encontrado. Para criar, envie 'create_project_flag=1'."})
        created, errors = [], []
        for item in preview:
            resp_id = await resolve_responsavel_id(client, item.get("responsavel"))
            try:
                created.append(await create_task_api(client, {
                    "nome": item["titulo"], "descricao": item["descricao"],
                    "projeto_id": resolved_project_id, "responsavel_id": resp_id,
                    "data_fim": item["prazo"], "data_inicio": today_date.isoformat(),
                    "documento_referencia": item["doc_ref"],
                    "status": "não iniciada", "prioridade": "média"
                }))
            except Exception as e:
                errors.append({"erro": str(e), "titulo": item["titulo"]})
    return {"mode": "assigned", "projeto_id": resolved_project_id, "criados": created, "total": len(created), "erros": errors}

# =========================
# Lógica da IA (do vertex_ai_service.py)
# =========================

SYSTEM_PROMPT = """
Você é o "Ache" — um assistente de produtividade virtual da plataforma Ache Flow.
Sua missão é ajudar colaboradores(as) como {nome_usuario} (email: {email_usuario}, id: {id_usuario}) a entender e gerenciar tarefas, projetos e prazos.
====================================================================
ESCOPO DE CONHECIMENTO (FOCO DUPLO)
====================================================================
1.  **FOCO PRINCIPAL (GERENCIAMENTO):** Sua prioridade MÁXIMA é responder sobre o Ache Flow. Se a pergunta for sobre 'projetos', 'tarefas', 'prazos', 'funcionários', 'criar', 'listar', ou 'atualizar', você DEVE usar as ferramentas.
2.  **FOCO SECUNDÁRIO (GERAL):** Se, e SOMENTE SE, a pergunta for CLARAMENTE sobre conhecimentos gerais (ex: 'me conte uma história', 'qual a receita de bolo de chocolate?', 'quem descobriu o brasil?'), e não puder ser respondida por nenhuma ferramenta, você pode usar seu conhecimento interno para responder.
3.  **REGRA DE PREFERÊNCIA:** Sempre dê preferência a usar uma ferramenta. Só responda com conhecimento geral se nenhuma ferramenta puder ajudar.
4.  **REGRA DE AMBIGUIDADE:** Se uma pergunta for ambígua (ex: "o que é um diferencial?", que pode ser sobre sua função OU sobre matemática), primeiro tente responder com seu conhecimento geral. Se a pergunta for *específica* sobre você (ex: "qual o *seu* diferencial?" ou "o que *você* faz?"), responda sobre sua missão.
====================================================================
REGRAS DE IMPORTAÇÃO (IMPORTANTE)
====================================================================
- O usuário pode enviar arquivos (xlsx, csv) pelo chat usando o botão de clipe.
- Se o usuário falar "quero importar" ou "enviar um arquivo", instrua-o a usar o botão de clipe.
- Se o usuário colar uma URL (http/https), sua intenção é importar daquela URL.
- Para importar (por URL), use a ferramenta `import_project_from_url`.
- **REGRA CRÍTICA:** Esta ferramenta precisa de 5 argumentos: `xlsx_url`, `projeto_nome`, `projeto_situacao`, `projeto_prazo` (YYYY-MM-DD), e `projeto_responsavel`.
- Você DEVE perguntar ao usuário por **todas** as informações que estiverem faltando ANTES de chamar a ferramenta.
- Exemplo de conversa:
    - Usuário: "cria um projeto pra mim com este arquivo: https://sharepoint.com/arquivo.xlsx"
    - Você: "Claro! Para criar este projeto, eu só preciso de mais alguns detalhes: Qual será o nome do projeto? Qual a situação dele (ex: Em andamento)? Qual o prazo final (no formato DD-MM-AAAA)? E quem será o responsável (email ou ID)?"
    - Usuário: "O nome é 'Projeto Teste', situação 'Em planejamento', prazo '31-12-2025' e eu serei o responsável."
    - (Neste caso, você usará "eu" como 'projeto_responsavel' e converterá a data para 2025-12-31 antes de chamar a ferramenta `import_project_from_url`)
====================================================================
TOM E ESTILO DE RESPOSTA
====================================================================
- Sempre fale em **português (PT-BR)**.
- Seja simpático(a), humano(a), colaborativo(a) e positivo(a).
- Fale diretamente com o(a) usuário(a) pelo nome (ex: "Oi, {nome_usuario}!"), mas **APENAS na primeira mensagem da conversa**. Não repita a saudação em todas as respostas.
- Use linguagem clara, leve e natural.
- Nunca use markdown, asteriscos (*), negrito, nem blocos de código.
- **REGRA CRÍTICA DE RESPOSTA:** Após usar uma ferramenta, você receberá os dados. Sua resposta final para o usuário deve ser um RESUMO em linguagem natural desses dados. NUNCA, em hipótese alguma, mostre o nome da ferramenta (como 'list_all_projects') ou qualquer pseudo-código (como 'print(...)') para o usuário. Apenas forneça a resposta em português.
- **REGRA DE FORMATAÇÃO DE LISTA:** Ao listar projetos ou tarefas, use listas simples (hífen e espaço). Os dados (como 'responsavel_nome' e 'prazo') já virão prontos para você. Formate a resposta de forma clara. Exemplo:
    - Projeto Phoenix (Responsável: João Silva, Prazo: 2025-12-31)
    - Projeto Kilo (Responsável: Maria Souza, Prazo: 2025-11-10)
- **NÃO PEÇA PERMISSÃO:** Você DEVE usar as ferramentas proativamente. Se uma pergunta pode ser respondida por uma ferramenta (como list_all_projects), USE A FERRAMENTA. Nunca pergunte "Quer que eu faça X?" ou "Posso buscar Y?". Apenas execute e retorne a resposta.
====================================================================
CONHECIMENTO E DADOS DISPONÍVEIS
====================================================================
As informações podem ser obtidas através das ferramentas (tools):
- Para perguntas sobre "quantos" ou "número total" de projetos, use as ferramentas 'count_all_projects' ou 'count_projects_by_status'.
- Para 'projetos ativos', 'em progresso', 'desenvolvimento', etc., use o status 'em andamento' nas ferramentas.
- list_all_projects / list_all_tasks / list_all_funcionarios
- list_tasks_by_deadline_range
- list_projects_by_status
- upcoming_deadlines
- update_project / update_task (para editar)
- create_project / create_task (para criar itens individuais)
- import_project_from_url (para importar arquivos .xlsx por URL)
====================================================================
INTERPRETAÇÃO DE DATAS (BASE)
====================================================================
- Hoje: {data_hoje}.
- Intervalo de "este mês": {inicio_mes} até {fim_mes}.
- **FORMATO DE DATA:** Sempre que pedir uma data ao usuário, peça no formato **DD-MM-AAAA**. Você deve converter internamente qualquer data DD-MM-AAAA para AAAA-MM-DD antes de usar nas ferramentas.
====================================================================
""" # --- CONTEXTO DO USUÁRIO REMOVIDO, POIS AGORA É TRATADO PELO CÓDIGO ---


# === INÍCIO DAS FUNÇÕES DE FERRAMENTA ATUALIZADAS (USANDO API) ===

async def list_all_projects(client: httpx.AsyncClient, top_k: int = 500) -> List[Dict[str, Any]]:
    """Lista todos os projetos via API."""
    url = f"{TASKS_API_BASE}{TASKS_API_PROJECTS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    r = await client.get(url, headers=auth_headers, timeout=TIMEOUT_S)
    r.raise_for_status()
    projects_raw = r.json()
    
    employee_map = await _get_employee_map(client)
    projects_clean = [sanitize_doc(p) for p in projects_raw]
    return [_enrich_doc_with_responsavel(p, employee_map) for p in projects_clean][:top_k]

async def list_all_tasks(client: httpx.AsyncClient, top_k: int = 2000) -> List[Dict[str, Any]]:
    """Lista todas as tarefas via API."""
    url = f"{TASKS_API_BASE}{TASKS_API_TASKS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    r = await client.get(url, headers=auth_headers, timeout=TIMEOUT_S)
    r.raise_for_status()
    tasks_raw = r.json()
    
    employee_map = await _get_employee_map(client)
    tasks_clean = [sanitize_doc(t) for t in tasks_raw]
    return [_enrich_doc_with_responsavel(t, employee_map) for t in tasks_clean][:top_k]

async def list_all_funcionarios(client: httpx.AsyncClient, top_k: int = 500) -> List[Dict[str, Any]]:
    """Lista todos os funcionários via API (implementação real)."""
    funcionarios = await list_funcionarios(client) # Chama a função de API
    return [sanitize_doc(f) for f in funcionarios][:top_k]

async def list_tasks_by_deadline_range(client: httpx.AsyncClient, start: str, end: str, top_k: int = 50) -> List[Dict[str, Any]]:
    """Lista tarefas por prazo via API (filtrando no Python)."""
    all_tasks = await list_all_tasks(client, top_k=2000) # Busca todas
    
    # Filtra no python
    filtered_tasks = [
        t for t in all_tasks 
        if t.get("prazo") and start <= t["prazo"] <= end
    ]
    return filtered_tasks[:top_k]

async def list_projects_by_status(client: httpx.AsyncClient, status: str, top_k: int = 50) -> List[Dict[str, Any]]:
    """Lista projetos por status via API (filtrando no Python)."""
    all_projects = await list_all_projects(client, top_k=2000) # Busca todos
    
    status_norm = (status or "").strip().lower()
    # --- REGEX ATUALIZADO ---
    if status_norm in {"em andamento", "andamento", "ativo", "em_progresso", "em progresso", "executando", "desenvolvimento"}:
        rx = re.compile(r"(andament|progres|ativo|execut|desenvolv)", re.IGNORECASE)
    else:
        rx = re.compile(re.escape(status_norm), re.IGNORECASE)
    
    # Filtra no python
    filtered_projects = [
        p for p in all_projects
        if p.get("situacao") and rx.search(p["situacao"])
    ]
    
    employee_map = await _get_employee_map(client)
    return [_enrich_doc_with_responsavel(p, employee_map) for p in filtered_projects][:top_k]

async def upcoming_deadlines(client: httpx.AsyncClient, days: int = 14, top_k: int = 50) -> List[Dict[str, Any]]:
    """Lista prazos futuros via API (filtrando no Python)."""
    today_iso = iso_date(today())
    limit_date = (today() + timedelta(days=days)).date().isoformat()
    
    all_tasks = await list_all_tasks(client, top_k=2000) # Busca todas
    
    # Filtra no python
    filtered_tasks = [
        t for t in all_tasks 
        if t.get("prazo") and today_iso <= t["prazo"] <= limit_date
    ]
    return filtered_tasks[:top_k]

async def count_all_projects(client: httpx.AsyncClient) -> int:
    """Conta todos os projetos via API."""
    try:
        projects = await list_all_projects(client, top_k=5000)
        return len(projects)
    except Exception as e:
        print(f"Erro ao contar projetos: {e}")
        return -1

async def count_projects_by_status(client: httpx.AsyncClient, status: str) -> int:
    """Conta projetos por status via API."""
    try:
        projects = await list_projects_by_status(client, status, top_k=5000)
        return len(projects)
    except Exception as e:
        print(f"Erro ao contar projetos por status: {e}")
        return -1

async def update_project(client: httpx.AsyncClient, pid: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    auth_headers = await get_api_auth_headers(client, use_json=True)
    allowed = {"nome", "descricao", "categoria", "situacao", "prazo", "responsavel_id"}
    payload = {k: v for k, v in patch.items() if k in allowed and v is not None}
    if not payload: raise ValueError("patch vazio")
    url = f"{TASKS_API_BASE}/projetos/{pid}" 
    resp = await client.put(url, json=payload, headers=auth_headers)
    resp.raise_for_status(); return resp.json()

async def create_project(client: httpx.AsyncClient, doc: Dict[str, Any]) -> Dict[str, Any]:
    data = pick(doc, ["nome", "situacao", "prazo", "descricao", "categoria"])
    if not data.get("nome"): raise ValueError("nome é obrigatório")
    
    responsavel_str = doc.get("responsavel") 
    resolved_id = await resolve_responsavel_id(client, responsavel_str)
    data["responsavel_id"] = resolved_id
    
    return await create_project_api(client, data)
    
async def create_task(client: httpx.AsyncClient, doc: Dict[str, Any]) -> Dict[str, Any]:
    data = pick(doc, ["nome", "descricao", "prioridade", "status", "data_inicio", "data_fim", "documento_referencia", "concluido"])
    if not data.get("nome"): raise ValueError("nome é obrigatório")

    # --- CORREÇÃO: RESOLVER NOME DO PROJETO PARA ID ---
    projeto_nome_ou_id = doc.get("projeto_id") # IA vai mandar nome ou ID aqui
    resolved_proj_id = None
    if projeto_nome_ou_id:
        if len(projeto_nome_ou_id) == 24 and all(c in '0123456789abcdef' for c in projeto_nome_ou_id):
             resolved_proj_id = projeto_nome_ou_id # Já é um ID
        else:
             resolved_proj_id = await find_project_id_by_name(client, projeto_nome_ou_id) # Busca pelo nome
    
    if not resolved_proj_id:
        raise ValueError(f"Projeto '{projeto_nome_ou_id}' não encontrado.")
    
    data["projeto_id"] = resolved_proj_id
    # --- FIM DA CORREÇÃO ---

    responsavel_str = doc.get("responsavel") 
    resolved_id = await resolve_responsavel_id(client, responsavel_str)
    data["responsavel_id"] = resolved_id
    
    return await create_task_api(client, data)
    
async def update_task(client: httpx.AsyncClient, tid: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    auth_headers = await get_api_auth_headers(client, use_json=True)
    allowed = {"nome", "descricao", "prioridade", "status", "data_inicio", "data_fim", "responsavel_id", "projeto_id"}
    payload = {k: v for k, v in patch.items() if k in allowed and v is not None}
    if not payload: raise ValueError("patch vazio")
    url = f"{TASKS_API_BASE}/tarefas/{tid}" 
    resp = await client.put(url, json=payload, headers=auth_headers)
    resp.raise_for_status(); return resp.json()

async def import_project_from_url_tool(
    client: httpx.AsyncClient, # Adicionado client
    xlsx_url: str, 
    projeto_nome: str, 
    projeto_situacao: str, 
    projeto_prazo: str, 
    projeto_responsavel: str,
    projeto_descricao: Optional[str] = None,
    projeto_categoria: Optional[str] = None
) -> Dict[str, Any]:
    # Esta função já usa a API, mas precisa do 'client'
    # que não estava sendo passado.
    # Vamos mockar o client dentro dela por enquanto,
    # pois a lógica de `tasks_from_xlsx_logic` é síncrona
    # e não está preparada para `await`.
    # NOTA: `tasks_from_xlsx_logic` deve ser refatorada para async
    # mas isso é uma mudança maior.
    
    # Solução rápida: `tasks_from_xlsx_logic` já usa `httpx.AsyncClient()`
    # internamente, então está OK.
    
    return await tasks_from_xlsx_logic(
        projeto_id=None, projeto_nome=projeto_nome,
        create_project_flag=1, projeto_situacao=projeto_situacao,
        projeto_prazo=projeto_prazo, projeto_responsavel=projeto_responsavel,
        projeto_descricao=projeto_descricao, projeto_categoria=projeto_categoria,
        xlsx_url=xlsx_url, file_bytes=None
    )
# --- FIM DAS FUNÇÕES DE FERRAMENTA ATUALIZADAS ---


def toolset() -> Tool:
    fns = [
        FunctionDeclaration(name="count_all_projects", description="Conta e retorna o número total de projetos.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="count_projects_by_status", description="Conta e retorna o número de projetos por status. Use 'em andamento' para status como 'ativo', 'desenvolvimento', 'em progresso', etc.", parameters={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}),
        FunctionDeclaration(name="list_all_projects", description="Lista todos os projetos.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_all_tasks", description="Lista todas as tarefas.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_all_funcionarios", description="Lista todos os funcionários.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_tasks_by_deadline_range", description="Lista tarefas com prazo entre datas (YYYY-MM-DD).", parameters={"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]}),
        FunctionDeclaration(name="upcoming_deadlines", description="Lista tarefas com prazo vencendo nos próximos X dias.", parameters={"type": "object", "properties": {"days": {"type": "integer"}}, "required": ["days"]}),
        FunctionDeclaration(name="list_projects_by_status", description="Lista projetos por status. Use 'em andamento' para status como 'ativo', 'desenvolvimento', 'em progresso', etc.", parameters={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}),
        FunctionDeclaration(name="update_project", description="Atualiza campos de um projeto.", parameters={"type": "object", "properties": {"project_id": {"type": "string"}, "patch": {"type": "object", "properties": {"nome": {"type": "string"}, "situacao": {"type": "string"}, "prazo": {"type": "string"}}}}, "required": ["project_id", "patch"]}),
        FunctionDeclaration(name="create_project", description="Cria um novo projeto.", parameters={"type": "object", "properties": {"nome": {"type": "string"}, "responsavel": {"type": "string"}, "situacao": {"type": "string"}, "prazo": {"type": "string"}}, "required": ["nome", "responsavel", "situacao", "prazo"]}),
        FunctionDeclaration(name="create_task", description="Cria uma nova tarefa.", parameters={"type": "object", "properties": {"nome": {"type": "string"}, "projeto_id": {"type": "string"}, "responsavel": {"type": "string"}, "data_fim": {"type": "string"}, "data_inicio": {"type": "string"}, "status": {"type": "string"}}, "required": ["nome", "projeto_id", "responsavel", "data_fim", "data_inicio", "status"]}),
        FunctionDeclaration(name="update_task", description="Atualiza campos de uma tarefa.", parameters={"type": "object", "properties": {"task_id": {"type": "string"}, "patch": {"type": "object", "properties": {"nome": {"type": "string"}, "status": {"type": "string"}, "data_fim": {"type": "string"}, "responsavel_id": {"type": "string"}}}}, "required": ["task_id", "patch"]}),
        FunctionDeclaration(name="import_project_from_url", description="Cria um projeto e importa tarefas a partir de uma URL de arquivo .xlsx.", parameters={"type": "object", "properties": {"xlsx_url": {"type": "string"}, "projeto_nome": {"type": "string"}, "projeto_situacao": {"type": "string"}, "projeto_prazo": {"type": "string"}, "projeto_responsavel": {"type": "string"}, "projeto_descricao": {"type": "string"}, "projeto_categoria": {"type": "string"}}, "required": ["xlsx_url", "projeto_nome", "projeto_situacao", "projeto_prazo", "projeto_responsavel"]}),
    ]
    return Tool(function_declarations=fns)

async def exec_tool(client: httpx.AsyncClient, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    """Executa a ferramenta chamada, agora passando o client."""
    try:
        # --- TODAS AS FUNÇÕES AGORA SÃO ASYNC E RECEBEM 'client' ---
        if name == "count_all_projects": return {"ok": True, "data": await count_all_projects(client)}
        if name == "count_projects_by_status": return {"ok": True, "data": await count_projects_by_status(client, args["status"])}
        if name == "list_all_projects": return {"ok": True, "data": await list_all_projects(client, args.get("top_k", 500))}
        if name == "list_all_tasks": return {"ok": True, "data": await list_all_tasks(client, args.get("top_k", 2000))}
        if name == "list_all_funcionarios": return {"ok": True, "data": await list_all_funcionarios(client, args.get("top_k", 500))}
        if name == "list_tasks_by_deadline_range": return {"ok": True, "data": await list_tasks_by_deadline_range(client, args["start"], args["end"], args.get("top_k", 50))}
        if name == "upcoming_deadlines": return {"ok": True, "data": await upcoming_deadlines(client, args.get("days", 14), args.get("top_k", 50))}
        if name == "list_projects_by_status": return {"ok": True, "data": await list_projects_by_status(client, args["status"], args.get("top_k", 50))}
        if name == "update_project": return {"ok": True, "data": await update_project(client, args["project_id"], args.get("patch", {}))}
        if name == "create_project": return {"ok": True, "data": await create_project(client, args)}
        if name == "create_task": return {"ok": True, "data": await create_task(client, args)}
        if name == "update_task": return {"ok": True, "data": await update_task(client, args["task_id"], args.get("patch", {}))}
        if name == "import_project_from_url": return {"ok": True, "data": await import_project_from_url_tool(client, **args)}
        return {"ok": False, "error": f"função desconhecida: {name}"}
    except Exception as e:
        detail = str(e)
        if isinstance(e, httpx.HTTPStatusError):
            try: 
                err_json = e.response.json()
                detail = err_json.get("detail", err_json.get("erro", str(e)))
            except Exception: 
                detail = e.response.text
        # --- MELHORIA NO LOG DE ERRO ---
        print(f"Erro ao executar ferramenta '{name}': {detail}")
        # --- FIM DA MELHORIA ---
        return {"ok": False, "error": detail}

def _normalize_answer(raw: str, nome_usuario: str) -> str:
    raw = re.sub(r"[*_`#>]+", "", raw).strip()
    if all(sym not in raw for sym in ("🙂", "😊", "👋")):
        raw = raw.rstrip(".") + " 🙂"
    return raw
def init_model(system_instruction: str) -> GenerativeModel:
    vertex_init(project=PROJECT_ID, location=LOCATION) 
    return GenerativeModel(GEMINI_MODEL_ID, system_instruction=system_instruction)
async def chat_with_tools(user_msg: str, history: Optional[List[HistoryMessage]] = None, nome_usuario: Optional[str] = None, email_usuario: Optional[str] = None, id_usuario: Optional[str] = None) -> Dict[str, Any]:
    data_hoje, (inicio_mes, fim_mes) = iso_date(today()), month_bounds(today())
    nome_usuario = nome_usuario or "você"
    email_usuario = email_usuario or "email.desconhecido"
    id_usuario = id_usuario or "id.desconhecido"
    system_prompt_filled = SYSTEM_PROMPT.format(
        nome_usuario=nome_usuario, email_usuario=email_usuario, id_usuario=id_usuario,
        data_hoje=data_hoje, inicio_mes=inicio_mes, fim_mes=fim_mes,
    )
    model = init_model(system_prompt_filled)
    contents: List[Content] = []
    if history:
        for h in history:
            role_from_frontend = h.sender
            gemini_role = "model" if role_from_frontend == "ai" else "user"
            text_content = h.content.conteudo_texto
            contents.append(Content(role=gemini_role, parts=[Part.from_text(text_content)]))
    contents.append(Content(role="user", parts=[Part.from_text(user_msg)]))
    tools = [toolset()]
    tool_steps: List[Dict[str, Any]] = []
    
    # --- NOVO: Criar um http client para ser usado por todas as ferramentas ---
    async with httpx.AsyncClient() as client:
        # Preenche o cache de autenticação antes de tudo
        await get_api_auth_headers(client) 
        
        for step in range(MAX_TOOL_STEPS):
            resp = model.generate_content(contents, tools=tools)
            calls = []
            model_response_content = None
            if resp.candidates and resp.candidates[0].content:
                model_response_content = resp.candidates[0].content
                if model_response_content.parts:
                    for part in model_response_content.parts:
                        if getattr(part, "function_call", None): 
                            calls.append(part.function_call)

            if not calls:
                final_text = ""
                if resp.candidates and resp.candidates[0].content and resp.candidates[0].content.parts:
                    final_text = getattr(resp.candidates[0].content.parts[0], "text", "") or ""
                final_text = re.sub(r"(?i)(aguarde( um instante)?|só um momento|apenas um instante)[^\n]*", "", final_text).strip()
                return {"answer": _normalize_answer(final_text, nome_usuario), "tool_steps": tool_steps}

            if model_response_content:
                contents.append(model_response_content) 
                
            for fc in calls:
                name, args = fc.name, {k: v for k, v in (fc.args or {}).items()}
                if name in ("list_projects_by_deadline_range", "list_tasks_by_deadline_range") and (not args.get("start") or not args.get("end")):
                    args["start"], args["end"] = inicio_mes, fim_mes
                
                # --- NOVO: Passa o 'client' para o executor ---
                result = await exec_tool(client, name, args)
                tool_steps.append({"call": {"name": name, "args": args}, "result": result})
                
                contents.append(Content(role="tool", parts=[Part.from_function_response(name=name, response=result)]))
            
    return {"answer": _normalize_answer("Concluí as ações solicitadas.", nome_usuario), "tool_steps": tool_steps}
# =========================
# Rotas FastAPI
# =========================
@app.post("/ai/chat")
async def ai_chat(req: ChatRequest, _=Depends(require_api_key)):
    out = await chat_with_tools(
        user_msg=req.pergunta, 
        history=req.history, 
        nome_usuario=req.nome_usuario,
        email_usuario=req.email_usuario,
        id_usuario=req.id_usuario
    )
    response_data = {
        "tipo_resposta": "TEXTO",
        "conteudo_texto": out.get("answer", "Desculpe, não consegui processar sua solicitação."),
        "dados": out.get("tool_steps")
    }
    return JSONResponse(response_data)

@app.post("/tasks/from-xlsx")
async def tasks_from_xlsx(
    _=Depends(require_api_key), 
    projeto_id: Optional[str] = Form(None),
    projeto_nome: Optional[str] = Form(None),
    create_project_flag: int = Form(0),
    projeto_situacao: Optional[str] = Form(None),
    projeto_prazo: Optional[str] = Form(None),
    projeto_responsavel: Optional[str] = Form(None),
    projeto_descricao: Optional[str] = Form(None),
    projeto_categoria: Optional[str] = Form(None),
    xlsx_url: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None)
):
    file_bytes = await file.read() if file else None
    result = await tasks_from_xlsx_logic(
        projeto_id=projeto_id, projeto_nome=projeto_nome,
        create_project_flag=create_project_flag, projeto_situacao=projeto_situacao,
        projeto_prazo=projeto_prazo, projeto_responsavel=projeto_responsavel,
        projeto_descricao=projeto_descricao, projeto_categoria=projeto_categoria,
        xlsx_url=xlsx_url, file_bytes=file_bytes
    )
    return result
@app.get("/")
def root():
    return {
        "status": "OK",
        "service": f"{APPLICATION_NAME} (Serviço Unificado de IA e Importação)",
        "model": GEMINI_MODEL_ID,
        "project": PROJECT_ID,
        "location": LOCATION,
        "main_api_target": TASKS_API_BASE,
    }