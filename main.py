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
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/acheflow")
COLL_PROJETOS = os.getenv("MONGO_COLL_PROJETOS", "projetos")
COLL_TAREFAS = os.getenv("MONGO_COLL_TAREFAS", "tarefas")
COLL_FUNCIONARIOS = os.getenv("MONGO_COLL_FUNCIONARIOS", "funcionarios")

TASKS_API_BASE           = os.getenv("TASKS_API_BASE", "https://ache-flow-back.onrender.com").rstrip("/")
TASKS_API_PROJECTS_PATH  = os.getenv("TASKS_API_PROJECTS_PATH", "/projetos")
TASKS_API_TASKS_PATH     = os.getenv("TASKS_API_TASKS_PATH", "/tarefas")
TASKS_API_TOKEN_PATH     = os.getenv("TASKS_API_TOKEN_PATH", "/token")
TASKS_API_USERNAME       = os.getenv("TASKS_API_USERNAME")
TASKS_API_PASSWORD       = os.getenv("TASKS_API_PASSWORD")
ACHEFLOW_MAIN_API_TOKEN = os.getenv("ACHEFLOW_API_TOKEN") 

TIMEOUT_S = 90
GENERIC_USER_AGENT = "ache-flow-ia/1.0"
PDF_USER_AGENT     = GENERIC_USER_AGENT

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
    "https.acheflow.web.app", # Exemplo de site no ar
    "https.acheflow.firebaseapp.com" # Exemplo de site no ar
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

def extract_gsheet_id(url: str) -> Optional[str]:
    """Extrai o ID de uma URL do Google Sheets."""
    if not url: return None
    # Regex para .../d/SHEET_ID/.... (IDs têm tipicamente 44 caracteres)
    m = re.search(r"/d/([a-zA-Z0-9_-]{40,})", url)
    if m:
        return m.group(1)
    return None

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

def mongo():
    if not MONGO_URI: 
        raise RuntimeError("MONGO_URI não foi definida")
    client = MongoClient(MONGO_URI)
    try:
        db = client.get_default_database()
        
        db.command("ping") 
        return db
    except Exception as e:
        raise RuntimeError(f"Não foi possível conectar ao MongoDB. Verifique a MONGO_URI e o firewall do Atlas. Erro: {e}")

def _parse_date_robust(date_str: str) -> str:
    """
    Tenta converter uma data (ex: DD/MM/AAAA ou DD-MM-AAAA) para AAAA-MM-DD.
    Se falhar, retorna o original (assumindo que já está AAAA-MM-DD).
    """
    date_str = (date_str or "").strip()
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date().isoformat()
    except ValueError:
        try:
            return datetime.strptime(date_str, "%d-%m-%Y").date().isoformat()
        except ValueError:
            return date_str

def _get_employee_map() -> Dict[str, str]:
    """
    Helper para buscar todos os funcionários e criar um mapa de 
    { "id_do_funcionario": "Nome Sobrenome" }.
    """
    try:
        # Busca apenas os campos necessários
        employees_raw = mongo()[COLL_FUNCIONARIOS].find({}, {"nome": 1, "sobrenome": 1, "_id": 1})
        employees = [sanitize_doc(x) for x in employees_raw]
        
        # O _id já é uma string por causa do sanitize_doc
        return {
            str(emp.get("_id")): f"{emp.get('nome', '')} {emp.get('sobrenome', '')}".strip()
            for emp in employees
            if emp.get("_id")
        }
    except Exception as e:
        print(f"Erro ao buscar mapa de funcionários: {e}")
        return {}

# Em main.py, substitua a função _enrich_doc_with_responsavel:

def _enrich_doc_with_responsavel(doc: Dict[str, Any], employee_map: Dict[str, str]) -> Dict[str, Any]:
    """
    Substitui 'responsavel' (que é um ID sanitizado) por 'responsavel_nome'.
    """
    resp_id_key = None
    if "responsavel" in doc:
        resp_id_key = "responsavel"
    elif "responsavel_id" in doc:
        resp_id_key = "responsavel_id"
    else:
        doc["responsavel_nome"] = "(Responsável não atribuído)"
        return doc

    resp_id = str(doc.get(resp_id_key)) 
    
    if resp_id and resp_id != "None":
        if resp_id in employee_map:
            doc["responsavel_nome"] = employee_map[resp_id]
        else:
            doc["responsavel_nome"] = f"(ID não encontrado: {resp_id})"
    else:
        doc["responsavel_nome"] = "(Responsável não atribuído)"

    if resp_id_key in doc:
        del doc[resp_id_key]
        
    return doc
# === FIM DAS NOVAS FUNÇÕES HELPER ===

# =========================
# Helpers de Download (PDF/XLSX)
# =========================
def fetch_bytes(url: str) -> bytes:
    if not url: raise ValueError("URL ausente")
    u = url.strip()
    if not u.lower().startswith(("http://", "https://")): raise ValueError("URL inválida")
    
    headers = {"User-Agent": GENERIC_USER_AGENT}
    
    is_google_export = "docs.google.com" in u and "export?format=xlsx" in u
    
    if is_google_export:
        with requests.get(u, timeout=TIMEOUT_S, allow_redirects=True, headers=headers, stream=True) as r:
            r.raise_for_status()
            
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "openxmlformats-officedocument.spreadsheetml.sheet" not in ctype:
                raise ValueError(f"URL do Google não retornou um XLSX. Content-Type: {ctype}")
            
            content_bytes = io.BytesIO()
            for chunk in r.iter_content(chunk_size=8192):
                content_bytes.write(chunk)
            return content_bytes.getvalue()
    else:
        r = requests.get(u, timeout=TIMEOUT_S, allow_redirects=True, headers=headers)
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
                if not url and isinstance(val, str) and val.strip().lower().startswith(("http://", "https://")): url = val.strip()
                record[header] = url or (val if val is not None else "")
            else: record[header] = val if val is not None else ""
        rows.append(record)
    return pd.DataFrame(rows)

def extract_full_pdf_text(pdf_bytes: bytes) -> str:
    """
    Extrai TODO o texto de todas as páginas de um PDF.
    Retorna o texto bruto, preservando as quebras de linha originais.
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text_all = "\n".join((page.extract_text() or "") for page in pdf.pages if page.extract_text())
        return text_all 
    except Exception as e:
        print(f"Erro ao extrair texto completo do PDF: {e}")
        return ""
    
def extract_hidden_message(raw_text: str) -> str:
    """
    Encontra palavras com letras maiúsculas "fora do lugar" e
    extrai as letras maiúsculas de cada palavra, unindo-as com espaços
    para formar a frase secreta.
    
    CORRIGIDO (V6): Ignora siglas (UPPER) e inícios de frase
    (incluindo frases com múltiplas palavras maiúsculas, como "Na Etapa...").
    
    (NOTA: Esta função não é mais a ideal, a IA fará isso)
    """
    if not raw_text:
        return ""
    
    secret_parts = []
    
    text = re.sub(r"([a-zA-Z])\s*\n\s*([a-zA-Z])", r"\1\2", raw_text)
    text = re.sub(r"\s+", " ", text)
    
    words = text.split()
    
    sentence_enders = (".", "!", "?", ":")

    for i, word in enumerate(words):
        clean_word = word.rstrip(f",;:.\"'{''.join(sentence_enders)}")
        
        if not clean_word or len(clean_word) < 1:
            continue

        if clean_word.islower():
            continue

        if clean_word.isupper():
            continue

        is_start_of_sentence = False
        if i == 0:
            is_start_of_sentence = True # É a primeira palavra do texto
        else:
            prev_word_raw = words[i-1]
            prev_word_clean = prev_word_raw.rstrip(f",;:.\"'{''.join(sentence_enders)}")

            if any(prev_word_raw.endswith(ender) for ender in sentence_enders):
                is_start_of_sentence = True
            
            elif not prev_word_clean.islower():
                if prev_word_clean.istitle() or prev_word_clean.isupper():
                    is_start_of_sentence = True
        
        if is_start_of_sentence and clean_word.istitle():
            continue

        caps = re.sub(r"[^A-Z]", "", word) 
        
        if caps:
            secret_parts.append(caps)

    return " ".join(secret_parts)
    
# =========================
# Auth (Falar com API Render)
# (Omitido por brevidade)
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
# (Omitido por brevidade)
# =========================
class CreateTaskItem(BaseModel):
    titulo: str; descricao: Optional[str] = None; responsavel: Optional[str] = None
    deadline: Optional[str] = None; doc_ref: Optional[str] = None; prazo_data: Optional[str] = None

async def create_project_api(client: httpx.AsyncClient, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{TASKS_API_BASE}{TASKS_API_PROJECTS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    payload = pick(data, ["nome", "responsavel_id", "situacao", "prazo", "descricao", "categoria"])
    
    r = await client.post(url, json=payload, headers=auth_headers, timeout=TIMEOUT_S)
    
    if r.status_code not in (200, 201):
        raise httpx.HTTPStatusError(
            f"Erro da API do Render: {r.status_code}", 
            request=r.request, 
            response=r
        )
    
    return r.json()

async def create_task_api(client: httpx.AsyncClient, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{TASKS_API_BASE}{TASKS_API_TASKS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    payload = pick(data, ["nome", "projeto_id", "responsavel_id", "descricao", "prioridade", "status", "prazo", "documento_referencia", "concluido"])
    payload = {k: v for k, v in payload.items() if v is not None}
    
    r = await client.post(url, json=payload, headers=auth_headers, timeout=TIMEOUT_S)
    
    if r.status_code not in (200, 201):
        raise httpx.HTTPStatusError(
            f"Erro da API do Render: {r.status_code}", 
            request=r.request, 
            response=r
        )
        
    return r.json()

async def find_project_id_by_name(client: httpx.AsyncClient, projeto_nome: str) -> Optional[str]:
    url = f"{TASKS_API_BASE}{TASKS_API_PROJECTS_PATH}"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    r = await client.get(url, headers=auth_headers, timeout=TIMEOUT_S)
    if r.status_code != 200: return None
    try:
        items = r.json()
        if isinstance(items, list):
            hit = next((p for p in items if str(p.get("nome")).lower() == projeto_nome.lower()), None)
            return (hit or {}).get("_id") if hit else None
    except Exception: return None
    return None

async def list_funcionarios(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    url = f"{TASKS_API_BASE}/funcionarios"
    auth_headers = await get_api_auth_headers(client, use_json=True)
    r = await client.get(url, headers=auth_headers, timeout=TIMEOUT_S)
    if r.status_code != 200: return []
    try: return r.json() if isinstance(r.json(), list) else []
    except Exception: return []

# Em main.py, substitua a função inteira (aprox. linha 583)

async def resolve_responsavel_id(
    client: httpx.AsyncClient, 
    nome_ou_email: Optional[str],
    default_user_id: Optional[str] = None
) -> Optional[str]:
    """
    Encontra um ID de funcionário.
    Prioridade:
    1. Busca por nome/email.
    2. Se não encontrar E nome_ou_email for 'eu', usa o default_user_id.
    3. Se não encontrar E nome_ou_email estiver VAZIO, usa o default_user_id (se chat) ou o token (se import).
    """
    nome_ou_email = (nome_ou_email or "").strip()
    
    fallback_id = default_user_id if default_user_id else _token_cache.get("user_id")

    if nome_ou_email.lower() in ("eu", "mim", "me"):
        return default_user_id if default_user_id else _token_cache.get("user_id")

    if not nome_ou_email: 
        return fallback_id

    pessoas = await list_funcionarios(client)
    key = nome_ou_email.lower()
    
    if len(key) == 24 and all(c in '0123456789abcdef' for c in key):
        if any(p.get("_id") == key for p in pessoas): return key
    for p in pessoas:
        if str(p.get("email") or "").lower() == key: return p.get("_id")
    for p in pessoas:
        full = f"{str(p.get('nome') or '').lower()} {str(p.get('sobrenome') or '').lower()}".strip()
        if full == key or str(p.get('nome') or '').lower() == key: return p.get("_id")
    
    return fallback_id

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
    user_id: Optional[str],
    create_project_flag: int,
    projeto_situacao: Optional[str],
    projeto_prazo: Optional[str],
    projeto_responsavel: Optional[str],
    projeto_descricao: Optional[str],
    projeto_categoria: Optional[str],
    xlsx_url: Optional[str],
    file_bytes: Optional[bytes]
) -> Dict[str, Any]:
    
    try:
        if file_bytes:
            df = xlsx_bytes_to_dataframe_preserving_hyperlinks(file_bytes)
        elif xlsx_url:
            xbytes = fetch_bytes(xlsx_url) 
            df = xlsx_bytes_to_dataframe_preserving_hyperlinks(xbytes)
        else:
            raise HTTPException(status_code=400, detail={"erro": "Nenhuma fonte de dados (file, xlsx_url, google_sheet_url) fornecida."})
    except Exception as e:
        raise HTTPException(status_code=422, detail={"erro": f"Falha ao processar planilha: {str(e)}"})

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
                # --- CORREÇÃO: Passa 'user_id' para 'resolve_responsavel_id' ---
                proj_resp_id = await resolve_responsavel_id(client, projeto_responsavel, default_user_id=user_id)
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
            # --- CORREÇÃO: Passa 'user_id' para 'resolve_responsavel_id' ---
            resp_id = await resolve_responsavel_id(client, item.get("responsavel"), default_user_id=user_id)
            try:
                # --- CORREÇÃO: Removido 'data_inicio' ---
                created.append(await create_task_api(client, {
                    "nome": item["titulo"], "descricao": item["descricao"],
                    "projeto_id": resolved_project_id, "responsavel_id": resp_id,
                    "prazo": item["prazo"],
                    "documento_referencia": item["doc_ref"],
                    "status": "não iniciada", "prioridade": "média"
                }))
            except Exception as e:
                errors.append({"erro": str(e), "titulo": item["titulo"]})
    return {"mode": "assigned", "projeto_id": resolved_project_id, "criados": created, "total": len(created), "erros": errors}

# =========================
# Lógica da IA (do vertex_ai_service.py)
# =========================
# Em main.py, substitua a variável SYSTEM_PROMPT inteira por esta:

SYSTEM_PROMPT = """
Você é o "Ache", um assistente de produtividade virtual da plataforma Ache Flow.
Sua missão é ajudar colaboradores(as) como {nome_usuario} (email: {email_usuario}, id: {id_usuario}) a entender e gerenciar tarefas, projetos e prazos.

====================================================================
REGRAS DE RESPOSTA (MAIS IMPORTANTE)
====================================================================
**REGRA DE OURO: NÃO INVENTE DADOS.**
- Se uma ferramenta for usada e retornar uma lista vazia (como `[]`), um valor 0, ou "não encontrado", sua resposta DEVE ser "Não encontrei [o que foi pedido]".
- NUNCA, SOB NENHUMA CIRCUNSTÂNCIA, invente nomes de projetos, tarefas ou pessoas.

**REGRA ANTI-CÓDIGO: VOCÊ É UM ASSISTENTE, NÃO UM PROGRAMADOR.**
- Sua resposta para o usuário NUNCA deve ser um trecho de código (`print()`, JSON, etc).
- Sua tarefa é: 1º *chamar* a ferramenta, 2º *esperar* o resultado, e 3º *depois* formular uma resposta em português.
- Se você responder com `print(defaultapi.createproject...)`, você falhou gravemente.
- Você NUNCA deve inventar prefixos como `defaultapi` ou `print()`.

1.  **REGRA DE FERRAMENTAS (PRIORIDADE 1):** Sua prioridade MÁXIMA é usar ferramentas.
    * **REFORÇO CRÍTICO:** Ao 'criar', 'atualizar' ou 'importar', você está PROIBIDO de responder "Projeto criado" ou "Tarefa atualizada" sem ANTES chamar a ferramenta e receber a confirmação.
    * **REGRA DE IMPORTAÇÃO (DESAMBIGUAÇÃO):** Se o usuário pedir para 'criar um projeto' E TAMBÉM fornecer uma URL (.xlsx ou Google Sheets) na *mesma* mensagem, ignore a ferramenta `create_project` e use APENAS a ferramenta `import_project_from_url`.
    * NUNCA pergunte "Posso buscar?". Apenas execute a ferramenta e retorne a resposta.
    * Sempre que usar uma ferramenta, resuma o resultado em português claro. NUNCA mostre nomes de funções (como 'list_all_projects') ou código.

2.  **REGRA DE CONHECIMENTO GERAL (PRIORIDADE 2):** Se a pergunta NÃO PUDER ser respondida por NENHUMA ferramenta, use seu conhecimento pré-treinado.
    * Você NÃO precisa de acesso à internet para isso. Responda diretamente.

3.  **REGRA DE AMBIGUIDADE:** Se uma pergunta for ambígua (ex: "o que é um diferencial?"), responda com seu conhecimento geral.

4.  **REGRA DE FORMATAÇÃO:**
    * Fale sempre em português (PT-BR), de forma simpática.
    * NUNCA use markdown, asteriscos (*), negrito, ou blocos de código.
    * Use hífens simples para listas.

====================================================================
REGRAS DE COLETA DE DADOS (PARA CRIAR/EDITAR)
====================================================================
Sua tarefa é preencher os argumentos para as ferramentas.

**REGRA PRINCIPAL:** Sempre tente extrair os parâmetros (como nome, prazo, etc.) da ÚLTIMA MENSAGEM DO USUÁRIO.

- **SE** você conseguir extrair TODOS os argumentos **OBRIGATÓRIOS** (como `nome`, `prazo`, `situacao`, `responsavel`):
    - **NÃO PERGUNTE NADA MAIS.** Chame a ferramenta imediatamente.
    - Use `None` (ou simplesmente omita) para quaisquer argumentos **OPCIONAIS** (como `projeto_descricao` ou `projeto_categoria`) que não foram fornecidos.
- **SE** algum argumento **OBRIGATÓRIO** estiver faltando:
    - **AÍ SIM,** pergunte APENAS pelos argumentos **OBRIGATÓRIOS** que faltam.
    - **NÃO** pergunte por argumentos opcionais.

**REGRA DE AÇÃO DIRETA (A MAIS IMPORTANTE):**
- **NUNCA** responda ao usuário com uma "confirmação" antes de agir.
- **ERRADO (NÃO FAÇA ISSO):** O usuário diz "prazo 31-12-2025". Você responde: "OK. Criando projeto com prazo 31-12-2025."
- **CORRETO (FAÇA ISSO):** O usuário diz "prazo 31-12-2025". Você *imediatamente* chama a ferramenta `create_project(...)` em segundo plano. Somente *depois* que a ferramenta retornar `{{"ok": True, "data": ...}}`, você responde ao usuário: "Projeto criado com sucesso! 🙂"
- Se o usuário disser "isso" ou "sim" para confirmar, isso é sua instrução para **CHAMAR A FERRAMENTA**, não para falar mais.

**1. PARA: `create_project` (Criar Projeto ÚNICO):**
* **Argumentos OBRIGATÓRIOS:** `nome`, `situacao`, `prazo` (DD-MM-AAAA), `responsavel` (nome ou email).
* **Argumentos Opcionais:** `descricao`, `categoria`.

**2. PARA: `import_project_from_url` (Importar Projeto):**
* **Argumentos OBRIGATÓRIOS:** `xlsx_url`, `projeto_nome`, `projeto_situacao`, `projeto_prazo`, `projeto_responsavel`.
* **Argumentos Opcionais:** `projeto_descricao`, `projeto_categoria`.

**3. PARA: `update_project` (Atualizar Projeto):**
* **Se faltar:** O `patch` (o que mudar). O nome ou ID do projeto geralmente já é conhecido.
* **Exemplo:** Se o usuário disser "vamos alterar o projeto Pega-Pega", você DEVE perguntar: "Claro! O que você gostaria de mudar no projeto 'Pega-Pega' (nome, situação, prazo, etc.)?"
* **NÃO** pergunte pelo ID se o nome já foi dado. A ferramenta encontrará pelo nome.

(O resto das regras de update_project, update_task e DADOS DE CONTEXTO permanecem iguais)
====================================================================
DADOS DE CONTEXTO
====================================================================
-   **Usuário Atual:** {nome_usuario} (ID: {id_usuario})
-   **Interpretação de "Eu":** Se o usuário disser "eu", "para mim", "sou eu", use a palavra "eu" no campo 'responsavel'. A ferramenta `resolve_responsavel_id` entenderá.
-   **Datas:** Hoje é {data_hoje}. "Este mês" vai de {inicio_mes} até {fim_mes}.
-   **Formato de Data:** Sempre que pedir uma data, peça em **DD-MM-AAAA**. Você deve converter internamente para **AAAA-MM-DD** antes de usar nas ferramentas.
"""

def list_all_projects(top_k: int = 500) -> List[Dict[str, Any]]:
    employee_map = _get_employee_map() # Pega o mapa de funcionários
    projects_raw = mongo()[COLL_PROJETOS].find({}).sort("prazo", 1).limit(top_k)
    projects_clean = [sanitize_doc(p) for p in projects_raw]
    # Enriquece cada projeto com o nome do responsável
    return [_enrich_doc_with_responsavel(p, employee_map) for p in projects_clean]

def list_all_tasks(top_k: int = 2000) -> List[Dict[str, Any]]:
    employee_map = _get_employee_map() # Pega o mapa de funcionários
    tasks_raw = mongo()[COLL_TAREFAS].find({}).sort("prazo", 1).limit(top_k)
    tasks_clean = [sanitize_doc(t) for t in tasks_raw]
    # Enriquece cada tarefa com o nome do responsável
    return [_enrich_doc_with_responsavel(t, employee_map) for t in tasks_clean]

def list_all_funcionarios(top_k: int = 500) -> List[Dict[str, Any]]:
    # Esta função não precisa de enriquecimento, ela é a fonte
    return [sanitize_doc(x) for x in mongo()[COLL_FUNCIONARIOS].find({}).sort("nome", 1).limit(top_k)]

def list_tasks_by_deadline_range(start: str, end: str, top_k: int = 50) -> List[Dict[str, Any]]:
    employee_map = _get_employee_map() # Pega o mapa de funcionários
    tasks_raw = mongo()[COLL_TAREFAS].find({"prazo": {"$gte": start, "$lte": end}}).sort("prazo", 1).limit(top_k)
    tasks_clean = [sanitize_doc(t) for t in tasks_raw]
    # Enriquece cada tarefa com o nome do responsável
    return [_enrich_doc_with_responsavel(t, employee_map) for t in tasks_clean]

def list_projects_by_status(status: str, top_k: int = 50) -> List[Dict[str, Any]]:
    status_norm = (status or "").strip()
    if not status_norm:
        return []

    rx = {"$regex": f"^{re.escape(status_norm)}$", "$options": "i"}
        
    employee_map = _get_employee_map() # Pega o mapa de funcionários
    projects_raw = mongo()[COLL_PROJETOS].find({"situacao": rx}).sort("prazo", 1).limit(top_k)
    projects_clean = [sanitize_doc(p) for p in projects_raw]
    return [_enrich_doc_with_responsavel(p, employee_map) for p in projects_clean]

def upcoming_deadlines(days: int = 14, top_k: int = 50) -> List[Dict[str, Any]]:
    today_iso = iso_date(today()); limit_date = (today() + timedelta(days=days)).date().isoformat()
    
    employee_map = _get_employee_map() # Pega o mapa de funcionários
    tasks_raw = mongo()[COLL_TAREFAS].find({"prazo": {"$gte": today_iso, "$lte": limit_date}}).sort("prazo", 1).limit(top_k)
    tasks_clean = [sanitize_doc(t) for t in tasks_raw]
    # Enriquece cada tarefa com o nome do responsável
    return [_enrich_doc_with_responsavel(t, employee_map) for t in tasks_clean]

def count_all_projects() -> int:
    """Conta o número total de projetos no banco."""
    try:
        return mongo()[COLL_PROJETOS].count_documents({})
    except Exception as e:
        print(f"Erro ao contar projetos: {e}")
        return -1

def count_projects_by_status(status: str) -> int:
    """Conta projetos com base em um status (ex: 'em andamento')."""
    status_norm = (status or "").strip()
    if not status_norm:
        return 0 # Retorna 0 se o status for vazio

    try:
        rx = {"$regex": f"^{re.escape(status_norm)}$", "$options": "i"}
        
        return mongo()[COLL_PROJETOS].count_documents({"situacao": rx})
    except Exception as e:
        print(f"Erro ao contar projetos por status: {e}")
        return -1
    
async def update_project(
    patch: Dict[str, Any],
    project_id: Optional[str] = None, 
    project_name: Optional[str] = None,
    user_id: Optional[str] = None
) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        resolved_pid = project_id
        if not resolved_pid and project_name:
            resolved_pid = await find_project_id_by_name(client, project_name)
        
        if not resolved_pid:
            raise ValueError(f"Projeto '{project_name or project_id}' não encontrado ou ID/Nome não fornecido.")

        auth_headers = await get_api_auth_headers(client, use_json=True)
        
        allowed = {"nome", "descricao", "categoria", "situacao"}
        payload = {k: v for k, v in patch.items() if k in allowed and v is not None}

        if "prazo" in patch and patch["prazo"]:
            payload["prazo"] = _parse_date_robust(patch["prazo"])
        
        if "responsavel" in patch and patch["responsavel"]:
            resp_id = await resolve_responsavel_id(client, patch["responsavel"], default_user_id=user_id)
            payload["responsavel_id"] = resp_id

        if not payload: raise ValueError("Nenhum campo válido para atualizar ('patch' vazio).")
        
        url = f"{TASKS_API_BASE}/projetos/{resolved_pid}" 
        resp = await client.put(url, json=payload, headers=auth_headers)
        resp.raise_for_status(); return resp.json()

async def create_project(doc: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        data = pick(doc, ["nome", "situacao", "descricao", "categoria"])
        if not data.get("nome"): raise ValueError("nome é obrigatório")
        
        prazo_raw = doc.get("prazo")
        if prazo_raw:
            data["prazo"] = _parse_date_robust(prazo_raw)

        responsavel_str = doc.get("responsavel") 
        
        resolved_id = await resolve_responsavel_id(
            client, 
            responsavel_str, 
            default_user_id=user_id
        )
        data["responsavel_id"] = resolved_id
        
        return await create_project_api(client, data)

# --- CORREÇÃO: Removido 'data_inicio' ---            
async def create_task(doc: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        data = pick(doc, ["nome", "projeto_id", "responsavel_id", "descricao", "prioridade", "status", "prazo", "documento_referencia", "concluido"])
        if not data.get("nome"): raise ValueError("nome é obrigatório")
        return await create_task_api(client, data)

# --- CORREÇÃO: Removido 'data_inicio' ---
async def update_task(tid: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    async with httpx.AsyncClient() as client:
        auth_headers = await get_api_auth_headers(client, use_json=True)
        allowed = {"nome", "descricao", "prioridade", "status", "prazo", "responsavel_id", "projeto_id"}
        payload = {k: v for k, v in patch.items() if k in allowed and v is not None}
        if not payload: raise ValueError("patch vazio")
        url = f"{TASKS_API_BASE}/tarefas/{tid}" 
        resp = await client.put(url, json=payload, headers=auth_headers)
        resp.raise_for_status(); return resp.json()

def list_tasks_by_status(status: str, top_k: int = 50) -> List[Dict[str, Any]]:
    """Lista tarefas com base em um status exato (ex: 'não iniciada', 'concluída')."""
    status_norm = (status or "").strip()
    if not status_norm:
        return []
    
    rx = {"$regex": f"^{re.escape(status_norm)}$", "$options": "i"}
    
    employee_map = _get_employee_map()
    tasks_raw = mongo()[COLL_TAREFAS].find({"status": rx}).sort("prazo", 1).limit(top_k)
    tasks_clean = [sanitize_doc(t) for t in tasks_raw]
    return [_enrich_doc_with_responsavel(t, employee_map) for t in tasks_clean]

def count_tasks_by_status(status: str) -> int:
    """Conta tarefas com base em um status exato (ex: 'não iniciada', 'concluída')."""
    status_norm = (status or "").strip()
    if not status_norm:
        return 0
    
    rx = {"$regex": f"^{re.escape(status_norm)}$", "$options": "i"}
    
    try:
        return mongo()[COLL_TAREFAS].count_documents({"status": rx})
    except Exception as e:
        print(f"Erro ao contar tarefas por status: {e}")
        return -1

def find_project_responsavel(project_name: str) -> str:
    """Encontra o nome do responsável por um projeto específico."""
    project_name_norm = (project_name or "").strip()
    if not project_name_norm:
        return "Nome do projeto não fornecido."

    rx = {"$regex": f"^{re.escape(project_name_norm)}$", "$options": "i"}
    proj = mongo()[COLL_PROJETOS].find_one({"nome": rx})
    
    if not proj:
        return f"Projeto '{project_name_norm}' não encontrado."

    proj_clean = sanitize_doc(proj)
    
    employee_map = _get_employee_map()
    enriched_proj = _enrich_doc_with_responsavel(proj_clean, employee_map) 
    
    return enriched_proj.get("responsavel_nome", "(Responsável não definido)")

def count_tasks_in_project(project_name: str) -> int:
    """Conta o número de tarefas associadas a um projeto específico."""
    project_name_norm = (project_name or "").strip()
    if not project_name_norm:
        return -1

    try:
        rx_proj = {"$regex": f"^{re.escape(project_name_norm)}$", "$options": "i"}
        proj = mongo()[COLL_PROJETOS].find_one({"nome": rx_proj}, {"_id": 1})
        
        if not proj:
            return -2

        project_id = proj.get("_id")
        
        task_query = {"projeto": DBRef(collection=COLL_PROJETOS, id=project_id)}
        
        return mongo()[COLL_TAREFAS].count_documents(task_query)
    except Exception as e:
        print(f"Erro ao contar tarefas no projeto: {e}")
        return -3

def count_projects_by_responsavel(responsavel_id_str: Optional[str]) -> int:
    """Conta projetos de um responsável específico pelo ID."""
    if not responsavel_id_str:
        return -1
    try:
        resp_oid = to_oid(responsavel_id_str)
        query = {"responsavel": DBRef(collection=COLL_FUNCIONARIOS, id=resp_oid)}
        return mongo()[COLL_PROJETOS].count_documents(query)
    except Exception as e:
        print(f"Erro ao contar projetos por responsável: {e}")
        return -2

def list_projects_by_responsavel(responsavel_id_str: Optional[str], top_k: int = 50) -> List[Dict[str, Any]]:
    """Lista projetos de um responsável específico pelo ID."""
    if not responsavel_id_str:
        return []
    try:
        resp_oid = to_oid(responsavel_id_str)
        query = {"responsavel": DBRef(collection=COLL_FUNCIONARIOS, id=resp_oid)}
        
        employee_map = _get_employee_map()
        projects_raw = mongo()[COLL_PROJETOS].find(query).sort("prazo", 1).limit(top_k)
        projects_clean = [sanitize_doc(p) for p in projects_raw]
        return [_enrich_doc_with_responsavel(p, employee_map) for p in projects_clean]
    except Exception as e:
        print(f"Erro ao listar projetos por responsável: {e}")
        return []

# --- ADIÇÃO: Nova função 'list_tasks_by_project_name' ---
def list_tasks_by_project_name(project_name: str, top_k: int = 10) -> List[Dict[str, Any]]:
    """Lista as primeiras N tarefas de um projeto específico, buscando pelo nome do projeto."""
    project_name_norm = (project_name or "").strip()
    if not project_name_norm:
        return []

    try:
        # 1. Encontrar o ID do projeto pelo nome
        rx_proj = {"$regex": f"^{re.escape(project_name_norm)}$", "$options": "i"}
        proj = mongo()[COLL_PROJETOS].find_one({"nome": rx_proj}, {"_id": 1})
        
        if not proj:
            return [] # Projeto não encontrado, retorna lista vazia

        project_id = proj.get("_id")
        
        # 2. Buscar tarefas que referenciam esse ObjectId
        task_query = {"projeto": DBRef(collection=COLL_PROJETOS, id=project_id)}
        
        employee_map = _get_employee_map()
        tasks_raw = mongo()[COLL_TAREFAS].find(task_query).sort("prazo", 1).limit(top_k)
        tasks_clean = [sanitize_doc(t) for t in tasks_raw]
        return [_enrich_doc_with_responsavel(t, employee_map) for t in tasks_clean]
        
    except Exception as e:
        print(f"Erro ao listar tarefas do projeto: {e}")
        return []
# --- FIM DA ADIÇÃO ---


async def import_project_from_url_tool(
    xlsx_url: str, 
    projeto_nome: str, 
    projeto_situacao: str, 
    projeto_prazo: str, 
    projeto_responsavel: str,
    projeto_descricao: Optional[str] = None,
    projeto_categoria: Optional[str] = None,
    user_id: Optional[str] = None
) -> Dict[str, Any]:
    effective_xlsx_url = xlsx_url
    if "docs.google.com/spreadsheets" in (xlsx_url or "").lower():
        sheet_id = extract_gsheet_id(xlsx_url)
        if sheet_id:
            effective_xlsx_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    prazo_formatado = _parse_date_robust(projeto_prazo)

    return await tasks_from_xlsx_logic(
        projeto_id=None, projeto_nome=projeto_nome,
        user_id=user_id,
        create_project_flag=1, projeto_situacao=projeto_situacao,
        projeto_prazo=prazo_formatado, projeto_responsavel=projeto_responsavel,
        projeto_descricao=projeto_descricao, projeto_categoria=projeto_categoria,
        xlsx_url=effective_xlsx_url, # Passa a URL tratada
        file_bytes=None
    )

async def get_pdf_content_from_url_impl(url: str) -> str:
    """
    Puxa o texto completo de um PDF de uma URL.
    Usado pela IA para "ler" um documento.
    """
    try:
        pdf_bytes = fetch_pdf_bytes(url)
        return extract_full_pdf_text(pdf_bytes)
    except Exception as e:
        return f"Erro ao processar PDF da URL: {str(e)}"

async def solve_pdf_enigma_from_url_impl(url: str) -> str:
    """
    Puxa um PDF de uma URL e tenta resolver o enigma da "frase secreta".
    (NOTA: Esta função ainda usa a lógica antiga de Python, que pode falhar)
    """
    try:
        pdf_bytes = fetch_pdf_bytes(url)
        full_text = extract_full_pdf_text(pdf_bytes)
        if not full_text:
            return "Não foi possível extrair texto do PDF."
        
        message = extract_hidden_message(full_text)
        return message if message else "Nenhuma mensagem secreta encontrada."
        
    except Exception as e:
        return f"Erro ao processar enigma do PDF: {str(e)}"

def toolset() -> Tool:
    fns = [
        FunctionDeclaration(name="count_all_projects", description="Conta e retorna o número total de projetos.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="count_projects_by_status", description="Conta e retorna o número de projetos por status (ex: 'em andamento').", parameters={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}),
        FunctionDeclaration(name="list_all_projects", description="Lista todos os projetos.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_all_tasks", description="Lista todas as tarefas.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_all_funcionarios", description="Lista todos os funcionários.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_tasks_by_deadline_range", description="Lista tarefas com prazo entre datas (YYYY-MM-DD).", parameters={"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]}),
        FunctionDeclaration(name="upcoming_deadlines", description="Lista tarefas com prazo vencendo nos próximos X dias.", parameters={"type": "object", "properties": {"days": {"type": "integer"}}, "required": ["days"]}),
        FunctionDeclaration(name="list_projects_by_status", description="Lista projetos por status (ex: 'em andamento').", parameters={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}),
        FunctionDeclaration(name="count_my_projects", description="Conta quantos projetos são de responsabilidade do usuário ATUAL.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="list_my_projects", description="Lista os projetos de responsabilidade do usuário ATUAL.", parameters={"type": "object", "properties": {}}),
        FunctionDeclaration(name="update_project", description="Atualiza campos de um projeto.", parameters={"type": "object", "properties": {"project_id": {"type": "string"}, "patch": {"type": "object", "properties": {"nome": {"type": "string"}, "situacao": {"type": "string"}, "prazo": {"type": "string"}}}}, "required": ["project_id", "patch"]}),        
        FunctionDeclaration(name="create_project", description="Cria um novo projeto.", parameters={"type": "object", "properties": {"nome": {"type": "string"}, "responsavel": {"type": "string"}, "situacao": {"type": "string"}, "prazo": {"type": "string"}}, "required": ["nome", "responsavel", "situacao", "prazo"]}),
        FunctionDeclaration(name="create_task", description="Cria uma nova tarefa.", parameters={"type": "object", "properties": {"nome": {"type": "string"}, "projeto_id": {"type": "string"}, "responsavel_id": {"type": "string"}, "prazo": {"type": "string"}, "status": {"type": "string"}}, "required": ["nome", "projeto_id", "responsavel_id", "prazo", "status"]}),
        FunctionDeclaration(name="update_task", description="Atualiza campos de uma tarefa.", parameters={"type": "object", "properties": {"task_id": {"type": "string"}, "patch": {"type": "object", "properties": {"nome": {"type": "string"}, "status": {"type": "string"}, "prazo": {"type": "string"}, "responsavel_id": {"type": "string"}}}}, "required": ["task_id", "patch"]}),
        FunctionDeclaration(name="import_project_from_url", description="Cria um projeto e importa tarefas a partir de uma URL de arquivo .xlsx ou Google Sheets.", parameters={"type": "object", "properties": {"xlsx_url": {"type": "string"}, "projeto_nome": {"type": "string"}, "projeto_situacao": {"type": "string"}, "projeto_prazo": {"type": "string"}, "projeto_responsavel": {"type": "string"}, "projeto_descricao": {"type": "string"}, "projeto_categoria": {"type": "string"}}, "required": ["xlsx_url", "projeto_nome", "projeto_situacao", "projeto_prazo", "projeto_responsavel"]}),
        FunctionDeclaration(name="list_tasks_by_status", description="Lista tarefas com base em um status exato (ex: 'não iniciada', 'concluída').", parameters={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}),
        FunctionDeclaration(name="count_tasks_by_status", description="Conta tarefas com base em um status exato (ex: 'não iniciada', 'concluída').", parameters={"type": "object", "properties": {"status": {"type": "string"}}, "required": ["status"]}),
        FunctionDeclaration(name="find_project_responsavel", description="Encontra o nome do responsável por um projeto (busca por nome exato).", parameters={"type": "object", "properties": {"project_name": {"type": "string"}}, "required": ["project_name"]}),
        FunctionDeclaration(name="count_tasks_in_project", description="Conta o número de tarefas em um projeto (busca por nome exato).", parameters={"type": "object", "properties": {"project_name": {"type": "string"}}, "required": ["project_name"]}),
        FunctionDeclaration(name="list_tasks_by_project_name", description="Lista as N primeiras tarefas de um projeto (busca por nome exato).", parameters={"type": "object", "properties": {"project_name": {"type": "string"}, "top_k": {"type": "integer"}}, "required": ["project_name"]}),
        FunctionDeclaration(name="get_pdf_content_from_url", description="Extrai e retorna todo o texto de um arquivo PDF hospedado em uma URL. Use isso para 'ler' ou 'analisar' um PDF.", parameters={"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}),
        FunctionDeclaration(name="solve_pdf_enigma_from_url", description="Encontra uma 'frase secreta' escondida em um PDF (letras maiúsculas fora de lugar) a partir de uma URL.", parameters={"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}),
    ]
    return Tool(function_declarations=fns)

async def exec_tool(name: str, args: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        if name == "count_all_projects": return {"ok": True, "data": count_all_projects()}
        if name == "count_projects_by_status": return {"ok": True, "data": count_projects_by_status(args["status"])}
        if name == "list_all_projects": return {"ok": True, "data": list_all_projects(args.get("top_k", 500))}
        if name == "list_all_tasks": return {"ok": True, "data": list_all_tasks(args.get("top_k", 2000))}
        if name == "list_all_funcionarios": return {"ok": True, "data": list_all_funcionarios(args.get("top_k", 500))}
        if name == "list_tasks_by_deadline_range": return {"ok": True, "data": list_tasks_by_deadline_range(args["start"], args["end"], args.get("top_k", 50))}
        if name == "upcoming_deadlines": return {"ok": True, "data": upcoming_deadlines(args.get("days", 14), args.get("top_k", 50))}
        if name == "count_my_projects": return {"ok": True, "data": count_projects_by_responsavel(user_id)}
        if name == "list_my_projects": return {"ok": True, "data": list_projects_by_responsavel(user_id, args.get("top_k", 50))}
        if name == "list_projects_by_status": return {"ok": True, "data": list_projects_by_status(args["status"], args.get("top_k", 50))}
        if name == "list_tasks_by_status": return {"ok": True, "data": list_tasks_by_status(args["status"], args.get("top_k", 50))}
        if name == "count_tasks_by_status": return {"ok": True, "data": count_tasks_by_status(args["status"])}
        if name == "find_project_responsavel": return {"ok": True, "data": find_project_responsavel(args["project_name"])}
        if name == "count_tasks_in_project": return {"ok": True, "data": count_tasks_in_project(args["project_name"])}
        if name == "list_tasks_by_project_name": return {"ok": True, "data": list_tasks_by_project_name(args["project_name"], args.get("top_k", 10))}
        if name == "update_project": return {"ok": True, "data": await update_project(patch=args.get("patch", {}), project_id=args.get("project_id"), project_name=args.get("project_name"), user_id=user_id)}              
        if name == "create_project": return {"ok": True, "data": await create_project(args, user_id=user_id)}
        if name == "create_task": return {"ok": True, "data": await create_task(args)}
        if name == "update_task": return {"ok": True, "data": await update_task(args["task_id"], args.get("patch", {}))}
        if name == "import_project_from_url": return {"ok": True, "data": await import_project_from_url_tool(**args, user_id=user_id)}
        if name == "get_pdf_content_from_url": return {"ok": True, "data": await get_pdf_content_from_url_impl(args["url"])}
        if name == "solve_pdf_enigma_from_url": return {"ok": True, "data": await solve_pdf_enigma_from_url_impl(args["url"])}
        return {"ok": False, "error": f"função desconhecida: {name}"}
    except Exception as e:
        detail = str(e)
        if isinstance(e, httpx.HTTPStatusError):
            try: 
                err_json = e.response.json() # --- BUG CORRIGIDO AQUI ---
                detail = err_json.get("detail", err_json.get("erro", str(e)))
            except Exception: 
                detail = e.response.text
        return {"ok": False, "error": detail}
# --- FIM DA CORREÇÃO ---

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
            role_from_frontend = h.sender # <-- CORREÇÃO (era h.get("role", ...))
            gemini_role = "model" if role_from_frontend == "ai" else "user"
            
            text_content = h.content.conteudo_texto # <-- CORREÇÃO (era h.get("content", ...))
    
            contents.append(Content(role=gemini_role, parts=[Part.from_text(text_content)]))
    contents.append(Content(role="user", parts=[Part.from_text(user_msg)]))
    tools = [toolset()]
    tool_steps: List[Dict[str, Any]] = []
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
            result = await exec_tool(name, args, id_usuario)
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
    id_usuario: Optional[str] = Form(None),
    create_project_flag: int = Form(0),
    projeto_situacao: Optional[str] = Form(None),
    projeto_prazo: Optional[str] = Form(None),
    projeto_responsavel: Optional[str] = Form(None),
    projeto_descricao: Optional[str] = Form(None),
    projeto_categoria: Optional[str] = Form(None),
    xlsx_url: Optional[str] = Form(None),
    google_sheet_url: Optional[str] = Form(None), # <-- NOVO CAMPO
    file: Optional[UploadFile] = File(None)
):
    file_bytes = await file.read() if file else None
    effective_xlsx_url = xlsx_url

    sources = sum(p is not None for p in [file_bytes, xlsx_url, google_sheet_url])
    
    if sources > 1:
        raise HTTPException(status_code=400, detail={"erro": "Use apenas UMA fonte de dados: 'google_sheet_url' OU 'xlsx_url' OU 'file'."})
    elif sources == 0:
        raise HTTPException(status_code=400, detail={"erro": "Nenhuma fonte de dados fornecida (google_sheet_url, xlsx_url, ou file)."})

    if google_sheet_url:
        sheet_id = extract_gsheet_id(google_sheet_url)
        if not sheet_id:
            raise HTTPException(status_code=400, detail={"erro": "Google Sheet URL inválida. Não foi possível extrair o ID."})
        
        effective_xlsx_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

    result = await tasks_from_xlsx_logic(
        projeto_id=projeto_id, projeto_nome=projeto_nome,
        user_id=id_usuario,
        create_project_flag=create_project_flag, projeto_situacao=projeto_situacao,
        projeto_prazo=projeto_prazo, projeto_responsavel=projeto_responsavel,
        projeto_descricao=projeto_descricao, projeto_categoria=projeto_categoria,
        xlsx_url=effective_xlsx_url, # Passa a URL (original ou GSheet-export)
        file_bytes=file_bytes       # Passa o arquivo (se houver)
    )
    return result

@app.post("/ai/chat-with-pdf")
async def ai_chat_with_pdf(
    pergunta: str = Form(...),
    file: UploadFile = File(...),
    nome_usuario: Optional[str] = Form(None),
    email_usuario: Optional[str] = Form(None),
    id_usuario: Optional[str] = Form(None),
    _ = Depends(require_api_key)
):
    """
    Endpoint de chat que "lê" um PDF enviado e usa o conteúdo
    como contexto para responder a pergunta do usuário.
    
    CORRIGIDO: A ferramenta 'solve_pdf_enigma' agora usa a própria IA
    para resolver o enigma, em vez da função extract_hidden_message.
    """
    try:
        pdf_bytes = await file.read()
        
        raw_pdf_text = extract_full_pdf_text(pdf_bytes)
        
        rag_text = clean_pdf_text(raw_pdf_text)
        
        if not raw_pdf_text and not rag_text:
            raise HTTPException(status_code=422, detail="Não foi possível extrair texto do PDF enviado.")

        pdf_tools_list = [
            FunctionDeclaration(
                name="solve_pdf_enigma",
                description="Resolve o enigma de 'frase secreta' do PDF. Use esta ferramenta se o usuário perguntar sobre 'enigma', 'frase secreta', 'código', 'mensagem escondida', etc.",
                parameters={"type": "object", "properties": {}}
            )
        ]
        pdf_tool = Tool(function_declarations=pdf_tools_list)

        contexto_prompt = f"""
        Use o CONTEÚDO DO DOCUMENTO abaixo E/OU a ferramenta 'solve_pdf_enigma'
        para responder a PERGUNTA DO USUÁRIO.

        - Se a pergunta for sobre o 'enigma', 'frase secreta', 'código' ou 'mensagem escondida', USE A FERRAMENTA 'solve_pdf_enigma'.
        - Para TODAS as outras perguntas (ex: 'quantos textos', 'qual o resumo'), responda APENAS com base no CONTEÚDO DO DOCUMENTO.

        ==================== CONTEÚDO DO DOCUMENTO (para RAG) ====================
        {rag_text[:10000]} 
        ===============================================================

        PERGUNTA DO USUÁRIO: {pergunta}
        """

        data_hoje, (inicio_mes, fim_mes) = iso_date(today()), month_bounds(today())
        nome_usuario_fmt = nome_usuario or "você"
        email_usuario_fmt = email_usuario or "email.desconhecido"
        id_usuario_fmt = id_usuario or "id.desconhecido"
        
        system_prompt_filled = SYSTEM_PROMPT.format(
            nome_usuario=nome_usuario_fmt, email_usuario=email_usuario_fmt, id_usuario=id_usuario_fmt,
            data_hoje=data_hoje, inicio_mes=inicio_mes, fim_mes=fim_mes,
        )
        
        model = init_model(system_prompt_filled)
        contents = [Content(role="user", parts=[Part.from_text(contexto_prompt)])]
        
        resp = model.generate_content(contents, tools=[pdf_tool])
        
        if (
            resp.candidates and resp.candidates[0].content and 
            resp.candidates[0].content.parts and 
            getattr(resp.candidates[0].content.parts[0], "function_call", None)
        ):
            call = resp.candidates[0].content.parts[0].function_call
            
            if call.name == "solve_pdf_enigma":
                                
                enigma_prompt = f"""
                Você é um especialista em decifrar enigmas.
                Abaixo está o texto completo de um documento PDF.
                Uma mensagem secreta está escondida nele, usando letras maiúsculas "fora de lugar" (no meio de palavras minúsculas ou em palavras TitleCase no meio de frases).

                Sua tarefa é encontrar essas letras e formar a mensagem secreta.
                
                REGRAS IMPORTANTES:
                1. Ignore siglas (palavras 100% maiúsculas, como ISO ou SUS).
                2. Ignore letras maiúsculas normais no início de frases (como "Para", "A coordenação", "No controle").
                3. Junte as letras que encontrar para formar PALAVRAS de verdade (ex: 'EQ', 'UI', 'PE' devem ser 'EQUIPE').
                4. Retorne APENAS a frase secreta completa, e nada mais.

                ==================== CONTEÚDO DO DOCUMENTO (para Enigma) ====================
                {raw_pdf_text} 
                ===============================================================

                Qual é a mensagem secreta?
                """
                
                enigma_contents = [Content(role="user", parts=[Part.from_text(enigma_prompt)])]
                
                enigma_resp = model.generate_content(enigma_contents, tools=[]) # Sem ferramentas
                
                message = "Nenhuma mensagem secreta encontrada."
                if enigma_resp.candidates and enigma_resp.candidates[0].content and enigma_resp.candidates[0].content.parts:
                    message = getattr(enigma_resp.candidates[0].content.parts[0], "text", "") or message
                    message = message.strip().replace("A frase secreta é: ", "").replace("A mensagem secreta é: ", "")

                final_answer = _normalize_answer(f"A frase secreta encontrada no arquivo é: {message}", nome_usuario_fmt)
                
                response_data = {
                    "tipo_resposta": "TEXTO_PDF",
                    "conteudo_texto": final_answer,
                    "dados": [{"tool_used": "solve_pdf_enigma (via LLM)"}] # Log
                }
                return JSONResponse(response_data)

        final_text = ""
        if resp.candidates and resp.candidates[0].content and resp.candidates[0].content.parts:
            final_text = getattr(resp.candidates[0].content.parts[0], "text", "") or ""

        final_text = re.sub(r"(?i)(aguarde( um instante)?|só um momento|apenas um instante)[^\n]*", "", final_text).strip()
        final_answer = _normalize_answer(final_text, nome_usuario_fmt)
        
        response_data = {
            "tipo_resposta": "TEXTO_PDF",
            "conteudo_texto": final_answer,
            "dados": [{"tool_used": "RAG"}] # Log
        }
        return JSONResponse(response_data)

    except Exception as e:
        raise e
                
@app.post("/pdf/extract-text")
async def pdf_extract_text(file: UploadFile = File(...), _ = Depends(require_api_key)):
    """
    Endpoint utilitário: Envie um PDF e receba o texto completo.
    """
    pdf_bytes = await file.read()
    text = extract_full_pdf_text(pdf_bytes)
    return {"filename": file.filename, "text": text}

@app.post("/pdf/solve-enigma")
async def pdf_solve_enigma(file: UploadFile = File(...), _ = Depends(require_api_key)):
    """
    Endpoint utilitário: Envie um PDF e receba a "frase secreta".
    (NOTA: Este endpoint ainda usa a função Python antiga, que pode falhar)
    """
    pdf_bytes = await file.read()
    text = extract_full_pdf_text(pdf_bytes)
    message = extract_hidden_message(text)
    return {"filename": file.filename, "message": message or "Nenhuma mensagem encontrada."}

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