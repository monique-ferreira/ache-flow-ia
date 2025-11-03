import os
import re
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv

from fastapi import FastAPI, Body, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pymongo import MongoClient, ReturnDocument
from bson import ObjectId

# Vertex AI (Gemini)
from vertexai import init as vertex_init
from vertexai.generative_models import (
    GenerativeModel,
    Tool,
    FunctionDeclaration,
    Part,
    Content,
)

# =========================
# Config
# =========================

load_dotenv()

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
APPLICATION_NAME = os.getenv("GOOGLE_CLOUD_APLICATION", "ai-rag-service")
GEMINI_MODEL_ID = os.getenv("GEMINI_MODEL_ID", "gemini-2.0-flash")
API_KEY = os.getenv("AI_API_KEY")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/acheflow")
COLL_PROJETOS = os.getenv("MONGO_COLL_PROJETOS", "projetos")
COLL_TAREFAS = os.getenv("MONGO_COLL_TAREFAS", "tarefas")
COLL_FUNCIONARIOS = os.getenv("MONGO_COLL_FUNCIONARIOS", "funcionarios")

MAX_TOOL_STEPS = 6
DEFAULT_TOP_K = 8

# =========================
# Datas
# =========================
def today() -> datetime:
    return datetime.utcnow()


def iso_date(d: datetime) -> str:
    return d.date().isoformat()


def month_bounds(d: datetime) -> Tuple[str, str]:
    first = d.replace(day=1).date().isoformat()
    if d.month == 12:
        nxt = d.replace(year=d.year + 1, month=1, day=1)
    else:
        nxt = d.replace(month=d.month + 1, day=1)
    last = (nxt - timedelta(days=1)).date().isoformat()
    return first, last


# =========================
# Prompt / Persona
# =========================
SYSTEM_PROMPT = """
Você é o "Ache" — um assistente de produtividade virtual da plataforma Ache Flow.

Sua missão é ajudar colaboradores(as) como {nome_usuario} a entender e gerenciar tarefas, projetos e prazos de forma natural, clara e produtiva.

====================================================================
TOM E ESTILO DE RESPOSTA
====================================================================
- Sempre fale em **português (PT-BR)**.
- Seja simpático(a), humano(a), colaborativo(a) e positivo(a).
- Fale diretamente com o(a) usuário(a) pelo nome, por exemplo: "Oi, {nome_usuario}!".
- Use linguagem clara, leve e natural, com frases curtas e parágrafos separados.
- Evite jargões técnicos ou texto excessivamente formal.
- Pode usar emojis, mas com moderação e profissionalismo (😊, 👌, 📅, etc).
- Nunca use markdown, asteriscos (*), negrito, nem blocos de código.
- Sempre formate com quebras de linha curtas para leitura fluida.
- Se não houver dados suficientes, diga isso de forma gentil e ofereça alternativas úteis.
- Seja proativo(a) e contextual — demonstre que entende o dia a dia da Ache Flow.

====================================================================
CONHECIMENTO E DADOS DISPONÍVEIS
====================================================================
Você tem acesso a dados de:
- Projetos: título, descrição, status, categoria, prazo e responsável.
- Tarefas: título, descrição, status, prazo, prioridade, vínculo com projeto e responsável.
- Colaboradores: nomes, cargos e envolvimento em tarefas/projetos.

As informações podem ser obtidas através das ferramentas (tools):
- list_projects_by_deadline_range → lista projetos com prazo entre duas datas.
- list_tasks_by_deadline_range → lista tarefas com prazo entre duas datas.
- list_projects_by_status → lista projetos por status (ex: "em andamento", "concluído").
- list_tasks_by_status → lista tarefas por status.
- upcoming_deadlines → tarefas ou projetos com prazos próximos.
- search_projects / search_tasks → busca livre por nome, descrição ou palavra-chave.
- update_project / update_task → altera campos (status, prioridade, prazos, etc).

====================================================================
COMPORTAMENTO INTELIGENTE
====================================================================
Analise a intenção do pedido e use a ferramenta adequada.
Você pode combinar mais de uma tool quando fizer sentido.

### STATUS E SITUAÇÃO
- "em andamento" → status que contenha "andamento", "em progresso", "ativo", "executando".
- "concluído" / "finalizado" → status que contenha "concluído", "finalizado", "feito", "encerrado".
- "pendente" → status "pendente", "aguardando", "bloqueado".
- "pausado" ou "congelado" → status "congelado", "pausado", "suspenso".
- Nunca pergunte o que significa um status. Interprete automaticamente.

### PRAZOS E DATAS
- "este mês" → período do primeiro ao último dia do mês atual.
- "semana que vem" → próximos 7 dias.
- "hoje" → data atual.
- "próximos dias" → até 7 dias.
- "próximos 30 dias" → até 30 dias.
- Se não encontrar resultados no período:
  - Primeiro amplie o intervalo (ex: próximos 30 dias).
  - Depois, traga itens "em andamento" como fallback.
  - Informe o usuário com naturalidade, por exemplo:
    "Não encontrei nada neste período, mas aqui estão os projetos em andamento que você pode acompanhar 👇"

### PRIORIDADE E URGÊNCIA
- "urgente" → prazo menor que 3 dias ou prioridade alta.
- "priorizar" → itens com prazo mais próximo.
- Se várias tarefas tiverem mesma prioridade, ordene pelo prazo.

### CONSULTAS E ATUALIZAÇÕES
- Se o usuário pedir para atualizar, alterar ou mudar algo:
  - Use update_project ou update_task.
  - Confirme gentilmente: "Perfeito, atualizei o status da tarefa para concluída 🚀"

### CASOS SEM DADOS
- Diga que não encontrou nada relevante, mas ofereça alternativas úteis.
- Nunca retorne erros técnicos para o usuário.

====================================================================
REGRAS DE RACIOCÍNIO
====================================================================
1. Identifique se o pedido é de listagem, resumo, atualização ou planejamento.
2. Chame as tools necessárias antes de responder.
3. Formule uma resposta natural, clara e objetiva com base nos dados obtidos.
4. Resuma listas longas:
   - Exemplo: "Há 5 projetos em andamento. Os principais são: Portal RH, Integração Financeira e App Mobile. 📋"
5. Sempre que possível, acrescente um insight:
   - Exemplo: "Talvez valha priorizar o App Mobile, que vence em 2 dias ⚡"

====================================================================
FORMATOS DE SAÍDA
====================================================================
- Nunca use Markdown, asteriscos, negrito ou blocos de código.
- Sempre inicie com "Oi, {nome_usuario}!".
- Use parágrafos e quebras de linha para clareza.
- Termine com tom positivo, empático ou sugestivo.
- Evite repetições e respostas robóticas.

====================================================================
INTERPRETAÇÃO DE DATAS (BASE)
====================================================================
- Hoje: {data_hoje}.
- Intervalo de "este mês": {inicio_mes} até {fim_mes}.

====================================================================
MISSÃO FINAL
====================================================================
Com base nas informações disponíveis e nas ferramentas acessíveis:
1. Entenda o pedido de {nome_usuario}.
2. Busque ou atualize os dados necessários com as tools.
3. Resuma o resultado de forma humana, direta e útil.
4. Caso não haja dados, informe gentilmente e ofereça ajuda alternativa.
"""

# =========================
# Utils & Mongo
# =========================
def require_api_key(x_api_key: Optional[str]):
    if API_KEY and (x_api_key or "") != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")


def to_oid(id_str: str) -> ObjectId:
    try:
        return ObjectId(id_str)
    except Exception:
        return id_str  # type: ignore


def parse_date_yyyy_mm_dd(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def pick(d: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    return {k: d.get(k) for k in keys if k in d}


def sanitize_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    if not doc:
        return doc
    out: Dict[str, Any] = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        else:
            out[k] = v
    return out


def mongo():
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI não definido")
    client = MongoClient(MONGO_URI)
    try:
        db = client.get_default_database()
        db_name = db.name if db else "acheflow"
    except Exception:
        db_name = "acheflow"
    return client[db_name]


# =========================
# Consultas (Read/Search)
# =========================
def search_projects(query: str, top_k: int = DEFAULT_TOP_K) -> List[Dict[str, Any]]:
    db = mongo()
    if not query:
        cur = db[COLL_PROJETOS].find({}).sort("prazo", 1).limit(top_k)
        return [sanitize_doc(x) for x in cur]
    regex = {"$regex": query, "$options": "i"}
    cur = db[COLL_PROJETOS].find(
        {"$or": [
            {"nome": regex},
            {"descricao": regex},
            {"categoria": regex},
            {"situacao": regex},
        ]}
    ).sort("prazo", 1).limit(top_k)
    return [sanitize_doc(x) for x in cur]


def search_tasks(query: str, project_id: Optional[str] = None, top_k: int = DEFAULT_TOP_K) -> List[Dict[str, Any]]:
    db = mongo()
    filt: Dict[str, Any] = {}
    if query:
        regex = {"$regex": query, "$options": "i"}
        filt["$or"] = [
            {"nome": regex},
            {"descricao": regex},
            {"prioridade": regex},
            {"status": regex},
        ]
    if project_id:
        filt["projeto_id"] = project_id
    cur = db[COLL_TAREFAS].find(filt).sort("prazo", 1).limit(top_k)
    return [sanitize_doc(x) for x in cur]


def get_project_by_id(pid: str) -> Optional[Dict[str, Any]]:
    db = mongo()
    doc = db[COLL_PROJETOS].find_one({"_id": to_oid(pid)})
    return sanitize_doc(doc) if doc else None


def get_project_by_name(name: str) -> Optional[Dict[str, Any]]:
    db = mongo()
    doc = db[COLL_PROJETOS].find_one({"nome": name})
    return sanitize_doc(doc) if doc else None


def list_tasks_by_project(pid: str, top_k: int = 50) -> List[Dict[str, Any]]:
    db = mongo()
    cur = db[COLL_TAREFAS].find({"projeto_id": pid}).sort("prazo", 1).limit(top_k)
    return [sanitize_doc(x) for x in cur]


def list_projects_by_deadline_range(start: str, end: str, top_k: int = 50) -> List[Dict[str, Any]]:
    db = mongo()
    cur = db[COLL_PROJETOS].find({"prazo": {"$gte": start, "$lte": end}}).sort("prazo", 1).limit(top_k)
    return [sanitize_doc(x) for x in cur]


def list_tasks_by_deadline_range(start: str, end: str, top_k: int = 50) -> List[Dict[str, Any]]:
    db = mongo()
    cur = db[COLL_TAREFAS].find({"prazo": {"$gte": start, "$lte": end}}).sort("prazo", 1).limit(top_k)
    return [sanitize_doc(x) for x in cur]


def upcoming_deadlines(days: int = 14, top_k: int = 50) -> List[Dict[str, Any]]:
    db = mongo()
    today_iso = iso_date(today())
    limit_date = (today() + timedelta(days=days)).date().isoformat()
    cur = db[COLL_TAREFAS].find({"prazo": {"$gte": today_iso, "$lte": limit_date}}).sort("prazo", 1).limit(top_k)
    return [sanitize_doc(x) for x in cur]

def list_projects_by_status(status: str, top_k: int = 50) -> List[Dict[str, Any]]:
    db = mongo()
    status_norm = (status or "").strip().lower()

    # sinônimos de "em andamento"
    em_andamento = {"em andamento", "andamento", "ativo", "em_progresso", "em progresso", "executando"}
    if status_norm in em_andamento:
        rx = {"$regex": "(andament|progres|ativo|execut)", "$options": "i"}
    else:
        rx = {"$regex": re.escape(status_norm), "$options": "i"}

    # checa tanto 'situacao' quanto 'status'
    cur = db[COLL_PROJETOS].find({
        "$or": [
            {"situacao": rx},
            {"status": rx},
        ]
    }).sort("prazo", 1).limit(top_k)

    return [sanitize_doc(x) for x in cur]

# =========================
# Edição (CRUD)
# =========================
def update_project(pid: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    db = mongo()
    allowed = {"nome", "descricao", "categoria", "situacao", "prazo", "responsavel_id"}
    patch2 = {k: v for k, v in patch.items() if k in allowed and v is not None}
    if not patch2:
        raise ValueError("patch vazio")
    res = db[COLL_PROJETOS].find_one_and_update(
        {"_id": to_oid(pid)},
        {"$set": patch2},
        return_document=ReturnDocument.AFTER,
    )
    if not res:
        raise ValueError("projeto não encontrado")
    return sanitize_doc(res)


def create_project(doc: Dict[str, Any]) -> Dict[str, Any]:
    db = mongo()
    base = pick(doc, ["nome", "responsavel_id", "situacao", "prazo", "descricao", "categoria"])
    if not base.get("nome"):
        raise ValueError("nome é obrigatório")
    ins = db[COLL_PROJETOS].insert_one(base)
    base["_id"] = str(ins.inserted_id)
    return sanitize_doc(base)


def create_task(doc: Dict[str, Any]) -> Dict[str, Any]:
    db = mongo()
    base = pick(doc, [
        "nome",
        "descricao",
        "prioridade",
        "status",
        "prazo",
        "projeto_id",
        "responsavel_id",
        "documento_referencia",
        "concluido",
    ])
    if not base.get("nome"):
        raise ValueError("nome é obrigatório")
    ins = db[COLL_TAREFAS].insert_one(base)
    base["_id"] = str(ins.inserted_id)
    return sanitize_doc(base)


def update_task(tid: str, patch: Dict[str, Any]) -> Dict[str, Any]:
    db = mongo()
    allowed = {
        "nome",
        "descricao",
        "prioridade",
        "status",
        "prazo",
        "responsavel_id",
        "concluido",
        "documento_referencia",
    }
    patch2 = {k: v for k, v in patch.items() if k in allowed and v is not None}
    if not patch2:
        raise ValueError("patch vazio")
    res = db[COLL_TAREFAS].find_one_and_update(
        {"_id": to_oid(tid)},
        {"$set": patch2},
        return_document=ReturnDocument.AFTER,
    )
    if not res:
        raise ValueError("tarefa não encontrada")
    return sanitize_doc(res)


# =========================
# Ferramentas (Function Calling)
# =========================

def toolset() -> Tool:
    fns = [
        # Search / Get
        FunctionDeclaration(
            name="search_projects",
            description="Busca projetos por texto (nome, descricao, categoria, situacao).",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "top_k": {"type": "integer"},
                },
            },
        ),
        FunctionDeclaration(
            name="search_tasks",
            description="Busca tarefas por texto e, opcionalmente, por projeto.",
            parameters={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "project_id": {"type": "string"},
                    "top_k": {"type": "integer"},
                },
            },
        ),
        FunctionDeclaration(
            name="get_project_by_id",
            description="Obtém um projeto pelo _id.",
            parameters={
                "type": "object",
                "properties": {"project_id": {"type": "string"}},
                "required": ["project_id"],
            },
        ),
        FunctionDeclaration(
            name="get_project_by_name",
            description="Obtém um projeto pelo nome exato.",
            parameters={
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        ),
        FunctionDeclaration(
            name="list_tasks_by_project",
            description="Lista tarefas pertencentes a um projeto.",
            parameters={
                "type": "object",
                "properties": {
                    "project_id": {"type": "string"},
                    "top_k": {"type": "integer"},
                },
                "required": ["project_id"],
            },
        ),
        FunctionDeclaration(
            name="list_projects_by_deadline_range",
            description="Lista projetos com prazo entre datas (YYYY-MM-DD).",
            parameters={
                "type": "object",
                "properties": {
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                    "top_k": {"type": "integer"},
                },
                "required": ["start", "end"],
            },
        ),
        FunctionDeclaration(
            name="list_tasks_by_deadline_range",
            description="Lista tarefas com prazo entre datas (YYYY-MM-DD).",
            parameters={
                "type": "object",
                "properties": {
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                    "top_k": {"type": "integer"},
                },
                "required": ["start", "end"],
            },
        ),
        FunctionDeclaration(
            name="upcoming_deadlines",
            description="Lista tarefas com prazo entre hoje e hoje+N dias (N padrão=14).",
            parameters={
                "type": "object",
                "properties": {
                    "days": {"type": "integer"},
                    "top_k": {"type": "integer"},
                },
            },
        ),
        # Updates / Creates
        FunctionDeclaration(
            name="update_project",
            description="Atualiza campos do projeto.",
            parameters={
                "type": "object",
                "properties": {
                    "project_id": {"type": "string"},
                    "patch": {
                        "type": "object",
                        "properties": {
                            "nome": {"type": "string"},
                            "descricao": {"type": "string"},
                            "categoria": {"type": "string"},
                            "situacao": {"type": "string"},
                            "prazo": {"type": "string", "description": "YYYY-MM-DD"},
                            "responsavel_id": {"type": "string"},
                        },
                    },
                },
                "required": ["project_id", "patch"],
            },
        ),
        FunctionDeclaration(
            name="create_project",
            description="Cria um novo projeto.",
            parameters={
                "type": "object",
                "properties": {
                    "nome": {"type": "string"},
                    "responsavel_id": {"type": "string"},
                    "situacao": {"type": "string"},
                    "prazo": {"type": "string", "description": "YYYY-MM-DD"},
                    "descricao": {"type": "string"},
                    "categoria": {"type": "string"},
                },
                "required": ["nome"],
            },
        ),
        FunctionDeclaration(
            name="create_task",
            description="Cria uma nova tarefa.",
            parameters={
                "type": "object",
                "properties": {
                    "nome": {"type": "string"},
                    "descricao": {"type": "string"},
                    "prioridade": {"type": "string"},
                    "status": {"type": "string"},
                    "prazo": {"type": "string", "description": "YYYY-MM-DD"},
                    "projeto_id": {"type": "string"},
                    "responsavel_id": {"type": "string"},
                    "documento_referencia": {"type": "string"},
                    "concluido": {"type": "boolean"},
                },
                "required": ["nome"],
            },
        ),
        FunctionDeclaration(
            name="update_task",
            description="Atualiza campos da tarefa.",
            parameters={
                "type": "object",
                "properties": {
                    "task_id": {"type": "string"},
                    "patch": {
                        "type": "object",
                        "properties": {
                            "nome": {"type": "string"},
                            "descricao": {"type": "string"},
                            "prioridade": {"type": "string"},
                            "status": {"type": "string"},
                            "prazo": {"type": "string", "description": "YYYY-MM-DD"},
                            "responsavel_id": {"type": "string"},
                            "documento_referencia": {"type": "string"},
                            "concluido": {"type": "boolean"},
                        },
                    },
                },
                "required": ["task_id", "patch"],
            },
        ),
    ]
    return Tool(function_declarations=fns)


def exec_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    try:
        if name == "search_projects":
            return {"ok": True, "data": search_projects(args.get("query", ""), args.get("top_k", DEFAULT_TOP_K))}
        if name == "search_tasks":
            return {"ok": True, "data": search_tasks(args.get("query", ""), args.get("project_id"), args.get("top_k", DEFAULT_TOP_K))}
        if name == "get_project_by_id":
            return {"ok": True, "data": get_project_by_id(args["project_id"])}
        if name == "get_project_by_name":
            return {"ok": True, "data": get_project_by_name(args["name"])}
        if name == "list_tasks_by_project":
            return {"ok": True, "data": list_tasks_by_project(args["project_id"], args.get("top_k", 50))}
        if name == "list_projects_by_deadline_range":
            return {"ok": True, "data": list_projects_by_deadline_range(args["start"], args["end"], args.get("top_k", 50))}
        if name == "list_tasks_by_deadline_range":
            return {"ok": True, "data": list_tasks_by_deadline_range(args["start"], args["end"], args.get("top_k", 50))}
        if name == "upcoming_deadlines":
            return {"ok": True, "data": upcoming_deadlines(args.get("days", 14), args.get("top_k", 50))}
        if name == "update_project":
            return {"ok": True, "data": update_project(args["project_id"], args.get("patch", {}))}
        if name == "create_project":
            return {"ok": True, "data": create_project(args)}
        if name == "create_task":
            return {"ok": True, "data": create_task(args)}
        if name == "update_task":
            return {"ok": True, "data": update_task(args["task_id"], args.get("patch", {}))}
        if name == "list_projects_by_status":
            return {"ok": True, "data": list_projects_by_status(args["status"], args.get("top_k", 50))}
        return {"ok": False, "error": f"função desconhecida: {name}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =========================
# LLM Orquestração
# =========================

def _normalize_answer(raw: str, nome_usuario: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r"[*_`#>]+", "", raw).strip()
    saud = f"Oi, {nome_usuario}! "
    if not raw.lower().startswith(("oi", "olá", "ola")):
        raw = saud + raw
    if all(sym not in raw for sym in ("🙂", "😊", "👋")):
        raw = raw.rstrip(".") + " 🙂"
    return raw


def init_model(system_instruction: str) -> GenerativeModel:
    vertex_init(project=PROJECT_ID, location=LOCATION)
    return GenerativeModel(GEMINI_MODEL_ID, system_instruction=system_instruction)


def chat_with_tools(user_msg: str, history: Optional[List[Dict[str, str]]] = None, nome_usuario: Optional[str] = None) -> Dict[str, Any]:
    # Datas e contexto dinâmico
    data_hoje = iso_date(today())
    inicio_mes, fim_mes = month_bounds(today())
    nome_usuario = nome_usuario or "você"

    # Preenche placeholders do mega prompt
    system_prompt_filled = SYSTEM_PROMPT.format(
        nome_usuario=nome_usuario,
        data_hoje=data_hoje,
        inicio_mes=inicio_mes,
        fim_mes=fim_mes,
    )

    dynamic_system_prompt = (
        f"{system_prompt_filled}\n\n"
        f"- CONTEXTO ADICIONAL:\n"
        f"  Data de hoje: {data_hoje}.\n"
        f"  Intervalo de 'este mês': {inicio_mes} a {fim_mes}.\n"
        f"  Nome do usuário: {nome_usuario}.\n"
    )

    model = init_model(dynamic_system_prompt)

    # -------------------------------
    # Roteador de intenção (curto-circuito)
    # -------------------------------
    msg_lc = (user_msg or "").lower()

    def _responder_lista_projetos_status(status_label: str) -> Dict[str, Any]:
        data = list_projects_by_status(status_label, top_k=50)
        if data:
            nomes = [p.get("nome") or str(p.get("_id")) for p in data[:10]]
            resp_txt = "Oi, {nome}! Encontrei {n} projeto(s) {rotulo}.\n\nExemplos: {ex}".format(
                nome=nome_usuario,
                n=len(data),
                rotulo=status_label,
                ex=", ".join(nomes),
            )
        else:
            resp_txt = (
                "Oi, {nome}! Não encontrei projetos {rotulo} no momento.\n\n"
                "Quer ver os próximos prazos (30 dias) ou todos os projetos ativos?"
            ).format(nome=nome_usuario, rotulo=status_label)
        return {
            "answer": _normalize_answer(resp_txt, nome_usuario),
            "tool_steps": [
                {
                    "call": {"name": "list_projects_by_status", "args": {"status": status_label, "top_k": 50}},
                    "result": {"ok": True, "data": data},
                }
            ],
        }

    # gatilhos para 'projetos em andamento' e 'projetos ativos'
    if ("projeto" in msg_lc or "projetos" in msg_lc) and ("andamento" in msg_lc) and ("prazo" not in msg_lc):
        return _responder_lista_projetos_status("em andamento")

    if ("projeto" in msg_lc or "projetos" in msg_lc) and (("ativo" in msg_lc) or ("ativos" in msg_lc)) and ("prazo" not in msg_lc):
        # mapeia "ativos" para a mesma lógica de "em andamento"
        return _responder_lista_projetos_status("em andamento")

    # -------------------------------
    # Monta histórico e mensagem do usuário (Content/Part)
    # -------------------------------
    contents: List[Content] = []
    if history:
        for h in history:
            r = h.get("role", "user")
            t = h.get("content", "")
            contents.append(Content(role=r, parts=[Part.from_text(t)]))

    contents.append(Content(role="user", parts=[Part.from_text(user_msg)]))

    tools = [toolset()]
    tool_steps: List[Dict[str, Any]] = []

    # -------------------------------
    # Loop de tool-calling
    # -------------------------------
    for step in range(MAX_TOOL_STEPS):
        resp = model.generate_content(contents, tools=tools)

        # Coleta function calls
        calls = []
        if resp.candidates and resp.candidates[0].content and resp.candidates[0].content.parts:
            for part in resp.candidates[0].content.parts:
                if getattr(part, "function_call", None):
                    calls.append(part.function_call)

        # Sem calls -> finalizar com o texto do modelo
        if not calls:
            final_text = ""
            if resp.candidates and resp.candidates[0].content and resp.candidates[0].content.parts:
                first_part = resp.candidates[0].content.parts[0]
                final_text = getattr(first_part, "text", "") or ""
            # remove frases de espera tipo "Aguarde um instante", "só um momento"
            final_text = re.sub(r"(?i)(aguarde( um instante)?|só um momento|apenas um instante)[^\n]*", "", final_text).strip()
            final_text = _normalize_answer(final_text, nome_usuario)
            return {"answer": final_text, "tool_steps": tool_steps}

        # Executa as ferramentas solicitadas
        for fc in calls:
            name = fc.name
            args = {k: v for k, v in (fc.args or {}).items()}

            # Ajuda de datas para ranges do mês
            if name in ("list_projects_by_deadline_range", "list_tasks_by_deadline_range"):
                start = args.get("start")
                end = args.get("end")
                if (not start or not end) or (start == "ESTE_MES_START" or end == "ESTE_MES_END"):
                    args["start"], args["end"] = inicio_mes, fim_mes

            result = exec_tool(name, args)
            tool_steps.append({"call": {"name": name, "args": args}, "result": result})

            # Injeta resposta de tool no conteúdo
            contents.append(
                Content(
                    role="tool",
                    parts=[
                        Part.from_function_response(
                            name=name,
                            response=result,
                        )
                    ],
                )
            )

            # Fallback automático: se pediu prazo do mês e veio vazio, tente "em andamento"
            if (
                name == "list_projects_by_deadline_range"
                and isinstance(result, dict)
                and result.get("ok")
                and not result.get("data")
            ):
                fb_args = {"status": "em andamento", "top_k": 50}
                fb_res = exec_tool("list_projects_by_status", fb_args)
                tool_steps.append({"call": {"name": "list_projects_by_status", "args": fb_args}, "result": fb_res})
                contents.append(
                    Content(
                        role="tool",
                        parts=[
                            Part.from_function_response(
                                name="list_projects_by_status",
                                response=fb_res,
                            )
                        ],
                    )
                )

    # Se saiu do loop sem resposta textual, devolve algo agradável
    last_text = "Tudo certo por aqui."
    last_text = _normalize_answer(last_text, nome_usuario)
    return {"answer": last_text, "tool_steps": tool_steps}

# =========================
# FastAPI
# =========================
app = FastAPI(title=APPLICATION_NAME)


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[Dict[str, str]]] = None
    nome_usuario: Optional[str] = None


class EditProjectBody(BaseModel):
    project_id: str
    patch: Dict[str, Any] = Field(default_factory=dict)


class EditTaskBody(BaseModel):
    task_id: str
    patch: Dict[str, Any] = Field(default_factory=dict)


@app.post("/ai/chat")
def ai_chat(req: ChatRequest, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    out = chat_with_tools(req.message, req.history, req.nome_usuario)
    return JSONResponse(out)


@app.post("/ai/project/edit")
def ai_edit_project(body: EditProjectBody, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    return JSONResponse({"ok": True, "data": update_project(body.project_id, body.patch)})


@app.post("/ai/task/edit")
def ai_edit_task(body: EditTaskBody, x_api_key: Optional[str] = Header(None)):
    require_api_key(x_api_key)
    return JSONResponse({"ok": True, "data": update_task(body.task_id, body.patch)})


@app.get("/")
def root():
    return {
        "status": "OK",
        "service": APPLICATION_NAME,
        "model": GEMINI_MODEL_ID,
        "project": PROJECT_ID,
        "location": LOCATION,
    }
