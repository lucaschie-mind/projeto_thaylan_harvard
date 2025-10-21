import os
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.templating import Jinja2Templates

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, DateTime, text
from sqlalchemy.pool import NullPool

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("Defina a variável de ambiente DATABASE_URL")

engine = create_engine(DATABASE_URL, poolclass=NullPool, future=True)
metadata = MetaData()

feedback_avaliacao = Table(
    "feedback_avaliacao",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("Feedback", Text, nullable=False),
    Column("Avaliador_1", String(255)),
    Column("Avaliador_2", String(255)),
    Column("Resposta_avaliador_1", String(10)),
    Column("Problemas_avaliador_1", Text),
    Column("Resposta_avaliador_2", String(10)),
    Column("Problemas_avaliador_2", Text),
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
    Column("updated_at", DateTime, nullable=False, default=datetime.utcnow),
)

app = FastAPI(title="Avaliador de Feedbacks")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")

def init_db():
    with engine.begin() as conn:
        metadata.create_all(conn)
init_db()

PROBLEMA_OPCOES = [
    "Texto genérico / vago",
    "Falta de exemplos",
    "Sem objetivo claro",
    "Linguagem muito dura",
    "Escrita impessoal ou mecânica",
    "Apenas em pontos negativos",
    "Enaltecer apenas o positivo",
    "Foco em traços pessoais, não comportamentos",
    "Falta de direcionamento",
]

def normalize_email(email: str) -> str:
    return (email or "").strip().lower()

def row_to_dict(row) -> dict:
    return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)

def pendentes_para_email(email: str):
    email = normalize_email(email)
    sql = text("""        SELECT *
        FROM feedback_avaliacao
        WHERE
            (LOWER(COALESCE("Avaliador_1", '')) = :em
             AND "Resposta_avaliador_1" IS NULL)
        OR
            (LOWER(COALESCE("Avaliador_2", '')) = :em
             AND "Resposta_avaliador_2" IS NULL)
        ORDER BY id ASC
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"em": email}).fetchall()
    itens = []
    for r in rows:
        d = row_to_dict(r)
        if normalize_email(d.get("Avaliador_1")) == email and d.get("Resposta_avaliador_1") is None:
            itens.append({"row": d, "papel": "Avaliador_1"})
        if normalize_email(d.get("Avaliador_2")) == email and d.get("Resposta_avaliador_2") is None:
            itens.append({"row": d, "papel": "Avaliador_2"})
    return itens

def proximo_pendente(email: str, after_id: Optional[int] = None):
    itens = pendentes_para_email(email)
    if after_id is None:
        return itens[0] if itens else None
    for it in itens:
        if it["row"]["id"] > after_id:
            return it
    return itens[0] if itens else None

def atualizar_resposta(item_id: int, papel: str, resposta: str, problemas: Optional[List[str]] = None):
    if papel == "Avaliador_1":
        col_resp = "Resposta_avaliador_1"
        col_prob = "Problemas_avaliador_1"
    elif papel == "Avaliador_2":
        col_resp = "Resposta_avaliador_2"
        col_prob = "Problemas_avaliador_2"
    else:
        raise HTTPException(status_code=400, detail="Papel inválido")

    # Agora aceita problemas mesmo com "Sim". Se vier lista vazia, limpa a coluna.
    problemas_csv = None
    if problemas is not None:
        validos = [p for p in problemas if p in PROBLEMA_OPCOES]
        problemas_csv = ", ".join(validos) if validos else None  # None -> grava NULL (limpa)

    campos = {col_resp: resposta, "updated_at": datetime.utcnow()}
    if problemas is not None:
        campos[col_prob] = problemas_csv  # atualiza/limpa explicitamente

    set_clause = ", ".join([f'"{k}" = :{k}' for k in campos.keys()])
    sql = text(f'UPDATE feedback_avaliacao SET {set_clause} WHERE id = :id')
    params = {**campos, "id": item_id}

    with engine.begin() as conn:
        res = conn.execute(sql, params)
        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="Registro não encontrado")

@app.get("/", response_class=HTMLResponse)
def index(request: Request, email: Optional[str] = None):
    if not email:
        return templates.TemplateResponse("index.html", {"request": request})

    email_norm = normalize_email(email)
    prox = proximo_pendente(email_norm)
    if prox:
        item = prox["row"]
        papel = prox["papel"]
        url = f"/avaliar?email={email_norm}&id={item['id']}&papel={papel}"
        return RedirectResponse(url=url, status_code=303)

    return templates.TemplateResponse(
        "lista.html",
        {"request": request, "email": email_norm, "itens": []},
    )

@app.get("/avaliar", response_class=HTMLResponse)
def avaliar(request: Request, email: str, id: int, papel: str):
    email_norm = normalize_email(email)
    sql = text('SELECT * FROM feedback_avaliacao WHERE id = :id')
    with engine.begin() as conn:
        row = conn.execute(sql, {"id": id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Registro não encontrado")

    d = row_to_dict(row)
    if normalize_email(d.get(papel)) != email_norm:
        raise HTTPException(status_code=403, detail="Você não está autorizado a avaliar este feedback.")

    return templates.TemplateResponse(
        "avaliar.html",
        {
            "request": request,
            "email": email_norm,
            "papel": papel,
            "item": d,
            "opcoes": PROBLEMA_OPCOES,
        },
    )

@app.post("/submit")
def submit(
    email: str = Form(...),
    id: int = Form(...),
    papel: str = Form(...),
    resposta: str = Form(...),
    problemas: Optional[List[str]] = Form(None),
    problemas_submitted: Optional[str] = Form(None),  # <-- novo
):
    resposta = resposta.strip()
    if resposta not in ("Sim", "Não"):
        raise HTTPException(status_code=400, detail="Resposta inválida")

    # Se o formulário foi enviado e nenhuma opção de problema foi marcada, limpamos a coluna
    if problemas_submitted is not None and problemas is None:
        problemas = []  # força limpar em atualizar_resposta

    atualizar_resposta(id, papel, resposta, problemas)

    email_norm = normalize_email(email)
    prox = proximo_pendente(email_norm, after_id=id)
    if prox:
        item = prox["row"]
        next_papel = prox["papel"]
        url = f"/avaliar?email={email_norm}&id={item['id']}&papel={next_papel}"
        return RedirectResponse(url=url, status_code=303)

    return RedirectResponse(url=f"/fim?email={email_norm}", status_code=303)

@app.get("/fim", response_class=HTMLResponse)
def fim(request: Request, email: str):
    return templates.TemplateResponse("fim.html", {"request": request, "email": email})

@app.get("/healthz")
def healthz():
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok"}
