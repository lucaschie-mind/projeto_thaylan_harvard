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
    "feedback_avaliacao", metadata,
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
    "Muito genérico",
    "Falta planos para o futuro",
    "Não parece agregar para pessoa",
    "Falta ponto positivo e de desenvolvimento",
]

def normalize_email(email: str) -> str:
    return (email or "").strip().lower()

def row_to_dict(row):
    return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)

@app.get("/", response_class=HTMLResponse)
def index(request: Request, email: Optional[str] = None):
    if not email:
        return templates.TemplateResponse("index.html", {"request": request})
    email_norm = normalize_email(email)
    sql = text('''
        SELECT * FROM feedback_avaliacao
        WHERE (LOWER(COALESCE("Avaliador_1",'')) = :em AND "Resposta_avaliador_1" IS NULL)
        OR (LOWER(COALESCE("Avaliador_2",'')) = :em AND "Resposta_avaliador_2" IS NULL)
        ORDER BY id
    ''')
    with engine.begin() as conn:
        rows = conn.execute(sql, {"em": email_norm}).fetchall()
    itens = []
    for r in rows:
        d = row_to_dict(r)
        if normalize_email(d.get("Avaliador_1")) == email_norm and d.get("Resposta_avaliador_1") is None:
            itens.append({"row": d, "papel": "Avaliador_1"})
        if normalize_email(d.get("Avaliador_2")) == email_norm and d.get("Resposta_avaliador_2") is None:
            itens.append({"row": d, "papel": "Avaliador_2"})
    return templates.TemplateResponse("lista.html", {"request": request, "email": email_norm, "itens": itens})

@app.get("/avaliar", response_class=HTMLResponse)
def avaliar(request: Request, email: str, id: int, papel: str):
    email_norm = normalize_email(email)
    sql = text('SELECT * FROM feedback_avaliacao WHERE id = :id')
    with engine.begin() as conn:
        row = conn.execute(sql, {"id": id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Registro não encontrado")
    d = row_to_dict(row)
    return templates.TemplateResponse("avaliar.html", {"request": request, "email": email_norm, "papel": papel, "item": d, "opcoes": PROBLEMA_OPCOES})

@app.post("/submit")
def submit(email: str = Form(...), id: int = Form(...), papel: str = Form(...),
           resposta: str = Form(...), problemas: Optional[List[str]] = Form(None)):
    col_resp = "Resposta_avaliador_1" if papel == "Avaliador_1" else "Resposta_avaliador_2"
    col_prob = "Problemas_avaliador_1" if papel == "Avaliador_1" else "Problemas_avaliador_2"
    problemas_csv = ", ".join(problemas) if resposta == "Não" and problemas else None
    sql = text(f'UPDATE feedback_avaliacao SET "{col_resp}" = :resp, "{col_prob}" = :prob, updated_at = :now WHERE id = :id')
    with engine.begin() as conn:
        conn.execute(sql, {"resp": resposta, "prob": problemas_csv, "id": id, "now": datetime.utcnow()})
    return RedirectResponse(url=f"/?email={email}", status_code=303)

@app.get("/healthz")
def healthz():
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok"}
