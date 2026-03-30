"""
sla_server.py
=============
Minimal FastAPI server that serves only the SLA Generator API.
Deploy this on Render (or any Python host) as a Web Service.

Start locally:
    uvicorn sla_server:app --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apis.sla.router import router as sla_router

app = FastAPI(
    title="Clearline SLA API",
    description="Generates and dispatches Clearline SLA documents",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cil-frontend.vercel.app", "http://localhost:3000"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

app.include_router(sla_router, prefix="/sla")


@app.get("/health")
def health():
    return {"status": "ok"}
