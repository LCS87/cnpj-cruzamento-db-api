"""
API REST - Sistema de Cruzamento de CNPJs
"""
import logging
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from utils import setup_logging
from config import SYSTEM_CONFIG

from .routes import cnpj, cruzamento, health

setup_logging(log_level=SYSTEM_CONFIG.get('log_level', 'INFO'))
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API de Cruzamento de CNPJs iniciada.")
    os.makedirs(SYSTEM_CONFIG['input_folder'], exist_ok=True)
    os.makedirs(SYSTEM_CONFIG['output_folder'], exist_ok=True)
    yield
    logger.info("API encerrada.")


app = FastAPI(
    title="CNPJ Cruzamento API",
    description="Cruzamento de CNPJs entre banco de dados local e API Invertexto.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, tags=["health"])
app.include_router(cnpj.router, prefix="/v1", tags=["cnpj"])
app.include_router(cruzamento.router, prefix="/v1", tags=["cruzamento"])
