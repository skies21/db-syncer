import logging
from fastapi import FastAPI

from api.routers import web, api_sync
from syncer.logging_config import setup_logging


app = FastAPI(title="DB Syncer")

setup_logging()
logger = logging.getLogger(__name__)

# Для докера
@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(web.router)
app.include_router(api_sync.router)
