from fastapi import FastAPI
from app.api.v1 import stock
from app.core.config import settings

app = FastAPI(
    title=settings.APP_NAME,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

app.include_router(stock.router, prefix="/api/v1")

@app.get("/health")
def health():
    return {"status": "ok"}
