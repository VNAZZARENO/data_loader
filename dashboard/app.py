"""Dashboard de gestion des univers. Aucune authentification (outil interne)."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from dl import requests_apply

from . import settings
from .routers import api, ui


def create_app() -> FastAPI:
    scheduler = None

    @asynccontextmanager
    async def lifespan(app):
        nonlocal scheduler
        if settings.START_SCHEDULER:
            from apscheduler.schedulers.background import BackgroundScheduler
            scheduler = BackgroundScheduler()
            scheduler.add_job(lambda: requests_apply.auto_apply_pending(settings.config()),
                              "interval", minutes=2, id="auto_apply", max_instances=1)
            scheduler.start()
        yield
        if scheduler:
            scheduler.shutdown(wait=False)

    app = FastAPI(title="DataLoader", lifespan=lifespan, docs_url="/api/docs", openapi_url="/api/openapi.json")
    app.include_router(api.router)
    app.include_router(ui.router)
    app.mount("/static", StaticFiles(directory=str(Path(__file__).parent / "static")), name="static")
    return app


app = create_app()
