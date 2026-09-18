from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.templating import Jinja2Templates

from dl import registry

from .. import settings

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent.parent / "templates"))


def _ctx(request: Request, **kw):
    # Derriere le proxy pergam-tools, le prefixe /DataLoader est retire et transmis ici.
    return {"base_url": request.headers.get("X-Proxy-Prefix", "").rstrip("/"), **kw}


@router.get("/")
def index(request: Request):
    return templates.TemplateResponse(request, "index.html", _ctx(request, page="index"))


@router.get("/u/{universe}")
def universe(request: Request, universe: str):
    if not registry.exists(universe, settings.config()):
        raise HTTPException(404, f"Univers inconnu: {universe}")
    return templates.TemplateResponse(request, "universe.html", _ctx(request, page="universe", universe=universe))


@router.get("/requests")
def requests_page(request: Request):
    return templates.TemplateResponse(request, "requests.html", _ctx(request, page="requests"))


@router.get("/new")
def new_page(request: Request):
    profiles = sorted(settings.config().get("agents", {}))
    return templates.TemplateResponse(request, "new.html", _ctx(request, page="new", profiles=profiles))
