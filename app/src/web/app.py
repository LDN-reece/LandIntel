"""Internal Phase One review UI for the canonical opportunity engine."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from config.settings import Settings, get_settings
from src.db import Database
from src.logging_config import configure_logging
from src.opportunity_engine.service import OpportunityService
from src.opportunity_engine.types import OpportunitySearchFilters


WEB_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    logger = configure_logging(settings)
    service = OpportunityService(settings, logger)

    app = FastAPI(title="LandIntel Phase One", version="1.0.0")
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.on_event("shutdown")
    def _shutdown() -> None:
        service.close()

    @app.get("/", response_class=HTMLResponse)
    def queue_dashboard(
        request: Request,
        q: str | None = Query(default=None),
        queue_name: str | None = Query(default=None),
        authority_name: str | None = Query(default=None),
        source_route: str | None = Query(default=None),
        size_band: str | None = Query(default=None),
        planning_context_band: str | None = Query(default=None),
        settlement_position: str | None = Query(default=None),
        location_band: str | None = Query(default=None),
        constraint_severity: str | None = Query(default=None),
        access_strength: str | None = Query(default=None),
        geometry_quality: str | None = Query(default=None),
        ownership_control_state: str | None = Query(default=None),
        title_state: str | None = Query(default=None),
        review_status: str | None = Query(default=None),
        resurfaced_only: str | None = Query(default=None),
    ) -> HTMLResponse:
        filters = OpportunitySearchFilters(
            query=q,
            queue_name=queue_name,
            authority_name=authority_name,
            source_route=source_route,
            size_band=size_band,
            planning_context_band=planning_context_band,
            settlement_position=settlement_position,
            location_band=location_band,
            constraint_severity=constraint_severity,
            access_strength=access_strength,
            geometry_quality=geometry_quality,
            ownership_control_state=ownership_control_state,
            title_state=title_state,
            review_status=review_status,
            resurfaced_only=_parse_optional_bool(resurfaced_only),
        )
        payload = service.search_opportunities(filters)
        return templates.TemplateResponse(
            "site_search.html",
            {
                "request": request,
                "results": payload["results"],
                "grouped_results": payload["grouped_results"],
                "options": payload["options"],
                "filters": filters,
            },
        )

    @app.get("/sites/{site_id}", response_class=HTMLResponse)
    def site_detail_page(request: Request, site_id: str) -> HTMLResponse:
        payload = service.get_opportunity_review(site_id)
        if not payload:
            raise HTTPException(status_code=404, detail="Unknown site")
        return templates.TemplateResponse(
            "site_detail.html",
            {
                "request": request,
                "detail": payload["detail"],
                "brief": payload["brief"],
            },
        )

    @app.post("/sites/{site_id}/review/status")
    def site_review_status_page(
        site_id: str,
        review_status: str = Form(...),
        actor_name: str = Form("LDN"),
        reason_text: str | None = Form(default=None),
    ) -> RedirectResponse:
        service.record_review_status(site_id, review_status, actor_name, reason_text)
        return RedirectResponse(url=f"/sites/{site_id}", status_code=303)

    @app.post("/sites/{site_id}/review/note")
    def site_review_note_page(
        site_id: str,
        actor_name: str = Form("LDN"),
        note_text: str = Form(...),
    ) -> RedirectResponse:
        service.record_review_note(site_id, actor_name, note_text)
        return RedirectResponse(url=f"/sites/{site_id}", status_code=303)

    @app.post("/sites/{site_id}/review/override")
    def site_review_override_page(
        site_id: str,
        actor_name: str = Form("LDN"),
        override_key: str = Form(...),
        override_value: str = Form(...),
        reason_text: str | None = Form(default=None),
    ) -> RedirectResponse:
        service.record_manual_override(
            site_id,
            actor_name,
            override_key,
            {"value": override_value},
            reason_text,
        )
        return RedirectResponse(url=f"/sites/{site_id}", status_code=303)

    @app.post("/sites/{site_id}/review/title")
    def site_review_title_page(
        site_id: str,
        action: str = Form(...),
        actor_name: str = Form("LDN"),
        reason_text: str | None = Form(default=None),
        title_number: str | None = Form(default=None),
    ) -> RedirectResponse:
        service.record_title_action(site_id, action, actor_name, reason_text, title_number)
        return RedirectResponse(url=f"/sites/{site_id}", status_code=303)

    @app.get("/api/sites")
    def search_sites_api(
        q: str | None = Query(default=None),
        queue_name: str | None = Query(default=None),
        authority_name: str | None = Query(default=None),
        source_route: str | None = Query(default=None),
        size_band: str | None = Query(default=None),
        planning_context_band: str | None = Query(default=None),
        settlement_position: str | None = Query(default=None),
        location_band: str | None = Query(default=None),
        constraint_severity: str | None = Query(default=None),
        access_strength: str | None = Query(default=None),
        geometry_quality: str | None = Query(default=None),
        ownership_control_state: str | None = Query(default=None),
        title_state: str | None = Query(default=None),
        review_status: str | None = Query(default=None),
        resurfaced_only: str | None = Query(default=None),
    ) -> dict[str, object]:
        filters = OpportunitySearchFilters(
            query=q,
            queue_name=queue_name,
            authority_name=authority_name,
            source_route=source_route,
            size_band=size_band,
            planning_context_band=planning_context_band,
            settlement_position=settlement_position,
            location_band=location_band,
            constraint_severity=constraint_severity,
            access_strength=access_strength,
            geometry_quality=geometry_quality,
            ownership_control_state=ownership_control_state,
            title_state=title_state,
            review_status=review_status,
            resurfaced_only=_parse_optional_bool(resurfaced_only),
        )
        payload = service.search_opportunities(filters)
        return {
            "filters": filters.__dict__,
            "results": payload["results"],
            "grouped_results": payload["grouped_results"],
        }

    @app.get("/api/sites/{site_id}")
    def site_detail_api(site_id: str) -> dict[str, object]:
        payload = service.get_opportunity_review(site_id)
        if not payload:
            raise HTTPException(status_code=404, detail="Unknown site")
        return payload

    @app.post("/api/sites/{site_id}/review/status")
    def site_review_status_api(
        site_id: str,
        review_status: str = Form(...),
        actor_name: str = Form("LDN"),
        reason_text: str | None = Form(default=None),
    ) -> dict[str, object]:
        service.record_review_status(site_id, review_status, actor_name, reason_text)
        return {"ok": True, "site_id": site_id, "review_status": review_status}

    @app.post("/api/sites/{site_id}/review/note")
    def site_review_note_api(
        site_id: str,
        actor_name: str = Form("LDN"),
        note_text: str = Form(...),
    ) -> dict[str, object]:
        service.record_review_note(site_id, actor_name, note_text)
        return {"ok": True, "site_id": site_id}

    @app.post("/api/sites/{site_id}/review/override")
    def site_review_override_api(
        site_id: str,
        actor_name: str = Form("LDN"),
        override_key: str = Form(...),
        override_value: str = Form(...),
        reason_text: str | None = Form(default=None),
    ) -> dict[str, object]:
        service.record_manual_override(site_id, actor_name, override_key, {"value": override_value}, reason_text)
        return {"ok": True, "site_id": site_id}

    @app.post("/api/sites/{site_id}/review/title")
    def site_review_title_api(
        site_id: str,
        action: str = Form(...),
        actor_name: str = Form("LDN"),
        reason_text: str | None = Form(default=None),
        title_number: str | None = Form(default=None),
    ) -> dict[str, object]:
        service.record_title_action(site_id, action, actor_name, reason_text, title_number)
        return {"ok": True, "site_id": site_id, "action": action}

    return app


def serve(settings: Settings | None = None, *, host: str = "127.0.0.1", port: int = 8000) -> None:
    settings = settings or get_settings()
    database = Database(settings)
    try:
        database.run_migrations()
    finally:
        database.dispose()

    import uvicorn

    uvicorn.run(create_app(settings), host=host, port=port)


def _parse_optional_bool(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None
