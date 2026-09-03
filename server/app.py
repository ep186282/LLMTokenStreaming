from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, AsyncIterator
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette import status

from server.config import Settings
from server.database import (
    Database,
    GenerationNotFound,
    IdempotencyConflict,
)
from server.generator import GenerationRunner
from server.hub import WakeupHub
from server.models import (
    CancelResult,
    GenerationAccepted,
    GenerationRequest,
    GenerationView,
    HealthResult,
)
from server.providers import build_provider
from server.reader import CursorAhead, ReaderGenerationNotFound, ReaderSession
from server.tasks import GenerationTaskRegistry


logger = logging.getLogger("uvicorn.error")


@dataclass(frozen=True, slots=True)
class Services:
    settings: Settings
    database: Database
    hub: WakeupHub
    runner: GenerationRunner
    tasks: GenerationTaskRegistry


def _services(request: Request) -> Services:
    return request.app.state.services


def _header_cursor(last_event_id: str | None) -> int:
    if last_event_id is None or not last_event_id.strip():
        return 0
    try:
        cursor = int(last_event_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Last-Event-ID must be a non-negative integer",
        ) from exc
    if cursor < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Last-Event-ID must be a non-negative integer",
        )
    return cursor


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        provider = build_provider(resolved_settings)
        database = Database(resolved_settings.database_url)
        interrupted_count = await database.open()
        hub = WakeupHub()
        tasks = GenerationTaskRegistry()
        runner = GenerationRunner(
            database=database,
            provider=provider,
            hub=hub,
            settings=resolved_settings,
        )
        application.state.services = Services(
            settings=resolved_settings,
            database=database,
            hub=hub,
            runner=runner,
            tasks=tasks,
        )
        if interrupted_count:
            logger.warning(
                "startup interrupted %d running generation(s)",
                interrupted_count,
            )
        try:
            yield
        finally:
            await tasks.shutdown()
            await database.close()

    application = FastAPI(
        title="LLM Token Streamer",
        version="1.0.0",
        lifespan=lifespan,
    )
    application.add_middleware(
        CORSMiddleware,
        allow_origins=list(resolved_settings.client_origins),
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=[
            "Accept",
            "Cache-Control",
            "Content-Type",
            "Idempotency-Key",
            "Last-Event-ID",
        ],
    )

    @application.post(
        "/generations",
        response_model=GenerationAccepted,
        status_code=status.HTTP_201_CREATED,
        responses={409: {"description": "Idempotency key request conflict"}},
    )
    async def create_generation(
        body: GenerationRequest,
        request: Request,
        response: Response,
        idempotency_key: Annotated[
            UUID,
            Header(alias="Idempotency-Key"),
        ],
    ) -> GenerationAccepted:
        services = _services(request)
        model = body.model or services.settings.model
        stored_request = {
            "model": model,
            "messages": body.messages,
        }
        try:
            generation, created = (
                await services.database.create_or_get_generation(
                    idempotency_key=idempotency_key,
                    model=model,
                    request=stored_request,
                )
            )
        except IdempotencyConflict as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key was already used for another request",
            ) from exc

        if generation["status"] == "running":
            saved_request = generation["request"]
            await services.tasks.ensure_started(
                generation["id"],
                lambda cancel_requested: services.runner.run(
                    generation["id"],
                    model=generation["model"],
                    messages=saved_request["messages"],
                    cancel_requested=cancel_requested,
                ),
            )

        if not created:
            response.status_code = status.HTTP_200_OK
        return GenerationAccepted(generation_id=generation["id"])

    @application.get(
        "/generations/{generation_id}",
        response_model=GenerationView,
    )
    async def get_generation(
        generation_id: UUID,
        request: Request,
    ) -> GenerationView:
        generation = await _services(request).database.get_generation_view(
            generation_id
        )
        if generation is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="generation not found",
            )
        return GenerationView.model_validate(generation)

    @application.get("/generations/{generation_id}/events")
    async def generation_events(
        generation_id: UUID,
        request: Request,
        after: Annotated[int, Query(ge=0)] = 0,
        last_event_id: Annotated[
            str | None,
            Header(alias="Last-Event-ID"),
        ] = None,
    ) -> StreamingResponse:
        services = _services(request)
        cursor = max(after, _header_cursor(last_event_id))
        try:
            reader = await ReaderSession.open(
                database=services.database,
                hub=services.hub,
                generation_id=generation_id,
                cursor=cursor,
                snapshot_threshold=services.settings.snapshot_threshold,
                heartbeat_s=services.settings.heartbeat_s,
            )
        except ReaderGenerationNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="generation not found",
            ) from exc
        except CursorAhead as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "message": "cursor exceeds current sequence",
                    "current_seq": exc.current_seq,
                },
            ) from exc

        return StreamingResponse(
            reader.events(),
            media_type="text/event-stream",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @application.post(
        "/generations/{generation_id}/cancel",
        response_model=CancelResult,
    )
    async def cancel_generation(
        generation_id: UUID,
        request: Request,
    ) -> CancelResult:
        services = _services(request)
        try:
            accepted = await services.database.request_cancel(generation_id)
        except GenerationNotFound as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="generation not found",
            ) from exc
        if not accepted:
            return CancelResult(status="already_terminal")

        await services.tasks.signal_cancel(generation_id)
        return CancelResult(status="cancelled")

    @application.get("/health", response_model=HealthResult)
    async def health(request: Request) -> HealthResult:
        services = _services(request)
        await services.database.healthcheck()
        return HealthResult(
            status="ok",
            provider="openrouter",
        )

    return application


def __getattr__(name: str) -> FastAPI:
    if name == "app":
        return create_app()
    raise AttributeError(name)
