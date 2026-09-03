from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from pathlib import Path


def _positive_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if not isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _expand_client_origins(raw: str) -> tuple[str, ...]:
    origins: list[str] = []
    seen: set[str] = set()
    for item in raw.split(","):
        origin = item.strip().rstrip("/")
        if not origin:
            continue
        candidates = [origin]
        if "://localhost" in origin:
            candidates.append(origin.replace("://localhost", "://127.0.0.1", 1))
        elif "://127.0.0.1" in origin:
            candidates.append(origin.replace("://127.0.0.1", "://localhost", 1))
        for candidate in candidates:
            if candidate not in seen:
                seen.add(candidate)
                origins.append(candidate)
    return tuple(origins)


def _load_dotenv() -> None:
    path = Path(__file__).resolve().parents[1] / ".env"
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    openrouter_api_key: str
    model: str
    openrouter_base_url: str
    flush_max_deltas: int
    flush_max_ms: float
    snapshot_threshold: int
    heartbeat_s: float
    client_origin: str

    @property
    def client_origins(self) -> tuple[str, ...]:
        origins = _expand_client_origins(self.client_origin)
        if not origins:
            raise ValueError("CLIENT_ORIGIN must not be empty")
        return origins

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        if environ is None:
            _load_dotenv()
            env = os.environ
        else:
            env = environ

        api_key = (env.get("OPENROUTER_API_KEY") or "").strip()
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY is required")

        model = env.get("MODEL", "openai/gpt-4o-mini").strip()
        if not model:
            raise ValueError("MODEL must not be empty")

        base_url = env.get(
            "OPENROUTER_BASE_URL",
            "https://openrouter.ai/api/v1",
        ).strip().rstrip("/")
        if not base_url:
            raise ValueError("OPENROUTER_BASE_URL must not be empty")

        client_origin = env.get("CLIENT_ORIGIN", "http://localhost:5173").strip()
        if not client_origin:
            raise ValueError("CLIENT_ORIGIN must not be empty")

        return cls(
            database_url=env.get(
                "DATABASE_URL",
                "postgres://postgres:postgres@localhost:5432/token_streamer",
            ),
            openrouter_api_key=api_key,
            model=model,
            openrouter_base_url=base_url,
            flush_max_deltas=_positive_int(env, "FLUSH_MAX_DELTAS", 1),
            flush_max_ms=_positive_float(env, "FLUSH_MAX_MS", 8.0),
            snapshot_threshold=_positive_int(env, "SNAPSHOT_THRESHOLD", 50),
            heartbeat_s=_positive_float(env, "HEARTBEAT_S", 15.0),
            client_origin=client_origin,
        )
