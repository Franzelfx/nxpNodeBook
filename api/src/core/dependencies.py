#!/usr/bin/env python3
"""
────────────────────────────────────────────────────────────────────────────
File:        api/src/core/dependencies.py
Author:      Fabian Franz
Company:     NexPatch AI
Created:     2026-09-13

Description:
  Dependency wiring, initialised in the FastAPI lifespan.

© 2026 NexPatch AI. All rights reserved.
────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..collectors.manager import CollectorManager
    from ..derive.runner import DeriveRunner
    from ..services.health import HealthService
    from ..services.metrics import MetricsService

_metrics_service: Optional["MetricsService"] = None
_health_service: Optional["HealthService"] = None
_derive_runner: Optional["DeriveRunner"] = None
_collectors: Optional["CollectorManager"] = None


def set_metrics_service(service: "MetricsService") -> None:
    global _metrics_service
    _metrics_service = service


def get_metrics_service() -> "MetricsService":
    if _metrics_service is None:
        raise RuntimeError("MetricsService not initialized")
    return _metrics_service


def set_health_service(service: "HealthService") -> None:
    global _health_service
    _health_service = service


def get_health_service() -> "HealthService":
    if _health_service is None:
        raise RuntimeError("HealthService not initialized")
    return _health_service


def set_derive_runner(runner: "DeriveRunner") -> None:
    global _derive_runner
    _derive_runner = runner


def get_derive_runner() -> "DeriveRunner":
    if _derive_runner is None:
        raise RuntimeError("DeriveRunner not initialized")
    return _derive_runner


def set_collectors(manager: "CollectorManager") -> None:
    global _collectors
    _collectors = manager


def get_collectors() -> "CollectorManager":
    if _collectors is None:
        raise RuntimeError("CollectorManager not initialized")
    return _collectors
