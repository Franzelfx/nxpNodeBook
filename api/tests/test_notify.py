"""
doc/54 E5 — the warehouse notification.

Strictly an optimisation, and these tests are mostly about proving it stays
one: unconfigured, rate-limited, or unreachable must all be quiet no-ops, and
none of them may raise into the writer.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from src.collectors import notify


@pytest.fixture(autouse=True)
def _reset():
    notify._last_sent = 0.0
    notify._warned = False
    yield
    notify._last_sent = 0.0


def _configure(monkeypatch, url="http://warehouse.test/v1/internal/stream/notify"):
    monkeypatch.setenv("NXP_WAREHOUSE_NOTIFY_URL", url)
    monkeypatch.setenv("NXP_STREAM_TICKET_SECRET", "test-secret")
    monkeypatch.setenv("NXP_WAREHOUSE_SOURCE_ID", "ef5ff508-921d-4944-9bf3-5f1f729c894c")


async def test_an_unconfigured_node_does_nothing(monkeypatch):
    """The poll is then the only path, which is the pre-E5 behaviour."""
    monkeypatch.delenv("NXP_WAREHOUSE_NOTIFY_URL", raising=False)
    assert await notify.announce(10) is False


async def test_a_partially_configured_node_does_nothing(monkeypatch):
    monkeypatch.setenv("NXP_WAREHOUSE_NOTIFY_URL", "http://warehouse.test/notify")
    monkeypatch.delenv("NXP_STREAM_TICKET_SECRET", raising=False)
    assert await notify.announce(10) is False


async def test_it_signs_what_it_sends(monkeypatch):
    """
    The warehouse verifies this exact signature. If the two drift, every
    notification is refused and nothing says why.
    """
    _configure(monkeypatch)
    sent = {}

    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"accepted": True}

    class _Client:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, json, headers):
            sent.update(url=url, json=json, headers=headers)
            return _Response()

    monkeypatch.setattr(notify.httpx, "AsyncClient", _Client)

    assert await notify.announce(7, watermark="2026-09-23T10:00:00Z") is True

    payload = b"ef5ff508-921d-4944-9bf3-5f1f729c894c:7:2026-09-23T10:00:00Z"
    assert sent["headers"]["X-Nxp-Signature"] == hmac.new(
        b"test-secret", payload, hashlib.sha256
    ).hexdigest()
    assert sent["json"]["rows"] == 7


async def test_it_rate_limits_itself(monkeypatch):
    """
    A flush runs every second or two; the warehouse only needs to know that
    something landed, not how much or how often.
    """
    _configure(monkeypatch)

    class _Client:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            class _R:
                status_code = 200

                @staticmethod
                def json():
                    return {"accepted": True}

            return _R()

    monkeypatch.setattr(notify.httpx, "AsyncClient", _Client)

    assert await notify.announce(1) is True
    assert await notify.announce(1) is False, "the second is inside the window"


async def test_an_unreachable_warehouse_is_a_shrug(monkeypatch):
    """
    This runs on the writer's loop. A raise here would reach the flush loop,
    and the writer's contract is that nothing may stall a collector.
    """
    _configure(monkeypatch)

    class _Client:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            raise ConnectionError("no route to warehouse")

    monkeypatch.setattr(notify.httpx, "AsyncClient", _Client)
    assert await notify.announce(1) is False


async def test_a_refusal_is_not_an_error(monkeypatch):
    _configure(monkeypatch)

    class _Client:
        def __init__(self, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            class _R:
                status_code = 401

                @staticmethod
                def json():
                    return {}

            return _R()

    monkeypatch.setattr(notify.httpx, "AsyncClient", _Client)
    assert await notify.announce(1) is False
