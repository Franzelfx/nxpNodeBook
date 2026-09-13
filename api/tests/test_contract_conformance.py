"""This node's models satisfy the shared contract from nxp-node-contract.

The CHECK is shared; the model being checked is this node's, which is why
these tests live here and not in the contract package.
"""

from __future__ import annotations

from nxp_node_contract.capability import NodeCapabilities
from nxp_node_contract.conformance import (
    assert_capabilities_conform,
    assert_health_response_conforms,
    assert_wire_versions,
)
from nxp_node_contract.health import compute_ok

from src.domain.capabilities import node_capabilities
from src.domain.schemas import HealthResponse


def test_wire_versions_are_the_ones_this_node_speaks() -> None:
    assert_wire_versions(health=1, coverage=1)


def test_health_response_conforms() -> None:
    assert_health_response_conforms(HealthResponse)


def test_capabilities_conform() -> None:
    assert_capabilities_conform(NodeCapabilities)
    doc = node_capabilities()
    assert doc.asset_prefixes == ["BOOK", "MICRO"]
    assert doc.periods == ["5m"]
    # Every column: absent-not-zero, never carried, bounded.
    for m in doc.metrics:
        assert m.empty_bucket.value == "absent", m.name
        assert m.fill.value == "none", m.name
        assert m.max_carry_seconds is None, m.name
        assert m.bounds is not None, m.name
        assert m.structural_absence, m.name


def test_ok_is_never_optimistic() -> None:
    """A dead collector makes ok false even with a fresh grid."""
    assert compute_ok(
        database_connected=True, producer_running=False, data_fresh=True,
        failed_gates=[], last_publish_status="published",
    ) is False
