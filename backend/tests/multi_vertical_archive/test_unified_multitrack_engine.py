import pytest
from core.case_registry import case_registry
from composite.unified_multitrack_engine import UnifiedMultiTrackEngine
from engine_core import JudiQEngine

def test_case_registry_composite_aliases():
    """Verify that all composite/multi-track aliases route to UnifiedMultiTrackEngine."""
    aliases = ["composite", "multi_track", "multitrack", "composite_recovery", "unified_npa", "unified", "all"]
    for alias in aliases:
        engine = case_registry.get(alias)
        assert engine is not None, f"Failed for alias: {alias}"
        assert engine.domain_name == "composite"

def test_unified_multitrack_concurrent_analysis():
    """Verify that a single loan account payload evaluates SARFAESI, 138, and Criminal concurrently."""
    case_payload = {
        "case_id": "SBI-NPA-CORP-9021",
        "case_type": "composite",
        "perspective": "creditor",
        "bank_name": "State Bank of India",
        "borrower_name": "Zenith Infra Pvt Ltd",
        "outstanding_amount": 50000000.0,
        "amount": 50000000.0,
        # SARFAESI Fields
        "npa_date": "2025-11-01",
        "notice_13_2_date": "2025-11-15",
        "cersai_registered": True,
        "is_agricultural_land": False,
        # Cheque Bounce (Sec 138) Fields
        "cheque_present": True,
        "dishonour_memo": True,
        "notice_sent": True,
        "debt_proven": True,
        "cheque_amount": 15000000.0,
        "date_of_dishonour": "2025-11-20",
        "date_of_notice": "2025-12-05",
        # Criminal (BNS / IPC) Fields
        "contract_exists": True,
        "entrustment_proven": True,
        "alienation_of_hypothecated_assets": True,
        "offense_type": "cheating_and_criminal_breach_of_trust"
    }

    result = UnifiedMultiTrackEngine.analyze(case_payload)

    assert result is not None
    assert result["domain"] == "composite"
    assert 0 <= result["score"] <= 100
    assert result["decision_status"] in ["PROCEED", "LAWYER_REVIEW_REQUIRED"]

    # Verify all 3 tracks are evaluated
    tracks = result["tracks"]
    assert "sarfaesi" in tracks
    assert "cheque_bounce_138" in tracks
    assert "criminal_bns_ipc" in tracks

    assert tracks["sarfaesi"]["score"] > 0
    assert tracks["cheque_bounce_138"]["score"] > 0
    assert tracks["criminal_bns_ipc"]["score"] > 0

    # Verify Cross-Track Synergy Matrix
    matrix = result["cross_track_matrix"]
    assert len(matrix["synergies"]) >= 2
    assert any("Transcore" in s["statutory_basis"] for s in matrix["synergies"])
    assert len(matrix["risks"]) >= 1
    assert any("Purushotama" in r["statutory_basis"] for r in matrix["risks"])

    # Verify Procedural Milestone Graph
    graph = result["procedural_graph"]
    assert len(graph["nodes"]) >= 10
    assert "Track 1 (SARFAESI Act)" in graph["tracks"]
    assert "Track 2 (Section 138 NI Act)" in graph["tracks"]
    assert "Track 3 (Criminal BNS/IPC)" in graph["tracks"]

    # Verify Next Best Actions
    actions = result["next_best_actions"]
    assert len(actions) >= 4
    tracks_in_actions = {a["track"] for a in actions}
    assert "SARFAESI Act" in tracks_in_actions
    assert "Section 138 NI Act" in tracks_in_actions
    assert "Criminal (BNS/IPC)" in tracks_in_actions

def test_judiq_engine_core_composite_dispatch():
    """Verify end-to-end JudiQEngine dispatch for multi-track case types."""
    case_payload = {
        "case_id": "HDFC-SME-2026-771",
        "case_type": "multi_track",
        "perspective": "creditor",
        "outstanding_amount": 25000000.0,
        "npa_date": "2026-01-01",
        "notice_13_2_date": "2026-01-15",
        "cersai_registered": True,
        "cheque_present": True,
        "dishonour_memo": True,
        "notice_sent": True,
        "date_of_dishonour": "2026-01-20",
        "date_of_notice": "2026-02-01",
        "offense_type": "cheating"
    }

    engine_res = JudiQEngine.analyze_case(case_payload)
    assert engine_res["domain"] == "composite"
    assert "tracks" in engine_res
    assert "cross_track_matrix" in engine_res
    assert engine_res["score"] > 0
