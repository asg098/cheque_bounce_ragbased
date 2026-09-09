import pytest
from core.case_registry import case_registry
from sarfaesi.sarfaesi_domain_engine import SarfaesiDomainEngine
from evidence.evidence_intelligence import EvidenceIntelligenceEngine
from procedural.procedural_graph_engine import ProceduralGraphEngine
from citation.citation_verifier import CitationVerifierEngine
from audit.audit_ledger import AuditLedger
from engine_core import JudiQEngine

def test_unified_case_registry():
    registered_domains = case_registry.list_registered_domains()
    assert "sarfaesi" in registered_domains
    assert "drt" in registered_domains
    assert "criminal" in registered_domains
    assert "ipc" in registered_domains

    engine = case_registry.get("sarfaesi")
    assert engine is not None
    assert engine.domain_name == "sarfaesi"

    crim_engine = case_registry.get("criminal")
    assert crim_engine is not None
    assert crim_engine.domain_name == "criminal"

def test_evidence_intelligence_classification_and_gaps():
    doc_meta = EvidenceIntelligenceEngine.classify_document("Demand notice under section 13(2).pdf")
    assert doc_meta["type"] == "NOTICE_13_2"

    case_data = {
        "perspective": "creditor",
        "cersai_registered": False,
        "possession_13_4_date": "2026-03-01",
        "newspaper_publication_done": False
    }
    gaps = EvidenceIntelligenceEngine.evaluate_evidence_gaps(case_data)
    assert len(gaps) >= 2
    provisions = [g["statutory_provision"] for g in gaps]
    assert any("Section 26D" in p for p in provisions)
    assert any("Rule 8(2)" in p for p in provisions)

def test_procedural_graph_and_next_best_actions():
    case_data = {
        "perspective": "creditor",
        "npa_date": "2026-01-01",
        "notice_13_2_date": "2026-01-15",
        "cersai_registered": True
    }
    graph = ProceduralGraphEngine.build_graph(case_data)
    assert graph["current_stage"] == "Section 13(2) Demand Window"
    assert len(graph["nodes"]) == 7

    actions = ProceduralGraphEngine.determine_next_best_actions(case_data, {"score": 80})
    assert len(actions) > 0
    assert "Section 13(4)" in actions[0]["action"]

def test_citation_verifier():
    verif_mardia = CitationVerifierEngine.verify_citation("Mardia Chemicals Ltd. v. Union of India")
    assert verif_mardia["status"] == "VERIFIED"
    assert verif_mardia["confidence"] == 1.0

    verif_sec26d = CitationVerifierEngine.verify_citation("Section 26D SARFAESI Act")
    assert verif_sec26d["status"] == "VERIFIED"

    verif_fake = CitationVerifierEngine.verify_citation("Random Fake Lawyer vs Imaginary Court 2099")
    assert verif_fake["status"] == "UNKNOWN"

def test_audit_ledger_and_lawyer_override():
    entry = AuditLedger.record_entry(
        case_id="TEST-CASE-99",
        finding_id="FINDING-001",
        finding_text="CERSAI non-registration defect identified",
        evidence_relied="CERSAI portal check",
        rule_applied="Section 26D SARFAESI Act",
        authority="Section 26D SARFAESI Act, 2002",
        confidence=0.95,
        verdict="HIGH RISK"
    )
    assert entry["review_status"] == "PENDING_REVIEW"

    updated = AuditLedger.apply_lawyer_override(
        case_id="TEST-CASE-99",
        finding_id="FINDING-001",
        action="MODIFY",
        override_reason="CERSAI registration completed offline on 10-Jan",
        lawyer_name="Senior Counsel Advocates"
    )
    assert updated is not None
    assert updated["review_status"] == "MODIFIED"
    assert updated["lawyer_override"]["action"] == "MODIFY"

def test_institutional_judiq_engine_output():
    case_data = {
        "case_id": "INSTITUTIONAL-SARFAESI-001",
        "case_type": "sarfaesi",
        "perspective": "creditor",
        "bank_name": "ICICI Bank",
        "borrower_name": "Apex Logistics",
        "outstanding_amount": 50000000.0,
        "npa_date": "2026-01-01",
        "notice_13_2_date": "2026-01-15",
        "cersai_registered": True
    }
    result = JudiQEngine.analyze_case(case_data)
    assert "evidence_gaps" in result
    assert "procedural_graph" in result
    assert "verified_authority" in result
    assert "next_best_actions" in result
    assert "audit_entry" in result
    assert result["verified_authority"]["status"] == "VERIFIED"
