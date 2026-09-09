import pytest
from fastapi.testclient import TestClient
from main import app
from banking.rule_registry import STATUTORY_RULE_REGISTRY, DefectSeverity
from banking.recovery_engine import BankRecoveryEngine

client = TestClient(app)


def test_rule_registry_provenance():
    """Verifies that all registered statutory rules have complete metadata and legal citations."""
    assert len(STATUTORY_RULE_REGISTRY) >= 10
    
    for rule_id, rule in STATUTORY_RULE_REGISTRY.items():
        assert rule.rule_id == rule_id
        assert len(rule.title) > 5
        assert len(rule.statute_source) > 5
        assert len(rule.section_provision) > 3
        assert len(rule.effective_date) == 10  # YYYY-MM-DD
        assert len(rule.authoritative_precedent) > 5
        assert len(rule.remediation_guidance) > 10


def test_clean_cheque_recovery_timeline():
    """Verifies that a 100% procedurally compliant S.138 bank recovery file has zero fatal defects and high score."""
    case_data = {
        "case_type": "Cheque Bounce (S.138)",
        "borrower_name": "M/s Apex Retailers Pvt Ltd",
        "loan_account_no": "SBI/SARB/MUM/2026/85012",
        "default_amount": 850000.0,
        "cheque_date": "2024-01-10",
        "dishonour_date": "2024-01-18",
        "notice_date": "2024-01-30",
        "delivery_date": "2024-02-04",
        "complaint_date": "2024-02-28",
        "has_original_cheque": True,
        "has_return_memo": True,
        "has_sanction_letter": True,
        "has_speed_post_receipt": True,
        "has_delivery_report": True,
        "has_account_statement": True
    }

    result = BankRecoveryEngine.evaluate_recovery_case(case_data)
    assert result["success"] is True
    assert result["recovery_score"] >= 85.0
    assert result["verdict"] == "READY_FOR_ADVOCATE_DISPATCH"
    assert len(result["fatal_defects"]) == 0
    assert len(result["limitation_warnings"]) == 0
    assert result["compliance_ledger_record"]["statutory_compliance_status"] == "VERIFIED_COMPLIANT"
    assert result["compliance_ledger_record"]["audit_hash"].startswith("SHA256:")


def test_cheque_3m_presentation_breach():
    """Verifies that cheque presented beyond 3 months triggers RULE_RBI_CHEQUE_3M_VALIDITY fatal defect."""
    case_data = {
        "case_type": "Cheque Bounce (S.138)",
        "borrower_name": "Stale Drawer Ltd",
        "loan_account_no": "LN/STALE/001",
        "default_amount": 500000.0,
        "cheque_date": "2024-01-01",
        "dishonour_date": "2024-04-20", # 110 days (> 3 months)
        "notice_date": "2024-04-25",
        "delivery_date": "2024-04-28",
        "complaint_date": "2024-05-20"
    }

    result = BankRecoveryEngine.evaluate_recovery_case(case_data)
    assert len(result["fatal_defects"]) >= 1
    fatal_rule_ids = [d["rule_id"] for d in result["fatal_defects"]]
    assert "RULE_RBI_CHEQUE_3M_VALIDITY" in fatal_rule_ids
    assert result["recovery_score"] < 60.0
    assert result["verdict"] == "FATAL_STATUTORY_BAR"


def test_statutory_notice_delay_30d():
    """Verifies that notice dispatched on day 35 post-dishonour triggers S.138(b) fatal bar."""
    case_data = {
        "case_type": "Cheque Bounce (S.138)",
        "borrower_name": "Delayed Notice Corp",
        "loan_account_no": "LN/DELAY/002",
        "default_amount": 300000.0,
        "cheque_date": "2024-01-10",
        "dishonour_date": "2024-01-15",
        "notice_date": "2024-02-25", # 41 days post dishonour (> 30 days)
        "delivery_date": "2024-02-28",
        "complaint_date": "2024-03-20"
    }

    result = BankRecoveryEngine.evaluate_recovery_case(case_data)
    fatal_rule_ids = [d["rule_id"] for d in result["fatal_defects"]]
    assert "RULE_NI_138B_NOTICE_30D" in fatal_rule_ids
    assert result["verdict"] == "FATAL_STATUTORY_BAR"


def test_premature_complaint_filing():
    """Verifies that filing complaint on day 8 post-notice delivery triggers premature filing fatal defect u/s 138(c)."""
    case_data = {
        "case_type": "Cheque Bounce (S.138)",
        "borrower_name": "Premature Complainant",
        "loan_account_no": "LN/PREM/003",
        "default_amount": 200000.0,
        "cheque_date": "2024-01-10",
        "dishonour_date": "2024-01-15",
        "notice_date": "2024-01-20",
        "delivery_date": "2024-01-24",
        "complaint_date": "2024-02-01" # 8 days after delivery (< 15 days)
    }

    result = BankRecoveryEngine.evaluate_recovery_case(case_data)
    fatal_rule_ids = [d["rule_id"] for d in result["fatal_defects"]]
    assert "RULE_NI_138C_CURE_15D" in fatal_rule_ids


def test_section_142_limitation_and_condonation():
    """Verifies Section 142 limitation calculation with and without condonation application."""
    # Case A: Without condonation application -> Limitation warning
    case_no_condonation = {
        "case_type": "Cheque Bounce (S.138)",
        "borrower_name": "Delayed Filing Ltd",
        "loan_account_no": "LN/LIMIT/004A",
        "default_amount": 1000000.0,
        "cheque_date": "2024-01-01",
        "dishonour_date": "2024-01-10",
        "notice_date": "2024-01-20",
        "delivery_date": "2024-01-25",
        "complaint_date": "2024-03-30", # ~65 days post delivery (~50 days post cure)
        "condonation_attached": False
    }
    res_a = BankRecoveryEngine.evaluate_recovery_case(case_no_condonation)
    limitation_rule_ids = [w["rule_id"] for w in res_a["limitation_warnings"]]
    assert "RULE_NI_142_LIMITATION_30D" in limitation_rule_ids

    # Case B: With condonation application -> Curable
    case_with_condonation = {**case_no_condonation, "condonation_attached": True}
    res_b = BankRecoveryEngine.evaluate_recovery_case(case_with_condonation)
    curable_rule_ids = [c["rule_id"] for c in res_b["curable_defects"]]
    assert "RULE_NI_142_LIMITATION_30D" in curable_rule_ids


def test_sarfaesi_26d_cersai_statutory_bar():
    """Verifies that missing CERSAI registration triggers absolute statutory bar under Section 26D."""
    case_data = {
        "case_type": "SARFAESI & Cheque Bounce Concurrent Recovery",
        "borrower_name": "Unregistered Mortgagor LLP",
        "loan_account_no": "BOB/SME/2026/005",
        "default_amount": 5000000.0,
        "is_secured": True,
        "cersai_registered": False # Missing CERSAI registration
    }

    result = BankRecoveryEngine.evaluate_recovery_case(case_data)
    fatal_rule_ids = [d["rule_id"] for d in result["fatal_defects"]]
    assert "RULE_SARFAESI_26D_CERSAI_BAR" in fatal_rule_ids


def test_bank_api_endpoints():
    """Tests the FastAPI banking router endpoints."""
    # 1. Test /api/v1/bank/rules
    resp_rules = client.get("/api/v1/bank/rules")
    assert resp_rules.status_code == 200
    data_rules = resp_rules.json()
    assert data_rules["success"] is True
    assert data_rules["total_rules"] >= 10

    # 2. Test /api/v1/bank/demo-cases
    resp_demo = client.get("/api/v1/bank/demo-cases")
    assert resp_demo.status_code == 200
    data_demo = resp_demo.json()
    assert data_demo["success"] is True
    assert len(data_demo["cases"]) >= 2

    # 3. Test /api/v1/bank/recovery-audit
    demo_case_payload = data_demo["cases"][0]["data"]
    resp_audit = client.post("/api/v1/bank/recovery-audit", json=demo_case_payload)
    assert resp_audit.status_code == 200
    data_audit = resp_audit.json()
    assert data_audit["success"] is True
    assert data_audit["recovery_score"] >= 80.0
    assert "advocate_dossier" in data_audit
    assert "compliance_ledger_record" in data_audit

    # 4. Test /api/v1/bank/dispatch-brief
    dispatch_payload = {
        "case_reference": demo_case_payload["loan_account_no"],
        "advocate_name": "Adv. S. Ramanathan (Senior Empaneled Counsel)",
        "advocate_email": "s.ramanathan@advocates.in",
        "officer_id": "OFFICER_MUM_SARB_104",
        "notes": "Initiate S.138 institution immediately and seek 20% interim deposit under S.143A."
    }
    resp_dispatch = client.post("/api/v1/bank/dispatch-brief", json=dispatch_payload)
    assert resp_dispatch.status_code == 200
    assert resp_dispatch.json()["success"] is True
