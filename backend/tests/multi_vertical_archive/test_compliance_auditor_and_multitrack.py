import pytest
from fastapi.testclient import TestClient
from main import app
from banking.compliance_auditor import ComplianceAuditor

client = TestClient(app)


def test_compliance_auditor_clean_case():
    payload = {
        "case_id": "CASE-CLEAN-001",
        "borrower_name": "Apex Global Traders Pvt Ltd",
        "amount": 1200000.0,
        "cheque_no": "908124",
        "cheque_date": "2024-01-10",
        "dishonor_date": "2024-01-18",
        "notice_sent_date": "2024-01-28",  # 10 days (<= 30)
        "notice_received_date": "2024-02-02",
        "complaint_filed_date": "2024-02-25",  # Day 23 after notice (Compliant)
        "defendant_is_company": True,
        "company_arraigned": True,
        "director_averments": "SPECIFIC",
        "documents_uploaded": ["cheque.pdf", "memo.pdf", "speed_post.pdf", "statement.pdf"],
        "s65b_certificate": True,
        "has_postal_tracking": True
    }
    res = client.post("/api/v1/analyze/section138", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["case_id"] == "CASE-CLEAN-001"
    assert data["compliance_score"] >= 90
    assert data["compliance_rating"] == "HIGH_COMPLIANCE"
    assert data["fatal_gaps"] == 0


def test_compliance_auditor_aneeta_hada_fatal_gap():
    # Company NOT arraigned as Accused No. 1
    payload = {
        "case_id": "CASE-ANEETA-002",
        "amount": 500000.0,
        "cheque_date": "2024-01-10",
        "dishonor_date": "2024-01-18",
        "notice_sent_date": "2024-01-25",
        "notice_received_date": "2024-01-30",
        "complaint_filed_date": "2024-02-20",
        "defendant_is_company": True,
        "company_arraigned": False,  # FATAL
        "documents_uploaded": ["cheque.pdf"]
    }
    res = client.post("/api/v1/analyze/section138", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["fatal_gaps"] >= 1
    assert data["compliance_rating"] == "CRITICAL_STATUTORY_DEFECTS"
    assert any("Aneeta Hada" in g["precedent"] for g in data["gaps"])


def test_compliance_auditor_kamlesh_kumar_delayed_notice():
    # Notice sent on Day 40 after memo (> 30 days)
    payload = {
        "case_id": "CASE-NOTICE-DELAY-003",
        "amount": 750000.0,
        "cheque_date": "2024-01-10",
        "dishonor_date": "2024-01-18",
        "notice_sent_date": "2024-02-28",  # 41 days after memo
        "notice_received_date": "2024-03-05",
        "complaint_filed_date": "2024-03-25",
        "defendant_is_company": False
    }
    res = client.post("/api/v1/analyze/section138", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["fatal_gaps"] >= 1
    assert any("Kamlesh Kumar" in g["precedent"] for g in data["gaps"])


def test_compliance_auditor_yogendra_premature_filing():
    # Complaint filed on Day 8 after notice (before Day 16)
    payload = {
        "case_id": "CASE-PREMATURE-004",
        "amount": 900000.0,
        "cheque_date": "2024-01-10",
        "dishonor_date": "2024-01-18",
        "notice_sent_date": "2024-01-25",
        "notice_received_date": "2024-01-30",
        "complaint_filed_date": "2024-02-05",  # Day 6 after notice (Premature)
        "defendant_is_company": False
    }
    res = client.post("/api/v1/analyze/section138", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["fatal_gaps"] >= 1
    assert any("Yogendra Pratap Singh" in g["precedent"] for g in data["gaps"])


def test_compliance_auditor_s142_delay_condonation():
    # Complaint filed 50 days after cure window (Late by 20 days)
    payload = {
        "case_id": "CASE-CONDONATION-005",
        "amount": 2500000.0,
        "cheque_date": "2024-01-10",
        "dishonor_date": "2024-01-18",
        "notice_sent_date": "2024-01-25",
        "notice_received_date": "2024-01-30",
        "complaint_filed_date": "2024-04-10",  # 70 days after notice (Condonable)
        "defendant_is_company": False,
        "documents_uploaded": ["cheque.pdf", "memo.pdf"]
    }
    res = client.post("/api/v1/analyze/section138", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert any("Birendra Prasad Sah" in g["precedent"] for g in data["gaps"])
    assert any("Section 142(1)(b)" in g["statute"] for g in data["gaps"])


def test_compliance_auditor_s65b_missing():
    payload = {
        "case_id": "CASE-65B-006",
        "amount": 1000000.0,
        "cheque_date": "2024-01-10",
        "dishonor_date": "2024-01-18",
        "notice_sent_date": "2024-01-25",
        "notice_received_date": "2024-01-30",
        "complaint_filed_date": "2024-02-25",
        "defendant_is_company": False,
        "documents_uploaded": ["bank_statement.pdf"],
        "s65b_certificate": False
    }
    res = client.post("/api/v1/analyze/section138", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert any("Arjun Panditrao Khotkar" in g["precedent"] for g in data["gaps"])


def test_multi_track_orchestrator_comprehensive_evaluation():
    payload = {
        "borrower_name": "Zenith Heavy Engineering Ltd",
        "loan_account_no": "SBI/LCR/2026/0912",
        "default_amount": 45000000.0,  # ₹4.50 Cr
        "is_corporate": True,
        "is_secured": True,
        "cersai_registered": True,
        "is_agricultural_land": False,
        "has_personal_guarantors": True,
        "has_dishonoured_cheques": True,
        "is_wilful_diversion_suspected": True
    }
    res = client.post("/api/v1/analyze/multi-track", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["borrower_name"] == "Zenith Heavy Engineering Ltd"
    assert "tracks" in data
    assert "conflicts" in data
    assert "recommended_sequence" in data
    assert len(data["recommended_sequence"]) >= 3
    assert "Transcore" in data["concurrent_forum_compatibility"]


def test_multi_track_orchestrator_agricultural_land_conflict():
    payload = {
        "borrower_name": "Agri Estate Farms Pvt Ltd",
        "loan_account_no": "BOB/AGRI/2026/881",
        "default_amount": 15000000.0,
        "is_corporate": True,
        "is_secured": True,
        "cersai_registered": True,
        "is_agricultural_land": True,  # AGRI LAND CONFLICT
        "has_personal_guarantors": True,
        "has_dishonoured_cheques": True
    }
    res = client.post("/api/v1/analyze/multi-track", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert any("Agricultural Land" in c["conflict"] for c in data["conflicts"])
    assert data["tracks"]["track_2_sarfaesi"]["statutory_status"] == "BARRED"
