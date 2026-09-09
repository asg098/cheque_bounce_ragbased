import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_multi_track_strategy_evaluation():
    payload = {
        "borrower_name": "M/s Sterling Steel Fabrication Pvt Ltd",
        "loan_account_no": "SBI/SARB/MUM/2026/88102",
        "default_amount": 35000000.0,  # ₹3.50 Crores
        "is_corporate": True,
        "is_secured": True,
        "cersai_registered": True,
        "is_agricultural_land": False,
        "has_personal_guarantors": True,
        "has_dishonoured_cheques": True,
        "cheque_dishonour_date": "2024-01-15",
        "npa_classification_date": "2023-11-30",
        "is_wilful_diversion_suspected": True,
        "has_foreign_travel_flight_risk": True,
        "current_ots_offer_amount": 25000000.0
    }
    res = client.post("/api/v1/bank/multi-track-strategy", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["borrower_name"] == "M/s Sterling Steel Fabrication Pvt Ltd"
    assert data["default_amount"] == 35000000.0
    assert "tracks" in data
    assert "track_1_s138" in data["tracks"]
    assert "track_2_sarfaesi" in data["tracks"]
    assert "track_3_drt" in data["tracks"]
    assert "track_4_ibc" in data["tracks"]
    assert "track_5_loc_wilful" in data["tracks"]
    assert data["tracks"]["track_1_s138"]["viability_score"] > 80
    assert data["tracks"]["track_2_sarfaesi"]["viability_score"] > 80
    assert data["tracks"]["track_3_drt"]["viability_score"] > 80
    assert data["tracks"]["track_4_ibc"]["viability_score"] >= 70
    assert "Transcore" in data["concurrent_forum_compatibility"]


def test_statutory_notice_drafting_s138():
    payload = {
        "document_type": "S138_DEMAND_NOTICE",
        "bank_name": "State Bank of India",
        "branch_name": "Stressed Asset Recovery Branch (SARB Mumbai)",
        "officer_name": "Rajesh Nambiar",
        "officer_designation": "Chief Manager & Authorized Officer",
        "borrower_name": "M/s Apex Retailers Pvt Ltd",
        "borrower_address": "Plot 42, Andheri East, Mumbai 400069",
        "loan_account_no": "SBI/SARB/MUM/2026/85012",
        "default_amount": 850000.0,
        "cheque_no": "881204",
        "cheque_date": "2024-01-10",
        "dishonour_date": "2024-01-18",
        "dishonour_reason": "Funds Insufficient",
        "notice_date": "2024-01-30"
    }
    res = client.post("/api/v1/bank/generate-statutory-notice", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert "SECTION 138" in data["title"]
    assert "FIFTEEN (15) DAYS" in data["markdown_content"]
    assert "Section 143A" in data["markdown_content"]
    assert len(data["mandatory_clauses_included"]) >= 3


def test_statutory_notice_drafting_sarfaesi():
    payload = {
        "document_type": "SARFAESI_13_2_NOTICE",
        "bank_name": "Punjab National Bank",
        "branch_name": "Large Corporate Recovery Division (Delhi)",
        "officer_name": "Vikram Rathore",
        "borrower_name": "Rathore Logistics Services Pvt Ltd",
        "loan_account_no": "PNB/CFS/DEL/2026/14092",
        "default_amount": 1400000.0,
        "property_description": "Commercial Warehouse Plot 19, Transport Nagar, Delhi"
    }
    res = client.post("/api/v1/bank/generate-statutory-notice", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert "SIXTY (60) DAYS" in data["markdown_content"]
    assert "Section 13(13)" in data["markdown_content"]


def test_statutory_evidence_certificate_65b():
    payload = {
        "document_type": "SECTION_65B_CERTIFICATE",
        "bank_name": "HDFC Bank",
        "branch_name": "Wholesale Recovery Dept (Mumbai)",
        "officer_name": "Anand Kulkarni",
        "borrower_name": "Kaveri Textiles & Apparels Pvt Ltd",
        "loan_account_no": "HDFC/WLR/CHE/2026/25041",
        "default_amount": 2500000.0
    }
    res = client.post("/api/v1/bank/generate-statutory-notice", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert "65B" in data["title"]
    assert "Arjun Panditrao Khotkar" in data["markdown_content"]


def test_ots_vs_litigation_npv_calculator():
    payload = {
        "default_principal": 5000000.0,
        "total_dues_with_interest": 6500000.0,
        "ots_offer_amount": 5200000.0,  # 20% haircut
        "anticipated_litigation_months": 24,
        "estimated_legal_and_court_costs": 250000.0,
        "estimated_recovery_probability": 0.75,
        "bank_discount_rate_annual": 0.09,
        "npa_age_years": 2.5
    }
    res = client.post("/api/v1/bank/ots-npv-calculator", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert data["ots_offer_amount"] == 5200000.0
    assert data["ots_haircut_percentage"] == 20.0
    assert data["litigation_net_realizable_value"] > 0
    assert data["rbi_provisioning_release_amount"] > 0
    assert data["recommendation_verdict"] in ["ACCEPT_OTS", "COUNTER_OFFER", "REJECT_AND_LITIGATE"]
    assert len(data["time_decay_breakdown"]) == 3


def test_empaneled_advocates_directory():
    res = client.get("/api/v1/bank/advocates")
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert data["total_advocates"] >= 4
    advs = data["advocates"]
    assert any(a["advocate_id"] == "ADV_MUM_01" for a in advs)
    assert any(a["advocate_id"] == "ADV_DEL_02" for a in advs)


def test_advocate_dispatch_endpoint():
    payload = {
        "case_reference": "SBI/SARB/MUM/2026/85012",
        "advocate_id": "ADV_MUM_01",
        "advocate_name": "Adv. Sudhir K. Deshmukh",
        "instructions": "Initiate Section 138 criminal complaint within 10 days of notice expiry."
    }
    res = client.post("/api/v1/bank/advocates/dispatch", json=payload)
    assert res.status_code == 200
    data = res.json()
    assert data["success"] is True
    assert data["status"] == "DISPATCHED_TO_PANEL"
    assert "ADV_MUM_01" in data["advocate_id"]


def test_sarfaesi_agricultural_land_hard_stop():
    payload = {
        "document_type": "SARFAESI_13_2_NOTICE",
        "bank_name": "State Bank of India",
        "branch_name": "SARB Mumbai",
        "officer_name": "Rajesh Kumar",
        "borrower_name": "Kisan Agro Products Pvt Ltd",
        "loan_account_no": "SBI/AGRI/2026/0192",
        "default_amount": 5000000.0,
        "is_agricultural_land": True,
        "cersai_registered": True
    }
    res = client.post("/api/v1/bank/generate-statutory-notice", json=payload)
    assert res.status_code == 400
    assert "Section 31(i)" in res.json()["detail"]


def test_sarfaesi_cersai_missing_hard_stop():
    payload = {
        "document_type": "SARFAESI_13_2_NOTICE",
        "bank_name": "Bank of Baroda",
        "branch_name": "CFS Delhi",
        "officer_name": "Amit Sharma",
        "borrower_name": "Delhi Retailers Ltd",
        "loan_account_no": "BOB/2026/991",
        "default_amount": 2000000.0,
        "is_agricultural_land": False,
        "cersai_registered": False
    }
    res = client.post("/api/v1/bank/generate-statutory-notice", json=payload)
    assert res.status_code == 400
    assert "Section 26D" in res.json()["detail"]


def test_advocate_review_watermark_present():
    payload = {
        "document_type": "S138_DEMAND_NOTICE",
        "bank_name": "State Bank of India",
        "branch_name": "SARB Mumbai",
        "officer_name": "Rajesh Kumar",
        "borrower_name": "Apex Infra",
        "loan_account_no": "SBI/2026/11",
        "default_amount": 1000000.0
    }
    res = client.post("/api/v1/bank/generate-statutory-notice", json=payload)
    assert res.status_code == 200
    assert "MANDATORY LEGAL NOTICE: FOR ADVOCATE REVIEW ONLY" in res.json()["markdown_content"]

