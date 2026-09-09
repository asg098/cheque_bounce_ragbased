import sys
import os

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, backend_dir)

from sarfaesi.edrt_export_engine import EdrtExportEngine
from sarfaesi.cersai_verification_engine import CersaiVerificationEngine
from sarfaesi.redemption_engine import RedemptionEngine
from sarfaesi.section14_affidavit_engine import Section14AffidavitEngine
from sarfaesi.sarfaesi_domain_engine import SarfaesiDomainEngine

def test_edrt_export_bundle():
    case_data = {
        "case_id": "EDRT-TEST-001",
        "outstanding_amount": 7500000.0,
        "branch_name": "Commercial Branch, Mumbai",
        "property_location": "Bandra Kurla Complex, Mumbai",
        "drt_application_type": "SA_17"
    }
    bundle = EdrtExportEngine.generate_edrt_bundle(case_data)
    assert bundle["xml_payload_ready"] == True
    assert "Mumbai" in bundle["drt_bench"]
    assert bundle["court_fee_details"]["court_fee_payable"] > 0
    print("[PASS] e-DRT Export Engine generates compliant bundle.")

def test_cersai_verification():
    # Unregistered case
    case_unreg = {"cersai_registered": False, "outstanding_amount": 5000000.0}
    audit_unreg = CersaiVerificationEngine.verify_cersai_compliance(case_unreg)
    assert audit_unreg["statutory_bar_active"] == True
    assert audit_unreg["status"] == "FATAL_STATUTORY_BAR"

    # Registered case
    case_reg = {"cersai_registered": True, "cersai_security_id": "CERSAI-SI-2025-998811", "outstanding_amount": 5000000.0}
    audit_reg = CersaiVerificationEngine.verify_cersai_compliance(case_reg)
    assert audit_reg["statutory_bar_active"] == False
    assert audit_reg["section_26d_compliant"] == True
    print("[PASS] CERSAI Section 26D & 26E Verification Engine passed.")

def test_redemption_and_celir_doctrine():
    # Open redemption
    case_open = {
        "outstanding_amount": 10000000.0,
        "npa_date": "2025-01-01",
        "interest_rate": 12.0
    }
    red_open = RedemptionEngine.evaluate_redemption_status(case_open)
    assert red_open["right_to_redeem_extinguished"] == False
    assert red_open["redemption_calculation"]["total_redemption_amount_payable"] > 10000000.0

    # Extinguished redemption (Auction Notice Published per Celir LLP)
    case_ext = {
        "outstanding_amount": 10000000.0,
        "npa_date": "2025-01-01",
        "auction_notice_date": "2025-08-01"
    }
    red_ext = RedemptionEngine.evaluate_redemption_status(case_ext)
    assert red_ext["right_to_redeem_extinguished"] == True
    assert "Celir LLP" in red_ext["statutory_cut_off_ruling"]
    print("[PASS] Section 13(8) Redemption Engine (Celir LLP Doctrine) passed.")

def test_section14_noble_kumar_affidavit():
    case_sec14 = {
        "bank_name": "State Bank of India",
        "branch_name": "Commercial Branch, Pune",
        "outstanding_amount": 8000000.0,
        "npa_date": "2025-03-01",
        "notice_13_2_date": "2025-04-01",
        "property_description": "Industrial Plot No. 44, MIDC Bhosari, Pune",
        "cersai_registered": True
    }
    audit = Section14AffidavitEngine.audit_section14_readiness(case_sec14)
    assert audit["compliance_score_pct"] >= 88.0
    affidavit_txt = Section14AffidavitEngine.generate_affidavit_text(case_sec14)
    assert "MANDATORY 9-POINT SWORN AFFIDAVIT" in affidavit_txt
    assert "Noble Kumar" in affidavit_txt or "Clause i" in affidavit_txt
    print("[PASS] Section 14 Noble Kumar 9-Point Affidavit Engine passed.")

def test_full_sarfaesi_domain_engine_integration():
    case_payload = {
        "case_type": "SARFAESI",
        "perspective": "creditor",
        "bank_name": "Axis Bank",
        "branch_name": "Nariman Point, Mumbai",
        "outstanding_amount": 6500000.0,
        "npa_date": "2025-02-15",
        "notice_13_2_date": "2025-03-01",
        "cersai_registered": True,
        "cersai_security_id": "CERSAI-SI-2025-001928"
    }
    res = SarfaesiDomainEngine.analyze(case_payload)
    assert res["domain"] == "sarfaesi"
    assert "cersai_audit" in res
    assert "redemption_analysis" in res
    assert "section14_audit" in res
    assert "edrt_bundle" in res
    print("[PASS] Full SarfaesiDomainEngine analyze() returns all 4 institutional modules.")

if __name__ == "__main__":
    test_edrt_export_bundle()
    test_cersai_verification()
    test_redemption_and_celir_doctrine()
    test_section14_noble_kumar_affidavit()
    test_full_sarfaesi_domain_engine_integration()
    print("\n[ALL 5 INSTITUTIONAL SARFAESI SUITE TESTS PASSED 100%]")
