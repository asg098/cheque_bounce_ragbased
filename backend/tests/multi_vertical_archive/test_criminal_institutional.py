import sys
import os

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, backend_dir)

from criminal.electronic_evidence_validator import ElectronicEvidenceValidator
from criminal.prosecution_rebuttal_engine import ProsecutionRebuttalEngine
from criminal.regional_bench_engine import RegionalBenchEngine
from criminal.ecourts_export_engine import EcourtsExportEngine
from criminal.criminal_engine import CriminalEngine

def test_electronic_evidence_hash_and_bsa_schedule():
    records = [
        {"file_name": "WhatsApp_Chat_Export.txt", "file_type": "text/plain", "content_str": "Payment confirmation"},
        {"file_name": "Bank_Ledger_Report.pdf", "file_type": "application/pdf", "content_str": "Official bank transaction ledger"}
    ]
    audit = ElectronicEvidenceValidator.validate_digital_evidence_payload(records, "Vikram Mehta")
    assert audit["all_evidence_admissible"] == True
    assert len(audit["forensic_audit_items"]) == 2
    assert "sha256_hash" in audit["forensic_audit_items"][0]
    assert "SCHEDULE OF ELECTRONIC RECORDS" in audit["statutory_schedule_text"]
    print("[PASS] Electronic Evidence Hash & BSA S.63(4) Schedule Validator passed.")

def test_prosecution_rebuttal_simulation():
    case_cheating = {
        "offense_type": "Section 420 IPC / Section 318 BNS",
        "cheque_amount": 10000000.0,
        "contract_exists": True,
        "accused_name": "ABC Global Pvt. Ltd."
    }
    rebuttal_res = ProsecutionRebuttalEngine.simulate_prosecution_counter_attacks(case_cheating)
    assert rebuttal_res["total_attack_vectors_simulated"] >= 3
    # Check that Mens Rea and Commercial Breach rebuttals are present
    attacks = rebuttal_res["prosecution_counter_attacks"]
    assert any("Mens Rea" in a["attack_title"] or "Deceptive" in a["attack_title"] for a in attacks)
    assert any("Dalip Kaur" in a["defense_precedent"] or "Bhajan Lal" in a["defense_precedent"] for a in attacks)
    print("[PASS] Prosecution Counter-Attack & Rebuttal Simulator passed.")

def test_regional_bench_customization():
    # Bombay High Court
    bombay = RegionalBenchEngine.format_quashing_petition_header({"court_name": "High Court of Bombay", "accused_city": "Mumbai"})
    assert "BOMBAY" in bombay["high_court_name"]
    assert len(bombay["mandatory_registry_declarations"]) >= 2

    # Delhi High Court
    delhi = RegionalBenchEngine.format_quashing_petition_header({"court_name": "Delhi High Court", "accused_city": "New Delhi"})
    assert "DELHI" in delhi["high_court_name"]
    assert "CRL.M.C." in delhi["statutory_petition_format"]
    print("[PASS] Regional High Court Bench Customizer passed.")

def test_ecourts_ingestion_bundle():
    case_payload = {
        "state": "Maharashtra",
        "district": "Mumbai",
        "accused_name": "Rohan Sharma",
        "complainant_name": "State of Maharashtra"
    }
    bundle = EcourtsExportEngine.generate_ecourts_ingestion_bundle(case_payload)
    assert bundle["cis_version"] == "CIS_3.2_ECOURTS_STANDARD"
    assert len(bundle["cnr_number"]) == 16
    assert bundle["court_fees_calculation"]["total_fee_payable"] > 0
    print("[PASS] e-Courts Services CIS 3.2 Ingestion Engine passed.")

def test_full_criminal_engine_analyze_integration():
    case_full = {
        "case_type": "criminal",
        "offense_type": "Section 420/406 IPC",
        "accused_name": "Vikram Mehta",
        "cheque_amount": 7500000.0,
        "contract_exists": True
    }
    res = CriminalEngine.analyze(case_full)
    assert res["domain"] == "criminal"
    assert "electronic_evidence_audit" in res
    assert "prosecution_rebuttal" in res
    assert "regional_bench_customization" in res
    assert "ecourts_bundle" in res
    print("[PASS] Full CriminalEngine.analyze() integration with 4 institutional modules passed.")

if __name__ == "__main__":
    test_electronic_evidence_hash_and_bsa_schedule()
    test_prosecution_rebuttal_simulation()
    test_regional_bench_customization()
    test_ecourts_ingestion_bundle()
    test_full_criminal_engine_analyze_integration()
    print("\n[ALL 5 CRIMINAL INSTITUTIONAL TEST SUITES PASSED 100%]")
