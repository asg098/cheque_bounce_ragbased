"""
tests/test_criminal_scenarios.py
---------------------------------
Comprehensive test suite for Criminal Subsystem evaluating High, Medium,
and Low severity/complexity criminal litigation scenarios across all major
offence categories under both IPC/CrPC and BNS/BNSS statutory frameworks.
"""
import pytest
from typing import Dict, Any, List
from engine_core import JudiQEngine
from criminal.criminal_engine import CriminalEngine
from criminal.criminal_scoring_engine import CriminalScoringEngine
from criminal.criminal_adversarial_engine import CriminalAdversarialEngine
from criminal.criminal_rules_engine import CriminalRulesEngine
from criminal.criminal_timeline_engine import CriminalTimelineEngine
from criminal.criminal_economics_engine import CriminalEconomicsEngine


# =============================================================================
# SCENARIO GENERATORS: HIGH SEVERITY / STRONG PROSECUTION (LOW ACCUSED SCORE)
# =============================================================================

def generate_high_conviction_scenarios() -> List[Dict[str, Any]]:
    """
    Cases where prosecution case is very strong (accused score < 40, conviction prob > 60).
    Heinous crimes, forensic corroboration, clear mens rea, flight/tampering risks.
    """
    scenarios = []

    # 1. Homicide / Murder (S.302 IPC / S.103 BNS) with eye-witness + weapon recovery
    for flight in [True, False]:
        for in_custody in [True, False]:
            scenarios.append({
                "case_type": "criminal",
                "client_role": "Accused",
                "offense_type": "302",
                "max_punishment_years": 20,
                "has_eyewitness": True,
                "weapon_recovered": True,
                "motive_established": True,
                "flight_risk": flight,
                "evidence_tampering_risk": True,
                "in_custody": in_custody,
                "days_in_custody": 20,
                "chargesheet_filed": True,
                "description": "Accused charged u/s 302 IPC for premeditated murder. Multiple ocular witnesses and weapon recovered."
            })

    # 2. Sexual Offence / POCSO (S.376 IPC / S.64 BNS) with medical corroboration
    for age in [12, 14, 16]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": "376 POCSO",
            "max_punishment_years": 20,
            "victim_age": age,
            "medical_corroboration": True,
            "dna_match": True,
            "flight_risk": True,
            "in_custody": True,
            "days_in_custody": 45,
            "description": "POCSO and rape charge with DNA test confirmation and medical evidence."
        })

    # 3. NDPS Commercial Quantity with full compliance of S.50 & S.52A
    for qty in ["Commercial 5kg Heroin", "Commercial 50kg Ganja"]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": "NDPS",
            "max_punishment_years": 20,
            "ndps_case": True,
            "contraband_quantity": qty,
            "s50_violation": False,
            "s52a_inventory_done": True,
            "gazetted_officer_present": True,
            "in_custody": True,
            "days_in_custody": 30,
            "description": "Commercial quantity NDPS seizure with strict adherence to Section 50 and 52A."
        })

    # 4. Large-scale Organized Financial Fraud (S.420 / 467 / 120B IPC)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "420, 467, 468, 120B",
        "max_punishment_years": 10,
        "amount_involved": 50000000,
        "fake_identity_used": True,
        "forged_seals_recovered": True,
        "shell_companies_detected": True,
        "contract_exists": False,
        "flight_risk": True,
        "description": "Organized syndicated fraud with forged government seals and shell companies."
    })

    return scenarios


# =============================================================================
# SCENARIO GENERATORS: MEDIUM SEVERITY / CONTESTED DEFENSE (SCORE 40 - 70)
# =============================================================================

def generate_medium_scenarios() -> List[Dict[str, Any]]:
    """
    Cases with balanced evidence, procedural contestability, or partial defenses.
    """
    scenarios = []

    # 1. Cheating / Breach (S.420 IPC / S.318 BNS) with disputed contract
    for amount in [100000, 500000, 2000000]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": "420",
            "max_punishment_years": 7,
            "amount_involved": amount,
            "contract_exists": True,
            "partial_performance_done": False, # Breach happened, but dispute on inception mens rea
            "flight_risk": False,
            "evidence_tampering_risk": False,
            "description": "FIR under 420 IPC for loan non-repayment. Contract exists but accused failed to pay."
        })

    # 2. Matrimonial Dispute (S.498A IPC / S.85 BNS) for Husband (Direct Impleadment)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "498A",
        "max_punishment_years": 3,
        "relative_impleaded": False,
        "separate_residence": False,
        "flight_risk": False,
        "arrested_during_investigation": False,
        "description": "FIR under 498A against husband. No physical injury reported, marital discord admitted."
    })

    # 3. Assault / Sudden Altercation (S.324 / 325 IPC <-> S.117 BNS)
    for weapon in ["Lathi / Stick", "Fist / Hands"]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": "325",
            "max_punishment_years": 7,
            "sudden_fight": True,
            "weapon_used": weapon,
            "flight_risk": False,
            "cross_fir_filed": True,
            "description": "Mutual altercation in village pathway. Both parties filed cross-FIRs."
        })

    # 4. Homicide with sudden provocation / Ocular-Medical Contradiction (S.304 Part II)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "304",
        "max_punishment_years": 10,
        "sudden_quarrel": True,
        "premeditation_absent": True,
        "medical_contradicts_ocular": True,
        "flight_risk": False,
        "in_custody": True,
        "days_in_custody": 40,
        "description": "Charge under 304 IPC. Sudden quarrel without premeditation. Medical evidence shows single blunt injury."
    })

    return scenarios


# =============================================================================
# SCENARIO GENERATORS: LOW SEVERITY / FATAL DEFECTS / HIGH ACQUITTAL (> 70)
# =============================================================================

def generate_low_conviction_fatal_defect_scenarios() -> List[Dict[str, Any]]:
    """
    Cases where prosecution case is fatally defective or statutory bar applies.
    High defense score (> 70), strong quashing/discharge/bail grounds.
    """
    scenarios = []

    # 1. Juvenile Accused (JJ Act Absolute Jurisdictional Bar)
    for age in [14, 15, 16, 17]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": "302",
            "age_at_incident": age,
            "description": f"Accused was a juvenile ({age} yrs) on the date of alleged crime."
        })

    # 2. Public Servant acting under official duty without S.197 / S.218 Sanction
    for offense in ["420", "409", "120B", "323", "PMLA"]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": offense,
            "is_public_servant": True,
            "sanction_obtained": False,
            "description": "Public servant charged with official acts without Section 197 sanction."
        })

    # 3. Limitation Bar (S.468 CrPC / S.514 BNSS)
    for (punishment, years) in [(1, 2.5), (3, 4.0), (0.5, 1.2)]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": "504, 506" if punishment == 1 else "323, 341",
            "max_punishment_years": punishment,
            "limitation_years_passed": years,
            "incident_date": "2020-01-01",
            "fir_date": "2024-06-01",
            "description": "FIR filed well beyond the statutory limitation period."
        })

    # 4. S.167(2) Default Bail Ripe (Custody >= Threshold without Chargesheet)
    for (offense, custody, punishment) in [("420", 65, 7), ("302", 95, 20), ("NDPS", 92, 20)]:
        scenarios.append({
            "case_type": "criminal",
            "client_role": "Accused",
            "offense_type": offense,
            "max_punishment_years": punishment,
            "days_in_custody": custody,
            "chargesheet_filed": False,
            "description": f"Accused in custody for {custody} days without charge sheet."
        })

    # 5. Civil Dispute Clothed as Criminal (S.420 / 318 - Bhajan Lal Quashing)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "420",
        "contract_exists": True,
        "commercial_dispute": True,
        "partial_performance_done": True,
        "recovery_suit_pending": True,
        "description": "Written commercial supply agreement with partial payments made before dispute."
    })

    # 6. Matrimonial Omnibus Impleadment of In-Laws (S.498A Kahkashan Kausar)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "498A, 406",
        "relative_impleaded": True,
        "separate_residence": True,
        "description": "Parents and married sister of husband living in another state impleaded omnibus."
    })

    # 7. NDPS S.50 Mandatory Search Violation
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "NDPS",
        "ndps_case": True,
        "s50_violation": True,
        "description": "Personal search conducted without offering option of Gazetted Officer or Magistrate."
    })

    # 8. Uncertified Electronic Evidence (S.65B IEA / S.63 BSA)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "420",
        "electronic_evidence": True,
        "s65b_certificate": False,
        "description": "Entire prosecution case rests on uncertified WhatsApp chat printouts."
    })

    # 9. Consensual Relationship Courtship (S.376 Pramod Suryabhan Pawar)
    scenarios.append({
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "376",
        "consensual_relationship": True,
        "courtship_failed": True,
        "description": "Consensual relationship of 3 years between adults; marriage could not materialize."
    })

    return scenarios


# =============================================================================
# SCENARIO GENERATORS: BAIL ASSESSMENT MATRIX (ANTIL CATEGORIES A, B, C, D)
# =============================================================================

def generate_bail_matrix_scenarios() -> List[Dict[str, Any]]:
    """
    Generates 40+ scenarios covering all 4 Antil Categories with combinations
    of flight risk, custody, and offense severity.
    """
    scenarios = []

    # Category A (Punishment <= 7 yrs)
    for off in ["420", "406", "323", "504", "506", "318", "316", "115"]:
        for not_arrested in [True, False]:
            scenarios.append({
                "offense_type": off,
                "punishment_years": 5 if off in ["420", "406", "318", "316"] else 1,
                "flight_risk": False,
                "evidence_tampering_risk": False,
                "arrested_during_investigation": not not_arrested,
                "in_custody": False
            })

    # Category B (Heinous Offenses: Murder, Rape, S.307)
    for off in ["302", "376", "307", "103", "64", "109"]:
        for flight in [True, False]:
            scenarios.append({
                "offense_type": off,
                "punishment_years": 20,
                "flight_risk": flight,
                "evidence_tampering_risk": flight,
                "in_custody": True,
                "days_in_custody": 30
            })

    # Category C (Special Acts: NDPS, PMLA, POCSO)
    for off in ["NDPS", "PMLA", "POCSO"]:
        for custody_days in [30, 200]: # 200 days tests Article 21 trial delay override
            scenarios.append({
                "offense_type": off,
                "punishment_years": 20,
                "in_custody": True,
                "days_in_custody": custody_days,
                "pmla_trial_delay": custody_days > 180
            })

    # Category D (Economic Offenses)
    for amount in [500000, 50000000]:
        scenarios.append({
            "offense_type": "420, 467, 468, 471",
            "amount_involved": amount,
            "punishment_years": 10,
            "flight_risk": amount > 10000000,
            "in_custody": False
        })

    return scenarios


# =============================================================================
# SCENARIO GENERATORS: ECONOMICS, COMPOUNDING & PLEA BARGAINING
# =============================================================================

def generate_economics_scenarios() -> List[Dict[str, Any]]:
    """
    Generates scenarios testing S.320 CrPC / S.359 BNSS compounding & S.265A Plea Bargaining.
    """
    scenarios = []

    # Compoundable without permission (S.320(1))
    for off in ["323", "341", "426", "447", "504", "506", "115(2)", "329", "351(2)"]:
        scenarios.append({
            "offense_type": off,
            "punishment_years": 1,
            "amount_involved": 10000,
            "expected_compoundable": True,
            "expected_plea": True
        })

    # Compoundable with permission of Court (S.320(2))
    for off in ["420", "406", "324", "325", "384", "417", "318(4)", "316(2)"]:
        scenarios.append({
            "offense_type": off,
            "punishment_years": 5,
            "amount_involved": 500000,
            "expected_compoundable": True,
            "expected_plea": True
        })

    # Non-compoundable & Plea-barred (S.302, S.376, POCSO, NDPS)
    for off in ["302", "376", "POCSO", "NDPS", "103", "64"]:
        scenarios.append({
            "offense_type": off,
            "punishment_years": 20,
            "amount_involved": 0,
            "expected_compoundable": False,
            "expected_plea": False
        })

    return scenarios


# =============================================================================
# TESTS: HIGH CONVICTION SCENARIOS
# =============================================================================

@pytest.mark.parametrize("scenario", generate_high_conviction_scenarios())
def test_high_conviction_scenarios_evaluation(scenario):
    result = JudiQEngine.analyze_case(scenario)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]

    # Accused perspective: Score should be <= 50 for strong prosecution cases
    assert score <= 50, f"Expected accused defense score <= 50 for high conviction case, got {score}"

    # Bail probability check for Category B heinous
    if any(x in str(scenario["offense_type"]) for x in ["302", "376", "NDPS"]):
        bail = result.get("bail_assessment", {})
        if scenario.get("flight_risk") and scenario.get("evidence_tampering_risk"):
            assert bail.get("probability") in ["VERY LOW", "LOW", "MODERATE"]


# =============================================================================
# TESTS: MEDIUM CONVICTION / CONTESTED SCENARIOS
# =============================================================================

@pytest.mark.parametrize("scenario", generate_medium_scenarios())
def test_medium_scenarios_evaluation(scenario):
    result = JudiQEngine.analyze_case(scenario)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]

    # Accused perspective: Score should generally fall in realistic middle range (25 - 80)
    assert 25 <= score <= 80, f"Expected score in 25-80 range for medium case, got {score}"

    # Check that tactical moves and checkpoints are populated
    assert "strengths" in result or "strategy" in result
    assert "adversarial_risk_model" in result


# =============================================================================
# TESTS: LOW CONVICTION / FATAL DEFECT SCENARIOS (HIGH DEFENSE SCORE)
# =============================================================================

@pytest.mark.parametrize("scenario", generate_low_conviction_fatal_defect_scenarios())
def test_low_conviction_fatal_defect_scenarios_evaluation(scenario):
    result = JudiQEngine.analyze_case(scenario)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]

    # Accused perspective: Score should be >= 50 (often >= 70) due to fatal prosecution defects
    assert score >= 50, f"Expected defense score >= 50 for fatal defect case, got {score}"

    # Verify that statutory rules caught the specific defect
    rules = result.get("statutory_rules", [])
    rule_names = " ".join([r.get("rule_name", "") for r in rules])

    if scenario.get("age_at_incident") is not None:
        assert "Juvenile" in rule_names
    elif scenario.get("is_public_servant") and not scenario.get("sanction_obtained"):
        assert "197" in rule_names or "Sanction" in rule_names
    elif scenario.get("limitation_years_passed") is not None:
        assert "468" in rule_names or "Limitation" in rule_names
    elif scenario.get("days_in_custody", 0) >= 60 and not scenario.get("chargesheet_filed"):
        assert "167(2)" in rule_names or "Default Bail" in rule_names
    elif scenario.get("s50_violation"):
        assert "50" in rule_names or "NDPS" in rule_names


# =============================================================================
# TESTS: BAIL MATRIX EVALUATION (SATENDER KUMAR ANTIL)
# =============================================================================

@pytest.mark.parametrize("scenario", generate_bail_matrix_scenarios())
def test_bail_matrix_evaluation(scenario):
    concepts = []
    if any(x in str(scenario["offense_type"]) for x in ["302", "376", "NDPS", "103", "64"]):
        concepts.append({"concept": "heinous_crime"})
    bail_res = CriminalEngine.assess_bail_probability(scenario, concepts)

    assert "probability" in bail_res
    assert "antil_category" in bail_res
    assert "strategic_rationale" in bail_res

    # Category A without custody must be VERY HIGH
    if bail_res["antil_category"] == "Category A (Punishment <= 7 Years)" and not scenario.get("in_custody") and not scenario.get("arrested_during_investigation"):
        assert bail_res["probability"] in ["VERY HIGH", "HIGH"]


# =============================================================================
# TESTS: ECONOMICS, COMPOUNDING & PLEA BARGAINING EVALUATION
# =============================================================================

@pytest.mark.parametrize("scenario", generate_economics_scenarios())
def test_economics_and_compounding_evaluation(scenario):
    econ = CriminalEconomicsEngine.calculate_economics(scenario)

    assert "bail_economics" in econ
    assert "compounding_and_settlement" in econ
    assert "trial_vs_plea" in econ

    assert econ["compounding_and_settlement"]["is_compoundable"] == scenario["expected_compoundable"]
    assert econ["trial_vs_plea"]["plea_bargain_eligible"] == scenario["expected_plea"]


# =============================================================================
# COMPLAINANT PERSPECTIVE DUAL EVALUATION
# =============================================================================

def test_complainant_perspective_scoring():
    """
    Tests that Complainant role returns high score for strong cases and low score for defective cases.
    """
    strong_complainant_case = {
        "case_type": "criminal",
        "client_role": "Complainant",
        "offense_type": "302",
        "has_eyewitness": True,
        "weapon_recovered": True,
        "motive_established": True,
        "description": "Complainant seeks conviction with strong ocular and forensic evidence."
    }
    strong_res = JudiQEngine.analyze_case(strong_complainant_case)
    assert strong_res["final_score"] >= 60

    weak_complainant_case = {
        "case_type": "criminal",
        "client_role": "Complainant",
        "offense_type": "504",
        "max_punishment_years": 1,
        "limitation_years_passed": 3,
        "description": "Complainant filing after limitation expired."
    }
    weak_res = JudiQEngine.analyze_case(weak_complainant_case)
    assert weak_res["final_score"] <= 40

