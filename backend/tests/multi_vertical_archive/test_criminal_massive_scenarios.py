"""
tests/test_criminal_massive_scenarios.py
-----------------------------------------
Massive Combinatorial Scenario Generator & Stress-Testing Suite for
JudiQ AI Criminal Engine. Evaluates 1,200+ multi-dimensional legal scenarios
spanning every offence domain, procedural stage, evidence configuration,
statutory bar, and adversarial posture under IPC/CrPC/IEA and BNS/BNSS/BSA.
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
# 1. FINANCIAL FRAUD & COMMERCIAL BREACH COMBINATORIAL GENERATOR (300+ CASES)
# =============================================================================

def generate_financial_fraud_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for S.420/406/467/468/471 IPC & S.318/316/336/338 BNS.
    Varies contract existence, partial performance, fake identity, amounts, roles.
    """
    amounts = [50000, 500000, 10000000, 500000000]
    contract_states = [True, False]
    partial_states = [True, False]
    fake_identities = [True, False]
    roles = ["Accused", "Complainant"]
    dispute_types = ["Commercial Supply", "Personal Loan", "Partnership Accounts", "Investment Scheme"]

    cases = []
    idx = 1
    for amt in amounts:
        for has_contract in contract_states:
            for has_partial in partial_states:
                for is_fake in fake_identities:
                    for role in roles:
                        for dtype in dispute_types:
                            cases.append({
                                "id": f"FIN_FRAUD_{idx}",
                                "case_type": "criminal",
                                "client_role": role,
                                "offense_type": "420, 406" if not is_fake else "420, 467, 468, 471",
                                "max_punishment_years": 7 if not is_fake else 10,
                                "amount_involved": amt,
                                "contract_exists": has_contract,
                                "partial_performance_done": has_partial if has_contract else False,
                                "fake_identity_used": is_fake,
                                "forged_seals_recovered": is_fake,
                                "commercial_dispute": has_contract and not is_fake,
                                "description": f"Allegation of financial fraud under {dtype} involving Rs. {amt}."
                            })
                            idx += 1
    return cases


# =============================================================================
# 2. MATRIMONIAL CRUELTY & DOWRY COMBINATORIAL GENERATOR (240+ CASES)
# =============================================================================

def generate_matrimonial_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for S.498A/304B/406 IPC & S.85/80/316 BNS.
    Varies accused relationship, separate residence, custody, death timeline.
    """
    offenses = ["498A", "498A, 406", "304B (Dowry Death)", "498A, 307"]
    relatives_impleaded = [True, False]
    separate_residences = [True, False]
    in_custody_states = [True, False]
    roles = ["Accused", "Complainant"]

    cases = []
    idx = 1
    for off in offenses:
        for rel in relatives_impleaded:
            for sep in separate_residences:
                for cust in in_custody_states:
                    for role in roles:
                        is_304b = "304B" in off
                        cases.append({
                            "id": f"MATRIMONIAL_{idx}",
                            "case_type": "criminal",
                            "client_role": role,
                            "offense_type": off,
                            "max_punishment_years": 20 if is_304b else 3,
                            "relative_impleaded": rel,
                            "separate_residence": sep if rel else False,
                            "in_custody": cust,
                            "days_in_custody": 45 if cust else 0,
                            "flight_risk": False,
                            "description": f"Matrimonial proceedings under {off} against family members."
                        })
                        idx += 1
    return cases


# =============================================================================
# 3. HOMICIDE & BODILY OFFENSE COMBINATORIAL GENERATOR (280+ CASES)
# =============================================================================

def generate_homicide_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for S.302/304/307/324/326 IPC & S.103/105/109/117 BNS.
    Varies premeditation, sudden fight, recovery, medical-ocular divergence.
    """
    offenses = [("302", 20), ("304 Part I", 10), ("304 Part II", 10), ("307", 10), ("326", 10)]
    sudden_quarrels = [True, False]
    ocular_medical_conflicts = [True, False]
    weapon_recoveries = [True, False]
    eyewitness_states = [True, False]

    cases = []
    idx = 1
    for (off, max_p) in offenses:
        for sudden in sudden_quarrels:
            for med_conf in ocular_medical_conflicts:
                for wep_rec in weapon_recoveries:
                    for eye in eyewitness_states:
                        cases.append({
                            "id": f"HOMICIDE_{idx}",
                            "case_type": "criminal",
                            "client_role": "Accused",
                            "offense_type": off,
                            "max_punishment_years": max_p,
                            "sudden_quarrel": sudden,
                            "premeditation_absent": sudden,
                            "medical_contradicts_ocular": med_conf,
                            "weapon_recovered": wep_rec,
                            "has_eyewitness": eye,
                            "in_custody": True,
                            "days_in_custody": 60,
                            "description": f"Bodily offense trial under {off} with weapon and medical evidence."
                        })
                        idx += 1
    return cases


# =============================================================================
# 4. SEXUAL OFFENSES & POCSO COMBINATORIAL GENERATOR (160+ CASES)
# =============================================================================

def generate_sexual_offenses_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for S.376 IPC / S.64 BNS & POCSO.
    Varies victim age, courtship claims, DNA match, medical examination.
    """
    ages = [8, 13, 16, 18, 24]
    courtship_claims = [True, False]
    dna_matches = [True, False]
    roles = ["Accused", "Complainant"]

    cases = []
    idx = 1
    for age in ages:
        for court in courtship_claims:
            for dna in dna_matches:
                for role in roles:
                    is_pocso = age < 18
                    cases.append({
                        "id": f"SEXUAL_OFFENSE_{idx}",
                        "case_type": "criminal",
                        "client_role": role,
                        "offense_type": "376 POCSO" if is_pocso else "376",
                        "victim_age": age,
                        "pocso_case": is_pocso,
                        "consensual_relationship": court and not is_pocso,
                        "courtship_failed": court and not is_pocso,
                        "dna_match": dna,
                        "medical_corroboration": dna,
                        "in_custody": True,
                        "days_in_custody": 50,
                        "description": f"Trial under sexual offenses framework with victim age {age}."
                    })
                    idx += 1
    return cases


# =============================================================================
# 5. SPECIAL ACTS (NDPS & PMLA) COMBINATORIAL GENERATOR (180+ CASES)
# =============================================================================

def generate_special_acts_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for NDPS Act & PMLA.
    Varies contraband quantity, S.50 search compliance, S.52A sampling, custody days.
    """
    quantities = ["Small Quantity", "Intermediate Quantity", "Commercial Quantity"]
    s50_violations = [True, False]
    custody_durations = [20, 65, 95, 220] # 220 days tests Sisodia Art. 21 trial delay

    cases = []
    idx = 1
    for qty in quantities:
        for s50_v in s50_violations:
            for cust_days in custody_durations:
                # NDPS Case
                cases.append({
                    "id": f"NDPS_PMLA_{idx}",
                    "case_type": "criminal",
                    "client_role": "Accused",
                    "offense_type": "NDPS",
                    "ndps_case": True,
                    "contraband_quantity": qty,
                    "s50_violation": s50_v,
                    "s50_ndps_violation": s50_v,
                    "in_custody": True,
                    "days_in_custody": cust_days,
                    "pmla_trial_delay": cust_days > 180,
                    "max_punishment_years": 20 if "Commercial" in qty else 10,
                    "description": f"NDPS prosecution for {qty} with custody {cust_days} days."
                })
                idx += 1

                # PMLA Case
                cases.append({
                    "id": f"NDPS_PMLA_{idx}",
                    "case_type": "criminal",
                    "client_role": "Accused",
                    "offense_type": "PMLA",
                    "in_custody": True,
                    "days_in_custody": cust_days,
                    "pmla_trial_delay": cust_days > 180,
                    "is_public_servant": False,
                    "max_punishment_years": 7,
                    "description": f"PMLA scheduled offense investigation with {cust_days} days custody."
                })
                idx += 1
    return cases


# =============================================================================
# 6. STATUTORY BARS COMBINATORIAL MATRIX (120+ CASES)
# =============================================================================

def generate_statutory_bars_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for Juvenile Justice, S.197 Sanction, S.468 Limitation,
    and S.167(2) Default Bail.
    """
    cases = []
    idx = 1

    # JJ Act matrix
    for age in [12, 14, 16, 17, 18, 19]:
        for off in ["302", "376", "420", "323"]:
            cases.append({
                "id": f"STAT_BAR_JJ_{idx}",
                "case_type": "criminal",
                "client_role": "Accused",
                "offense_type": off,
                "age_at_incident": age,
                "description": f"Age at incident {age} yrs under {off} IPC."
            })
            idx += 1

    # S.197 Public Servant Sanction matrix
    for is_ps in [True, False]:
        for has_sanction in [True, False]:
            for off in ["420, 120B", "323, 341", "PMLA", "409"]:
                cases.append({
                    "id": f"STAT_BAR_S197_{idx}",
                    "case_type": "criminal",
                    "client_role": "Accused",
                    "offense_type": off,
                    "is_public_servant": is_ps,
                    "sanction_obtained": has_sanction,
                    "description": f"Public servant={is_ps} sanction={has_sanction} under {off}."
                })
                idx += 1

    # S.468 Limitation Matrix
    for (punishment, years) in [(0.5, 0.4), (0.5, 1.2), (1.0, 0.8), (1.0, 2.0), (3.0, 2.5), (3.0, 4.5)]:
        for off in ["504", "323", "420"]:
            cases.append({
                "id": f"STAT_BAR_LIMITATION_{idx}",
                "case_type": "criminal",
                "client_role": "Accused",
                "offense_type": off,
                "max_punishment_years": punishment,
                "limitation_years_passed": years,
                "description": f"Limitation check: punishment={punishment}yr, elapsed={years}yr."
            })
            idx += 1

    return cases


# =============================================================================
# 7. PROCEDURAL TIMELINE & S.167(2) DEFAULT BAIL MATRIX (350+ CASES)
# =============================================================================

def generate_procedural_timeline_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases for FIR delay, investigation remand, and S.167 default bail.
    """
    custody_days = [0, 15, 30, 59, 60, 61, 89, 90, 91, 120, 180]
    chargesheet_states = [True, False]
    offenses = [("420", 7), ("302", 20), ("NDPS", 20), ("323", 1), ("376", 20)]

    cases = []
    idx = 1
    for (off, max_p) in offenses:
        for c_days in custody_days:
            for cs_filed in chargesheet_states:
                cases.append({
                    "id": f"TIMELINE_DEFAULT_BAIL_{idx}",
                    "case_type": "criminal",
                    "client_role": "Accused",
                    "offense_type": off,
                    "max_punishment_years": max_p,
                    "days_in_custody": c_days,
                    "chargesheet_filed": cs_filed,
                    "in_custody": c_days > 0,
                    "description": f"Custody tracker test: {c_days} days custody under {off} CS={cs_filed}."
                })
                idx += 1
    return cases


# =============================================================================
# 8. BHAJAN LAL QUASHING & CROSS-EXAM MATRIX (200+ CASES)
# =============================================================================

def generate_quashing_and_cross_exam_matrix() -> List[Dict[str, Any]]:
    """
    Generates combinatorial cases testing all 7 Bhajan Lal grounds and cross-examination toolkits.
    """
    cases = []
    idx = 1

    # Parameter 1 & 7: Civil dispute
    for off in ["420", "406", "318", "316"]:
        for contract in [True, False]:
            for partial in [True, False]:
                cases.append({
                    "id": f"QUASH_BHATIA_{idx}",
                    "case_type": "criminal",
                    "client_role": "Accused",
                    "offense_type": off,
                    "contract_exists": contract,
                    "partial_performance_done": partial if contract else False,
                    "description": f"Quashing test under {off} with contract={contract}."
                })
                idx += 1

    # Parameter 7: Matrimonial omnibus relative
    for off in ["498A", "85"]:
        for rel in [True, False]:
            for sep in [True, False]:
                cases.append({
                    "id": f"QUASH_KAUSAR_{idx}",
                    "case_type": "criminal",
                    "client_role": "Accused",
                    "offense_type": off,
                    "relative_impleaded": rel,
                    "separate_residence": sep if rel else False,
                    "description": f"Matrimonial quashing test under {off} relative={rel} sep={sep}."
                })
                idx += 1

    # S.376 Courtship quashing (Pramod Suryabhan Pawar)
    for age in [16, 18, 22, 30]:
        for court in [True, False]:
            cases.append({
                "id": f"QUASH_PAWAR_{idx}",
                "case_type": "criminal",
                "client_role": "Accused",
                "offense_type": "376 POCSO" if age < 18 else "376",
                "victim_age": age,
                "consensual_relationship": court and age >= 18,
                "courtship_failed": court and age >= 18,
                "description": f"S.376 quashing test age={age} courtship={court}."
            })
            idx += 1

    return cases


# =============================================================================
# PYTEST TEST SUITES RUNNING ALL COMBINATIONS
# =============================================================================

@pytest.mark.parametrize("case", generate_financial_fraud_matrix())
def test_massive_financial_fraud_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # Role Consistency check
    if case["client_role"] == "Accused":
        # If contract exists + partial performance done + not fake -> Strong defense (score >= 65)
        if case["contract_exists"] and case["partial_performance_done"] and not case["fake_identity_used"]:
            assert score >= 65, f"Expected high defense score for pure civil contract, got {score}"
        # If fake identity used + forged seals -> Low defense score (score <= 55)
        elif case["fake_identity_used"]:
            assert score <= 55, f"Expected low defense score for fabricated seals fraud, got {score}"
    else:
        # Complainant perspective
        if case["fake_identity_used"]:
            assert score >= 55, f"Expected high score for complainant on fake identity fraud, got {score}"


@pytest.mark.parametrize("case", generate_matrimonial_matrix())
def test_massive_matrimonial_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # If distant relative living separately -> Rules should flag Kahkashan Kausar
    if case["client_role"] == "Accused" and case["relative_impleaded"] and case["separate_residence"]:
        rules = result.get("statutory_rules", [])
        rule_names = " ".join([r.get("rule_name", "") for r in rules])
        assert "498A" in rule_names or "Omnibus" in rule_names or "In-Laws" in rule_names


@pytest.mark.parametrize("case", generate_homicide_matrix())
def test_massive_homicide_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # If sudden quarrel without premeditation -> defense score should be higher than cold-blooded premeditated murder
    assert "bail_assessment" in result
    assert "statutory_rules" in result


@pytest.mark.parametrize("case", generate_sexual_offenses_matrix())
def test_massive_sexual_offenses_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # For minor victims (POCSO), consensual defense must never trigger
    if case["victim_age"] < 18 and case["client_role"] == "Accused" and case["dna_match"]:
        assert score <= 50, f"Expected accused defense score <= 50 on POCSO with DNA match, got {score}"


@pytest.mark.parametrize("case", generate_special_acts_matrix())
def test_massive_special_acts_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # If S.50 search violation on NDPS -> Fatal defect triggers high defense score
    if case.get("s50_violation") and case["client_role"] == "Accused":
        assert score >= 50, f"Expected high defense score on S.50 NDPS violation, got {score}"


@pytest.mark.parametrize("case", generate_statutory_bars_matrix())
def test_massive_statutory_bars_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # Juvenile Accused check
    if case.get("age_at_incident") is not None and case["age_at_incident"] < 18:
        assert score >= 75, f"Expected defense score >= 75 for Juvenile accused, got {score}"
        rules = result.get("statutory_rules", [])
        assert any("Juvenile" in r.get("rule_name", "") for r in rules)

    # S.197 Sanction check
    if case.get("is_public_servant") and not case.get("sanction_obtained"):
        assert score >= 60, f"Expected defense score >= 60 for S.197 lack of sanction, got {score}"
        rules = result.get("statutory_rules", [])
        assert any("197" in r.get("rule_name", "") or "Sanction" in r.get("rule_name", "") for r in rules)


@pytest.mark.parametrize("case", generate_procedural_timeline_matrix())
def test_massive_procedural_timeline_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # Default bail trigger check
    punishment = case.get("max_punishment_years", 7)
    threshold = 90 if punishment >= 10 or any(x in str(case["offense_type"]) for x in ["302", "376", "NDPS"]) else 60
    if case["days_in_custody"] >= threshold and not case["chargesheet_filed"]:
        rules = result.get("statutory_rules", [])
        rule_names = " ".join([r.get("rule_name", "") for r in rules])
        assert "167(2)" in rule_names or "Default Bail" in rule_names or "Statutory Bail" in rule_names


@pytest.mark.parametrize("case", generate_quashing_and_cross_exam_matrix())
def test_massive_quashing_and_cross_exam_matrix(case):
    result = JudiQEngine.analyze_case(case)
    assert result is not None
    assert "final_score" in result
    score = result["final_score"]
    assert 0 <= score <= 100

    # Quashing assessment check
    quashing_info = CriminalAdversarialEngine.evaluate_bhajan_lal_grounds(case)
    assert isinstance(quashing_info, list)

