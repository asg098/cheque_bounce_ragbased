"""
Comprehensive Test Suite for Institutional Civil & Commercial Litigation Engine
Validates CPC 1908, Commercial Courts Act 2015, Specific Relief Act 1963, and Limitation Act 1963.
"""

import pytest
from civil.civil_engine import CivilEngine
from civil.civil_scoring_engine import CivilScoringEngine
from civil.cpc_statutory_rules import CPCStatutoryRules
from civil.injunction_evaluator import InjunctionEvaluator
from civil.specific_performance_engine import SpecificPerformanceEngine
from civil.order37_summary_suit_engine import Order37SummarySuitEngine
from civil.civil_defence_catalogue import CivilDefenceCatalogue
from civil.civil_suit_classifier import CivilSuitClassifier

def test_cpc_limitation_timely():
    res = CPCStatutoryRules.evaluate_limitation(
        cause_of_action_date="2025-01-01",
        filing_date="2026-06-01",
        article_key="article_55"
    )
    assert res["valid"] is True
    assert res["status"] == "WITHIN_LIMITATION"
    assert res["defect"] is None

def test_cpc_limitation_expired_fatal():
    res = CPCStatutoryRules.evaluate_limitation(
        cause_of_action_date="2020-01-01",
        filing_date="2026-06-01",
        article_key="article_55"
    )
    assert res["valid"] is False
    assert res["fatal"] is True
    assert "barred by limitation" in res["defect"]
    assert "Section 3 Limitation Act" in res["authority"]

def test_cpc_limitation_section_18_acknowledgment_renewal():
    res = CPCStatutoryRules.evaluate_limitation(
        cause_of_action_date="2020-01-01",
        filing_date="2026-06-01",
        article_key="article_55",
        written_acknowledgment_date="2024-05-01"
    )
    assert res["valid"] is True
    assert "Section 18" in res["renewal_applied"]

def test_cpc_limitation_pims_exclusion():
    # 3 years = 1095 days. Elapsed = 1150 days. But PIMS mediation = 90 days. Effective = 1060 days (within limit).
    res = CPCStatutoryRules.evaluate_limitation(
        cause_of_action_date="2023-01-01",
        filing_date="2026-02-25",
        article_key="article_55",
        pims_duration_days=90
    )
    assert res["valid"] is True
    assert res["status"] == "WITHIN_LIMITATION"
    assert res["pims_days_excluded"] == 90

def test_order21_execution_2year_notice():
    res_recent = CPCStatutoryRules.evaluate_order21_execution_timeline("2025-01-01", "2026-01-01")
    assert res_recent["notice_required"] is False

    res_old = CPCStatutoryRules.evaluate_order21_execution_timeline("2023-01-01", "2026-01-01")
    assert res_old["notice_required"] is True
    assert "Order XXI Rule 22" in res_old["statutory_rule"]

def test_commercial_pims_omission_without_urgent_relief_fatal():
    case = {
        "is_commercial": True,
        "suit_valuation_amount": 10000000.0,
        "s12a_pims_status": "Not Initiated (No Urgent Relief - Fatal Defect)",
        "urgent_interim_relief_prayed": False
    }
    res = CPCStatutoryRules.evaluate_commercial_courts_compliance(case, is_commercial=True)
    assert res["valid"] is False
    assert res["fatal"] is True
    assert any("MANDATORY_PIMS_BREACH" in d for d in res["defects"])

def test_commercial_pims_exemption_with_urgent_relief():
    case = {
        "is_commercial": True,
        "suit_valuation_amount": 10000000.0,
        "s12a_pims_status": "Not Initiated (Urgent Interim Relief Prayed)",
        "urgent_interim_relief_prayed": True
    }
    res = CPCStatutoryRules.evaluate_commercial_courts_compliance(case, is_commercial=True)
    assert res["valid"] is True
    assert res["fatal"] is False

def test_written_statement_120_day_forfeiture_commercial():
    res = CPCStatutoryRules.evaluate_written_statement_timeline(days_to_ws=125, is_commercial=True)
    assert res["valid"] is False
    assert res["fatal"] is True
    assert "SCG Contracts" in res["authority"]

def test_injunction_golden_triad_satisfaction():
    case = {
        "prima_facie_case_evidence": "Strong Documentary Title / Clear Unbroken Contract",
        "balance_of_convenience": "Favours Plaintiff (Greater Inconvenience)",
        "irreparable_injury_pleaded": "Irreversible Monetary / Physical Loss Incapable of Compensation"
    }
    res = InjunctionEvaluator.evaluate_order_39_injunction(case)
    assert res["golden_triad_satisfied"] is True
    assert res["injunction_granted_probability"] >= 90.0
    assert "Dalpat Kumar" in res["authority"]

def test_injunction_infrastructure_project_section_20a_bar():
    case = {
        "infrastructure_project": True,
        "prima_facie_case_evidence": "Strong Documentary Title"
    }
    res = InjunctionEvaluator.evaluate_order_39_injunction(case)
    assert res["golden_triad_satisfied"] is False
    assert res["injunction_granted_probability"] == 0.0
    assert "SECTION_20A_INFRASTRUCTURE_BAR" in res["fatal_bar"]

def test_specific_performance_missing_financial_capacity():
    case = {
        "readiness_and_willingness_proof": "No Financial Capacity Proof (Fatal Defect u/s 16(c))",
        "agreement_registered_and_stamped": "Duly Stamped & Registered"
    }
    res = SpecificPerformanceEngine.evaluate_specific_performance_claim(case)
    assert res["maintainable"] is False
    assert "SECTION_16C_READINESS_FATAL" in res["fatal_defect"]
    assert "U.N. Krishnamurthy" in res["key_precedent"]

def test_order37_summary_suit_sham_defense_hubtown():
    case = {
        "order37_instrument_type": "Bill of Exchange / Hundi",
        "leave_to_defend_days": 8,
        "defense_nature": "Frivolous / Sham / Moonshine Defense (Leave Refused)"
    }
    res = Order37SummarySuitEngine.evaluate_summary_suit(case)
    assert res["is_qualifying_instrument"] is True
    assert res["leave_to_defend_category"] == "LEAVE_REFUSED"
    assert res["plaintiff_entitled_to_immediate_decree"] is True

def test_civil_defence_arbitration_bar():
    case = {
        "arbitration_clause_exists": True
    }
    defenses = CivilDefenceCatalogue.analyze_defenses(case)
    assert len(defenses) >= 1
    assert any("Section 8" in d["name"] for d in defenses)

def test_full_civil_commercial_engine_analysis():
    case = {
        "case_type": "Civil",
        "suit_type": "Commercial Suit",
        "suit_valuation_amount": 50000000.0,
        "cause_of_action_date": "2025-05-10",
        "filing_date": "2026-06-15",
        "s12a_pims_status": "Mediation Attempted / Failed",
        "statement_of_truth_signed": True,
        "agreement_registered_and_stamped": "Duly Stamped & Registered",
        "urgent_interim_relief_prayed": True,
        "prima_facie_case_evidence": "Strong Documentary Title / Clear Unbroken Contract",
        "balance_of_convenience": "Favours Plaintiff (Greater Inconvenience)",
        "irreparable_injury_pleaded": "Irreversible Monetary / Physical Loss Incapable of Compensation"
    }
    result = CivilEngine.analyze(case)
    assert result["domain"] == "civil"
    assert result["score"] >= 75.0
    assert result["verdict"] == "STRONG_SUIT_VIABILITY"
    assert "procedural_graph" in result
    assert result["procedural_graph"]["suit_type_key"] == "commercial_suit"
    assert len(result["next_actions"]) >= 1
