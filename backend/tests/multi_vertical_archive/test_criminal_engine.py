"""
tests/test_criminal_engine.py
------------------------------
Unit and integration tests for Criminal Engine backend components.
"""
import sys
import os

# Ensure backend root is on the path when running pytest from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from fastapi.testclient import TestClient
from main import app
from engine_core import JudiQEngine

# Fixed imports: modules live in the criminal package, not the backend root
from criminal.criminal_engine import CriminalEngine
from criminal.criminal_adversarial_engine import CriminalAdversarialEngine
from criminal.criminal_scoring_engine import CriminalScoringEngine
from criminal.criminal_timeline_engine import CriminalTimelineEngine
from criminal.criminal_rules_engine import CriminalRulesEngine
from criminal.criminal_economics_engine import CriminalEconomicsEngine

client = TestClient(app)


def test_criminal_case_full_analysis():
    case_data = {
        "case_type": "criminal",
        "client_role": "Accused",
        "offense_type": "420",
        "description": "FIR registered under Section 420 IPC alleging cheating. However, a written contract exists and partial payment was made.",
        "contract_exists": True,
        "partial_performance_done": True,
        "incident_date": "2025-01-10",
        "fir_date": "2025-02-15",
        "delay_explanation": None
    }
    result = JudiQEngine.analyze_case(case_data)

    assert result is not None
    # The engine returns 'final_score' or 'score' depending on the case type
    score_key = "final_score" if "final_score" in result else "score"
    assert score_key in result
    assert isinstance(result[score_key], (int, float))
    # Criminal-specific keys are nested under 'criminal_strategy'
    assert "bail_assessment" in result or "criminal_strategy" in result


def test_criminal_bail_assessment_arnesh_kumar():
    case_data = {
        "offense_type": "420",
        "flight_risk": False,
        "evidence_tampering_risk": False
    }
    concepts = []
    bail_info = CriminalEngine.assess_bail_probability(case_data, concepts)

    assert bail_info["probability"] == "VERY HIGH"
    assert bail_info["factors"]["arnesh_kumar_applicable"] is True
    assert "Arnesh Kumar" in bail_info["strategic_rationale"]


def test_criminal_bail_assessment_heinous():
    case_data = {
        "offense_type": "302",
        "flight_risk": True,
        "evidence_tampering_risk": True
    }
    concepts = [{"concept": "heinous_crime"}]
    bail_info = CriminalEngine.assess_bail_probability(case_data, concepts)

    assert bail_info["probability"] == "VERY LOW"
    assert bail_info["factors"]["heinous_offense"] is True


def test_criminal_timeline_default_bail():
    case_data = {
        "incident_date": "2025-01-01",
        "fir_date": "2025-01-02",
        "arrest_date": "2025-01-03",
        "chargesheet_date": None,
        "offense_type": "420"
    }
    res = CriminalTimelineEngine.analyze_timelines(case_data)
    assert "anomalies" in res
    assert "opportunities" in res


def test_criminal_rules_juvenile_and_sanction():
    case_data = {
        "age_at_incident": 16,
        "is_public_servant": True,
        "sanction_obtained": False
    }
    rules = CriminalRulesEngine.evaluate_rules(case_data)
    rule_names = [r["rule_name"] for r in rules]

    assert any("Juvenile" in name for name in rule_names)
    assert any("197" in name for name in rule_names)


def test_criminal_economics():
    case_data = {
        "punishment_years": 5,
        "offense_type": "420",
        "amount_involved": 500000
    }
    econ = CriminalEconomicsEngine.calculate_economics(case_data)
    assert "bail_economics" in econ
    assert "trial_vs_plea" in econ
    assert econ["trial_vs_plea"]["plea_bargain_eligible"] is True


def test_api_criminal_endpoints():
    response = client.post(
        "/api/v1/criminal/bail",
        json={"offense_type": "420", "flight_risk": False}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "bail_assessment" in data

    response_q = client.post(
        "/api/v1/criminal/quashing",
        json={"offense_type": "420", "contract_exists": True, "partial_performance_done": True}
    )
    assert response_q.status_code == 200
    data_q = response_q.json()
    assert data_q["success"] is True
    assert "quashing_grounds" in data_q
