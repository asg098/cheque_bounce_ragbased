"""
tests/test_engine.py
--------------------
Integration tests for JudiQEngine.analyze_case().

Design decisions:
- `llm_disabled` fixture patches llm_engine.LLM_AVAILABLE to False so tests
  run fully deterministically in CI without any Groq API key.
- All assertions are structural (key presence) or numeric (score thresholds).
  No string matching against reasoning_trace or draft content — those are
  presentation details that change frequently and produce brittle tests.
- Realistic, legally-coherent case data is provided so the rule engine's
  pillar scoring can reach meaningful score ranges.
"""
import pytest
import llm_engine
from engine_core import JudiQEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def llm_disabled(monkeypatch):
    """Force the engine into fully deterministic (LLM-less) mode."""
    monkeypatch.setattr(llm_engine, "LLM_AVAILABLE", False)
    # Also patch the module-level flag read by engine_core at import time
    import engine_core
    monkeypatch.setattr(engine_core, "LLM_AVAILABLE", False)


# ---------------------------------------------------------------------------
# Structural smoke test — no case data needed
# ---------------------------------------------------------------------------

def test_engine_has_analyze_case():
    """JudiQEngine must expose a callable analyze_case class method."""
    assert hasattr(JudiQEngine, "analyze_case")
    assert callable(JudiQEngine.analyze_case)


# ---------------------------------------------------------------------------
# Cheque bounce — strong / all-pillars-present case
# ---------------------------------------------------------------------------

def test_cheque_bounce_strong_case(llm_disabled):
    """
    All four S.138 NI Act pillars are present.
    Engine should score >= 60 for a file-worthy verdict.
    """
    case_data = {
        "case_type": "cheque_bounce",
        "client_role": "Complainant",
        "cheque_present": True,
        "cheque_proof_type": "original",
        "dishonour_memo": True,
        "notice_sent": True,
        "notice_date": "2026-02-05",
        "notice_received_type": "Delivered",
        "notice_received_date": "2026-02-08",
        "dishonour_date": "2026-01-15",
        "filing_date": "2026-03-01",
        "within_30_days": "Yes",
        "debt_proven": True,
        "cheque_amount": "500000",
        "payee_bank_city": "Mumbai",
        "loan_via_bank": True,
        "complainant_itr_available": True,
        "evidence_texts": {"itr": "income tax pan return"},
        "description": (
            "The accused issued a cheque of Rs. 5,00,000 towards repayment of "
            "a loan. The cheque was dishonoured due to insufficient funds. A "
            "legal demand notice was served within 30 days of dishonour."
        ),
    }
    result = JudiQEngine.analyze_case(case_data)

    assert result is not None
    assert "final_score" in result
    assert isinstance(result["final_score"], (int, float))
    assert result["final_score"] >= 60, (
        f"Expected final_score >= 60 for a strong cheque case, got {result['final_score']}"
    )


# ---------------------------------------------------------------------------
# Cheque bounce — fatal: no legal notice sent
# ---------------------------------------------------------------------------

def test_cheque_bounce_fatal_no_notice(llm_disabled):
    """
    Without a demand notice (S.138(b)), the complaint is non-maintainable.
    Score must be < 50, OR the verdict must signal non-filing.
    """
    case_data = {
        "case_type": "cheque_bounce",
        "client_role": "Complainant",
        "cheque_present": True,
        "cheque_proof_type": "original",
        "dishonour_memo": True,
        "notice_sent": False,
        "notice_date": None,
        "within_30_days": "Yes",
        "debt_proven": True,
        "cheque_amount": "500000",
        "payee_bank_city": "Mumbai",
        "description": (
            "Cheque of Rs. 5,00,000 dishonoured. No demand notice was issued."
        ),
    }
    result = JudiQEngine.analyze_case(case_data)

    assert result is not None
    assert "final_score" in result

    score = result["final_score"]
    verdict = result.get("verdict", "")
    assert score < 50 or verdict in ("DO NOT FILE", "REPAIR BEFORE FILING"), (
        f"Expected score < 50 or a non-filing verdict; got score={score}, verdict={verdict!r}"
    )


# ---------------------------------------------------------------------------
# Criminal case — graceful handling
# ---------------------------------------------------------------------------

def test_criminal_case_graceful(llm_disabled):
    """
    Submitting a non-138 criminal case to the dedicated Section 138 platform
    must trigger an explicit, clean domain guardrail ValueError.
    """
    case_data = {
        "case_type": "Criminal",
        "client_role": "Accused",
        "offense_type": "420",
        "has_eyewitness": True,
        "fir_copy": True,
        "police_complaint_filed": True,
        "description": (
            "FIR filed under Section 420 IPC for alleged cheating. "
            "Eyewitness testimony available. Accused seeks bail."
        ),
    }
    with pytest.raises(ValueError, match="Section 138"):
        JudiQEngine.analyze_case(case_data)


# ---------------------------------------------------------------------------
# Hybrid case — limitation issue forces a fatal score
# ---------------------------------------------------------------------------

def test_hybrid_fatal_limitation(llm_disabled):
    """
    A case with limitation_issue=True must score below 60.
    This validates that the fatal-defect hard-cap mechanism fires correctly.
    """
    case_data = {
        "case_type": "cheque_bounce",
        "client_role": "Complainant",
        "cheque_present": True,
        "dishonour_memo": True,
        "notice_sent": True,
        "within_30_days": "Yes",
        "debt_proven": True,
        "cheque_amount": "200000",
        "limitation_issue": True,           # signal the limitation bar
        "limitation_barred": True,          # belt-and-suspenders for scoring engine
        "payee_bank_city": "Pune",
        "description": (
            "Cheque dishonoured. However, the complaint is filed well beyond "
            "the three-year limitation period — limitation bar applies."
        ),
    }
    result = JudiQEngine.analyze_case(case_data)

    assert result is not None
    assert "final_score" in result
    assert result["final_score"] < 60, (
        f"Expected final_score < 60 due to limitation issue, got {result['final_score']}"
    )
