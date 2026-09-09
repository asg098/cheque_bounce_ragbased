import pytest
from typing import Dict, Any
from sarfaesi.sarfaesi_domain_engine import SarfaesiDomainEngine
from scoring_engine import ScoringEngineV12

# -----------------------------------------------------------------------------
# SARFAESI HARD CASE SCENARIO GENERATORS (600 PARAMETERIZED LEGAL TEST CASES)
# -----------------------------------------------------------------------------

def generate_npa_s13_2_scenarios():
    """Generates 100 hard cases for NPA Classification & S.13(2) Demand Notice."""
    npa_days = [30, 60, 89, 90, 91, 120, 180, 365]
    itemized_breakup = [True, False]
    borrower_types = ["Individual", "Pvt Ltd Company", "Partnership Firm", "Guarantor", "Deceased Borrower"]
    notice_days = [15, 30, 45, 60, 61, 90]

    cases = []
    idx = 1
    for nd in npa_days:
        for ib in itemized_breakup:
            for bt in borrower_types:
                for n_days in notice_days:
                    cases.append({
                        "id": f"SARFAESI_NPA_{idx}",
                        "case_type": "SARFAESI",
                        "perspective": "borrower",
                        "npa_days": nd,
                        "npa_date": "2025-10-01",
                        "notice_13_2_date": "2026-01-01",
                        "itemized_breakup_provided": ib,
                        "borrower_type": bt,
                        "notice_duration_days": n_days,
                        "loan_amount": 5000000.0
                    })
                    idx += 1
                    if len(cases) >= 100:
                        return cases
    return cases


def generate_s13_3a_objection_scenarios():
    """Generates 100 hard cases for S.13(3A) Objection & Mandatory Bank Reply."""
    representation_filed = [True, False]
    representation_days = [5, 10, 15, 30, 45]
    bank_replied = [True, False]
    reply_days = [5, 10, 15, 16, 20, 30]
    reasoned_order = [True, False]

    cases = []
    idx = 1
    for rf in representation_filed:
        for r_days in representation_days:
            for br in bank_replied:
                for reply_d in reply_days:
                    for ro in reasoned_order:
                        cases.append({
                            "id": f"SARFAESI_13_3A_{idx}",
                            "case_type": "SARFAESI",
                            "perspective": "borrower",
                            "borrower_representation_filed": rf,
                            "representation_days_post_notice": r_days,
                            "bank_reply_13_3a_sent": br,
                            "bank_reply_days": reply_d,
                            "reasoned_order_passed": ro,
                            "loan_amount": 7500000.0
                        })
                        idx += 1
                        if len(cases) >= 100:
                            return cases
    return cases


def generate_s13_4_auction_scenarios():
    """Generates 100 hard cases for S.13(4) Possession & Rule 8/9 Auction Sale Rules."""
    possession_types = ["Symbolic", "Physical"]
    newspapers_published = [0, 1, 2]
    vernacular_included = [True, False]
    sale_notice_days = [15, 29, 30, 31, 45]
    valuation_report_attached = [True, False]

    cases = []
    idx = 1
    for pt in possession_types:
        for np in newspapers_published:
            for vi in vernacular_included:
                for snd in sale_notice_days:
                    for vra in valuation_report_attached:
                        cases.append({
                            "id": f"SARFAESI_13_4_{idx}",
                            "case_type": "SARFAESI",
                            "perspective": "borrower",
                            "possession_type": pt,
                            "newspapers_count": np,
                            "vernacular_newspaper": vi,
                            "sale_notice_days": snd,
                            "valuation_report_attached": vra,
                            "loan_amount": 12000000.0
                        })
                        idx += 1
                        if len(cases) >= 100:
                            return cases
    return cases


def generate_s14_dm_order_scenarios():
    """Generates 100 hard cases for Section 14 DM/CMM Applications & Affidavits."""
    affidavit_filed = [True, False]
    affidavit_points_covered = [5, 7, 9]
    tenant_in_possession = [True, False]
    registered_lease = [True, False]
    dm_order_days = [15, 30, 45, 60, 90]

    cases = []
    idx = 1
    for af in affidavit_filed:
        for apc in affidavit_points_covered:
            for tip in tenant_in_possession:
                for rl in registered_lease:
                    for dmd in dm_order_days:
                        cases.append({
                            "id": f"SARFAESI_S14_{idx}",
                            "case_type": "SARFAESI",
                            "perspective": "borrower",
                            "sec_14_affidavit_filed": af,
                            "affidavit_points_count": apc,
                            "tenant_in_possession": tip,
                            "registered_lease_deed": rl,
                            "dm_order_days": dmd,
                            "loan_amount": 8500000.0
                        })
                        idx += 1
                        if len(cases) >= 100:
                            return cases
    return cases


def generate_cersai_exemptions_scenarios():
    """Generates 100 hard cases for S.26D CERSAI Bar & Statutory Exemptions."""
    cersai_registered = [True, False]
    property_types = ["Agricultural Land", "Residential Flat", "Commercial Complex", "Industrial Plot"]
    debt_amounts = [50000.0, 99000.0, 150000.0, 5000000.0]
    remaining_pct = [15.0, 19.9, 20.0, 50.0]

    cases = []
    idx = 1
    for cr in cersai_registered:
        for pt in property_types:
            for da in debt_amounts:
                for rp in remaining_pct:
                    cases.append({
                        "id": f"SARFAESI_CERSAI_{idx}",
                        "case_type": "SARFAESI",
                        "perspective": "borrower",
                        "cersai_registered": cr,
                        "property_type": pt,
                        "outstanding_debt": da,
                        "debt_remaining_percentage": rp,
                        "loan_amount": da
                    })
                    idx += 1
                    if len(cases) >= 100:
                        return cases
    return cases


def generate_s17_drt_limitation_scenarios():
    """Generates 100 hard cases for S.17 Securitisation Application & DRT Limitation."""
    sa_filing_days = [10, 30, 44, 45, 46, 60, 90]
    applicant_roles = ["Borrower", "Guarantor", "Tenant", "Third Party Purchaser", "Unrelated Party"]
    drat_appeal = [True, False]
    pre_deposit_pct = [15, 25, 50, 0]

    cases = []
    idx = 1
    for sfd in sa_filing_days:
        for ar in applicant_roles:
            for da in drat_appeal:
                for pdp in pre_deposit_pct:
                    cases.append({
                        "id": f"SARFAESI_S17_{idx}",
                        "case_type": "SARFAESI",
                        "perspective": "borrower" if ar in ["Borrower", "Guarantor"] else "applicant",
                        "sa_filing_days": sfd,
                        "applicant_role": ar,
                        "drat_appeal_filed": da,
                        "pre_deposit_percentage": pdp,
                        "loan_amount": 15000000.0
                    })
                    idx += 1
                    if len(cases) >= 100:
                        return cases
    return cases


# Collect 600 total hard case scenarios (100 per module)
ALL_NPA_CASES = generate_npa_s13_2_scenarios()
ALL_13_3A_CASES = generate_s13_3a_objection_scenarios()
ALL_13_4_CASES = generate_s13_4_auction_scenarios()
ALL_S14_CASES = generate_s14_dm_order_scenarios()
ALL_CERSAI_CASES = generate_cersai_exemptions_scenarios()
ALL_S17_CASES = generate_s17_drt_limitation_scenarios()

TOTAL_SARFAESI_HARD_CASES = ALL_NPA_CASES + ALL_13_3A_CASES + ALL_13_4_CASES + ALL_S14_CASES + ALL_CERSAI_CASES + ALL_S17_CASES


# -----------------------------------------------------------------------------
# BENCHMARK TEST SUITE
# -----------------------------------------------------------------------------

def test_total_sarfaesi_hard_case_count():
    assert len(TOTAL_SARFAESI_HARD_CASES) >= 600, f"Expected 600+ hard cases, found {len(TOTAL_SARFAESI_HARD_CASES)}"


@pytest.mark.parametrize("case_data", ALL_NPA_CASES)
def test_hard_npa_scenarios(case_data: Dict[str, Any]):
    res = SarfaesiDomainEngine.analyze(case_data)
    assert "score" in res
    assert "procedural_graph" in res
    assert "next_actions" in res


@pytest.mark.parametrize("case_data", ALL_13_3A_CASES)
def test_hard_13_3a_scenarios(case_data: Dict[str, Any]):
    res = SarfaesiDomainEngine.analyze(case_data)
    assert "score" in res
    assert "procedural_graph" in res


@pytest.mark.parametrize("case_data", ALL_13_4_CASES)
def test_hard_13_4_scenarios(case_data: Dict[str, Any]):
    res = SarfaesiDomainEngine.analyze(case_data)
    assert "score" in res
    assert "procedural_graph" in res


@pytest.mark.parametrize("case_data", ALL_S14_CASES)
def test_hard_s14_scenarios(case_data: Dict[str, Any]):
    res = SarfaesiDomainEngine.analyze(case_data)
    assert "score" in res
    assert "procedural_graph" in res


@pytest.mark.parametrize("case_data", ALL_CERSAI_CASES)
def test_hard_cersai_scenarios(case_data: Dict[str, Any]):
    res = SarfaesiDomainEngine.analyze(case_data)
    assert "score" in res

    # S.26D CERSAI Statutory Bar Check
    if not case_data["cersai_registered"]:
        actions = res["next_actions"]
        assert any("CERSAI" in a["action"] or "26D" in a["authority"] for a in actions)


@pytest.mark.parametrize("case_data", ALL_S17_CASES)
def test_hard_s17_scenarios(case_data: Dict[str, Any]):
    res = SarfaesiDomainEngine.analyze(case_data)
    assert "score" in res
    assert "procedural_graph" in res
