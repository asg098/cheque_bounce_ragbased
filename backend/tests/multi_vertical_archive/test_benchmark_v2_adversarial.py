import pytest
import json
import os
from engine_core import JudiQEngine

def load_v2_benchmark_suite():
    json_path = os.path.join(os.path.dirname(__file__), "benchmark_v2_adversarial_25.json")
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

BENCHMARK_V2_DATA = load_v2_benchmark_suite()

# Flatten test cases for parameterized pytest execution
ALL_V2_CASES = []
for cat in BENCHMARK_V2_DATA["categories"]:
    for tc in cat["cases"]:
        ALL_V2_CASES.append((cat["category_id"], cat["name"], tc["id"], tc["name"], tc["input"], tc["expected"]))

@pytest.mark.parametrize("cat_id,cat_name,case_id,case_name,input_data,expected", ALL_V2_CASES)
def test_benchmark_v2_case(cat_id, cat_name, case_id, case_name, input_data, expected):
    res = JudiQEngine.analyze_case(input_data)
    assert res is not None, f"Analysis failed for {case_id}: {case_name}"

    # 1. Check abstain_recommended if specified
    if expected.get("abstain_recommended"):
        abstain = res.get("abstain_recommended") or res.get("decision", {}).get("abstain") or (res.get("decision_status") in ["INSUFFICIENT_EVIDENCE", "LAWYER_REVIEW_REQUIRED"])
        assert abstain, f"[{case_id}] Engine failed to abstain on adversarial case! Got abstain={abstain}"

    # 2. Check decision_status if specified
    if "decision_status" in expected:
        status = res.get("decision_status") or res.get("decision", {}).get("status")
        assert status == expected["decision_status"], f"[{case_id}] Expected status '{expected['decision_status']}', got '{status}'"

    # 3. Check lawyer_override_required if specified
    if expected.get("lawyer_override_required"):
        req = res.get("lawyer_override_required") or (res.get("audit_entry", {}).get("status") == "AWAITING_OVERRIDE") or bool(res.get("cross_document_contradictions"))
        assert req, f"[{case_id}] Expected lawyer override requirement to be flagged."

    # 4. Check evidence_gap_contains if specified
    if "evidence_gap_contains" in expected:
        gaps = res.get("evidence_gaps", [])
        combined_gaps = " ".join([str(g.get("document_required", "")) + " " + str(g.get("consequence", "")) for g in gaps])
        assert expected["evidence_gap_contains"].lower() in combined_gaps.lower(), f"[{case_id}] Expected evidence gap '{expected['evidence_gap_contains']}', got '{combined_gaps}'"

    # 5. Check contradiction_contains if specified
    if "contradiction_contains" in expected:
        contradictions = res.get("cross_document_contradictions") or res.get("contradictions", [])
        combined_contra = " ".join([str(c.get("issue", "")) + " " + str(c.get("details", "")) for c in contradictions])
        assert expected["contradiction_contains"].lower() in combined_contra.lower(), f"[{case_id}] Expected contradiction '{expected['contradiction_contains']}', got '{combined_contra}'"

    # 6. Check citation_status if specified
    if "citation_status" in expected:
        auth = res.get("verified_authority", {})
        cit_status = auth.get("status")
        assert cit_status == expected["citation_status"], f"[{case_id}] Expected citation status '{expected['citation_status']}', got '{cit_status}'"

def test_v2_baseline_summary_scorecard():
    cat_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    cat_totals = {1: 5, 2: 5, 3: 5, 4: 5, 5: 5}

    for cat_id, cat_name, case_id, case_name, input_data, expected in ALL_V2_CASES:
        try:
            res = JudiQEngine.analyze_case(input_data)
            if res is not None:
                # Basic check if it ran
                cat_counts[cat_id] += 1
        except Exception:
            pass

    print("\n" + "=" * 60)
    print("      JUDIQ BENCHMARK V2 (ADVERSARIAL/UNSEEN) SCORECARD")
    print("=" * 60)
    for cat_id in range(1, 6):
        print(f"  Category {cat_id}: {cat_counts[cat_id]}/{cat_totals[cat_id]} Executed")
    print("-" * 60)
    total_passed = sum(cat_counts.values())
    print(f"  OVERALL BENCHMARK V2 RESULT: {total_passed}/25 Executed")
    print("=" * 60)
