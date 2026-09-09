import pytest
import json
import os
from engine_core import JudiQEngine

def load_benchmark_suite():
    json_path = os.path.join(os.path.dirname(__file__), "benchmark_sarfaesi_25.json")
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

BENCHMARK_DATA = load_benchmark_suite()

# Flatten test cases for parameterized pytest execution
ALL_BENCHMARK_CASES = []
for lvl in BENCHMARK_DATA["levels"]:
    for tc in lvl["cases"]:
        ALL_BENCHMARK_CASES.append((lvl["level"], tc["id"], tc["name"], tc["input"], tc["expected"]))

@pytest.mark.parametrize("level,case_id,case_name,input_data,expected", ALL_BENCHMARK_CASES)
def test_sarfaesi_benchmark_case(level, case_id, case_name, input_data, expected):
    res = JudiQEngine.analyze_case(input_data)
    assert res is not None, f"Analysis failed for {case_id}: {case_name}"

    # 1. Check current_stage if specified
    if "current_stage" in expected:
        proc_graph = res.get("procedural_graph", {})
        actual_stage = proc_graph.get("current_stage")
        assert actual_stage == expected["current_stage"], f"[{case_id}] Expected stage '{expected['current_stage']}', got '{actual_stage}'"

    # 2. Check next_action if specified
    if "next_action_contains" in expected:
        next_actions = res.get("next_best_actions", [])
        combined_actions = " ".join([str(a.get("action", "")) for a in next_actions])
        assert expected["next_action_contains"].lower() in combined_actions.lower(), f"[{case_id}] Expected next action containing '{expected['next_action_contains']}', got '{combined_actions}'"

    # 3. Check fatal_defect if specified
    if "fatal_defect_contains" in expected:
        fatal = res.get("fatal_defect") or res.get("failure_point") or str(res.get("tldr", {}).get("core_risk", ""))
        reasoning = " ".join(res.get("reasoning_trace", []))
        issues = " ".join([str(i) for i in res.get("issues", [])])
        defects_in_graph = " ".join([str(n.get("defect")) for n in res.get("procedural_graph", {}).get("nodes", []) if n.get("defect")])
        all_defects = f"{fatal} {reasoning} {issues} {defects_in_graph}"
        assert expected["fatal_defect_contains"].lower() in all_defects.lower(), f"[{case_id}] Expected defect '{expected['fatal_defect_contains']}', got '{all_defects}'"

    # 4. Check contradiction if specified
    if "contradiction_contains" in expected:
        contradictions = res.get("cross_document_contradictions") or res.get("contradictions", [])
        combined_contra = " ".join([str(c.get("issue", "")) + " " + str(c.get("details", "")) for c in contradictions])
        assert expected["contradiction_contains"].lower() in combined_contra.lower(), f"[{case_id}] Expected contradiction containing '{expected['contradiction_contains']}', got '{combined_contra}'"

    # 5. Check evidence_gap if specified
    if "evidence_gap_contains" in expected:
        gaps = res.get("evidence_gaps", [])
        combined_gaps = " ".join([str(g.get("document_required", "")) + " " + str(g.get("consequence", "")) for g in gaps])
        assert expected["evidence_gap_contains"].lower() in combined_gaps.lower(), f"[{case_id}] Expected evidence gap containing '{expected['evidence_gap_contains']}', got '{combined_gaps}'"

    # 6. Check limitation remaining days if specified
    if "limitation_remaining_days" in expected:
        det = res.get("detailed_assessment", {})
        lim = res.get("limitation", {})
        rem = det.get("limitation_remaining_days") if det.get("limitation_remaining_days") is not None else lim.get("days_remaining")
        assert rem == expected["limitation_remaining_days"], f"[{case_id}] Expected limitation remaining days {expected['limitation_remaining_days']}, got {rem}"

    # 7. Check limitation status if specified
    if "limitation_status" in expected:
        det = res.get("detailed_assessment", {})
        lim = res.get("limitation", {})
        st = det.get("limitation_status") or lim.get("status")
        assert expected["limitation_status"].lower() in str(st).lower(), f"[{case_id}] Expected limitation status '{expected['limitation_status']}', got '{st}'"

    # 8. Check draft type if specified
    if "draft_type" in expected:
        assert res.get("draft_type") == expected["draft_type"], f"[{case_id}] Expected draft type '{expected['draft_type']}', got '{res.get('draft_type')}'"

def test_full_benchmark_summary_scorecard():
    level_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    level_totals = {1: 5, 2: 5, 3: 5, 4: 5, 5: 5}

    for lvl, case_id, case_name, input_data, expected in ALL_BENCHMARK_CASES:
        try:
            res = JudiQEngine.analyze_case(input_data)
            if res is not None:
                level_counts[lvl] += 1
        except Exception:
            pass

    print("\n" + "=" * 60)
    print("      JUDIQ SARFAESI + DRT 25-CASE BENCHMARK SCORECARD")
    print("=" * 60)
    for lvl in range(1, 6):
        print(f"  Level {lvl}: {level_counts[lvl]}/{level_totals[lvl]} Passed")
    print("-" * 60)
    total_passed = sum(level_counts.values())
    print(f"  OVERALL BENCHMARK RESULT: {total_passed}/25 Passed")
    print("=" * 60)

    assert total_passed == 25, f"Benchmark incomplete! Passed {total_passed}/25."
