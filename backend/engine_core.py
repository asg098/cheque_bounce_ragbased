import logging
import importlib
logger = logging.getLogger(__name__)
def _safe_call(fn, *args, fallback, context=""):
    try:
        return fn(*args)
    except Exception as exc:
        logger.error(f"[ENGINE] {context} failed: {exc}", exc_info=True)
        return fallback
class EngineRegistry:
    def __init__(self):
        self._modules = {
            "scoring":     "scoring_engine.ScoringEngineV12",
            "criminal_scoring": "criminal_scoring_engine.CriminalScoringEngine",
            "semantic":    "semantic_engine.SemanticEngineV12",
            "adversarial": "adversarial_engine.AdversarialEngine",
            "criminal_adversarial": "criminal_adversarial_engine.CriminalAdversarialEngine",
            "strategy":    "strategy_engine.StrategyEngine",
            "criminal_strategy": "criminal_engine.CriminalEngine",
            "draft":       "draft_engine.DraftEngine",
            "reasoning":   "reasoning_engine.ReasoningEngine",
            "timeline":    "timeline_engine.TimelineEngine",
            "simulator":   "simulator_engine.SimulatorEngine",
            "defence":     "defence_engine.DefenceEngineV12",
            "decision":    "decision_support_engine.DecisionSupportEngine",
            "document_intelligence": "document_intelligence.DocumentIntelligence",
            "sarfaesi_scoring": "sarfaesi_scoring_engine.SarfaesiScoringEngine",
            "sarfaesi_adversarial": "sarfaesi_adversarial_engine.SarfaesiAdversarialEngine",
            "sarfaesi_timeline": "sarfaesi_timeline_engine.SarfaesiTimelineEngine",
            "criminal_timeline": "criminal_timeline_engine.CriminalTimelineEngine"
        }
        self._instances = {}
    def get(self, module_name: str):
        if module_name not in self._modules:
            raise ImportError(f"Engine Component '{module_name}' not found in registry.")
        if module_name not in self._instances:
            path = self._modules[module_name]
            module_path, class_name = path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            self._instances[module_name] = getattr(module, class_name)
        return self._instances[module_name]
    def preload_all(self):
        for module_name in self._modules:
            try:
                self.get(module_name)
            except Exception as e:
                logger.error(f"Failed to preload {module_name}: {e}")
registry = EngineRegistry()
from llm_engine import LLM_AVAILABLE
try:
    from rag_engine import rag_manager
except Exception as _e:
    logger.warning(f"[ENGINE] rag_engine import failed: {_e}")
    rag_manager = None
try:
    from judicial_engine import judicial_engine as _judicial_engine
except Exception as _e:
    logger.warning(f"[ENGINE] judicial_engine import failed: {_e}")
    _judicial_engine = None
SYNTHETIC_TEXT_MAP = {
    "cheque_present":  "cheque dishonoured and bounced by bank",
    "dishonour_memo":  "bank issued dishonour memo and return slip",
    "notice_sent":     "legal notice served on accused",
    "debt_proven":     "loan agreement executed and legally enforceable debt established",
}
def scan_fatal_defects(case_data, contradictions, adversarial_result, limitation, verification_penalties, jurisdiction_info=None):
    is_fatal = False
    fatal_reason = ""
    if case_data.get("fatal_defect"):
        return True, str(case_data.get("fatal_defect"))
    if "analysis_nodes" in adversarial_result:
        for node in adversarial_result["analysis_nodes"]:
            if node.get("severity") == "FATAL":
                return True, node.get("risk_explained", "Fatal Adversarial Risk")
    if verification_penalties < 0:
        return True, "Evidentiary Fraud / Document Intelligence Override"
    limitation_status = limitation.get("status")
    has_condonation = str(case_data.get("condonation_attached", "")).lower() in ["yes", "true", "1"] or str(case_data.get("condonation_attached", "")).startswith("yes")
    if limitation.get("fatal_defect") or limitation.get("is_premature") or limitation_status == "NOTICE_INVALID":
        return True, limitation.get("fatal_defect", "Invalid Timeline/Notice Issue")
    if limitation_status in {"TIME_BARRED", "EXPIRED"} and not has_condonation:
        return True, "Limitation Period Expired (No Condonation Application Attached)"
    if jurisdiction_info and jurisdiction_info.get("status") == "INVALID":
        return True, "Court lacks territorial jurisdiction over the subject matter."
    return False, ""
class JudiQEngine:
    @classmethod
    def analyze_case(cls, raw_data: dict, analysis_mode: str = "detailed") -> dict:
        from normalizer import normalize_input
        from response_builder import ResponseBuilder
        from schemas import CaseInput
        from pydantic import ValidationError as PydanticValidationError
        try:
            normalized_raw = normalize_input(raw_data)
            validated_input = CaseInput(**normalized_raw)
            case_data = validated_input.model_dump()
        except PydanticValidationError as e:
            logger.error(f"Schema Validation failed: {e}")
            raise ValueError(f"Invalid case input schema: {e}")
        except Exception as e:
            logger.error(f"Normalization failed: {e}")
            raise
        if "has_65b_certificate" not in case_data:
            case_data["has_65b_certificate"] = str(case_data.get("bsa_certificate", "")).lower() == "yes"
        if "has_bsa_certificate" not in case_data:
            case_data["has_bsa_certificate"] = case_data["has_65b_certificate"]
        case_data["analysis_mode"] = analysis_mode
        logger.info(f"[JUDIQ] Core analysis triggered for: {case_data.get('case_id', 'ANON')} (Type: {case_data.get('case_type', 'Cheque Bounce')})")

        # Domain Dispatch Routing — Criminal, SARFAESI, Civil, and Composite engines are disabled.
        # Only Section 138 Negotiable Instruments Act (Cheque Bounce) engine is active.
        case_type_clean = (case_data.get("case_type") or "").strip().lower()
        if case_type_clean in ("criminal", "ipc", "bns", "crpc", "bnss", "sarfaesi", "drt", "securitisation", "civil", "cpc", "composite", "multi_track", "multitrack"):
            logger.warning(f"[JUDIQ] Requested disabled engine '{case_type_clean}'. Section 138 NI Act is the only active engine.")
            raise ValueError(f"The '{case_type_clean}' litigation engine is currently disabled. Only Section 138 Negotiable Instruments Act (Cheque Bounce) Litigation Intelligence is enabled.")


        doc_intel = registry.get("document_intelligence")
        verification_flags = _safe_call(
            doc_intel.validate_claims, case_data,
            fallback={"overrides": {}, "verification_penalties": 0},
            context="DocumentIntelligence"
        )
        if verification_flags.get("overrides"):
            for k, v in verification_flags["overrides"].items():
                case_data[k] = v
                logger.warning(f"[ENGINE] Overriding user input {k} -> {v}")
        case_data["verification_penalties"] = verification_flags.get("verification_penalties", 0)
        text = case_data.get("description", "").strip()
        parts = []
        if text:
            parts.append(text)
        else:
            synth_parts = [phrase for key, phrase in SYNTHETIC_TEXT_MAP.items() if case_data.get(key)]
            if synth_parts:
                parts.extend(synth_parts)
        purpose = case_data.get("purpose", "").strip()
        if purpose and purpose != "Not Provided":
            parts.append(f"Purpose of transaction: {purpose}")
        notes = case_data.get("additional_notes", "").strip()
        if notes:
            parts.append(notes)
        text = ". ".join(parts)
        fact_graph = None
        if LLM_AVAILABLE:
            try:
                from llm_engine import extract_fact_graph
                fact_graph = _safe_call(
                    extract_fact_graph, text,
                    fallback=None,
                    context="LLM_FactGraph"
                )
            except Exception as e:
                logger.error(f"Failed to load LLM fact graph: {e}")
        case_data["fact_graph"] = fact_graph
        semantic_engine = registry.get("semantic")
        semantic_result = _safe_call(
            semantic_engine.analyze_text, text,
            fallback={"concepts_detected": [], "entities": []},
            context="SemanticEngine"
        )
        concepts = semantic_result.get("concepts_detected") or []
        existing_concepts = {c["concept"] for c in concepts}
        if case_data.get("debt_acknowledged") and "debt_acknowledgment" not in existing_concepts:
            concepts.append({
                "concept": "debt_acknowledgment",
                "confidence": 0.9,
                "matched_phrases": ["debt acknowledgment (structured)"],
                "legal_impact": "Strongly triggers presumption under Section 139 NI Act",
                "polarity": 1
            })
        if case_data.get("cheque_security_claim") and "security_cheque" not in existing_concepts:
            concepts.append({
                "concept": "security_cheque",
                "confidence": 0.8,
                "matched_phrases": ["security cheque claim (structured)"],
                "legal_impact": "Common defence; countered by Sripati Singh precedent",
                "polarity": 1
            })
        if not case_data.get("complainant_itr_available") and "financial_capacity_challenge" not in existing_concepts:
            concepts.append({
                "concept": "financial_capacity_challenge",
                "confidence": 0.75,
                "matched_phrases": ["missing ITR (structured)"],
                "legal_impact": "Invokes Basalingappa rule; requires complainant to prove source of funds",
                "polarity": 1
            })
        text_lower = text.lower()
        explicit_type = str(case_data.get("case_type", "")).lower()
        is_sarfaesi = explicit_type == "sarfaesi" or "sarfaesi" in text_lower or "securitisation" in text_lower
        is_criminal = explicit_type == "criminal"
        is_cheque_bounce = explicit_type in ("cheque bounce", "cheque_bounce")
        if not is_criminal and not is_cheque_bounce and not is_sarfaesi:
            is_criminal = "criminal" in text_lower or "fir" in text_lower or case_data.get("offense_type") is not None
            is_cheque_bounce = "cheque" in text_lower or case_data.get("cheque_present")
        if is_sarfaesi:
            logger.info("SARFAESI & DRT case type detected. Routing to SARFAESI Engine suite.")
            adv_modules = ["sarfaesi_adversarial"]
            strat_modules = ["strategy"]
            scoring_modules = ["sarfaesi_scoring"]
        elif is_criminal and is_cheque_bounce:
            logger.info("Mixed case detected (Criminal + Cheque Bounce). Orchestrating combined analysis.")
            adv_modules = ["adversarial", "criminal_adversarial"]
            strat_modules = ["strategy", "criminal_strategy"]
            scoring_modules = ["scoring", "criminal_scoring"]
        elif is_criminal:
            adv_modules = ["criminal_adversarial"]
            strat_modules = ["criminal_strategy"]
            scoring_modules = ["criminal_scoring"]
        else:
            adv_modules = ["adversarial"]
            strat_modules = ["strategy"]
            scoring_modules = ["scoring"]
        attack_chains = []
        adversarial_engine = None  # Initialize before loop to prevent NameError if loop is empty
        adversarial_result = {"risks_and_rebuttals": [], "contradictions": []}  # Safe default
        for adv_module in adv_modules:
            adversarial_engine = registry.get(adv_module)
            adversarial_result = _safe_call(
                adversarial_engine.audit_case, case_data, concepts,
                fallback={"risks_and_rebuttals": [], "contradictions": []},
                context=f"{adv_module}"
            )
            attack_chains.extend(adversarial_result.get("risks_and_rebuttals", []))
        if adversarial_engine is None:
            raise RuntimeError("No adversarial engine could be loaded. Check registry configuration.")
        contradictions = _safe_call(
            adversarial_engine.detect_contradictions, case_data, concepts,
            fallback=[],
            context="ContradictionEngine"
        )
        timeline_anomalies = _safe_call(
            adversarial_engine.detect_timeline_anomalies, case_data,
            fallback=[],
            context="TimelineAnomalyDetector"
        )
        adversarial_risk = _safe_call(
            adversarial_engine.calculate_adversarial_risk, attack_chains,
            fallback=0.2,
            context="AdversarialEngine.risk"
        )
        evidence_dependencies = _safe_call(
            adversarial_engine.map_evidence_dependencies, case_data,
            fallback=[],
            context="EvidenceDependencyMapping"
        )
        red_team_attacks = _safe_call(
            adversarial_engine.run_strategic_audit, case_data, concepts,
            fallback=[],
            context="StrategicAudit"
        )
        witness_pressure = _safe_call(
            adversarial_engine.simulate_witness_pressure, case_data, adversarial_risk,
            fallback={},
            context="WitnessPressure"
        )
        timeline_engine = registry.get("criminal_timeline") if is_criminal else (registry.get("sarfaesi_timeline") if is_sarfaesi else registry.get("timeline"))
        timeline = _safe_call(
            timeline_engine.generate_timeline, case_data,
            fallback=[],
            context="TimelineEngine.generate"
        )
        limitation_checker = timeline_engine.check_limitation if (is_sarfaesi or is_criminal) else timeline_engine.check_limitation
        limitation = _safe_call(
            limitation_checker, case_data,
            fallback={"is_barred": False, "status": "CALCULATION_ERROR"},
            context="TimelineEngine.limitation"
        )
        scoring_results = []
        for scoring_module in scoring_modules:
            scoring_engine = registry.get(scoring_module)
            if "criminal" in scoring_module:
                res = _safe_call(
                    scoring_engine.calculate_score, case_data, concepts, contradictions, limitation,
                    fallback={"score": 50, "final_score": 50, "reasoning_trace": ["Internal scoring error."]},
                    context="CriminalScoringEngine"
                )
            else:
                res = _safe_call(
                    scoring_engine.calculate_score_with_trace, case_data, concepts, contradictions, limitation, {},
                    fallback={"score": 50, "final_score": 50, "reasoning_trace": ["Internal scoring error."]},
                    context="ScoringEngine"
                )
            scoring_results.append(res)
        scoring_result = scoring_results[0]
        if len(scoring_results) > 1:
            min_score = min(float(r.get("final_score") or r.get("score") or 50) for r in scoring_results)
            scoring_result["final_score"] = min_score
            scoring_result["score"] = min_score
            trace_1 = [f"[Cheque Engine] {t}" for t in scoring_results[0].get("reasoning_trace", [])]
            trace_2 = [f"[Criminal Engine] {t}" for t in scoring_results[1].get("reasoning_trace", [])]
            scoring_result["reasoning_trace"] = trace_1 + ["---"] + trace_2
        final_score = float(scoring_result.get("final_score") or scoring_result.get("score") or 50)
        strategy_results = []
        for strat_module in strat_modules:
            strategy_engine = registry.get(strat_module)
            strategy_result = _safe_call(
                strategy_engine.generate_strategy if hasattr(strategy_engine, 'generate_strategy') else strategy_engine.generate_litigation_map, 
                case_data, concepts, int(final_score), adversarial_risk,
                fallback={"litigation_strategy": "Maintain standard procedural posture."},
                context="StrategyEngine"
            )
            strategy_results.append(strategy_result)
        strategy_result = strategy_results[0]
        if len(strategy_results) > 1:
            strategy_result["litigation_strategy"] = strategy_results[0].get("litigation_strategy", "") + "\n\nCRIMINAL OVERLAY:\n" + strategy_results[1].get("litigation_strategy", "")
        reasoning_engine = registry.get("reasoning")
        causal_story = _safe_call(
            reasoning_engine.generate_causal_story, case_data, concepts,
            fallback=[],
            context="CausalStoryBuilder"
        )
        reasoning_trail = _safe_call(
            reasoning_engine.generate_reasoning_trail, 
            case_data, 
            semantic_result.get("concepts_detected", []), 
            scoring_result.get("score", 0),
            scoring_result,                         
            fallback=[],
            context="Reasoning Trail"
        )
        precedents = _safe_call(
            reasoning_engine.match_precedents, case_data, concepts,
            fallback=[],
            context="ReasoningEngine.precedents"
        )
        try:
            from llm_engine import analyze_precedent_relationships
            enriched_precedents = _safe_call(
                analyze_precedent_relationships, case_data, precedents,
                fallback=precedents,
                context="LLM_PrecedentRelationships"
            )
            precedents = enriched_precedents or precedents
        except Exception as e:
            logger.error(f"Failed to load LLM precedent relationships: {e}")
        statutory_interpretation = _safe_call(
            reasoning_engine.interpret_statutes, case_data, concepts,
            fallback=[],
            context="Statutes"
        )
        case_summary = _safe_call(
            reasoning_engine.summarize_case, case_data,
            fallback="Case assessment based on statutory pillars.",
            context="ReasoningEngine.summary"
        )
        if rag_manager is not None:
            precedent_intelligence = _safe_call(
                rag_manager.get_precedent_intelligence, case_data,
                fallback={"supporting": [], "opposing": [], "distinguishable": [], "all_relevant": []},
                context="RAGManager"
            )
        else:
            precedent_intelligence = {"supporting": [], "opposing": [], "distinguishable": [], "all_relevant": []}
        try:
            from jurisdiction_engine import map_jurisdiction
            jurisdiction_info = map_jurisdiction(case_data)
        except Exception as e:
            logger.error(f"[ENGINE] Jurisdiction mapping failed: {e}")
            jurisdiction_info = {"status": "ERROR"}
        judicial_report = {}
        if _judicial_engine:
            judicial_report = _safe_call(
                _judicial_engine.generate_judicial_intelligence_report, case_data, final_score,
                fallback={},
                context="JudicialEngine"
            )
        from jurisdiction_engine import apply_jurisdiction_guards
        judicially_adjusted_score = apply_jurisdiction_guards(jurisdiction_info, concepts, final_score)
        if judicially_adjusted_score < final_score:
            scoring_result.setdefault("causality_map", []).append({
                "fact": "S.142(2) Territorial Mismatch",
                "impact": -35,
                "type": "negative",
                "rationale": "Jurisdiction is territorially defective."
            })
        is_fatal, fatal_reason = scan_fatal_defects(
            case_data, 
            contradictions, 
            adversarial_result, 
            limitation, 
            case_data.get("verification_penalties", 0),
            jurisdiction_info
        )
        if is_fatal:
            case_data["fatal_defect"] = fatal_reason
            final_score = min(final_score, 25.0)
            judicially_adjusted_score = min(judicially_adjusted_score, 25.0)
        draft_engine = registry.get("draft")
        from draft_engine import decide_draft_type
        if is_fatal:
            draft_type = "LEGAL_OPINION"
            logger.warning(f"GLOBAL FATAL OVERRIDE: Forcing draft_type to LEGAL_OPINION due to {fatal_reason}")
        else:
            draft_type = raw_data.get("force_draft_type") or decide_draft_type(int(final_score), concepts, case_data)
        case_data["failure_point_injected"] = fatal_reason if is_fatal else scoring_result.get("failure_point", "")
        draft_content = _safe_call(
            draft_engine.generate_draft, draft_type, int(final_score), concepts, case_data,
            fallback="Legal draft generation failed. Please use manual templates.",
            context="DraftEngine"
        )
        logger.info(f"DRAFT_ENGINE: Type={draft_type}, Size={len(draft_content) if draft_content else 0}")
        decision_engine = registry.get("decision")
        outcome_prediction = _safe_call(
            decision_engine.predict_outcome, final_score,
            fallback={"prediction": "Unknown", "probability": "0%"},
            context="DecisionSupportEngine.outcome"
        )
        if is_fatal:
            outcome_prediction = {"prediction": f"DO NOT FILE - {fatal_reason}", "probability": "0%"}
            scoring_result["verdict"] = "DO NOT FILE"
        translated_verdict = _safe_call(
            decision_engine.translate_verdict, scoring_result.get("verdict", "MODERATE"), case_data.get("target_lang", "hindi"),
            fallback="à¤®à¤œà¤¬à¥‚à¤¤ à¤®à¤¾à¤®à¤²à¤¾",
            context="DecisionSupportEngine.translate"
        )
        evidence_suggestions = []
        decision_risks = []
        if decision_engine:
            evidence_suggestions = _safe_call(
                decision_engine.suggest_evidence_gaps, case_data,
                fallback=[],
                context="DecisionSupportEngine.evidence"
            )
            decision_risks = _safe_call(
                decision_engine.identify_risks_and_rebuttals, concepts, case_data, limitation.get("status", ""),
                fallback=[],
                context="DecisionSupportEngine.risks"
            )
        if "risks_and_rebuttals" not in adversarial_result:
            adversarial_result["risks_and_rebuttals"] = []
        agreement_type = str(case_data.get("agreement_type", "")).strip()
        is_commercial = agreement_type == "Commercial Invoice"
        if is_commercial:
            adversarial_result["risks_and_rebuttals"] = [
                r for r in adversarial_result["risks_and_rebuttals"] 
                if "security cheque" not in str(r.get("adversarial_vector", "")).lower()
            ]
        existing_risk_titles = {str(r.get("adversarial_vector", r.get("risk", ""))).lower() for r in adversarial_result["risks_and_rebuttals"]}
        for dr in decision_risks:
            dr_title = str(dr["risk"]).lower()
            if dr_title not in existing_risk_titles:
                existing_risk_titles.add(dr_title)
                adversarial_result["risks_and_rebuttals"].append({
                    "adversarial_vector": dr["risk"],
                    "risk": dr["risk"],
                    "severity": dr.get("severity", "HIGH"),
                    "description": dr.get("description", ""),
                    "rebuttal": dr.get("rebuttal", ""),
                    "strategic_chain": [dr.get("description", "")],
                    "rebuttal_tree": {
                        "complainant_counter": dr.get("rebuttal", ""),
                        "magistrate_view": f"High attention to {dr.get('case_law', 'relevant statutes')}"
                    },
                    "survival_probability": f"{100 - (85 if dr.get('severity') == 'CRITICAL' else 65 if dr.get('severity') == 'HIGH' else 35 if dr.get('severity') == 'MEDIUM' else 15)}%",
                    "collapse_risk": f"{(85 if dr.get('severity') == 'CRITICAL' else 65 if dr.get('severity') == 'HIGH' else 35 if dr.get('severity') == 'MEDIUM' else 15)}%",
                    "why_applied": dr.get("description", "Applicable based on case facts.")
                })
        tldr = {
            "core_risk": "Procedural Technicality" if final_score < 40 else ("Evidentiary Gap" if final_score < 70 else "Minimal"),
            "top_threat": adversarial_result["risks_and_rebuttals"][0]["adversarial_vector"] if adversarial_result["risks_and_rebuttals"] else "None identified",
            "best_move": scoring_result.get("remediation_roadmap", [{"action": "Maintain posture"}])[0]["action"],
            "confidence": f"{int(final_score)}%",
            "one_liner": f"Case is {outcome_prediction.get('prediction', 'stable')} with {len(contradictions)} logical inconsistencies detected."
        }
        from evidence.evidence_intelligence import EvidenceIntelligenceEngine
        from procedural.procedural_graph_engine import ProceduralGraphEngine
        from citation.citation_verifier import CitationVerifierEngine
        from audit.audit_ledger import AuditLedger

        evidence_gaps = EvidenceIntelligenceEngine.evaluate_evidence_gaps(case_data)
        cross_doc_contradictions = EvidenceIntelligenceEngine.detect_cross_document_contradictions(case_data)
        procedural_graph = ProceduralGraphEngine.build_graph(case_data) if is_sarfaesi else {"nodes": [], "current_stage": "Active Litigation"}

        detailed_assessment = {}
        if is_sarfaesi:
            perspective = str(case_data.get("perspective", "creditor")).lower()
            if perspective in ["borrower", "debtor", "applicant"]:
                from sarfaesi.sarfaesi_borrower_engine import SarfaesiBorrowerEngine
                detailed_assessment = SarfaesiBorrowerEngine.evaluate_borrower_position(case_data)
            else:
                from sarfaesi.sarfaesi_bank_engine import SarfaesiBankEngine
                detailed_assessment = SarfaesiBankEngine.evaluate_bank_position(case_data)
        # Citation Trap Verification
        user_citation = case_data.get("user_supplied_citation")
        if user_citation:
            verified_authority = CitationVerifierEngine.verify_citation(user_citation)
        else:
            verified_authority = CitationVerifierEngine.verify_citation("Mardia Chemicals Ltd. v. Union of India (2004)") if is_sarfaesi else CitationVerifierEngine.verify_citation("Sunil Todi v. State of Gujarat (2021)")

        abstain_recommended = False
        decision_status = "EVALUATED"

        # Incomplete-Data Guarding Check (SARFAESI specific)
        if is_sarfaesi:
            has_dates = bool(case_data.get("notice_13_2_date") or case_data.get("npa_date") or case_data.get("possession_13_4_date"))
            has_property = bool(case_data.get("property_description") or case_data.get("mortgage_survey_number"))
            has_text = len(case_data.get("description", "").strip()) > 80

            if not has_dates or not has_property or (not has_dates and not has_text):
                abstain_recommended = True
                decision_status = "INSUFFICIENT_EVIDENCE"
                scoring_result["verdict"] = "INSUFFICIENT_EVIDENCE"
            elif case_data.get("missing_service_proof") or case_data.get("service_proof_available") == False:
                abstain_recommended = True

        # Safety & Abstention Check (High-Complexity / Sub-Judice / Moratorium)
        if case_data.get("nclt_ibc_moratorium_active") or case_data.get("drat_order_reserved") or case_data.get("signature_disputed") or case_data.get("conflicting_hc_orders") or (case_data.get("consortium_lenders_count") and case_data.get("consortium_lenders_count") > 3):
            abstain_recommended = True
            decision_status = "LAWYER_REVIEW_REQUIRED"
            case_data["lawyer_review_required"] = True
            scoring_result["verdict"] = "LAWYER_REVIEW_REQUIRED"

        case_data["abstain_recommended"] = abstain_recommended
        case_data["decision_status"] = decision_status

        # Record Audit Entry
        audit_entry = AuditLedger.record_entry(
            case_id=case_data.get("case_id", "ANON"),
            finding_id="JUDIQ_EVAL_01",
            finding_text=f"Analyzed {explicit_type or 'cheque_bounce'} case. Score: {final_score}.",
            evidence_relied=f"User input narrative & {len(concepts)} detected concepts.",
            rule_applied=f"Statutory pillars for {explicit_type or 'cheque_bounce'}.",
            authority=verified_authority["citation"],
            confidence=final_score / 100.0,
            verdict=scoring_result.get("verdict", "MODERATE")
        )

        next_best_actions = ProceduralGraphEngine.determine_next_best_actions(case_data, scoring_result) if is_sarfaesi else []

        engine_output = {
            "final_score": judicially_adjusted_score,
            "theoretical_score": final_score,
            "judicial_report": judicial_report,
            "jurisdiction_info": jurisdiction_info,
            "precedent_intelligence": precedent_intelligence,
            "tldr": tldr,
            "evidence_gaps": evidence_gaps,
            "cross_document_contradictions": cross_doc_contradictions,
            "procedural_graph": procedural_graph,
            "verified_authority": verified_authority,
            "detailed_assessment": detailed_assessment,
            "next_best_actions": next_best_actions,
            "abstain_recommended": abstain_recommended,
            "decision_status": decision_status,
            "audit_entry": audit_entry,
            "reasoning_trace": scoring_result.get("reasoning_trace", []),
            "reasoning_trail": reasoning_trail,
            "causal_story": causal_story,
            "contradictions": contradictions,
            "timeline_anomalies": timeline_anomalies,
            "evidence_dependencies": evidence_dependencies,
            "strategic_audit": red_team_attacks,
            "witness_pressure": witness_pressure,
            "uncertainty_intelligence": scoring_result.get("uncertainty_intelligence", []),
            "judicial_mode": scoring_result.get("judicial_mode", "Balanced"),
            "self_challenge": scoring_result.get("self_challenge", {}),
            "reliability_matrix": scoring_result.get("reliability_matrix", {}),
            "case_similarity": scoring_result.get("case_similarity", {}),
            "failure_point": scoring_result.get("failure_point", ""),
            "senior_brief": scoring_result.get("senior_brief", {}),
            "score_breakdown": scoring_result.get("breakdown", {}),
            "concepts": concepts,
            "adversarial_result": adversarial_result,
            "outcome_prediction": outcome_prediction,
            "translated_verdict": translated_verdict,
            "evidence_suggestions": evidence_suggestions,
            "case_summary": case_summary,
            "precedents": precedents,
            "statutory_interpretation": statutory_interpretation,
            "fact_graph": case_data.get("fact_graph", {}),
            "draft": draft_content,
            "draft_type": draft_type,
            "timeline": timeline,
            "limitation": limitation,
            "strategy_result": strategy_result,
            "adversarial_risk": adversarial_risk,
            "bail_assessment": strategy_result.get("bail_assessment", {}),
            "criminal_economics": strategy_result.get("economics", {}),
            "statutory_rules": strategy_result.get("statutory_rules", []),
            "advocate_checkpoints": strategy_result.get("checkpoints", []),
            "litigation_map": strategy_result.get("litigation_map", {})
        }
        full_result = {**scoring_result, **adversarial_result, **engine_output}
        return ResponseBuilder.build_final_response(full_result, case_data)
analyze_case = JudiQEngine.analyze_case
try:
    registry.preload_all()
except Exception as _preload_err:
    logger.warning(f"[ENGINE] Lazy preload encountered non-fatal error: {_preload_err}")
