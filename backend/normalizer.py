import logging
import re
from typing import Any, Optional
logger = logging.getLogger(__name__)
MAX_TEXT_LENGTH  = 10_000               
MAX_AMOUNT       = 1_000_000_000                      
SAFE_STRING_RE   = re.compile(r"[^\w\s\-\.,/()@#:;'\"&%₹\n]", re.UNICODE)
VALID_ACCUSED_TYPES = {
    "Individual", "Pvt Ltd/Ltd Company", "Company",
    "Partnership Firm", "LLP", "HUF", "Trust"
}
VALID_DISHONOUR_REASONS = {
    "Insufficient Funds", "Account Closed", "Payment Stopped",
    "Signature Mismatch", "Exceed Arrangement", "Frozen Account",
    "Refer to Drawer", "Technical Reason", "Other"
}
class ValidationError(ValueError):
    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.field = field
        self.message = message
def _safe_str(value: Any, max_len: int = 500, field_name: str = "") -> str:
    if value is None:
        return ""
    s = str(value).strip()
    s = re.sub(r'<[^>]*?>', '', s)
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', s)
    if len(s) > max_len:
        logger.warning(f"Field '{field_name}' truncated from {len(s)} to {max_len} chars.")
        s = s[:max_len]
    return s
def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        v_lower = value.strip().lower()
        if v_lower in ("true", "yes", "1", "on"):
            return True
        truthy_terms = ("yes", "true", "original", "copy", "extensive", "limited", "partial", "signed", "written", "promissory", "deni", "in progress", "progress")
        if any(term in v_lower for term in truthy_terms):
            return True
        return False
    return default
def _safe_amount(value: Any) -> Any:
    if value is None or value == "":
        return 0
    try:
        cleaned = re.sub(r"[₹,\s]", "", str(value))
        num = float(cleaned)
        if num < 0:
            logger.warning(f"Negative amount {num} normalised to 0.")
            return 0
        if num > MAX_AMOUNT:
            logger.warning(f"Amount {num} exceeds sanity cap, capped at {MAX_AMOUNT}.")
            return MAX_AMOUNT
        return int(num) if num == int(num) else num
    except (ValueError, TypeError):
        logger.warning(f"Could not parse amount '{value}', defaulting to 0.")
        return 0
def _safe_accused_type(value: Any) -> str:
    if not value:
        return "Individual"
    s = str(value).strip()
    if s in VALID_ACCUSED_TYPES:
        return s
    s_lower = s.lower()
    if any(kw in s_lower for kw in ("pvt", "ltd", "limited", "company", "corp")):
        return "Pvt Ltd/Ltd Company"
    if any(kw in s_lower for kw in ("partner", "firm")):
        return "Partnership Firm"
    if "llp" in s_lower:
        return "LLP"
    logger.warning(f"Unknown accused_type '{s}' defaulted to 'Individual'.")
    return "Individual"
def _get_nested(primary: dict, *fallbacks, default=None):
    for obj, key in fallbacks:
        val = obj.get(key) if isinstance(obj, dict) else None
        if val is not None and val != "":
            return val
    return default
def validate_minimum_viability(data: dict) -> None:
    if not isinstance(data, dict):
        raise ValidationError("Request body must be a JSON object.", field="root")
    if not data:
        raise ValidationError("Empty request body — no case data provided.", field="root")
    has_description  = bool(data.get("description") or data.get("caseDescription") or data.get("case_description"))
    has_pillars      = any(data.get(k) for k in ["cheque_present", "chequePresent", "dishonour_memo", "notice_sent"])
    has_wizard_cheque = bool((data.get("cheque") or {}).get("cheque_number"))
    has_parties      = bool((data.get("parties") or {}).get("complainant") or data.get("complainant_name") or data.get("bank_name") or data.get("accused_name"))
    is_quick_mode    = data.get("analysis_mode") == "quick"
    if not (has_description or has_pillars or has_wizard_cheque or has_parties or is_quick_mode):
        raise ValidationError(
            "Insufficient case data: provide at least a case description or fill the Section 138 case details.",
            field="description"
        )
def normalize_input(data: dict) -> dict:
    if not isinstance(data, dict):
        logger.error("normalize_input received non-dict input; returning empty defaults.")
        data = {}
    tx_obj   = data.get("transaction")   if isinstance(data.get("transaction"), dict) else {}
    cq_obj   = data.get("cheque")        if isinstance(data.get("cheque"), dict) else {}
    ds_obj   = data.get("dishonour")     if isinstance(data.get("dishonour"), dict) else {}
    nt_obj   = data.get("notice")        if isinstance(data.get("notice"), dict) else {}
    id_obj   = data.get("case_identity") if isinstance(data.get("case_identity"), dict) else {}
    pt_obj   = data.get("parties")       if isinstance(data.get("parties"), dict) else {}
    comp_obj = pt_obj.get("complainant") if isinstance(pt_obj.get("complainant"), dict) else {}
    accu_obj = pt_obj.get("accused")     if isinstance(pt_obj.get("accused"), dict) else {}
    meta_obj = data.get("metadata")      if isinstance(data.get("metadata"), dict) else {}
    raw_amount = (data.get("amount") or data.get("caseAmount")
                  or data.get("debt_amount") or data.get("cheque_amount")
                  or tx_obj.get("debt_amount") or cq_obj.get("cheque_amount") or 0)
    amount = _safe_amount(raw_amount)
    raw_desc = (data.get("description") or data.get("caseDescription")
                or data.get("case_description") or "")
    description = _safe_str(raw_desc, max_len=MAX_TEXT_LENGTH, field_name="description")
    raw_reason = (data.get("dishonour_reason") or ds_obj.get("dishonour_reason") or "")
    dishonour_reason = _safe_str(raw_reason, max_len=100, field_name="dishonour_reason")
    cheque_present  = _safe_bool(data.get("cheque_present") or data.get("chequePresent") or data.get("original_cheque") or cq_obj.get("cheque_present", False))
    dishonour_memo  = _safe_bool(data.get("dishonour_memo") or data.get("dishonourMemo") or data.get("bank_memo_received") or ds_obj.get("bank_memo_received", False))
    notice_sent     = _safe_bool(data.get("notice_sent") or data.get("noticeSent") or nt_obj.get("notice_sent", False))
    raw_debt_proven = data.get("debt_proven") or data.get("debtProven") or data.get("debt_acknowledgment") or data.get("supporting_documents") or tx_obj.get("debt_acknowledged") or tx_obj.get("debt_proven", False)
    debt_proven     = _safe_bool(raw_debt_proven)
    directors_named = _safe_bool(data.get("directors_named") or accu_obj.get("directors_named", False))
    is_authorized   = _safe_bool(data.get("is_authorized") or data.get("complainant_authorized") or comp_obj.get("is_authorized", False))
    signature_disp  = _safe_bool(data.get("signature_dispute") or data.get("signatureDispute", False))
    debt_denial     = _safe_bool(data.get("debt_denial") or data.get("debtDenial", False))
    security_claim  = _safe_bool(data.get("cheque_security_claim") or data.get("chequeSecurityClaim", False))
    raw_accused_type = (data.get("accused_type") or accu_obj.get("type") or "Individual")
    accused_type = _safe_accused_type(raw_accused_type)
    normalized = {
        "case_id":  _safe_str(data.get("case_id",  data.get("caseId",  id_obj.get("case_id",  "API_CASE"))), 100, "case_id"),
        "user_id":  _safe_str(data.get("user_id",  data.get("userId",  meta_obj.get("user_id", "ANONYMOUS"))), 100, "user_id"),
        "court_name": _safe_str(data.get("court_name", id_obj.get("court_name", "")), 200, "court_name"),
        "cheque_present": cheque_present,
        "dishonour_memo": dishonour_memo,
        "notice_sent":    notice_sent,
        "debt_proven":    debt_proven,
        "cheque_proof_type":   _safe_str(data.get("cheque_proof_type", cq_obj.get("cheque_proof_type", "original")), 50),
        "memo_type":           _safe_str(data.get("memo_type", ds_obj.get("memo_type", "original")), 50),
        "notice_served_proof": _safe_bool(data.get("notice_served_proof", nt_obj.get("notice_received", True))),
        "debt_proof_type":     _safe_str(data.get("debt_proof_type", data.get("agreement_type", tx_obj.get("agreement_type", "written_agreement"))), 50),
        "debt_evidence_type":  _safe_str(data.get("debt_evidence_type", "Documentary"), 50), 
        "within_30_days":      _safe_str(data.get("within_30_days", nt_obj.get("within_statutory_period", "Yes")), 10),
        "communication_records": _safe_bool(data.get("communication_records", nt_obj.get("communication_records", data.get("evidence", {}).get("communication_records", False)))),
        "description": description,
        "amount":        amount,
        "cheque_amount": _safe_amount(data.get("cheque_amount", cq_obj.get("cheque_amount", amount))),
        "complainant_name":    _safe_str(data.get("complainant_name",    comp_obj.get("name",    "")), 200, "complainant_name"),
        "complainant_address": _safe_str(data.get("complainant_address", comp_obj.get("address", "")), 500, "complainant_address"),
        "complainant_phone":   _safe_str(data.get("complainant_phone",   comp_obj.get("phone",   "")), 20,  "complainant_phone"),
        "accused_name":        _safe_str(data.get("accused_name",        accu_obj.get("name",    "")), 200, "accused_name"),
        "names_directors_roles": _safe_str(data.get("names_directors_roles", 
                                           f"{data.get('director_names', accu_obj.get('director_names', ''))} - {data.get('director_roles', accu_obj.get('director_roles', ''))}".strip(" -")), 
                                           500, "names_directors_roles"),
        "accused_address":     _safe_str(data.get("accused_address",     accu_obj.get("address", "")), 500, "accused_address"),
        "cheque_number": _safe_str(data.get("cheque_number", cq_obj.get("cheque_number", "")), 30, "cheque_number"),
        "cheque_date":   _safe_str(data.get("cheque_date",   cq_obj.get("cheque_date",   "")), 30, "cheque_date"),
        "bank_name":     _safe_str(data.get("bank_name",     cq_obj.get("bank_name",     "")), 100, "bank_name"),
        "branch_name":   _safe_str(data.get("branch_name",   cq_obj.get("branch_name",   "")), 100, "branch_name"),
        "payee_bank_name": _safe_str(data.get("payee_bank_name", cq_obj.get("payee_bank_name", data.get("bank_name", ""))), 100, "payee_bank_name"),
        "payee_branch":    _safe_str(data.get("payee_branch",    cq_obj.get("payee_branch",    data.get("branch_name", ""))), 100, "payee_branch"),
        "payee_bank_city": _safe_str(data.get("payee_bank_city", cq_obj.get("payee_bank_city", "")), 100, "payee_bank_city"),
        "drawer_bank_name": _safe_str(data.get("drawer_bank_name", ""), 100, "drawer_bank_name"),
        "drawer_bank_city": _safe_str(data.get("drawer_bank_city", ""), 100, "drawer_bank_city"),
        "accused_city":     _safe_str(data.get("accused_city", ""), 100, "accused_city"),
        "dishonour_date":    _safe_str(data.get("dishonour_date", ds_obj.get("dishonour_date", ds_obj.get("date", ""))), 30, "dishonour_date"),
        "dishonour_reason":  dishonour_reason,
        "memo_date":         _safe_str(data.get("memo_date",         ds_obj.get("memo_date",         "")), 30, "memo_date"),
        "presentation_date": _safe_str(data.get("presentation_date", ds_obj.get("presentation_date", "")), 30, "presentation_date"),
        "memo_signed":       _safe_str(data.get("memo_signed", ""), 50, "memo_signed"),
        "condonation_attached": _safe_bool(data.get("condonation_attached", False)),
        "agreement_type": _safe_str(data.get("agreement_type", tx_obj.get("agreement_type", "")), 100, "agreement_type"),
        "notice_date":          _safe_str(data.get("notice_date",     nt_obj.get("date_sent", "")), 30, "notice_date"),
        "notice_mode":          _safe_str(data.get("notice_mode",     nt_obj.get("notice_mode",     "")), 50, "notice_mode"),
        "notice_received_type": _safe_str(data.get("notice_received_type", nt_obj.get("delivery_status", "Success")), 50),
        "notice_received_date": _safe_str(data.get("notice_received_date", nt_obj.get("notice_received_date", "")), 30, "notice_received_date"),
        "transaction_date": _safe_str(data.get("transaction_date", tx_obj.get("transaction_date", "")), 30, "transaction_date"),
        "filing_date":      _safe_str(data.get("filing_date",      data.get("filing_date", id_obj.get("filing_date", ""))), 30, "filing_date"),
        "complainant_type": _safe_str(data.get("complainant_type", id_obj.get("complainant_type", "Individual")), 50),
        "is_authorized":    is_authorized,
        "accused_type":     accused_type,
        "directors_named":  directors_named,
        "complainant_itr_available": _safe_bool(data.get("complainant_itr_available", data.get("itr_available", False))),
        "loan_via_bank":             _safe_bool(data.get("loan_via_bank", False)),
        "loan_advanced_via":         _safe_str(data.get("loan_advanced_via", "")),
        "draft_tone": _safe_str(data.get("draft_tone", "standard"), 50),
        "signature_dispute":     signature_disp,
        "debt_denial":           debt_denial,
        "cheque_security_claim": security_claim,
        "director_names":        _safe_str(data.get("director_names", data.get("accused_directors", accu_obj.get("director_names", ""))), 500, "director_names"),
        "court_attendance":      _safe_str(data.get("court_attendance", ""), 50, "court_attendance"),
        "analysis_mode":         _safe_str(data.get("analysis_mode", "detailed"), 20),
        "proof_present":         _safe_bool(data.get("proof_present", True)),
        "debt_acknowledged":     _safe_bool(data.get("debt_acknowledged") or data.get("debt_acknowledgment") or tx_obj.get("debt_acknowledged", False)),
    }
    normalized = resolve_logical_contradictions(normalized)
    for k, v in data.items():
        if k not in normalized:
            normalized[k] = v
    return normalized
def resolve_logical_contradictions(data: dict) -> dict:
    from datetime import datetime
    notice_status = str(data.get("notice_received_type", "")).lower()
    if notice_status in ["refused", "door locked", "unclaimed", "addressee moved"]:
        data["notice_received_type"] = "Deemed Service"
        data["notice_served_proof"] = True
    elif notice_status in ["incorrect address", "no such person"]:
        data["notice_received_type"] = "Failed Service"
        data["notice_served_proof"] = False
        data["fatal_defect"] = "Notice sent to incorrect address. Not a valid service."
    try:
        notice_date_raw = str(data.get("notice_date", "") or "").strip()
        filing_date_raw = str(data.get("filing_date", "") or "").strip()
        if notice_date_raw and filing_date_raw:
            n_date = datetime.strptime(notice_date_raw, "%Y-%m-%d").date()
            f_date = datetime.strptime(filing_date_raw, "%Y-%m-%d").date()
            if n_date > f_date:
                existing = data.get("fatal_defect") or ""
                if not existing:
                    data["fatal_defect"] = "Date Contradiction: Notice date is after filing date — please verify dates."
    except Exception:
        pass
    try:
        if data.get("dishonour_date") and data.get("notice_date"):
            d_date = datetime.strptime(data["dishonour_date"], "%Y-%m-%d").date()
            n_date = datetime.strptime(data["notice_date"], "%Y-%m-%d").date()
            if d_date > n_date:
                data["fatal_defect"] = "Contradiction: Notice sent before the cheque was dishonoured."
    except Exception:
        pass
    return data
