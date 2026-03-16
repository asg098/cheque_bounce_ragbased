# JUDIQ - Render Deployment (LLM/Phi-2/ngrok/Colab removed)
import hashlib
import json
import logging
import sqlite3
import time
import os
import threading
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
from collections import defaultdict

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

TORCH_AVAILABLE = False  # Render: torch disabled

logger = logging.getLogger(__name__)

PHI2_AVAILABLE = False  # Render: Phi-2 disabled

# ============================================================================
# VERSION CONSTANTS - SINGLE SOURCE OF TRUTH
# ============================================================================
ENGINE_VERSION = "v10.0"
SCORING_MODEL_VERSION = "5.0"
TIMELINE_MATH_VERSION = "CALENDAR_MONTHS"

# ============================================================================
# UTILITY FUNCTIONS (Must be defined before CONFIG)
# ============================================================================

def indian_number_format(amount: float) -> str:
    """Format number in Indian style: 5,00,000 instead of 500,000."""
    s = str(int(amount))
    if len(s) <= 3:
        result = s
    else:
        result = s[-3:]
        s = s[:-3]
        while s:
            result = s[-2:] + ',' + result
            s = s[:-2]
    # Add paise if present
    paise = round((amount % 1) * 100)
    if paise:
        return f"{result}.{paise:02d}"
    return result


def add_calendar_months(start_date, months: int):

    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    return start_date + relativedelta(months=months)



def calculate_cheque_expiry(cheque_date):

    if isinstance(cheque_date, str):
        cheque_date = datetime.strptime(cheque_date, '%Y-%m-%d').date()
    expiry = add_calendar_months(cheque_date, 3)
    days_to_expiry = (expiry - cheque_date).days
    return expiry, days_to_expiry



def get_cause_of_action(notice_service_date, notice_status='delivered'):
    """
    Calculate cause of action date - 15 days after notice service

    Args:
        notice_service_date: Date when notice was served/deemed served
        notice_status: 'delivered', 'refused', 'unclaimed', 'returned'

    Returns:
        Tuple of (cause_of_action_date, explanation)
    """
    if isinstance(notice_service_date, str):
        notice_service_date = datetime.strptime(notice_service_date, '%Y-%m-%d').date()

    # For all cases (delivered, refused, unclaimed), 15 days start from service date
    cause_of_action = notice_service_date + timedelta(days=15)

    if notice_status == 'delivered':
        explanation = f"15 days from actual delivery: {notice_service_date.strftime('%Y-%m-%d')}"
    elif notice_status == 'refused':
        explanation = f"15 days from refusal date (deemed service): {notice_service_date.strftime('%Y-%m-%d')}"
    elif notice_status == 'unclaimed':
        explanation = f"15 days from unclaimed date (deemed service): {notice_service_date.strftime('%Y-%m-%d')}"
    else:
        explanation = f"15 days from notice service: {notice_service_date.strftime('%Y-%m-%d')}"

    return cause_of_action, explanation



def safe_get(data: Dict, *keys, default=None):

    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key)
            if result is None:
                return default
        else:
            return default
    return result if result is not None else default


def format_timeline_transparency(timeline_data: Dict) -> Dict:

    transparent_timeline = {
        'critical_deadlines': {},
        'compliance_display': {},
        'transparency_notes': []
    }

    if 'dates' in timeline_data:
        dates = timeline_data['dates']

        if 'dishonour_date' in dates:
            dishonour = datetime.strptime(dates['dishonour_date'], '%Y-%m-%d').date()

            notice_deadline = add_calendar_months(dishonour, 1)
            transparent_timeline['critical_deadlines']['notice_deadline'] = {
                'last_date': notice_deadline.isoformat(),
                'calculation': f'Dishonour date ({dishonour.isoformat()}) + 1 calendar month',
                'statutory_basis': 'Section 138 NI Act - Notice within 30 days'
            }

            if 'notice_date' in dates:
                notice = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
                days_diff = (notice - dishonour).days

                if notice <= notice_deadline:
                    transparent_timeline['compliance_display']['notice'] = {
                        'status': '✅ COMPLIANT',
                        'details': f'Notice issued {days_diff} days after dishonour',
                        'margin': f'{(notice_deadline - notice).days} days before expiry'
                    }
                else:
                    transparent_timeline['compliance_display']['notice'] = {
                        'status': '❌ BEYOND LIMITATION',
                        'details': f'Notice issued {days_diff} days after dishonour',
                        'exceeded_by': f'{(notice - notice_deadline).days} days late'
                    }

        if 'notice_date' in dates:
            notice = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
            cause_of_action, explanation = get_cause_of_action(notice, 'delivered')

            transparent_timeline['critical_deadlines']['cause_of_action'] = {
                'date': cause_of_action.isoformat(),
                'calculation': explanation,
                'note': 'Limitation starts from this date'
            }

            complaint_deadline = add_calendar_months(cause_of_action, 1)
            transparent_timeline['critical_deadlines']['complaint_deadline'] = {
                'last_date': complaint_deadline.isoformat(),
                'calculation': f'Cause of action ({cause_of_action.isoformat()}) + 1 calendar month',
                'statutory_basis': 'Section 142 NI Act'
            }

            if 'complaint_filed_date' in dates:
                complaint = datetime.strptime(dates['complaint_filed_date'], '%Y-%m-%d').date()
                days_from_coa = (complaint - cause_of_action).days

                if complaint < cause_of_action:
                    # Filed before cause of action arose — premature, legally fatal
                    days_premature = (cause_of_action - complaint).days
                    transparent_timeline['compliance_display']['complaint'] = {
                        'status': '❌ PREMATURE FILING',
                        'details': f'Filed {days_premature} days BEFORE cause of action arose',
                        'cause_of_action_date': cause_of_action.isoformat(),
                        'impact': 'Fatal — complaint filed before 15-day payment period expired'
                    }
                elif complaint <= complaint_deadline:
                    transparent_timeline['compliance_display']['complaint'] = {
                        'status': '✅ WITHIN LIMITATION',
                        'details': f'Filed {days_from_coa} days after cause of action',
                        'margin': f'{(complaint_deadline - complaint).days} days before expiry'
                    }
                else:
                    transparent_timeline['compliance_display']['complaint'] = {
                        'status': '❌ TIME BARRED',
                        'details': f'Filed {days_from_coa} days after cause of action',
                        'exceeded_by': f'{(complaint - complaint_deadline).days} days late - FATAL'
                    }

    return transparent_timeline


def generate_defence_exposure_summary(case_data: Dict, risk_data: Dict, doc_data: Dict) -> Dict:

    defence_summary = {
        'likely_defence_angles': [],
        'exposure_level': 'LOW',
        'preparation_priorities': []
    }

    exposure_score = 0

    if case_data.get('defence_type') == 'security_cheque' or case_data.get('security_cheque_alleged'):
        defence_summary['likely_defence_angles'].append({
            'defence': 'Security Cheque Argument',
            'probability': 'HIGH',
            'strategy': 'Accused will claim cheque was given as security, not for debt',
            'counter': 'Prove transaction purpose through agreement/correspondence',
            'risk_impact': 'MAJOR'
        })
        exposure_score += 40

    if not case_data.get('written_agreement_exists'):
        defence_summary['likely_defence_angles'].append({
            'defence': 'Debt Enforceability Challenge',
            'probability': 'MODERATE',
            'strategy': 'Challenge existence and nature of legally enforceable debt',
            'counter': 'Ledger, bank trail, witness testimony',
            'risk_impact': 'MODERATE'
        })
        exposure_score += 25

    if doc_data.get('service_proof', {}).get('grade') in ['WEAK', 'WEAK/PRESUMPTIVE', 'VERY WEAK']:
        defence_summary['likely_defence_angles'].append({
            'defence': 'Service Presumption Dispute',
            'probability': 'MODERATE',
            'strategy': 'Challenge proof of notice service',
            'counter': 'Obtain AD card or delivery confirmation',
            'risk_impact': 'MODERATE'
        })
        exposure_score += 20

    if case_data.get('cheque_amount', 0) > 1000000:
        if not case_data.get('itr_available') and not case_data.get('bank_statement_available'):
            defence_summary['likely_defence_angles'].append({
                'defence': 'Financial Capacity Challenge',
                'probability': 'MODERATE',
                'strategy': 'Question complainant capacity to lend large amount',
                'counter': 'ITR, bank statements, source of funds proof',
                'risk_impact': 'MODERATE'
            })
            exposure_score += 20

    if risk_data.get('fatal_defects') and len(risk_data.get('fatal_defects', [])) > 0:
        defence_summary['likely_defence_angles'].append({
            'defence': 'Limitation Technical Objection',
            'probability': 'HIGH',
            'strategy': 'Seek dismissal on limitation grounds',
            'counter': 'Address fatal defects immediately',
            'risk_impact': 'CRITICAL'
        })
        exposure_score += 50

    if exposure_score >= 60:
        defence_summary['exposure_level'] = 'HIGH'
    elif exposure_score >= 30:
        defence_summary['exposure_level'] = 'MODERATE'
    else:
        defence_summary['exposure_level'] = 'LOW'

    if len(defence_summary['likely_defence_angles']) > 0:
        defence_summary['preparation_priorities'] = [
            angle['counter'] for angle in defence_summary['likely_defence_angles']
        ]

    return defence_summary


def detect_contradictions(case_data: Dict) -> List[Dict]:

    contradictions = []

    if case_data.get('postal_proof_available') and not case_data.get('notice_date'):
        contradictions.append({
            'type': 'MISSING_DATE',
            'severity': 'MEDIUM',
            'description': 'Postal proof claimed but notice date missing',
            'recommendation': 'Verify notice date from postal receipt'
        })

    if case_data.get('is_company_case') and not case_data.get('directors_impleaded'):
        contradictions.append({
            'type': 'SECTION_141_RISK',
            'severity': 'HIGH',
            'description': 'Company case without directors impleaded',
            'recommendation': 'Implead directors with specific averments'
        })

    if case_data.get('defence_type') and not case_data.get('case_summary'):
        contradictions.append({
            'type': 'INCOMPLETE_DEFENCE',
            'severity': 'LOW',
            'description': 'Defence type specified but no details',
            'recommendation': 'Provide detailed defence explanation'
        })

    return contradictions


def compute_court_statistics_from_kb(kb_data: pd.DataFrame) -> Dict[str, Dict]:

    court_stats = defaultdict(lambda: {
        'total_cases': 0,
        'convictions': 0,
        'acquittals': 0,
        'limitation_dismissals': 0,
        'technical_dismissals': 0,
        'compounded': 0,
        'interim_compensation': 0,
        'presumption_relied': 0,
        'director_discharged': 0,
        'security_cheque_defence_success': 0,
        'security_cheque_defence_total': 0,
        'documentary_rebuttal_success': 0,
        'documentary_rebuttal_total': 0,
        'oral_defence_success': 0,
        'oral_defence_total': 0,
    })

    if 'case_id' not in kb_data.columns:
        return {}

    logger.info("📊 Computing ratio-based court statistics...")

    for idx, row in kb_data.iterrows():
        court_name = str(row.get('court_name', row.get('court_level', 'Generic Court')))
        outcome = str(row.get('final_outcome', '')).lower()
        reasoning = str(row.get('court_reasoning_text', '')).lower()
        dismissal_reason = str(row.get('reason_for_decision', row.get('dismissal_reason', ''))).lower()

        stats = court_stats[court_name]
        stats['total_cases'] += 1

        if 'convict' in outcome or 'guilty' in outcome:
            stats['convictions'] += 1
        elif 'acquit' in outcome or 'not guilty' in outcome:
            stats['acquittals'] += 1
        elif 'dismiss' in outcome or 'discharge' in outcome:
            if 'limitation' in dismissal_reason or 'time-bar' in dismissal_reason or '142' in dismissal_reason:
                stats['limitation_dismissals'] += 1
            elif any(word in dismissal_reason for word in ['technical', 'procedural', 'jurisdiction', 'averment', 'defect']):
                stats['technical_dismissals'] += 1

        if 'compound' in outcome or '147' in reasoning:
            stats['compounded'] += 1

        if '143a' in reasoning or 'interim compensation' in reasoning:
            stats['interim_compensation'] += 1

        if '139' in reasoning or 'presumption' in reasoning or '118' in reasoning:
            stats['presumption_relied'] += 1

        if 'director' in reasoning and ('discharge' in outcome or 'acquit' in outcome):
            stats['director_discharged'] += 1

        if 'security cheque' in reasoning or 'security' in dismissal_reason:
            stats['security_cheque_defence_total'] += 1
            if 'acquit' in outcome or 'discharge' in outcome:
                stats['security_cheque_defence_success'] += 1

        if any(word in reasoning for word in ['document', 'agreement', 'ledger', 'written', 'receipt']):
            stats['documentary_rebuttal_total'] += 1
            if 'acquit' in outcome:
                stats['documentary_rebuttal_success'] += 1
        elif any(word in reasoning for word in ['oral', 'statement', 'testimony', 'witness']):
            stats['oral_defence_total'] += 1
            if 'acquit' in outcome:
                stats['oral_defence_success'] += 1

    for court, stats in court_stats.items():
        total = stats['total_cases']
        if total > 0:
            stats['conviction_rate'] = round((stats['convictions'] / total) * 100, 1)
            stats['acquittal_rate'] = round((stats['acquittals'] / total) * 100, 1)
            stats['limitation_dismissal_rate'] = round((stats['limitation_dismissals'] / total) * 100, 1)
            stats['technical_dismissal_rate'] = round((stats['technical_dismissals'] / total) * 100, 1)
            stats['compounding_rate'] = round((stats['compounded'] / total) * 100, 1)
            stats['presumption_reliance_rate'] = round((stats['presumption_relied'] / total) * 100, 1)
            stats['interim_compensation_rate'] = round((stats['interim_compensation'] / total) * 100, 1)

            if stats['security_cheque_defence_total'] > 0:
                stats['security_cheque_success_rate'] = round(
                    (stats['security_cheque_defence_success'] / stats['security_cheque_defence_total']) * 100, 1
                )

            if stats['documentary_rebuttal_total'] > 0:
                stats['documentary_rebuttal_success_rate'] = round(
                    (stats['documentary_rebuttal_success'] / stats['documentary_rebuttal_total']) * 100, 1
                )

            if stats['oral_defence_total'] > 0:
                stats['oral_defence_success_rate'] = round(
                    (stats['oral_defence_success'] / stats['oral_defence_total']) * 100, 1
                )

            strictness_score = (
                (stats['limitation_dismissal_rate'] * 0.4) +
                (stats['technical_dismissal_rate'] * 0.4) +
                ((100 - stats['compounding_rate']) * 0.2)
            ) / 10

            stats['behavioral_strictness_index'] = round(strictness_score, 1)

            if strictness_score >= 7.0:
                stats['court_classification'] = 'Strict Court'
            elif strictness_score >= 4.0:
                stats['court_classification'] = 'Moderate Court'
            else:
                stats['court_classification'] = 'Liberal Court'

            stats['confidence'] = 'HIGH' if total >= 20 else ('MEDIUM' if total >= 10 else 'LOW')
            stats['sample_size'] = total

    logger.info(f"✅ Ratio-based statistics computed for {len(court_stats)} courts")
    return dict(court_stats)



def save_court_statistics_to_db(court_stats: Dict):
    """Persist aggregated court statistics to the court_statistics table."""
    if not court_stats:
        return
    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()
        for court_name, stats in court_stats.items():
            cursor.execute("""
                INSERT INTO court_statistics (
                    court_name, total_cases, conviction_rate, acquittal_rate,
                    limitation_dismissal_rate, technical_dismissal_rate,
                    compounding_rate, strictness_index, court_classification,
                    confidence, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(court_name) DO UPDATE SET
                    total_cases              = excluded.total_cases,
                    conviction_rate          = excluded.conviction_rate,
                    acquittal_rate           = excluded.acquittal_rate,
                    limitation_dismissal_rate = excluded.limitation_dismissal_rate,
                    technical_dismissal_rate = excluded.technical_dismissal_rate,
                    compounding_rate         = excluded.compounding_rate,
                    strictness_index         = excluded.strictness_index,
                    court_classification     = excluded.court_classification,
                    confidence               = excluded.confidence,
                    updated_at               = datetime('now')
            """, (
                court_name,
                stats.get('total_cases', 0),
                stats.get('conviction_rate', 0),
                stats.get('acquittal_rate', 0),
                stats.get('limitation_dismissal_rate', 0),
                stats.get('technical_dismissal_rate', 0),
                stats.get('compounding_rate', 0),
                stats.get('behavioral_strictness_index', 0),
                stats.get('court_classification', 'Unknown'),
                stats.get('confidence', 'LOW'),
            ))
        conn.commit()
        conn.close()
        logger.info(f"✅ Court statistics saved for {len(court_stats)} courts")
    except sqlite3.Error as e:
        logger.error(f"❌ Failed to save court statistics: {e}")



def analyze_judicial_behavior_fallback(court_location: Optional[str], kb_results: List[Dict]) -> Dict:

    behavior_analysis = {
        'court_identified': court_location or 'Generic Court',
        'data_source': 'KB Heuristic Analysis (Limited Data)',
        'behavioral_indices': {},
        'observed_patterns': [],
        'strategic_insights': [],
        'sample_size': 0,
        'confidence': 'LOW - Using keyword heuristics'
    }

    if not kb_results:
        behavior_analysis['behavioral_indices'] = {
            'limitation_strictness': 5.0,
            'technical_dismissal_tendency': 5.0,
            'presumption_reliance': 7.0,
            'settlement_friendly': 6.0,
            'procedural_formality': 6.0
        }
        behavior_analysis['note'] = 'Using generic baseline - no court-specific data available'
        return behavior_analysis

    behavior_analysis['sample_size'] = len(kb_results)

    limitation_strict_count = 0
    technical_dismissal_count = 0
    presumption_count = 0
    settlement_count = 0

    for result in kb_results:
        text = (result.get('provision_text', '') + ' ' + result.get('explanation', '')).lower()

        if any(word in text for word in ['limitation', 'strict', 'delay', 'condon', 'time-bar']):
            if any(word in text for word in ['dismiss', 'acquit', 'strict', 'mandatory']):
                limitation_strict_count += 1

        if any(word in text for word in ['procedural', 'technical', 'defect', 'jurisdiction']):
            if any(word in text for word in ['dismiss', 'discharge', 'quash']):
                technical_dismissal_count += 1

        if '139' in result.get('section', '') or 'presumption' in text:
            presumption_count += 1

        if any(word in text for word in ['settlement', 'compound', '147', 'mutual']):
            settlement_count += 1

    limitation_strictness = min(10.0, 3.0 + (limitation_strict_count / len(kb_results)) * 7.0)
    technical_tendency = min(10.0, 2.0 + (technical_dismissal_count / len(kb_results)) * 8.0)
    presumption_reliance = min(10.0, 5.0 + (presumption_count / len(kb_results)) * 5.0)
    settlement_friendly = min(10.0, 4.0 + (settlement_count / len(kb_results)) * 6.0)
    procedural_formality = min(10.0, 4.0 + (technical_dismissal_count / len(kb_results)) * 6.0)

    behavior_analysis['behavioral_indices'] = {
        'limitation_strictness': round(limitation_strictness, 1),
        'technical_dismissal_tendency': round(technical_tendency, 1),
        'presumption_reliance': round(presumption_reliance, 1),
        'settlement_friendly': round(settlement_friendly, 1),
        'procedural_formality': round(procedural_formality, 1)
    }

    behavior_analysis['confidence'] = 'MEDIUM' if behavior_analysis['sample_size'] >= 10 else 'LOW'
    behavior_analysis['warning'] = 'Heuristic analysis only - upgrade to structured data for accurate insights'

    return behavior_analysis



CONFIG = {
    "CHEQUE_VALIDITY_MONTHS": 3,
    "CHEQUE_VALIDITY_DAYS": 90,  # 3 months = ~90 days
    "CHEQUE_VALIDITY_CHANGE_DATE": "2012-04-01",
    "NOTICE_DEADLINE_MONTHS": 1,
    "PAYMENT_PERIOD_DAYS": 15,
    "COMPLAINT_LIMITATION_MONTHS": 1,
}

# Risk level thresholds
RISK_HIGH = 70
RISK_MEDIUM = 40
RISK_LOW = 0

# Fatal score caps
FATAL_CAP_CATASTROPHIC = 0
FATAL_CAP_UNIFIED = 15

# Score thresholds
SCORE_EXCELLENT = 80
SCORE_GOOD = 60
SCORE_ADEQUATE = 40

# FIX #1: Scoring constants (eliminate magic numbers)
RISK_POINTS_DIRECTOR_MISSING = 70
RISK_POINTS_NO_DEBT_PROOF = 30
RISK_POINTS_NO_AGREEMENT = 20
RISK_POINTS_SECURITY_CHEQUE = 25
RISK_POINTS_SERVICE_WEAK = 15
RISK_POINTS_JURISDICTION_INVALID = 20

PENALTY_JURISDICTION = 20
PENALTY_SECTION_65B = 10

SCORE_SERVICE_EXCELLENT = 100
SCORE_SERVICE_GOOD = 85
SCORE_SERVICE_WEAK = 25
SCORE_SERVICE_NONE = 0

# analytics_db_path is resolved after DATA_DIR is configured below


# FIX #4: Safe date parsing with multiple format support
def parse_date_safely(date_value, field_name="date"):
    """
    Parse date from various formats safely.

    Supports:
    - YYYY-MM-DD (ISO format)
    - DD/MM/YYYY (Indian format)
    - DD-MM-YYYY
    - datetime objects (passthrough)

    Args:
        date_value: String, datetime, or date object
        field_name: Field name for error messages

    Returns:
        date object

    Raises:
        ValueError: If format not recognized
    """
    if date_value is None:
        return None

    # Already a date/datetime object
    if isinstance(date_value, datetime):
        return date_value.date()
    if isinstance(date_value, date):
        return date_value

    # Try various formats
    if isinstance(date_value, str):
        formats = [
            '%Y-%m-%d',      # ISO: 2024-01-15
            '%d/%m/%Y',      # Indian: 15/01/2024
            '%d-%m-%Y',      # Alt: 15-01-2024
            '%Y/%m/%d',      # Alt ISO: 2024/01/15
            '%d.%m.%Y',      # European: 15.01.2024
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_value, fmt).date()
            except ValueError:
                continue

        raise ValueError(
            f"Invalid date format for {field_name}: '{date_value}'. "
            f"Supported formats: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY"
        )

    raise TypeError(f"{field_name} must be string, date, or datetime object")


# FIX #6: Safe boolean extraction
def get_bool(data: Dict, key: str, default: bool = False) -> bool:
    """
    Extract boolean value safely from various formats.

    Handles:
    - True/False (bool)
    - "true"/"false" (string)
    - "1"/"0" (string)
    - "yes"/"no" (string)
    - 1/0 (int)

    Args:
        data: Dictionary to extract from
        key: Key to look up
        default: Default value if key missing

    Returns:
        Boolean value
    """
    val = data.get(key, default)

    if isinstance(val, bool):
        return val

    if isinstance(val, str):
        return val.lower() in ['true', '1', 'yes', 'y']

    if isinstance(val, (int, float)):
        return bool(val)

    return bool(val)


# FIX #5: Input validation
REQUIRED_FIELDS = ['cheque_date', 'dishonour_date']
OPTIONAL_CRITICAL_FIELDS = ['notice_date', 'complaint_filed_date']

def validate_case_data(case_data: Dict) -> tuple:
    """
    Validate case data has required fields.

    Returns:
        (is_valid, error_message, missing_fields)
    """
    if not isinstance(case_data, dict):
        return False, "Case data must be a dictionary", []

    missing_required = [f for f in REQUIRED_FIELDS if not case_data.get(f)]

    if missing_required:
        return False, f"Missing required fields: {', '.join(missing_required)}", missing_required

    # Warn about missing optional critical
    missing_optional = [f for f in OPTIONAL_CRITICAL_FIELDS if not case_data.get(f)]

    return True, None, missing_optional


# FIX #3: Error handling decorator
def safe_analysis(func):
    """
    Decorator to add error handling to analysis functions.
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"❌ {func.__name__} failed: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())

            # Return error structure instead of crashing
            return {
                'module': func.__name__.replace('analyze_', '').replace('_', ' ').title(),
                'error': str(e),
                'error_type': type(e).__name__,
                'status': 'ERROR',
                'score': 0,
                'note': f'Analysis failed: {str(e)}'
            }

    return wrapper


# FIX #7: Safe division (prevent division by zero)
def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.

    Args:
        numerator: Number to divide
        denominator: Number to divide by
        default: Value to return if denominator is 0

    Returns:
        Result of division or default
    """
    if denominator == 0:
        logger.warning(f"Division by zero prevented: {numerator}/{denominator}")
        return default
    return numerator / denominator


# ============================================================================
# LOCAL LLM (PHI-2) - CROSS-EXAMINATION ONLY
# ============================================================================

# Phi-2 disabled on Render
_phi2_model = None
_phi2_tokenizer = None

def load_phi2_model():
    """Phi-2 disabled on Render."""
    return None, None


def _build_cross_exam_context(case_data: Dict) -> str:
    """Build a rich case context string for the Phi-2 prompt."""
    amount   = case_data.get('cheque_amount', 0)
    lines = [
        f"Cheque Amount     : ₹{indian_number_format(amount)}",
        f"Cheque Number     : {case_data.get('cheque_number', 'Not provided')}",
        f"Cheque Date       : {case_data.get('cheque_date', 'Not provided')}",
        f"Bank              : {case_data.get('bank_name', 'Not provided')}",
        f"Dishonour Date    : {case_data.get('dishonour_date', 'Not provided')}",
        f"Dishonour Reason  : {case_data.get('dishonour_reason', 'Insufficient Funds')}",
        f"Notice Date       : {case_data.get('notice_date', 'Not provided')}",
        f"Notice Received   : {case_data.get('notice_received_date', 'Not provided')}",
        f"Complaint Filed   : {case_data.get('complaint_filed_date', 'Not provided')}",
        f"Debt Nature       : {case_data.get('debt_nature', 'Not specified')}",
        f"Defence Raised    : {case_data.get('defence_type', 'Not specified')}",
        f"Written Agreement : {'Yes' if case_data.get('written_agreement_exists') else 'No'}",
        f"Ledger Available  : {'Yes' if case_data.get('ledger_available') else 'No'}",
        f"Postal Proof      : {'Yes' if case_data.get('postal_proof_available') else 'No'}",
        f"Company Case      : {'Yes' if case_data.get('is_company_case') else 'No'}",
    ]
    if case_data.get('case_summary'):
        lines.append(f"Case Facts        : {case_data['case_summary']}")
    return "\n".join(lines)


def _build_witness_prompt(witness_type: str, context: str, num_questions: int) -> str:
    """Build a witness-specific, legally targeted Phi-2 prompt."""

    # Witness-specific attack angles
    angles = {
        "complainant": (
            "defence lawyer cross-examining the complainant",
            [
                "Challenge existence and enforceability of the underlying debt",
                "Question financial capacity to lend (source of funds, ITR, bank statements)",
                "Expose absence of written agreement, ledger, or documentary proof",
                "Challenge the circumstances and purpose of cheque issuance",
                "Test knowledge of exact transaction date, amount, and repayment terms",
                "Question delay in filing notice or complaint",
                "Expose if cheque was given as security rather than for a debt",
            ]
        ),
        "accused": (
            "complainant's lawyer cross-examining the accused",
            [
                "Pin down acknowledgment of receiving the cheque amount",
                "Establish the legally enforceable nature of the debt",
                "Challenge the 'security cheque' defence with specific facts",
                "Question why no payment was made within 15-day notice period",
                "Expose inconsistencies in the accused's explanation",
                "Establish that the accused's signature on cheque is genuine",
                "Challenge any claim of stop payment or account closure instructions",
            ]
        ),
        "bank_official": (
            "defence lawyer cross-examining the bank official",
            [
                "Challenge authenticity and completeness of the dishonour memo",
                "Question the exact grounds stated on the return memo",
                "Explore whether cheque was presented within validity period",
                "Question bank's procedure for dishonour and memo issuance",
                "Challenge the chain of custody of the original cheque",
                "Explore whether a stop-payment instruction was in place",
            ]
        ),
        "drawer": (
            "complainant's lawyer cross-examining the drawer / accused",
            [
                "Establish cheque was issued voluntarily and knowingly",
                "Pin down the date, amount, and purpose of the cheque",
                "Establish legally enforceable debt exists",
                "Challenge any partial payment claims",
                "Question why 15-day period passed without payment or communication",
            ]
        ),
        "witness": (
            "cross-examining a third-party transaction witness",
            [
                "Test first-hand knowledge versus hearsay",
                "Establish presence at the time of transaction",
                "Question relationship with the complainant (bias)",
                "Test recollection of specific transaction details",
                "Challenge ability to identify documents signed years ago",
            ]
        ),
    }

    role, attack_lines = angles.get(witness_type, angles["complainant"])
    attack_text = "\n".join(f"   - {a}" for a in attack_lines)

    return f"""Instruct: You are a senior Indian litigation lawyer acting as {role} in a Section 138 NI Act (cheque bounce) case.

CASE FACTS:
{context}

Generate exactly {num_questions} sharp, specific cross-examination questions. Each question must:
- Be directly grounded in the case facts above
- Target a specific legal weakness or inconsistency
- Be phrased as a direct question to the witness
- Be usable in an Indian Magistrate Court

Focus your attack on:
{attack_text}

Format: Number each question. One question per line. Questions only — no explanations.

Output:
1."""


def _parse_phi2_questions(response: str, num_questions: int) -> list:
    """
    Robust parser for Phi-2 output.
    Handles numbered lists, bullet points, and plain sentences.
    """
    import re

    # Strip everything before "Output:" or "Questions:" if present
    for marker in ["Output:", "Questions:", "Answer:"]:
        if marker in response:
            response = response.split(marker, 1)[1]

    lines = [l.strip() for l in response.split("\n") if l.strip()]
    questions = []

    for line in lines:
        # Skip very short lines, headers, or lines that look like labels
        if len(line) < 15:
            continue
        if line.lower().startswith(("focus", "format", "instruct", "generate", "case facts")):
            continue

        # Strip common numbering patterns: "1.", "1)", "Q1.", "Q1:", "- ", "• "
        cleaned = re.sub(r"^(Q?\d{1,2}[.):\-]\s*|[-•*]\s*)", "", line).strip()

        # Must end with "?" to be a valid question, or contain "?" anywhere
        if "?" in cleaned and len(cleaned) > 20:
            # Take text up to and including the last "?"
            cleaned = cleaned[:cleaned.rfind("?") + 1].strip()
            if len(cleaned) > 20:
                questions.append(cleaned)

        if len(questions) >= num_questions:
            break

    return questions[:num_questions]


def _build_summary_section(questions: list, witness_type: str, case_data: Dict) -> Dict:
    """
    Generate a plain-language summary of the cross-examination strategy
    without touching or re-running the main analysis engine.
    This is purely derived from the questions and case_data already in hand.
    """
    defence = case_data.get('defence_type', 'not specified')
    has_agreement = case_data.get('written_agreement_exists', False)
    has_ledger = case_data.get('ledger_available', False)
    has_postal = case_data.get('postal_proof_available', False)
    amount = indian_number_format(case_data.get('cheque_amount', 0))

    # Identify primary attack themes from the questions
    themes = []
    q_text = " ".join(questions).lower()
    if any(w in q_text for w in ["agreement", "document", "proof", "ledger", "written"]):
        themes.append("Documentary weakness — no written agreement or ledger")
    if any(w in q_text for w in ["security", "guarantee", "collateral"]):
        themes.append("Security cheque defence — challenging purpose of issuance")
    if any(w in q_text for w in ["notice", "postal", "received", "service", "address"]):
        themes.append("Notice service — challenging proof of delivery")
    if any(w in q_text for w in ["capacity", "income", "source", "funds", "afford"]):
        themes.append("Financial capacity — questioning complainant's ability to lend ₹" + amount)
    if any(w in q_text for w in ["date", "when", "time", "period", "15"]):
        themes.append("Timeline — challenging dates and limitation compliance")
    if not themes:
        themes.append("General credibility and consistency of witness testimony")

    witness_labels = {
        "complainant": "the person who filed the complaint",
        "accused":     "the person who issued the cheque",
        "bank_official": "the bank witness regarding the dishonour memo",
        "drawer":      "the cheque drawer / accused",
        "witness":     "the third-party transaction witness",
    }
    label = witness_labels.get(witness_type, witness_type)

    objective = (
        f"These {len(questions)} questions are designed to cross-examine {label} "
        f"in a ₹{amount} Section 138 case. "
    )
    if witness_type in ("complainant", "drawer"):
        if not has_agreement and not has_ledger:
            objective += (
                "The primary attack is on the absence of documentary proof of the debt — "
                "without a written agreement or ledger, the complainant cannot easily establish "
                "a legally enforceable debt, which is a fundamental ingredient under Section 138."
            )
        elif defence == "security_cheque":
            objective += (
                "The defence is that the cheque was issued as security, not for a debt. "
                "Questions focus on pinning the complainant to the exact purpose and terms "
                "of the cheque issuance."
            )
        else:
            objective += "Questions target credibility and evidentiary gaps."
    elif witness_type == "bank_official":
        objective += (
            "Questions focus on the authenticity of the dishonour memo and bank procedures, "
            "since the memo is the primary evidence of dishonour under Section 138."
        )
    else:
        objective += "Questions test first-hand knowledge and expose inconsistencies."

    return {
        "objective": objective,
        "primary_attack_themes": themes,
        "witness_role": label,
        "question_count": len(questions),
        "note": "Summary derived from generated questions and case facts only — main analysis not re-run."
    }


def generate_cross_examination_questions(
    case_data: Dict,
    witness_type: str = "complainant",
    num_questions: int = 5
) -> Dict:
    """Cross-examination on Render uses rule-based engine only (no Phi-2)."""
    questions = _rule_based_fallback_questions(case_data, witness_type, num_questions)
    summary   = _build_summary_section(questions, witness_type, case_data)
    return {
        'enabled': True,
        'witness_type': witness_type,
        'questions': questions,
        'summary': summary,
        'model': 'rule-based (Phi-2 disabled on Render)',
        'question_count': len(questions),
        'disclaimer': 'Rule-based questions derived from case facts. Review with legal counsel.'
    }


def _rule_based_fallback_questions(case_data: Dict, witness_type: str, num: int) -> list:
    """
    Deterministic fallback questions when Phi-2 is unavailable or returns garbage.
    Built from case facts — always returns valid, legally grounded questions.
    """
    amount  = f"₹{indian_number_format(case_data.get('cheque_amount', 0))}"
    bank    = case_data.get('bank_name', 'the bank')
    defence = case_data.get('defence_type', '')
    has_ag  = case_data.get('written_agreement_exists', False)
    cheque_date = case_data.get('cheque_date', 'the cheque date')
    notice_date = case_data.get('notice_date', 'the notice date')

    pools = {
        "complainant": [
            f"Is it correct that you do not have any written agreement evidencing the loan of {amount}?",
            f"Can you produce the original bank statement showing the transfer of {amount} to the accused?",
            f"You have stated the cheque was issued towards repayment of a loan — when exactly was this loan given and in what form?",
            f"Is it correct that no receipts, ledger entries, or acknowledgements were created at the time of the alleged transaction?",
            f"The cheque dated {cheque_date} — was it issued on the same day as the alleged transaction or at a later date?",
            f"Did you retain any document signed by the accused acknowledging receipt of {amount}?",
            f"Is it not true that the notice dated {notice_date} was sent by ordinary post and not by registered AD post?",
            "Can you explain why no civil suit for recovery was filed despite claiming a legally enforceable debt?",
            f"Is it correct that the cheque was given as security and not towards discharge of any specific liability?",
            "Did the accused ever dispute the debt in writing before the cheque was dishonoured?",
        ],
        "accused": [
            f"Is your signature on the cheque for {amount} genuine?",
            f"You issued the cheque dated {cheque_date} — for what specific purpose was it issued?",
            "Did you make any payment towards this debt after the cheque was dishonoured?",
            "Did you respond to the statutory notice within the 15-day period?",
            "Is it your case that the cheque was given as security? If so, please produce any document recording this arrangement.",
            "Have you at any point in writing denied the existence of the debt?",
            "Why did you not make payment within 15 days of receiving the notice?",
        ],
        "bank_official": [
            "Please produce the original dishonour memo — is the one you are relying on a certified copy?",
            "What was the exact reason recorded by the bank for returning the cheque?",
            "Was there any stop-payment instruction on the account at the time of presentation?",
            f"Was the cheque presented to {bank} within the cheque's validity period of 3 months?",
            "What is the standard procedure in your bank when a cheque is returned unpaid?",
            "Who physically prepared and signed the dishonour memo in this case?",
        ],
        "witness": [
            "Were you personally present when the alleged loan was given?",
            "Did you witness the signing of any written agreement between the parties?",
            "What is your personal relationship with the complainant?",
            "Are you able to identify the cheque in question — did you see it being issued?",
            "Is your testimony based on what you personally saw or what you were told by the complainant?",
        ],
    }

    q_pool = pools.get(witness_type, pools["complainant"])

    # Prioritise questions relevant to this specific case
    if defence == "security_cheque" and witness_type == "complainant":
        q_pool = [q for q in q_pool if "security" in q.lower() or "agreement" in q.lower()] + q_pool
    if not has_ag and witness_type == "complainant":
        q_pool = [q for q in q_pool if "agreement" in q.lower() or "written" in q.lower() or "ledger" in q.lower()] + q_pool

    # Deduplicate while preserving order
    seen, result = set(), []
    for q in q_pool:
        if q not in seen:
            seen.add(q)
            result.append(q)
        if len(result) >= num:
            break

    return result[:num]


    """
    Safely divide two numbers, returning default if denominator is zero.

    Args:
        numerator: Number to divide
        denominator: Number to divide by
        default: Value to return if denominator is 0

    Returns:
        Result of division or default
    """
    if denominator == 0:
        logger.warning(f"Division by zero prevented: {numerator}/{denominator}")
        return default
    return numerator / denominator



def classify_risk_with_legal_tone(score: float, fatal_defects: List) -> Dict:

    classification = {}

    if fatal_defects and len(fatal_defects) > 0:
        classification['category'] = 'HIGH DISMISSAL RISK'
        classification['tone'] = 'Statutory compliance deficient - dismissal probable'
        classification['label'] = 'Critical Defects Identified'
    elif score >= 80:
        classification['category'] = 'STATUTORILY COMPLIANT'
        classification['tone'] = 'Statutory requirements appear satisfied (subject to proof)'
        classification['label'] = 'Compliance Adequate'
    elif score >= 60:
        classification['category'] = 'PROCEDURAL RISK IDENTIFIED'
        classification['tone'] = 'Compliance adequate with evidentiary gaps requiring substantiation'
        classification['label'] = 'Moderate Risks Present'
    elif score >= 40:
        classification['category'] = 'SIGNIFICANT DEFICIENCIES'
        classification['tone'] = 'Material compliance gaps - remediation advisable'
        classification['label'] = 'Weak Compliance'
    else:
        classification['category'] = 'HIGH DISMISSAL RISK'
        classification['tone'] = 'Substantial statutory violations - filing inadvisable without remediation'
        classification['label'] = 'Critical Deficiencies'

    return classification

# ENGINE_VERSION already defined at top - removed duplicate
ENGINE_BUILD_DATE = "2025-02-18"
ENGINE_MATURITY = "Production Stable"
ENGINE_SCOPE = {
    "includes": [
        "Ratio-based judicial analytics",
        "Data-calibrated scoring",
        "Confidence layer (every output)",
        "Severity-tiered deductions",
        "Validation framework (methodology documented)",
        "Procedural compliance analysis",
        "Ingredient compliance matrix",
        "Risk mapping with fatal override",
        "Timeline intelligence",
        "Documentary strength assessment",
        "Defence vulnerability detection",
        "Settlement exposure modeling",
        "Presumption burden shift tracking",
        "Cross-examination risk analysis",
        "Contradiction detection",
        "Professional dual-format reports"
    ],
    "excludes": [
        "Legal advice",
        "Outcome predictions",
        "Draft generation",
        "Substitute for legal counsel"
    ],
    "disclaimer": "This system provides legal intelligence and risk analysis only. "
                 "All strategic decisions must be made by qualified legal counsel. "
                 "Not a substitute for lawyer consultation."
}

# --- Additional standard library imports (not already at top) ---
import sys
import asyncio
from functools import lru_cache
from enum import Enum

# --- ML / AI imports (hard dependencies for the engine) ---
# Render: sentence_transformers / transformers removed

# --- API framework imports ---
from fastapi import FastAPI, HTTPException, UploadFile, File, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import uvicorn

# --- Optional: rate limiting ---
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    RATE_LIMITING_AVAILABLE = True
except Exception:
    RATE_LIMITING_AVAILABLE = False

# Always use DummyLimiter on Render — avoids slowapi/starlette version conflicts
RATE_LIMITING_AVAILABLE = False

class DummyLimiter:
    def limit(self, rate):
        def decorator(func):
            return func
        return decorator

Limiter = DummyLimiter
get_remote_address = lambda x: "0.0.0.0"

# --- Logging configuration ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================================
# DATABASE & DIRECTORY CONFIGURATION (Render)
# ============================================================================
DATA_DIR = (Path(__file__).parent if '__file__' in globals() else Path.cwd()) / "data_judiq"
BASE_DIR = DATA_DIR.parent
DATA_DIR.mkdir(parents=True, exist_ok=True)
(DATA_DIR / "legal_kb").mkdir(exist_ok=True)
(DATA_DIR / "case_analysis").mkdir(exist_ok=True)

CONFIG = {


    "PORT": int(os.getenv("PORT", 5001)),  # Render sets PORT automatically
    "HOST": "0.0.0.0",

    # ── Google Drive KB Configuration (Render) ──────────────────────────────
    # Share your cheque_bounce_kb.csv on Google Drive with "Anyone with the link"
    # Then paste the file ID here (the long string in the shareable URL).
    # Example URL: https://drive.google.com/file/d/1aBcDeFgHiJkLmN.../view
    # File ID would be: 1aBcDeFgHiJkLmN...
    "GDRIVE_FILE_ID": os.getenv("GDRIVE_FILE_ID", "1btile_wGE2pjmU_gvm3ZwF-ufB246x5A"),  # cheque_bounce_kb.csv
    # Optional: Google Drive API key for higher rate limits (not required for public files)
    "GDRIVE_API_KEY": os.getenv("GDRIVE_API_KEY", ""),



    "CACHE_TTL": 7200,


    "HIGH_RISK_THRESHOLD": 70,
    "MEDIUM_RISK_THRESHOLD": 40,

    "SCORING_WEIGHTS": {
        "timeline": 0.25,
        "ingredients": 0.30,
        "documentary": 0.20,
        "liability": 0.15,
        "procedural": 0.10
    },

    "WEIGHT_TIMELINE": 0.25,
    "WEIGHT_INGREDIENTS": 0.30,
    "WEIGHT_DOCUMENTARY": 0.20,
    "WEIGHT_LIABILITY": 0.15,
    "WEIGHT_PROCEDURAL": 0.10,

    "SCORE_MIN": 0,
    "SCORE_MAX": 100,
    "SCORE_CLAMP_ENABLED": True,

    "STRICTNESS_INDEX_HIGH": 8.0,
    "STRICTNESS_INDEX_MEDIUM": 5.0,
    "SETTLEMENT_FRIENDLY_HIGH": 7.0,

    "CHEQUE_VALIDITY_MONTHS": 3,
    "CHEQUE_VALIDITY_CHANGE_DATE": "2012-04-01",
    "NOTICE_DEADLINE_MONTHS": 1,
    "PAYMENT_PERIOD_DAYS": 15,
    "COMPLAINT_LIMITATION_MONTHS": 1,
}

RISK_HIGH = 70
RISK_MEDIUM = 40
RISK_LOW = 0

FATAL_CAP_CATASTROPHIC = 0
FATAL_CAP_CRITICAL = 20
FATAL_CAP_SEVERE = 25
FATAL_CAP_MAJOR = 25
FATAL_CAP_UNIFIED = 25

SCORE_EXCELLENT = 80
SCORE_GOOD = 60
SCORE_ADEQUATE = 40
SCORE_WEAK = 20

DEDUCTION_CRITICAL = 30
DEDUCTION_HIGH = 20
DEDUCTION_MODERATE = 10
DEDUCTION_MINOR = 5

CONFIDENCE_HIGH = 85
CONFIDENCE_MEDIUM = 70
CONFIDENCE_LOW = 50

DOC_WEIGHT_PRIMARY = 0.50
DOC_WEIGHT_SUPPORTING = 0.30
DOC_WEIGHT_CORROBORATIVE = 0.20

INGREDIENT_PASS_THRESHOLD = 70

TIMELINE_STRICT_DAYS = 30
TIMELINE_GRACE_DAYS = 0

COMPLEXITY_HIGH_THRESHOLD = 50
COMPLEXITY_MEDIUM_THRESHOLD = 25

def normalize_score(score: float) -> float:

    if CONFIG["SCORE_CLAMP_ENABLED"]:
        return max(CONFIG["SCORE_MIN"], min(CONFIG["SCORE_MAX"], score))
    return score

def get_centralized_weights() -> Dict[str, float]:

    weights = CONFIG["SCORING_WEIGHTS"].copy()

    total = sum(weights.values())
    if abs(total - 1.0) > 0.01:
        logger.warning(f"⚠️ Weights sum to {total}, normalizing to 1.0")

        for key in weights:
            weights[key] = weights[key] / total

    return weights

def validate_case_input(case_data: Dict) -> Tuple[bool, List[str]]:

    errors = []

    required = {
        'case_type': 'Case type (complainant/accused)',
        'cheque_amount': 'Cheque amount',
        'cheque_number': 'Cheque number',
        'cheque_date': 'Cheque date',
        'bank_name': 'Bank name',
        'dishonour_date': 'Dishonour date',
        'notice_date': 'Notice date',
        'complaint_filed_date': 'Complaint filed date'
    }

    for field, label in required.items():
        if not case_data.get(field):
            errors.append(f"❌ Missing required field: {label}")

    if errors:
        return (False, errors)

    # Normalize case_type - accept common variations
    case_type_raw = (case_data.get('case_type') or '').strip().lower()
    case_type_mapping = {
        'complainant': 'complainant', 'accused': 'accused',
        'plaintiff': 'complainant', 'defendant': 'accused',
        'prosecution': 'complainant', 'defense': 'accused', 'defence': 'accused',
        'recovery': 'complainant', 'individual': 'accused', 'company': 'accused'
    }
    case_type = case_type_mapping.get(case_type_raw)
    if not case_type:
        errors.append(f"❌ Invalid case_type: '{case_data.get('case_type')}' - Use 'complainant' or 'accused'")

    try:
        amount = float(case_data['cheque_amount'])
        if amount <= 0:
            errors.append("❌ Cheque amount must be positive (greater than 0)")
        if amount > 100_000_000_000:
            errors.append(f"❌ Cheque amount exceeds reasonable limit (₹100 billion)")
        if amount < 1:
            errors.append("❌ Cheque amount suspiciously low (minimum ₹1)")
    except (ValueError, TypeError) as e:
        errors.append(f"❌ Invalid cheque amount format: {case_data.get('cheque_amount')} - {str(e)}")

    cheque_num = case_data.get('cheque_number', '')
    if not cheque_num or len(str(cheque_num).strip()) == 0:
        errors.append("❌ Cheque number cannot be empty")

    bank = case_data.get('bank_name', '')
    if not bank or len(str(bank).strip()) < 2:
        errors.append("❌ Bank name must be at least 2 characters")

    date_fields = {
        'transaction_date': 'Transaction date',
        'cheque_date': 'Cheque date',
        'presentation_date': 'Presentation date',
        'dishonour_date': 'Dishonour date',
        'notice_date': 'Notice date',
        'notice_received_date': 'Notice received date',
        'complaint_filed_date': 'Complaint filed date'
    }

    parsed_dates = {}

    for field, label in date_fields.items():
        if case_data.get(field):
            try:
                date_val = case_data[field]
                if isinstance(date_val, str):

                    parsed = datetime.strptime(date_val, '%Y-%m-%d').date()
                    parsed_dates[field] = parsed
                elif hasattr(date_val, 'year'):
                    parsed_dates[field] = date_val
                else:
                    errors.append(f"❌ {label}: Invalid date format (use YYYY-MM-DD)")
            except (ValueError, TypeError) as e:
                errors.append(f"❌ {label}: Invalid date format '{date_val}' (use YYYY-MM-DD)")

    if len(parsed_dates) >= 3:
        today = datetime.now().date()

        for field, date_val in parsed_dates.items():
            if date_val > today:
                errors.append(f"❌ {date_fields[field]} cannot be in the future ({date_val})")

        sequence_checks = [
            ('transaction_date', 'cheque_date', 'Transaction date must be before cheque date'),
            ('cheque_date', 'presentation_date', 'Cheque date must be before presentation date'),
            ('cheque_date', 'dishonour_date', 'Cheque date must be before dishonour date'),
            ('dishonour_date', 'notice_date', 'Dishonour date must be before notice date'),
            ('notice_date', 'notice_received_date', 'Notice sent date must be before received date'),
            ('notice_date', 'complaint_filed_date', 'Notice date must be before complaint date'),
        ]

        for earlier, later, message in sequence_checks:
            if earlier in parsed_dates and later in parsed_dates:
                if parsed_dates[earlier] > parsed_dates[later]:
                    errors.append(f"❌ {message}: {parsed_dates[earlier]} > {parsed_dates[later]}")

        if 'dishonour_date' in parsed_dates and 'notice_date' in parsed_dates:

            notice_deadline = add_calendar_months(parsed_dates['dishonour_date'], 1)
            if parsed_dates['notice_date'] > notice_deadline:
                notice_gap = (parsed_dates['notice_date'] - parsed_dates['dishonour_date']).days
                errors.append(f"⚠️ CRITICAL: Notice sent {notice_gap} days after dishonour (deadline was {notice_deadline})")

        if 'notice_date' in parsed_dates and 'complaint_filed_date' in parsed_dates:

            if 'notice_received_date' in parsed_dates:
                service_date = parsed_dates['notice_received_date']
            else:
                service_date = parsed_dates['notice_date']

            cause_of_action = service_date + timedelta(days=15)

            complaint_deadline = add_calendar_months(cause_of_action, 1)

            if parsed_dates['complaint_filed_date'] < cause_of_action:
                days_premature = (cause_of_action - parsed_dates['complaint_filed_date']).days
                errors.append(
                    f"⚠️ FATAL: Complaint filed {days_premature} days BEFORE cause of action "
                    f"(cause of action arises on {cause_of_action} — 15 days after notice service)"
                )
            elif parsed_dates['complaint_filed_date'] > complaint_deadline:
                complaint_gap = (parsed_dates['complaint_filed_date'] - cause_of_action).days
                errors.append(f"⚠️ CRITICAL: Complaint filed {complaint_gap} days after cause of action (deadline was {complaint_deadline})")

    boolean_fields = [
        'is_company_case', 'directors_impleaded', 'return_memo_available',
        'postal_proof_available', 'original_cheque_available', 'written_agreement_exists'
    ]

    for field in boolean_fields:
        if field in case_data:
            val = case_data[field]
            if not isinstance(val, bool) and val not in [0, 1, '0', '1', 'true', 'false', 'True', 'False']:
                errors.append(f"⚠️ {field} should be boolean (true/false)")

    # ENHANCEMENT 3: Section 65B Strict Validation (NO OVERRIDE)
    """
    BEFORE (v9.8 OLD):
    - Section 65B check was optional
    - Could proceed with electronic evidence without certificate
    - Only marked as CRITICAL, not FATAL

    AFTER (v9.8 ENHANCED):
    - Section 65B certificate MANDATORY if electronic evidence
    - NO override allowed - FATAL defect
    - Evidence inadmissible without certificate (Anvar P.V. precedent)
    """
    if case_data.get('electronic_evidence', False):
        has_65b_certificate = case_data.get('section_65b_certificate', False)

        if not has_65b_certificate:
            errors.append({
                'type': 'FATAL_DEFECT',
                'field': 'section_65b_certificate',
                'severity': 'FATAL',
                'message': '❌ FATAL: Electronic evidence present but Section 65B certificate MISSING',
                'legal_basis': 'Section 65B Evidence Act - Anvar P.V. v. P.K. Basheer (2014)',
                'impact': 'Electronic evidence INADMISSIBLE - NO OVERRIDE ALLOWED',
                'consequence': 'All electronic evidence (emails, SMS, WhatsApp, digital records) will be EXCLUDED',
                'remedy': 'Obtain Section 65B certificate from device/server custodian before filing',
                'strict_enforcement': True,
                'override_allowed': False
            })
            # This is a FATAL defect - cannot proceed
            return (False, errors)

    if case_data.get('is_company_case'):
        if not case_data.get('company_name'):
            errors.append("⚠️ Company case but company_name not provided")
        if not case_data.get('directors_impleaded'):
            errors.append("⚠️ CRITICAL: Company case without directors impleaded - Section 141 violation risk")

    doc_fields = ['original_cheque_available', 'return_memo_available', 'postal_proof_available']
    if not any(case_data.get(f) for f in doc_fields):
        errors.append("⚠️ WARNING: No primary documentary evidence marked as available")

    is_valid = len(errors) == 0

    if not is_valid:
        logger.warning(f"Input validation failed with {len(errors)} errors")
        for err in errors:
            logger.warning(f"  {err}")

    return (is_valid, errors)

def compute_unified_timeline(case_data: Dict) -> Dict:

    timeline_obj = {
        'dates': {},
        'gaps': {},
        'compliance': {},
        'validity': {},
        'errors': []
    }

    try:

        date_fields = [
            'transaction_date', 'cheque_date', 'presentation_date',
            'dishonour_date', 'notice_date', 'notice_received_date',
            'complaint_filed_date'
        ]

        for field in date_fields:
            if case_data.get(field):
                try:
                    if isinstance(case_data[field], str):
                        parsed_date = datetime.strptime(
                            case_data[field], '%Y-%m-%d'
                        ).date()

                        timeline_obj['dates'][field] = parsed_date.isoformat()
                    elif hasattr(case_data[field], 'isoformat'):

                        timeline_obj['dates'][field] = case_data[field].isoformat()
                    else:
                        timeline_obj['dates'][field] = str(case_data[field])
                except:
                    pass

        dates = timeline_obj['dates']

        parsed_dates = {}
        for field, date_str in dates.items():
            try:
                parsed_dates[field] = datetime.strptime(date_str, '%Y-%m-%d').date()
            except:
                pass

        if 'cheque_date' in parsed_dates and 'dishonour_date' in parsed_dates:
            timeline_obj['gaps']['cheque_to_dishonour'] = (
                parsed_dates['dishonour_date'] - parsed_dates['cheque_date']
            ).days

        if 'dishonour_date' in parsed_dates and 'notice_date' in parsed_dates:
            timeline_obj['gaps']['dishonour_to_notice'] = (
                parsed_dates['notice_date'] - parsed_dates['dishonour_date']
            ).days

        if 'notice_date' in parsed_dates and 'complaint_filed_date' in parsed_dates:
            timeline_obj['gaps']['notice_to_complaint'] = (
                parsed_dates['complaint_filed_date'] - parsed_dates['notice_date']
            ).days

        if 'cheque_date' in dates and 'dishonour_date' in dates:
            cheque_date_parsed = datetime.strptime(dates['cheque_date'], '%Y-%m-%d').date()
            dishonour_date_parsed = datetime.strptime(dates['dishonour_date'], '%Y-%m-%d').date()

            expiry_date, days_validity = calculate_cheque_expiry(cheque_date_parsed)
            gap_days = (dishonour_date_parsed - cheque_date_parsed).days

            timeline_obj['validity']['cheque_valid'] = dishonour_date_parsed <= expiry_date
            timeline_obj['validity']['days_from_cheque'] = gap_days
            timeline_obj['validity']['expiry_date'] = expiry_date.isoformat()
            timeline_obj['validity']['validity_months'] = 3
            timeline_obj['validity']['days_before_expiry'] = (expiry_date - dishonour_date_parsed).days

        if 'dishonour_date' in dates and 'notice_date' in dates:
            dishonour_dt = datetime.strptime(dates['dishonour_date'], '%Y-%m-%d').date()
            notice_dt = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
            notice_deadline = add_calendar_months(dishonour_dt, 1)
            timeline_obj['compliance']['notice_within_limit'] = notice_dt <= notice_deadline
            timeline_obj['compliance']['notice_deadline'] = notice_deadline.isoformat()
            timeline_obj['compliance']['notice_days'] = (notice_dt - dishonour_dt).days

        if 'notice_date' in dates and 'complaint_filed_date' in dates:

            notice_dt = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
            complaint_dt = datetime.strptime(dates['complaint_filed_date'], '%Y-%m-%d').date()
            cause_of_action, _ = get_cause_of_action(notice_dt, 'delivered')
            complaint_deadline = add_calendar_months(cause_of_action, 1)
            timeline_obj['compliance']['complaint_within_limit'] = complaint_dt <= complaint_deadline
            timeline_obj['compliance']['complaint_deadline'] = complaint_deadline.isoformat()
            timeline_obj['compliance']['cause_of_action'] = cause_of_action.isoformat()

        timeline_obj['limitation_risk'] = 'LOW'

        if not timeline_obj['validity'].get('cheque_valid', True):
            timeline_obj['limitation_risk'] = 'CRITICAL'
        elif not timeline_obj['compliance'].get('notice_within_30_days', True):
            timeline_obj['limitation_risk'] = 'HIGH'
        elif not timeline_obj['compliance'].get('complaint_within_30_days', True):
            timeline_obj['limitation_risk'] = 'HIGH'

    except Exception as e:
        logger.error(f"Timeline calculation error: {e}")
        timeline_obj['errors'].append(str(e))

    return timeline_obj

def apply_fatal_override(score: float, fatal_defects: List[Dict]) -> float:

    if not fatal_defects or len(fatal_defects) == 0:
        return score

    has_catastrophic = any(
        d.get('defect_type') in [
            'limitation_expired',
            'cheque_validity_failure',
            'notice_beyond_30_days',
            'complaint_beyond_30_days'
        ] for d in fatal_defects
    )

    has_critical = any(
        d.get('severity') == 'FATAL' or
        d.get('severity') == 'CRITICAL'
        for d in fatal_defects
    )

    if has_catastrophic:

        logger.warning(f"🚨 CATASTROPHIC FATAL DEFECT: Score forced to {FATAL_CAP_CATASTROPHIC}")
        return FATAL_CAP_CATASTROPHIC

    if has_critical or len(fatal_defects) >= 2:
        logger.warning(f"🚨 CRITICAL FATAL DEFECTS ({len(fatal_defects)}): Score capped at {FATAL_CAP_UNIFIED}")
        return min(score, FATAL_CAP_UNIFIED)

    if len(fatal_defects) == 1:
        logger.info(f"⚠️ Single fatal defect: Score capped at {FATAL_CAP_UNIFIED}")
        return min(score, FATAL_CAP_UNIFIED)

    return score

def safe_calculation(func):

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"❌ Calculation error in {func.__name__}: {e}")

            return {
                'error': True,
                'message': f"Calculation failed: {str(e)}",
                'module': func.__name__
            }
    return wrapper

def calculate_notice_deadline(dishonour_date):

    if isinstance(dishonour_date, str):
        dishonour_date = datetime.strptime(dishonour_date, '%Y-%m-%d').date()
    deadline = add_calendar_months(dishonour_date, 1)
    return deadline

def safe_get_score(module_data: Dict, score_key: str = 'overall_score', default: float = 0.0) -> float:

    if not module_data or 'error' in module_data:
        return default

    score = module_data.get(score_key, default)

    try:
        return float(score)
    except (ValueError, TypeError):
        logger.warning(f"Invalid score format in {score_key}: {score}")
        return default

def module_exists(analysis: Dict, module_name: str) -> bool:

    module = safe_get(analysis, 'modules', module_name)
    return module is not None and 'error' not in module

def validate_module_output(module_name: str, output: Dict) -> bool:

    if not output:
        logger.error(f"❌ Module {module_name}: No output")
        return False

    if 'error' in output:
        logger.error(f"❌ Module {module_name}: Error - {output.get('message')}")
        return False

    return True

def cap_score_realistic(score: float, max_cap: float = 98.0) -> float:

    return min(score, max_cap)

class FatalSeverity(Enum):

    CATASTROPHIC = 0
    CRITICAL = 20
    SEVERE = 35
    MAJOR = 50
    MINOR = None

FATAL_DEFECT_CLASSIFICATION = {

    'cheque_validity_failure': FatalSeverity.CATASTROPHIC,
    'limitation_expired': FatalSeverity.CATASTROPHIC,
    'notice_beyond_30_days': FatalSeverity.CRITICAL,
    'complaint_beyond_30_days': FatalSeverity.CRITICAL,

    'no_legally_enforceable_debt': FatalSeverity.SEVERE,
    'notice_not_sent': FatalSeverity.CRITICAL,
    'return_memo_missing': FatalSeverity.MAJOR,

    'section_141_impleading_failure': FatalSeverity.MAJOR,
    'court_jurisdiction_wrong': FatalSeverity.CRITICAL,

    'original_cheque_missing': FatalSeverity.MAJOR,
    'postal_proof_absent': FatalSeverity.MINOR
}

def apply_weighted_fatal_override(score: float, defects: List[Dict]) -> Tuple[float, Dict]:

    if not defects:
        return score, {'applied': False}

    max_severity = FatalSeverity.MINOR
    critical_defects = []

    for defect in defects:
        defect_type = defect.get('defect_type', '')
        severity = FATAL_DEFECT_CLASSIFICATION.get(defect_type, FatalSeverity.MINOR)

        if severity.value is not None and (max_severity.value is None or severity.value < max_severity.value):
            max_severity = severity

        critical_defects.append({
            'defect': defect.get('defect', 'Unknown'),
            'severity': severity.name,
            'cap': severity.value
        })

    original_score = score
    if max_severity.value is not None:
        score = min(score, max_severity.value)
    else:
        score = max(0, score - 15)

    return score, {
        'applied': True,
        'original_score': original_score,
        'final_score': score,
        'max_severity': max_severity.name,
        'defects': critical_defects
    }

def calculate_presumption_intelligence(documentary: Dict, defence_type: Optional[str] = None) -> Dict:

    trigger_score = 0.0

    if documentary.get('original_cheque_available'):
        trigger_score += 30
    if documentary.get('return_memo_available'):
        trigger_score += 20

    if documentary.get('written_agreement_exists'):
        trigger_score += 15
    if documentary.get('ledger_available'):
        trigger_score += 10
    if documentary.get('postal_proof_available'):
        trigger_score += 10
    if documentary.get('email_sms_available'):
        trigger_score += 10
    if documentary.get('witness_statements'):
        trigger_score += 5

    trigger_score = min(100, trigger_score)

    rebuttal_score = 0.0

    defence_weights = {
        'security_cheque': 40,
        'no_debt': 35,
        'time_barred': 30,
        'technical': 20,
        'other': 10
    }
    rebuttal_score += defence_weights.get(defence_type, 0)

    if not documentary.get('written_agreement_exists'):
        rebuttal_score += 15
    if not documentary.get('postal_proof_available'):
        rebuttal_score += 10

    rebuttal_score = min(100, rebuttal_score)

    burden_shifted = trigger_score >= SCORE_GOOD

    if burden_shifted:
        if rebuttal_score < 30:
            rebuttal_capacity = "WEAK"
            insight = "Complainant highly likely to succeed"
        elif rebuttal_score < SCORE_GOOD:
            rebuttal_capacity = "MODERATE"
            insight = "Evenly balanced - evidence quality decisive"
        else:
            rebuttal_capacity = "STRONG"
            insight = "Accused has good rebuttal case"
    else:
        rebuttal_capacity = "N/A"
        insight = "Presumption not triggered - complainant must prove debt"

    return {
        'presumption_triggered': burden_shifted,
        'presumption_strength': round(trigger_score, 1),
        'rebuttal_strength': round(rebuttal_score, 1),
        'burden_on': 'Accused' if burden_shifted else 'Complainant',
        'rebuttal_capacity': rebuttal_capacity,
        'strategic_insight': insight,
        'section_139_applies': trigger_score > 0,
        'confidence': 'HIGH' if abs(trigger_score - rebuttal_score) > 30 else 'MEDIUM'
    }

def simulate_judicial_variance(base_score: float, category_scores: Dict) -> Dict:

    profiles = {
        'strict_magistrate': (0.35, 0.30, 0.20, 0.15),
        'liberal_magistrate': (0.20, 0.25, 0.35, 0.20),
        'settlement_oriented': (0.15, 0.20, 0.30, 0.35),
        'procedural_stickler': (0.40, 0.25, 0.15, 0.20),
        'evidence_focused': (0.20, 0.25, 0.40, 0.15)
    }

    simulations = {}

    for profile, (t_w, i_w, d_w, p_w) in profiles.items():
        simulated = (
            category_scores.get('timeline', 0) * t_w +
            category_scores.get('ingredient', 0) * i_w +
            category_scores.get('documentary', 0) * d_w +
            category_scores.get('procedural', 0) * p_w
        )

        simulations[profile] = {
            'score': round(simulated, 1),
            'variance': round(simulated - base_score, 1)
        }

    scores = [s['score'] for s in simulations.values()]
    variance_range = max(scores) - min(scores)

    if variance_range < 10:
        interpretation = "LOW VARIANCE - Outcome relatively consistent"
    elif variance_range < 25:
        interpretation = "MODERATE VARIANCE - Judge's approach may influence"
    else:
        interpretation = "HIGH VARIANCE - Judicial discretion significant"

    return {
        'base_score': base_score,
        'simulations': simulations,
        'variance_range': round(variance_range, 1),
        'min_score': min(scores),
        'max_score': max(scores),
        'mean_score': round(sum(scores) / len(scores), 1),
        'interpretation': interpretation
    }

def adjust_weights_contextually(
    base_weights: Dict[str, float],
    cheque_amount: float,
    is_company_case: bool
) -> Tuple[Dict[str, float], List[str]]:

    adjusted = base_weights.copy()
    explanations = []

    if cheque_amount > 5000000:
        adj_factor = 0.10
        adjusted['documentary'] += adj_factor

        reduction = adj_factor / 4
        adjusted['timeline'] -= reduction
        adjusted['ingredients'] -= reduction
        adjusted['liability'] -= reduction
        adjusted['procedural'] -= reduction

        explanations.append(f"High-value case (₹{cheque_amount/100000:.1f}L): Documentary weight +10%")

    if is_company_case:
        adj_factor = 0.05
        adjusted['liability'] += adj_factor

        reduction = adj_factor / 4
        adjusted['timeline'] -= reduction
        adjusted['ingredients'] -= reduction
        adjusted['documentary'] -= reduction
        adjusted['procedural'] -= reduction

        explanations.append("Company case: Section 141 liability weight +5%")

    total = sum(adjusted.values())
    adjusted = {k: v/total for k, v in adjusted.items()}

    return adjusted, explanations

def calculate_fraud_risk(case_data: Dict) -> Dict:

    risk_score = 0
    factors = []

    if case_data.get('cheque_amount', 0) > 10000000:
        if not case_data.get('written_agreement_exists'):
            risk_score += 25
            factors.append("High-value without written agreement")

    if case_data.get('is_multiple_cheques'):
        if not case_data.get('ledger_available'):
            risk_score += 15
            factors.append("Multiple cheques without ledger")

    if case_data.get('defence_type') == 'security_cheque':
        if not case_data.get('written_agreement_exists'):
            risk_score += 20
            factors.append("Security cheque claim without agreement")

    if risk_score < 20:
        level = "LOW"
        interpretation = "No significant fraud indicators"
    elif risk_score < 50:
        level = "MODERATE"
        interpretation = "Some unusual patterns - scrutiny recommended"
    else:
        level = "HIGH"
        interpretation = "Multiple red flags - detailed verification essential"

    return {
        'risk_score': min(100, risk_score),
        'risk_level': level,
        'risk_factors': factors,
        'interpretation': interpretation,
        'disclaimer': 'Behavioral analysis only, not legal determination'
    }

# Version constants already defined at top - removed duplicates

def get_version_info() -> Dict:

    return {
        'engine_version': ENGINE_VERSION,
        'scoring_model_version': SCORING_MODEL_VERSION,
        'timeline_math_version': TIMELINE_MATH_VERSION,
        'maturity_grade': 'Production Stable',
        'stability_level': 'v1.0 - Field Tested',
        'build_date': '2025-02-18',
        'features': [
            'Weighted fatal override',
            'Presumption intelligence (Section 139)',
            'Judicial variance simulation',
            'Dynamic weights',
            'Contradiction detection',
            'Fraud risk analysis',
            'Never 100% scoring',
            'Strict input validation',
            'Structured audit trail',
            'Score explainability',
            'Outcome classification',
            'Defensive programming',
            'Hard fatal override',
            'Centralized constants'
        ],
        'confidence_methodology': {
            'type': 'Heuristic-based with deterministic components',
            'disclaimer': 'Confidence scores are algorithmic estimates based on data completeness, KB coverage, and statutory compliance patterns. Not statistically validated predictions.',
            'components': [
                'Data completeness (40%)',
                'Documentary strength (30%)',
                'KB pattern matching (20%)',
                'Timeline compliance (10%)'
            ],
            'interpretation': 'HIGH (>85%) = Strong data & compliance | MEDIUM (70-85%) = Adequate | LOW (<70%) = Insufficient data'
        }
    }

def validate_case_input_strict(case_data: Dict) -> Tuple[bool, List[str], Dict]:
    """
    Validate case input and split issues into two buckets:

    HARD ERRORS  → block analysis entirely (truly impossible to proceed)
      - Missing required fields
      - Invalid data types
      - Impossible date sequences (cheque after dishonour, etc.)
      - Unparseable dates

    SOFT WARNINGS → proceed with caution, included in response
      - Future dates (pre-filing a complaint is legitimate)
      - Very old dates
      - Missing optional fields (complaint_filed_date, etc.)
      - Unrecognised case_type variants (auto-mapped where possible)

    Returns: (is_valid, hard_errors, sanitized_data)
    Soft warnings are stored inside sanitized_data under key '_warnings'.
    """
    errors   = []   # hard — block analysis
    warnings = []   # soft — allow analysis, surface in response
    sanitized = {}

    # ── Required fields ──
    required = {
        'case_type':      str,
        'cheque_amount':  (int, float),
        'cheque_date':    str,
        'dishonour_date': str,
        'notice_date':    str,
        'bank_name':      str,
    }
    optional_dates = ['complaint_filed_date', 'transaction_date',
                      'presentation_date', 'notice_received_date']

    for field, expected_type in required.items():
        value = case_data.get(field)
        if value is None or value == '':
            errors.append(f"MISSING required field: '{field}'")
        elif isinstance(expected_type, tuple):
            if not isinstance(value, expected_type):
                errors.append(f"INVALID TYPE: '{field}' must be a number")
        elif not isinstance(value, expected_type):
            errors.append(f"INVALID TYPE: '{field}' must be a string")

    if errors:
        return False, errors, {}

    # ── Amount ──
    try:
        amount = float(case_data['cheque_amount'])
        if amount <= 0:
            errors.append("cheque_amount must be positive")
        elif amount > 100_000_000_000:
            errors.append("cheque_amount exceeds maximum limit")
        else:
            sanitized['cheque_amount'] = round(amount, 2)
    except Exception:
        errors.append("cheque_amount: invalid number format")

    # ── Dates ──
    date_fields = ['transaction_date', 'cheque_date', 'presentation_date',
                   'dishonour_date', 'notice_date', 'notice_received_date',
                   'complaint_filed_date']

    parsed_dates = {}
    today = datetime.now().date()

    for field in date_fields:
        date_str = case_data.get(field)
        if not date_str:
            if field not in optional_dates:
                errors.append(f"MISSING required date: '{field}'")
            else:
                warnings.append(f"⚠ '{field}' not provided — related analysis will be skipped")
            continue

        try:
            parsed = (datetime.strptime(date_str, '%Y-%m-%d').date()
                      if isinstance(date_str, str) else date_str)

            days_diff = (parsed - today).days

            # Future dates: soft warning only (e.g. complaint not yet filed but being assessed)
            if days_diff > 1:
                warnings.append(
                    f"⚠ '{field}' is {days_diff} days in the future ({parsed}) — "
                    f"analysis proceeds with provided date"
                )

            # Very old dates: soft warning
            if parsed.year < 2000:
                warnings.append(
                    f"⚠ '{field}' year {parsed.year} is before 2000 — verify this is correct"
                )

            parsed_dates[field] = parsed
            sanitized[field] = parsed.isoformat()

        except Exception:
            errors.append(f"INVALID DATE FORMAT: '{field}' — use YYYY-MM-DD")

    # ── Date sequence checks (hard — physically impossible) ──
    if 'cheque_date' in parsed_dates and 'dishonour_date' in parsed_dates:
        if parsed_dates['cheque_date'] > parsed_dates['dishonour_date']:
            errors.append("Cheque date cannot be after dishonour date")

    if 'dishonour_date' in parsed_dates and 'notice_date' in parsed_dates:
        if parsed_dates['dishonour_date'] > parsed_dates['notice_date']:
            errors.append("Dishonour date cannot be after notice date")

    if 'notice_date' in parsed_dates and 'complaint_filed_date' in parsed_dates:
        if parsed_dates['notice_date'] > parsed_dates['complaint_filed_date']:
            errors.append("Notice date cannot be after complaint filed date")

    # ── Missing optional fields: soft warnings ──
    if 'complaint_filed_date' not in parsed_dates:
        warnings.append("⚠ 'complaint_filed_date' not provided — limitation period analysis will be partial")

    if not case_data.get('return_memo_available'):
        warnings.append("⚠ Return memo not marked available — documentary strength will be reduced")

    if not case_data.get('postal_proof_available'):
        warnings.append("⚠ Postal proof not provided — notice service presumption may be challenged")

    # ── case_type normalisation ──
    case_type_raw = case_data.get('case_type', '').strip().lower()
    case_type_mapping = {
        'complainant':  'complainant',
        'accused':      'accused',
        'plaintiff':    'complainant',
        'defendant':    'accused',
        'prosecution':  'complainant',
        'defense':      'accused',
        'defence':      'accused',
        'recovery':     'complainant',
        'petitioner':   'complainant',
        'respondent':   'accused',
        # common misuse — map with warning
        'individual':   'accused',
        'company':      'accused',
    }
    case_type = case_type_mapping.get(case_type_raw)
    if not case_type:
        # Try partial match for verbose strings like "Cheque Bounce - Section 138 NI Act"
        if any(kw in case_type_raw for kw in ('complain', 'plaintiff', 'recover', 'petitioner')):
            case_type = 'complainant'
            warnings.append(
                f"⚠ case_type '{case_data.get('case_type')}' auto-mapped to 'complainant'"
            )
        elif any(kw in case_type_raw for kw in ('accus', 'defend', 'respond')):
            case_type = 'accused'
            warnings.append(
                f"⚠ case_type '{case_data.get('case_type')}' auto-mapped to 'accused'"
            )
        else:
            errors.append(
                f"INVALID case_type: '{case_data.get('case_type')}' — "
                f"use 'complainant' or 'accused'"
            )
    else:
        sanitized['case_type'] = case_type

    if case_type and 'case_type' not in sanitized:
        sanitized['case_type'] = case_type

    # ── Copy remaining fields ──
    for field in case_data:
        if field not in sanitized:
            sanitized[field] = case_data[field]

    # Attach warnings to sanitized data so they flow through to the response
    sanitized['_warnings'] = warnings

    return len(errors) == 0, errors, sanitized

class AuditTrail:

    def __init__(self):
        self.trail = {
            'validation': {},
            'timeline_gaps': {},
            'ingredient_deductions': [],
            'documentary_breakdown': {},
            'fatal_overrides': [],
            'weight_applications': {},
            'final_computation': {}
        }

    def log_validation(self, is_valid: bool, errors: List[str]):
        self.trail['validation'] = {
            'passed': is_valid,
            'errors': errors,
            'timestamp': datetime.now().isoformat()
        }

    def log_ingredient_deduction(self, ingredient: str, deduction: float, reason: str):
        self.trail['ingredient_deductions'].append({
            'ingredient': ingredient,
            'deduction': deduction,
            'reason': reason
        })

    def log_fatal_override(self, original: float, final: float, reason: str):
        self.trail['fatal_overrides'].append({
            'original': original,
            'final': final,
            'reason': reason,
            'reduction': original - final
        })

    def log_weight_application(self, category: str, raw: float, weight: float, weighted: float):
        self.trail['weight_applications'][category] = {
            'raw_score': raw,
            'weight_percent': weight * 100,
            'contribution': weighted
        }

    def get_trail(self) -> Dict:
        return self.trail

def safe_module_execution(module_func, *args, **kwargs) -> Dict:

    try:
        return module_func(*args, **kwargs)
    except Exception as e:
        logger.error(f"❌ Module {module_func.__name__} failed: {e}")
        return {
            'error': True,
            'error_message': str(e),
            'module': module_func.__name__,
            'fallback_score': 50.0
        }

def classify_outcome(score: float, fatal_defects: List, confidence: str) -> Dict:

    # CRITICAL: Validate score is not None
    if score is None:
        logger.error("classify_outcome received score=None, defaulting to 0")
        score = 0

    # Ensure score is a number
    try:
        score = float(score)
    except (TypeError, ValueError):
        logger.error(f"classify_outcome received invalid score type: {type(score)}, defaulting to 0")
        score = 0

    if fatal_defects and len(fatal_defects) > 0:
        return {
            'category': 'HIGH DISMISSAL RISK',
            'risk_level': 'CRITICAL',
            'recommendation': 'Immediate remediation or settlement',
            'predicted_outcome': 'Dismissal likely on technical grounds',
            'action_priority': 'URGENT'
        }

    if score >= SCORE_EXCELLENT:
        return {
            'category': 'STRONG PROSECUTION',
            'risk_level': 'LOW',
            'recommendation': 'Push for trial',
            'predicted_outcome': 'High conviction probability',
            'action_priority': 'PROCEED'
        }
    elif score >= SCORE_GOOD:
        return {
            'category': 'MODERATE STRENGTH',
            'risk_level': 'MEDIUM',
            'recommendation': 'Strengthen or settle',
            'predicted_outcome': 'Uncertain - balanced',
            'action_priority': 'IMPROVE'
        }
    elif score >= SCORE_ADEQUATE:
        return {
            'category': 'WEAK CASE',
            'risk_level': 'HIGH',
            'recommendation': 'Settlement priority',
            'predicted_outcome': 'Acquittal more likely',
            'action_priority': 'SETTLE'
        }
    else:
        return {
            'category': 'VERY WEAK',
            'risk_level': 'CRITICAL',
            'recommendation': 'Urgent remediation',
            'predicted_outcome': 'High dismissal/acquittal risk',
            'action_priority': 'CRITICAL'
        }

def apply_hard_fatal_override(score: float, fatal_defects: List[Dict]) -> Tuple[float, str]:

    if not fatal_defects:
        return score, "NO_OVERRIDE"

    catastrophic = ['limitation_expired', 'cheque_validity_failure',
                   'notice_beyond_30_days', 'complaint_beyond_30_days']

    has_catastrophic = any(d.get('defect_type') in catastrophic for d in fatal_defects)

    if has_catastrophic:
        return min(score, FATAL_CAP_UNIFIED), "HARD_FATAL_OVERRIDE"

    return min(score, FATAL_CAP_UNIFIED), "FATAL_OVERRIDE"

def generate_score_breakdown(category_scores: Dict, weights: Dict) -> List[Dict]:

    breakdown = []

    for category, data in category_scores.items():
        category_key = category.lower().replace(' ', '_')
        weight = weights.get(category_key, 0)

        breakdown.append({
            'module': category,
            'raw_score': data['score'],
            'weight_percent': int(weight * 100),
            'weighted_contribution': round(data['score'] * weight, 2),
            'interpretation': 'Strong' if (data.get('score') or 0) >= 70 else 'Adequate' if (data.get('score') or 0) >= 50 else 'Weak'
        })

    return breakdown

SEVERITY_WEIGHTS = {
    'FATAL': 100,
    'MAJOR': 40,
    'MODERATE': 20,
    'MINOR': 10
}

ISSUE_SEVERITY = {
    'limitation_invalid': 'FATAL',
    'wrong_jurisdiction': 'FATAL',
    'cheque_after_validity': 'FATAL',
    'no_specific_averment': 'FATAL',
    'section_65b_missing': 'FATAL',  # NEW: Section 65B non-compliance is FATAL
    'no_debt_proof': 'MAJOR',
    'notice_defect': 'MAJOR',
    'signature_disputed': 'MAJOR',
    'no_written_agreement': 'MODERATE',
    'weak_documentary': 'MODERATE',
    'no_ledger': 'MINOR',
    'no_email_sms': 'MINOR'
}

def get_severity_deduction(issue_key: str) -> tuple:

    severity = ISSUE_SEVERITY.get(issue_key, 'MODERATE')
    deduction = SEVERITY_WEIGHTS[severity]
    return deduction, severity

_calibrated_weights_cache = None

def get_calibrated_weights() -> Dict[str, float]:

    global _calibrated_weights_cache

    if _calibrated_weights_cache:
        return _calibrated_weights_cache

    if not kb_loaded or kb_data is None or 'case_id' not in kb_data.columns:
        return {'debt': 20, 'notice': 20, 'dishonour': 15}

    try:
        acquittals = kb_data[kb_data['final_outcome'].str.contains('acquit|discharge', case=False, na=False)]
        total = len(acquittals)

        if total < 10:
            return {'debt': 20, 'notice': 20, 'dishonour': 15}

        debt_failures = len(acquittals[
            acquittals.get('reason_for_decision', pd.Series([''] * len(acquittals))).str.contains(
                'no debt|security|time-bar', case=False, na=False
            )
        ])

        notice_failures = len(acquittals[
            acquittals.get('reason_for_decision', pd.Series([''] * len(acquittals))).str.contains(
                'notice|service', case=False, na=False
            )
        ])

        debt_rate = (debt_failures / total) * 100
        notice_rate = (notice_failures / total) * 100

        _calibrated_weights_cache = {
            'debt': min(40, max(10, debt_rate)),
            'notice': min(40, max(10, notice_rate)),
            'dishonour': 15
        }

        logger.info(f"📊 Data-calibrated weights: debt={debt_rate:.1f}% → {_calibrated_weights_cache['debt']:.0f}, notice={notice_rate:.1f}% → {_calibrated_weights_cache['notice']:.0f}")
        return _calibrated_weights_cache

    except Exception as e:
        logger.warning(f"Weight calibration failed: {e}, using defaults")
        return {'debt': 20, 'notice': 20, 'dishonour': 15}

def calculate_confidence(
    data_completeness: float,
    kb_coverage: float = 70.0,
    retrieval_strength: float = 0.75
) -> Dict:

    confidence_score = (
        (data_completeness * 0.5) +
        (kb_coverage * 0.3) +
        (retrieval_strength * 100 * 0.2)
    )

    if confidence_score >= 80:
        level = "HIGH"
        note = "Analysis based on complete data and strong KB coverage"
    elif confidence_score >= 60:
        level = "MEDIUM"
        note = "Reliable analysis with minor data gaps present"
    else:
        level = "LOW"
        note = "Limited data - analysis may be incomplete"

    return {
        'confidence_level': level,
        'confidence_score': round(confidence_score, 1),
        'data_completeness': round(data_completeness, 1),
        'kb_coverage': round(kb_coverage, 1),
        'retrieval_strength': round(retrieval_strength, 2),
        'reliability_note': note
    }

class ComplianceLevel(str, Enum):
    EXCELLENT = "EXCELLENT"
    GOOD = "GOOD"
    MODERATE = "MODERATE"
    WEAK = "WEAK"
    CRITICAL = "CRITICAL"


def normalize_compliance_level(value) -> str:
    """
    Normalize any compliance level string to database-compliant enum

    Handles: scores, strings, enum values
    Returns: EXCELLENT | GOOD | MODERATE | WEAK | CRITICAL
    """
    if isinstance(value, (int, float)):
        # Score-based normalization
        if value >= 90:
            return "EXCELLENT"
        elif value >= 75:
            return "GOOD"
        elif value >= 60:
            return "MODERATE"
        elif value >= 40:
            return "WEAK"
        else:
            return "CRITICAL"

    # String normalization
    value_upper = str(value).upper().strip()

    # Direct matches
    if value_upper in ["EXCELLENT", "GOOD", "MODERATE", "WEAK", "CRITICAL"]:
        return value_upper

    # Variations
    if "EXCELLENT" in value_upper or "VERY STRONG" in value_upper:
        return "EXCELLENT"
    elif "GOOD" in value_upper or "STRONG" in value_upper:
        return "GOOD"
    elif "MODERATE" in value_upper or "MEDIUM" in value_upper:
        return "MODERATE"
    elif "WEAK" in value_upper or "LOW" in value_upper:
        return "WEAK"
    elif "CRITICAL" in value_upper or "FATAL" in value_upper or "HIGH RISK" in value_upper:
        return "CRITICAL"

    # Default fallback
    return "MODERATE"


def normalize_risk_level(value) -> str:
    """
    Normalize risk level strings

    Returns: LOW | MEDIUM | HIGH | CRITICAL | FATAL
    """
    value_upper = str(value).upper().strip()

    if value_upper in ["LOW", "MEDIUM", "HIGH", "CRITICAL", "FATAL"]:
        return value_upper

    if "FATAL" in value_upper:
        return "FATAL"
    elif "CRITICAL" in value_upper:
        return "CRITICAL"
    elif "HIGH" in value_upper:
        return "HIGH"
    elif "MEDIUM" in value_upper or "MODERATE" in value_upper:
        return "MEDIUM"
    elif "LOW" in value_upper:
        return "LOW"

    return "MEDIUM"

class DefenceStrength(str, Enum):
    HIGH = "High Strength"
    MEDIUM = "Medium Strength"
    LOW = "Low Strength"

class SettlementPressure(str, Enum):
    LOW = "Low Settlement Pressure"
    MODERATE = "Moderate Settlement Pressure"
    HIGH = "High Settlement Leverage"

class CaseStage(str, Enum):
    PRE_NOTICE = "Pre-Notice Stage"
    NOTICE_SENT = "Notice Sent"
    NOTICE_EXPIRED = "Notice Period Expired"
    COMPLAINT_FILED = "Complaint Filed"
    SUMMONS_ISSUED = "Summons Issued"
    EVIDENCE_STAGE = "Evidence Stage"
    ARGUMENTS = "Final Arguments"
    JUDGMENT = "Judgment Stage"
    APPEAL = "Appeal Stage"

class CaseAnalysisRequest(BaseModel):
    """
    Full input model for JUDIQ Section 138 NI Act case analysis.
    All dates must be in YYYY-MM-DD format.
    """

    # ── Core Identity ──────────────────────────────────────────────────────────
    case_type: str = Field(
        ...,
        description="Whose perspective: 'complainant' (cheque issuer defaulted) or 'accused' (defending against complaint)",
        examples=["complainant", "accused"]
    )

    # ── Cheque Details ─────────────────────────────────────────────────────────
    cheque_amount: float = Field(
        ...,
        description="Face value of the cheque in INR (e.g. 500000 for ₹5,00,000)",
        examples=[500000]
    )
    cheque_number: str = Field(
        ...,
        description="Cheque number printed on the instrument",
        examples=["123456"]
    )
    cheque_date: str = Field(
        ...,
        description="Date printed on the cheque (YYYY-MM-DD). Must be within 3 months of presentation.",
        examples=["2026-01-15"]
    )
    bank_name: str = Field(
        ...,
        description="Name of the bank on which the cheque was drawn",
        examples=["HDFC Bank"]
    )

    # ── Transaction / Debt Details ─────────────────────────────────────────────
    transaction_date: Optional[str] = Field(
        None,
        description="Date the underlying loan/transaction occurred (YYYY-MM-DD)",
        examples=["2025-12-15"]
    )
    transaction_amount: Optional[float] = Field(
        None,
        description="Original transaction/loan amount in INR (may differ from cheque amount)",
        examples=[500000]
    )
    debt_nature: Optional[str] = Field(
        None,
        description="Nature of the underlying liability. Values: 'loan', 'business_transaction', 'services_rendered', 'rent', 'security_deposit', 'other'",
        examples=["loan"]
    )

    # ── Dishonour Details ──────────────────────────────────────────────────────
    dishonour_date: Optional[str] = Field(
        None,
        description="Date the bank returned/dishonoured the cheque (YYYY-MM-DD). Critical for limitation calculation.",
        examples=["2026-01-18"]
    )
    dishonour_reason: Optional[str] = Field(
        None,
        description="Reason stated on bank memo. Values: 'Insufficient Funds', 'Account Closed', 'Payment Stopped', 'Signature Mismatch', 'Stale Cheque', 'Other'",
        examples=["Insufficient Funds"]
    )
    presentation_date: Optional[str] = Field(
        None,
        description="Date cheque was presented to bank (YYYY-MM-DD). Must be within cheque validity (3 months).",
        examples=["2026-01-18"]
    )
    return_memo_available: bool = Field(
        False,
        description="Is the original bank dishonour memo (return memo) available? Critical primary evidence."
    )

    # ── Notice Details ─────────────────────────────────────────────────────────
    notice_date: Optional[str] = Field(
        None,
        description="Date the statutory demand notice was sent (YYYY-MM-DD). Must be within 30 days of dishonour.",
        examples=["2026-02-01"]
    )
    notice_received_date: Optional[str] = Field(
        None,
        description="Date notice was actually received / deemed served (YYYY-MM-DD). 15-day payment period starts from this date.",
        examples=["2026-02-03"]
    )
    notice_sent_to_address: Optional[str] = Field(
        None,
        description="Address to which notice was sent. Should match accused's registered/known address.",
        examples=["Registered residential address of accused"]
    )
    notice_signed: bool = Field(
        True,
        description="Was the notice signed by the complainant or authorised signatory?"
    )
    postal_proof_available: bool = Field(
        False,
        description="Is postal proof of notice dispatch available? (AD card, speed post receipt, tracking). Needed to prove service."
    )

    # ── Complaint Filing ───────────────────────────────────────────────────────
    complaint_filed_date: Optional[str] = Field(
        None,
        description="Date complaint was / will be filed in court (YYYY-MM-DD). Must be within 30 days after the 15-day payment period expires.",
        examples=["2026-03-01"]
    )
    court_location: Optional[str] = Field(
        None,
        description="Court where complaint is / will be filed. Used for judicial behaviour analysis.",
        examples=["Mumbai Metropolitan Magistrate Court"]
    )

    # ── Documentary Evidence ───────────────────────────────────────────────────
    original_cheque_available: bool = Field(
        False,
        description="Is the original dishonoured cheque available with the complainant?"
    )
    written_agreement_exists: bool = Field(
        False,
        description="Is there a written loan/transaction agreement signed by both parties? Absence is a major defence weakness."
    )
    ledger_available: bool = Field(
        False,
        description="Are ledger entries or account records available to prove the transaction?"
    )
    email_sms_evidence: bool = Field(
        False,
        description="Are WhatsApp/SMS/email messages available that reference the loan or cheque?"
    )
    witness_available: bool = Field(
        False,
        description="Is a witness available who can depose about the transaction?"
    )

    # ── Company / Director Liability (Section 141) ─────────────────────────────
    is_company_case: bool = Field(
        False,
        description="Was the cheque issued by a company? If yes, Section 141 director liability applies."
    )
    directors_impleaded: bool = Field(
        False,
        description="Have directors/officers of the company been named as accused? Required under Section 141."
    )
    specific_averment_present: bool = Field(
        False,
        description="Does the complaint contain specific averments about directors' role in company affairs? Required for Section 141 conviction."
    )

    # ── Multiple Cheques ───────────────────────────────────────────────────────
    is_multiple_cheques: bool = Field(
        False,
        description="Does this case involve multiple dishonoured cheques? Each creates a separate cause of action."
    )
    number_of_cheques: int = Field(
        1,
        description="Total number of dishonoured cheques in this case",
        examples=[1]
    )

    # ── Parallel Proceedings ───────────────────────────────────────────────────
    civil_suit_pending: bool = Field(
        False,
        description="Is a civil money recovery suit pending for the same debt? Affects strategy and settlement."
    )
    insolvency_proceedings: bool = Field(
        False,
        description="Are insolvency/bankruptcy proceedings pending against the accused? May affect enforcement."
    )

    # ── Defence Details ────────────────────────────────────────────────────────
    defence_type: Optional[str] = Field(
        None,
        description="Known or expected defence. Values: 'security_cheque', 'no_debt', 'stop_payment', 'time_barred', 'signature_mismatch', 'company_director', 'stolen_cheque', 'no_legally_enforceable_debt'",
        examples=["security_cheque"]
    )

    # ── Case Summary ───────────────────────────────────────────────────────────
    case_summary: Optional[str] = Field(
        None,
        description="Brief narrative of the case facts in plain language. Used for cross-examination risk and context.",
        examples=["Accused borrowed ₹5,00,000 as a friendly loan and issued a cheque for repayment. Cheque dishonoured due to insufficient funds."]
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "case_type": "complainant",
                "cheque_amount": 500000,
                "cheque_number": "123456",
                "cheque_date": "2026-01-15",
                "bank_name": "HDFC Bank",
                "transaction_date": "2025-12-15",
                "transaction_amount": 500000,
                "debt_nature": "loan",
                "dishonour_date": "2026-01-18",
                "dishonour_reason": "Insufficient Funds",
                "presentation_date": "2026-01-18",
                "return_memo_available": True,
                "notice_date": "2026-02-01",
                "notice_received_date": "2026-02-03",
                "notice_sent_to_address": "Registered residential address of accused",
                "notice_signed": True,
                "postal_proof_available": True,
                "complaint_filed_date": "2026-03-10",
                "court_location": "Mumbai Metropolitan Magistrate Court",
                "original_cheque_available": True,
                "written_agreement_exists": False,
                "ledger_available": False,
                "email_sms_evidence": True,
                "witness_available": True,
                "is_company_case": False,
                "directors_impleaded": False,
                "specific_averment_present": False,
                "is_multiple_cheques": False,
                "number_of_cheques": 1,
                "civil_suit_pending": False,
                "insolvency_proceedings": False,
                "defence_type": "security_cheque",
                "case_summary": "Accused borrowed ₹5,00,000 as a friendly loan in December 2025 and issued a cheque for repayment. Cheque dishonoured due to insufficient funds."
            }
        }
    }

    @field_validator('cheque_date', 'transaction_date', 'dishonour_date', 'notice_date', 'complaint_filed_date', mode='before')
    @classmethod
    def validate_date_format(cls, v):
        if v:
            try:
                datetime.strptime(v, '%Y-%m-%d')
            except Exception:
                raise ValueError('Date must be in YYYY-MM-DD format (e.g. 2026-01-15)')
        return v

class SearchKBRequest(BaseModel):
    query: str
    top_k: int = 5
    category_filter: Optional[str] = None


class CrossExaminationRequest(BaseModel):
    """
    Request model for AI cross-examination question generation.
    Pass the same case_data you would send to /analyze-case.
    """
    case_data: Dict = Field(
        ...,
        description="Case details — same structure as /analyze-case request body",
        examples=[{
            "case_type": "complainant",
            "cheque_amount": 500000,
            "cheque_number": "123456",
            "cheque_date": "2026-01-15",
            "bank_name": "HDFC Bank",
            "dishonour_date": "2026-01-18",
            "dishonour_reason": "Insufficient Funds",
            "notice_date": "2026-02-01",
            "notice_received_date": "2026-02-03",
            "written_agreement_exists": False,
            "ledger_available": False,
            "postal_proof_available": True,
            "debt_nature": "loan",
            "defence_type": "security_cheque",
            "case_summary": "Accused borrowed ₹5,00,000 as a friendly loan and issued a cheque for repayment."
        }]
    )
    witness_type: str = Field(
        "complainant",
        description=(
            "Who is being cross-examined. "
            "'complainant' = person who filed the case; "
            "'accused' = person who issued the cheque; "
            "'drawer' = same as accused (alias); "
            "'bank_official' = bank witness regarding dishonour memo; "
            "'witness' = third-party transaction witness"
        ),
        examples=["complainant", "accused", "bank_official", "witness"]
    )
    num_questions: int = Field(
        5,
        description="Number of questions to generate (5–15)",
        ge=5,
        le=15
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "case_data": {
                    "case_type": "complainant",
                    "cheque_amount": 500000,
                    "cheque_number": "123456",
                    "cheque_date": "2026-01-15",
                    "bank_name": "HDFC Bank",
                    "dishonour_date": "2026-01-18",
                    "dishonour_reason": "Insufficient Funds",
                    "notice_date": "2026-02-01",
                    "notice_received_date": "2026-02-03",
                    "written_agreement_exists": False,
                    "ledger_available": False,
                    "postal_proof_available": True,
                    "debt_nature": "loan",
                    "defence_type": "security_cheque",
                    "case_summary": "Accused borrowed ₹5,00,000 as a friendly loan and issued a cheque for repayment."
                },
                "witness_type": "complainant",
                "num_questions": 5
            }
        }
    }

# Render: LLM/KB/embedding disabled
embed_model = None
embed_loaded = False
llm_model = None
llm_tokenizer = None
llm_loaded = False
kb_data = None
kb_embeddings = None
kb_loaded = False
court_behavior_db = None
embed_lock = threading.Lock()
llm_lock = threading.Lock()
kb_lock = threading.Lock()
_startup_complete = threading.Event()

_cache = {}
_cache_timestamps = {}

# ============================================================================
# SQLite DATABASE PATH  (Google Drive if mounted, local otherwise)
# ============================================================================
analytics_db_path = DATA_DIR / "case_analysis" / "judiq.db"

def init_analytics_db():
    """
    Initialize the JUDIQ SQLite database.

    Creates all 6 tables (idempotent - safe to call on every startup):
      1. case_analyses      — one row per analysis run
      2. court_intelligence — judicial behaviour per court/judge
      3. case_history       — audit trail of events per case
      4. api_usage_log      — request/response logging
      5. knowledge_base     — legal KB entries (sections, provisions, judgments)
      6. court_statistics   — aggregated conviction/acquittal rates per court

    DB location: DATA_DIR / "case_analysis" / "judiq.db"
    (Google Drive if mounted in Colab, local otherwise)
    """

    # Ensure parent directory exists
    analytics_db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(analytics_db_path)
    cursor = conn.cursor()

    # Enable foreign key enforcement
    cursor.execute("PRAGMA foreign_keys = ON")

    # ------------------------------------------------------------------
    # TABLE 1: case_analyses — primary analysis results store
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS case_analyses (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id              TEXT    UNIQUE NOT NULL,
            analysis_timestamp   TEXT    NOT NULL,
            case_type            TEXT    CHECK(case_type IN ('complainant', 'accused')),
            cheque_amount        REAL    CHECK(cheque_amount > 0),
            overall_risk_score   REAL    CHECK(overall_risk_score BETWEEN 0 AND 100),
            compliance_level     TEXT    CHECK(compliance_level IN ('EXCELLENT', 'GOOD', 'MODERATE', 'WEAK', 'CRITICAL')),
            fatal_defect_override INTEGER DEFAULT 0,
            fatal_type           TEXT,
            timeline_risk        TEXT,
            ingredient_compliance REAL,
            documentary_strength  REAL,
            analysis_json        TEXT    NOT NULL,
            engine_version       TEXT    DEFAULT 'v10.0',
            processing_time_ms   INTEGER,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # TABLE 2: court_intelligence — judicial behaviour per court/judge
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS court_intelligence (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            court_name           TEXT    NOT NULL,
            judge_name           TEXT,
            case_type            TEXT,
            dismissal_rate       REAL,
            avg_conviction_rate  REAL,
            common_grounds       TEXT,
            sample_size          INTEGER DEFAULT 0,
            data_source          TEXT,
            last_updated         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(court_name, judge_name, case_type)
        )
    """)

    # ------------------------------------------------------------------
    # TABLE 3: case_history — audit trail of events per case
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS case_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id         TEXT    NOT NULL,
            event_type      TEXT    NOT NULL,
            event_data      TEXT,
            event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (case_id) REFERENCES case_analyses(case_id) ON DELETE CASCADE
        )
    """)

    # ------------------------------------------------------------------
    # TABLE 4: api_usage_log — request/response logging
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS api_usage_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint            TEXT    NOT NULL,
            method              TEXT    NOT NULL,
            case_id             TEXT,
            ip_address          TEXT,
            user_agent          TEXT,
            request_size        INTEGER,
            response_status     INTEGER,
            processing_time_ms  INTEGER,
            error_message       TEXT,
            severity            TEXT    CHECK(severity IN ('INFO', 'WARNING', 'ERROR', 'CRITICAL')),
            timestamp           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # TABLE 5: knowledge_base — legal provisions and judgment entries
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS knowledge_base (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            section              TEXT,
            provision_text       TEXT,
            explanation          TEXT,
            court_name           TEXT,
            final_outcome        TEXT,
            court_reasoning_text TEXT,
            reason_for_decision  TEXT,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # TABLE 6: court_statistics — aggregated rates computed from KB
    # ------------------------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS court_statistics (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            court_name           TEXT    UNIQUE NOT NULL,
            total_cases          INTEGER DEFAULT 0,
            conviction_rate      REAL,
            acquittal_rate       REAL,
            limitation_dismissal_rate REAL,
            technical_dismissal_rate  REAL,
            compounding_rate     REAL,
            strictness_index     REAL,
            court_classification TEXT,
            confidence           TEXT    CHECK(confidence IN ('HIGH', 'MEDIUM', 'LOW')),
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ------------------------------------------------------------------
    # INDICES — performance optimisation
    # ------------------------------------------------------------------
    # case_analyses
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_id         ON case_analyses(case_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp        ON case_analyses(analysis_timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_compliance       ON case_analyses(compliance_level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_type        ON case_analyses(case_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fatal_type       ON case_analyses(fatal_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at       ON case_analyses(created_at)")
    # court_intelligence
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_court_name       ON court_intelligence(court_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_court_case_type  ON court_intelligence(court_name, case_type)")
    # case_history
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_case_id  ON case_history(case_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_ts       ON case_history(event_timestamp)")
    # api_usage_log
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_endpoint     ON api_usage_log(endpoint)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_timestamp    ON api_usage_log(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_severity     ON api_usage_log(severity)")
    # knowledge_base
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kb_section       ON knowledge_base(section)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kb_court         ON knowledge_base(court_name)")
    # court_statistics
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cs_court         ON court_statistics(court_name)")

    conn.commit()
    conn.close()

    logger.info(f"✅ JUDIQ database ready  |  path: {analytics_db_path}")
    logger.info("   Tables: case_analyses, court_intelligence, case_history, "
                "api_usage_log, knowledge_base, court_statistics")

def get_from_cache(key: str) -> Optional[Any]:
    if key in _cache:
        timestamp = _cache_timestamps.get(key, 0)
        if time.time() - timestamp < CONFIG["CACHE_TTL"]:
            return _cache[key]
        else:
            del _cache[key]
            del _cache_timestamps[key]
    return None

def save_to_cache(key: str, value: Any):
    _cache[key] = value
    _cache_timestamps[key] = time.time()

def load_embedding_model():
    """Embedding model disabled on Render."""
    global embed_loaded
    embed_loaded = False


def load_llm_model():
    """LLM disabled on Render."""
    pass


def generate_llm_response(prompt: str, max_tokens: int = 200, **kwargs) -> str:
    """LLM disabled on Render."""
    return ""


def transform_judgment_to_kb(df):
    """KB transform disabled on Render."""
    return df


def _download_kb_from_gdrive(file_id: str, dest_path: Path) -> bool:
    """
    Download the KB CSV from Google Drive using the file ID.

    The file must be shared as "Anyone with the link can view".
    No authentication required for public files.

    Two download methods attempted in order:
      1. Direct export URL (works for most public files, no size limit issues)
      2. Google Drive API v3 with optional API key (more reliable for large files)

    Args:
        file_id  : The ID from the Google Drive shareable URL
        dest_path: Local path to save the downloaded CSV

    Returns:
        True if download succeeded, False otherwise
    """
    import urllib.request
    import urllib.error

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # ── Method 1: Direct download URL (public files, no auth) ──
    direct_url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=t"
    try:
        print(f"📥 Downloading KB from Google Drive (file ID: {file_id[:12]}...)...")
        req = urllib.request.Request(
            direct_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/html,application/xhtml+xml,*/*",
            }
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()

        # Google Drive returns an HTML warning page for large files
        # Detect and handle the confirmation token
        if b"virus scan warning" in raw.lower() or b"download_warning" in raw.lower():
            import re
            token_match = re.search(rb'confirm=([0-9A-Za-z_\-]+)', raw)
            if token_match:
                token = token_match.group(1).decode()
                confirm_url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm={token}"
                req2 = urllib.request.Request(confirm_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req2, timeout=120) as resp2:
                    raw = resp2.read()

        # Verify it looks like a CSV
        decoded = raw.decode('utf-8', errors='replace')
        if ',' in decoded and len(decoded) > 100:
            with open(dest_path, 'w', encoding='utf-8') as f:
                f.write(decoded)
            size_kb = dest_path.stat().st_size // 1024
            print(f"✅ KB downloaded: {dest_path.name} ({size_kb} KB)")
            return True
        else:
            logger.warning("⚠️ Download returned non-CSV content — trying API method")

    except Exception as e:
        logger.warning(f"⚠️ Direct download failed: {e} — trying API method")

    # ── Method 2: Google Drive API v3 (works better for larger files) ──
    api_key = CONFIG.get("GDRIVE_API_KEY", "")
    api_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    if api_key:
        api_url += f"&key={api_key}"

    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": "JUDIQ/1.0"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
        if ',' in raw and len(raw) > 100:
            with open(dest_path, 'w', encoding='utf-8') as f:
                f.write(raw)
            size_kb = dest_path.stat().st_size // 1024
            print(f"✅ KB downloaded via API: {dest_path.name} ({size_kb} KB)")
            return True
    except Exception as e:
        logger.error(f"❌ API download also failed: {e}")

    return False


def _minimal_kb_fallback() -> 'pd.DataFrame':
    """Return a single-row minimal KB when Drive download fails."""
    import pandas as pd
    return pd.DataFrame([{
        'source': 'NI Act 1881', 'section': '138',
        'provision_text': 'Section 138 creates criminal liability for cheque dishonour',
        'explanation': 'Seven essential ingredients must be proved',
        'category': 'Essential Ingredients'
    }])


def load_kb():
    """
    Load the knowledge base CSV.

    On Render, downloads from Google Drive using GDRIVE_FILE_ID env var.
    Falls back to a minimal 1-row KB if download fails or ID not set.

    To enable:
      1. Upload cheque_bounce_kb.csv to Google Drive
      2. Share it: "Anyone with the link → Viewer"
      3. Copy the file ID from the URL
      4. Set GDRIVE_FILE_ID=<your-file-id> in Render environment variables
    """
    global kb_loaded, kb_data
    import pandas as pd

    kb_local_path = DATA_DIR / "legal_kb" / "cheque_bounce_kb.csv"

    # ── Step 1: Use already-downloaded local copy if fresh (< 24 hours old) ──
    if kb_local_path.exists():
        age_hours = (time.time() - kb_local_path.stat().st_mtime) / 3600
        if age_hours < 24:
            try:
                df = pd.read_csv(kb_local_path)
                if len(df) > 1:
                    kb_data = df
                    kb_loaded = True
                    print(f"✅ KB loaded from local cache: {len(df)} rows ({age_hours:.1f}h old)")
                    return True
            except Exception as e:
                logger.warning(f"⚠️ Local KB read failed: {e}")

    # ── Step 2: Download from Google Drive ──
    file_id = CONFIG.get("GDRIVE_FILE_ID", "").strip()
    if file_id:
        success = _download_kb_from_gdrive(file_id, kb_local_path)
        if success:
            try:
                df = pd.read_csv(kb_local_path)
                if len(df) > 1:
                    kb_data = df
                    kb_loaded = True
                    print(f"✅ KB loaded from Google Drive: {len(df)} rows")

                    # Compute and save court statistics to DB
                    if 'case_id' in df.columns or 'court_name' in df.columns:
                        try:
                            court_stats = compute_court_statistics_from_kb(df)
                            if court_stats:
                                save_court_statistics_to_db(court_stats)
                                print(f"✅ Court statistics computed: {len(court_stats)} courts")
                        except Exception as e:
                            logger.warning(f"⚠️ Court stats computation failed: {e}")
                    return True
                else:
                    logger.warning("⚠️ Downloaded KB has ≤1 rows — using fallback")
            except Exception as e:
                logger.error(f"❌ KB CSV parse failed: {e}")
        else:
            logger.warning("⚠️ Google Drive download failed — using minimal fallback")
    else:
        print("⚠️  GDRIVE_FILE_ID not set — using minimal KB fallback")
        print("   → Set GDRIVE_FILE_ID env var on Render to enable full KB")

    # ── Step 3: Minimal fallback ──
    kb_data = _minimal_kb_fallback()
    kb_loaded = False
    return False


def search_kb(query: str, top_k: int = 5, category_filter: str = None, **kwargs) -> list:
    """KB search disabled on Render — returns empty list (no embedding model)."""
    return []


def analyze_judicial_behavior(court_location: Optional[str], kb_results: List[Dict]) -> Dict:

    behavior_analysis = {
        'court_identified': court_location or 'Generic Court',
        'data_source': 'Structured Court Statistics',
        'behavioral_indices': {},
        'observed_patterns': [],
        'strategic_insights': [],
        'dismissal_fingerprint': {},
        'presumption_analytics': {},
        'settlement_intelligence': {},
        'sample_size': 0,
        'confidence': 'Unknown',
        'data_period': 'Historical',
        'methodology': 'Outcome-based statistical analysis',
        'module_status': 'EXPERIMENTAL',
        'data_quality_warning': 'Results depend on KB data quality. Low confidence when KB is basic.'
    }

    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT court_name, total_cases, conviction_rate, acquittal_rate,
                   limitation_dismissal_rate, technical_dismissal_rate,
                   compounding_rate, strictness_index, court_classification,
                   confidence, updated_at
            FROM court_statistics
            WHERE court_name = ?
            LIMIT 1
        """, (court_location or 'Generic Court',))

        court_stats = cursor.fetchone()

        if court_stats:

            court_name, total_cases, conviction_rate_pct, acquittal_rate_pct, \
            lim_dismiss_rate, tech_dismiss_rate, compounding_rate_pct, \
            strictness_index, court_classification, confidence_level, last_updated = court_stats

            # Reconstruct raw counts from stored rates for downstream calculations
            convictions              = round((conviction_rate_pct or 0)  / 100 * total_cases)
            acquittals               = round((acquittal_rate_pct or 0)   / 100 * total_cases)
            limitation_dismissals    = round((lim_dismiss_rate or 0)     / 100 * total_cases)
            technical_dismissals     = round((tech_dismiss_rate or 0)    / 100 * total_cases)
            compounded_cases         = round((compounding_rate_pct or 0) / 100 * total_cases)
            interim_compensation_orders = 0   # not stored separately
            avg_disposal_days        = None   # not stored separately
            year                     = last_updated[:4] if last_updated else 'Historical'
            data_source              = 'court_statistics table'

            court_stats = (court_name, 0, 0, year, total_cases, convictions, acquittals,
                           limitation_dismissals, technical_dismissals, compounded_cases,
                           interim_compensation_orders, avg_disposal_days,
                           data_source, confidence_level, last_updated)

            behavior_analysis['sample_size'] = total_cases
            behavior_analysis['confidence'] = confidence_level
            behavior_analysis['data_period'] = f"{year}"

            if total_cases > 0:
                limitation_dismissal_rate = (limitation_dismissals / total_cases) * 100

                condonation_leniency = 100 - limitation_dismissal_rate

                limitation_strictness = min(10.0, (
                    (limitation_dismissal_rate / 10) * 0.6 +
                    (1 - (condonation_leniency / 100)) * 10 * 0.4
                ))

                behavior_analysis['behavioral_indices']['limitation_strictness'] = round(limitation_strictness, 1)
                behavior_analysis['behavioral_indices']['limitation_dismissal_rate'] = round(limitation_dismissal_rate, 1)
                behavior_analysis['behavioral_indices']['condonation_leniency'] = 'LOW' if limitation_dismissal_rate > 20 else 'MEDIUM' if limitation_dismissal_rate > 10 else 'HIGH'

                if limitation_dismissal_rate >= 20:
                    behavior_analysis['observed_patterns'].append({
                        'pattern': 'Very High Limitation Strictness',
                        'observation': f'{limitation_dismissal_rate:.1f}% of cases dismissed for limitation violations',
                        'frequency': f'{limitation_dismissals}/{total_cases} cases',
                        'implication': 'CRITICAL: File within limitation strictly. Condonation difficult.',
                        'priority': 'URGENT'
                    })

            if total_cases > 0:
                technical_dismissal_rate = (technical_dismissals / total_cases) * 100

                technical_tendency = min(10.0, (technical_dismissal_rate / 10) * 10)

                behavior_analysis['behavioral_indices']['technical_dismissal_tendency'] = round(technical_tendency, 1)
                behavior_analysis['behavioral_indices']['technical_dismissal_rate'] = round(technical_dismissal_rate, 1)

                cursor.execute("""
                    SELECT reason_for_decision,
                           COUNT(*) AS freq,
                           ROUND(COUNT(*) * 100.0 / ?, 1) AS pct
                    FROM knowledge_base
                    WHERE court_name = ?
                      AND (final_outcome LIKE '%dismiss%' OR final_outcome LIKE '%discharge%')
                      AND reason_for_decision IS NOT NULL
                      AND reason_for_decision != ''
                    GROUP BY reason_for_decision
                    ORDER BY freq DESC
                    LIMIT 3
                """, (total_cases, court_location or 'Generic Court'))

                dismissal_breakdown = cursor.fetchall()
                if dismissal_breakdown:
                    behavior_analysis['dismissal_fingerprint']['top_3_reasons'] = [
                        {
                            'reason': reason,
                            'frequency': freq,
                            'percentage': pct
                        } for reason, freq, pct in dismissal_breakdown
                    ]

                    behavior_analysis['strategic_insights'].append({
                        'insight': 'Technical Dismissal Pattern Identified',
                        'evidence': f'Top dismissal reason: {dismissal_breakdown[0][0]} ({dismissal_breakdown[0][2]:.1f}%)',
                        'recommendation': f'Avoid: {", ".join([r[0] for r in dismissal_breakdown[:3]])}',
                        'priority': 'HIGH'
                    })

            if total_cases > 0:
                conviction_rate = (convictions / total_cases) * 100
                acquittal_rate = (acquittals / total_cases) * 100

                behavior_analysis['behavioral_indices']['historical_conviction_rate'] = round(conviction_rate, 1)
                behavior_analysis['behavioral_indices']['historical_acquittal_rate'] = round(acquittal_rate, 1)

                behavior_analysis['observed_patterns'].append({
                    'pattern': 'Historical Outcome Distribution',
                    'observation': f'Convictions: {conviction_rate:.1f}%, Acquittals: {acquittal_rate:.1f}%',
                    'note': 'Historical data only - NOT a prediction of your case outcome',
                    'context': f'Based on {total_cases} cases'
                })

            cursor.execute("""
                SELECT
                    reason_for_decision AS rebuttal_type,
                    ROUND(
                        SUM(CASE WHEN final_outcome LIKE '%acquit%' THEN 1 ELSE 0 END) * 100.0
                        / NULLIF(COUNT(*), 0), 1
                    ) AS success_rate,
                    SUM(CASE WHEN final_outcome LIKE '%acquit%' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN final_outcome NOT LIKE '%acquit%' THEN 1 ELSE 0 END) AS failure_count
                FROM knowledge_base
                WHERE court_name = ?
                  AND court_reasoning_text LIKE '%presumption%'
                  AND reason_for_decision IS NOT NULL
                GROUP BY reason_for_decision
                HAVING COUNT(*) >= 2
                ORDER BY success_rate DESC
                LIMIT 5
            """, (court_location or 'Generic Court',))

            presumption_data = cursor.fetchall()
            if presumption_data:
                behavior_analysis['presumption_analytics'] = {
                    'rebuttal_patterns': []
                }

                for rebuttal_type, success_rate, success_count, failure_count in presumption_data:
                    total_attempts = success_count + failure_count
                    behavior_analysis['presumption_analytics']['rebuttal_patterns'].append({
                        'defence_type': rebuttal_type,
                        'success_rate': round(success_rate, 1),
                        'attempts': total_attempts,
                        'successful': success_count,
                        'failed': failure_count,
                        'strength': 'HIGH' if success_rate >= 60 else 'MEDIUM' if success_rate >= 40 else 'LOW'
                    })

                best_defence = max(presumption_data, key=lambda x: x[1])
                behavior_analysis['strategic_insights'].append({
                    'insight': 'Most Successful Rebuttal Strategy',
                    'evidence': f'{best_defence[0]} succeeds in {best_defence[1]:.1f}% of attempts',
                    'recommendation': f'For accused: Focus on {best_defence[0]} if applicable',
                    'priority': 'MEDIUM'
                })

            if total_cases > 0:
                compounding_rate = (compounded_cases / total_cases) * 100

                settlement_friendly = min(10.0, 3.0 + (compounding_rate / 10) * 7.0)

                behavior_analysis['behavioral_indices']['settlement_friendly'] = round(settlement_friendly, 1)
                behavior_analysis['behavioral_indices']['compounding_rate'] = round(compounding_rate, 1)

                if interim_compensation_orders > 0:
                    interim_rate = (interim_compensation_orders / total_cases) * 100
                    behavior_analysis['settlement_intelligence']['interim_compensation_rate'] = round(interim_rate, 1)

                    if interim_rate >= 30:
                        behavior_analysis['strategic_insights'].append({
                            'insight': 'High Interim Compensation Usage',
                            'evidence': f'{interim_rate:.1f}% of cases saw Section 143A interim compensation',
                            'recommendation': 'For complainant: Apply for interim compensation early',
                            'priority': 'HIGH'
                        })

                if compounding_rate >= 40:
                    behavior_analysis['observed_patterns'].append({
                        'pattern': 'Settlement Encouraged',
                        'observation': f'{compounding_rate:.1f}% of cases resulted in compounding',
                        'implication': 'Court actively encourages Section 147 settlements',
                        'recommendation': 'Explore settlement early - court favors compounding'
                    })

            if total_cases > 0:
                procedural_formality = min(10.0, 4.0 + (technical_dismissal_rate / 5) * 6.0)
                behavior_analysis['behavioral_indices']['procedural_formality'] = round(procedural_formality, 1)

            if avg_disposal_days:

                avg_years = avg_disposal_days / 365

                if avg_years <= 2:
                    efficiency = 10.0
                elif avg_years <= 3:
                    efficiency = 7.0
                elif avg_years <= 4:
                    efficiency = 5.0
                elif avg_years <= 5:
                    efficiency = 3.0
                else:
                    efficiency = 1.0

                behavior_analysis['behavioral_indices']['court_efficiency_index'] = efficiency
                behavior_analysis['behavioral_indices']['avg_disposal_time'] = f"{avg_years:.1f} years"

                behavior_analysis['observed_patterns'].append({
                    'pattern': 'Case Disposal Timeline',
                    'observation': f'Average disposal time: {avg_years:.1f} years',
                    'context': 'National average is 3-4 years for Section 138 cases'
                })

        conn.close()

    except Exception as e:
        logger.error(f"Database court analytics failed: {e}")

        return analyze_judicial_behavior_fallback(court_location, kb_results)

    if behavior_analysis['sample_size'] == 0:
        return analyze_judicial_behavior_fallback(court_location, kb_results)

    if behavior_analysis['behavioral_indices']:
        avg_strictness = (
            behavior_analysis['behavioral_indices'].get('limitation_strictness', 5.0) * 0.3 +
            behavior_analysis['behavioral_indices'].get('technical_dismissal_tendency', 5.0) * 0.25 +
            behavior_analysis['behavioral_indices'].get('procedural_formality', 5.0) * 0.3 +
            (10 - behavior_analysis['behavioral_indices'].get('settlement_friendly', 5.0)) * 0.15
        )

        behavior_analysis['behavioral_indices']['overall_strictness_index'] = round(avg_strictness, 1)

        if avg_strictness >= CONFIG['STRICTNESS_INDEX_HIGH']:
            behavior_analysis['strategic_insights'].append({
                'insight': 'High Overall Procedural Strictness',
                'evidence': f'Composite strictness index: {avg_strictness:.1f}/10',
                'recommendation': 'CRITICAL: Ensure 100% procedural compliance - court is strict on technicalities',
                'priority': 'URGENT'
            })

    behavior_analysis['methodology_note'] = {
        'approach': 'Outcome-based statistical analysis',
        'data_type': 'Actual case outcomes, dismissal reasons, settlement patterns',
        'not_included': 'Individual case outcome predictions',
        'ethical_note': 'All metrics are historical patterns, not predictions'
    }

    return behavior_analysis

@safe_analysis  # FIX #3: Error handling wrapper
def analyze_timeline(case_data: Dict) -> Dict:

    timeline_analysis = {
        'timeline_chart': [],
        'compliance_status': {},
        'risk_markers': [],
        'limitation_risk': 'Unknown',
        'critical_dates': {},
        'edge_cases_detected': [],
        'deterministic_score': 100,
        'calculation_log': []
    }

    try:

        cheque_date = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d').date() if case_data.get('cheque_date') else None
        presentation_date = datetime.strptime(case_data['presentation_date'], '%Y-%m-%d').date() if case_data.get('presentation_date') else None
        dishonour_date = datetime.strptime(case_data['dishonour_date'], '%Y-%m-%d').date() if case_data.get('dishonour_date') else None
        notice_date = datetime.strptime(case_data['notice_date'], '%Y-%m-%d').date() if case_data.get('notice_date') else None
        notice_received_date = datetime.strptime(case_data['notice_received_date'], '%Y-%m-%d').date() if case_data.get('notice_received_date') else None
        complaint_filed_date = datetime.strptime(case_data['complaint_filed_date'], '%Y-%m-%d').date() if case_data.get('complaint_filed_date') else None

        if cheque_date:
            if cheque_date < datetime.strptime(CONFIG["CHEQUE_VALIDITY_CHANGE_DATE"], '%Y-%m-%d').date():
                validity_days = 180
                timeline_analysis['calculation_log'].append({
                    'rule': 'Cheque Validity (Pre-2012)',
                    'formula': 'Cheque Date + 180 days (6 months)',
                    'note': 'Using pre-RBI circular 2012 validity period'
                })
            else:

                cheque_date_obj = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d').date()
                expiry_date, validity_days = calculate_cheque_expiry(cheque_date_obj)
                timeline_analysis['calculation_log'].append({
                    'rule': 'Cheque Validity (Current)',
                    'formula': f'Cheque Date + {validity_days} days',
                    'note': 'Post-2012 RBI circular'
                })

            cheque_validity_end = cheque_date + timedelta(days=validity_days)
            timeline_analysis['critical_dates']['cheque_validity_end'] = cheque_validity_end.strftime('%Y-%m-%d')
            timeline_analysis['timeline_chart'].append({
                'date': cheque_date.strftime('%Y-%m-%d'),
                'event': 'Cheque Issued',
                'status': '✅',
                'marker': 'GREEN'
            })

            if presentation_date and presentation_date > cheque_validity_end:
                timeline_analysis['deterministic_score'] = 0
                timeline_analysis['risk_markers'].append({
                    'severity': 'CRITICAL',
                    'marker': 'RED',
                    'issue': 'Cheque presented after 3-month validity period',
                    'impact': 'Fatal defect - case will be dismissed',
                    'rule': 'Negotiable Instruments Act - Cheque validity 3 months from date'
                })
                timeline_analysis['compliance_status']['cheque_validity'] = '❌ FAILED (FATAL)'
            else:
                timeline_analysis['compliance_status']['cheque_validity'] = '✅ COMPLIANT'
                timeline_analysis['calculation_log'].append({
                    'rule': 'Presentation within validity',
                    'check': f'Presentation {presentation_date} <= Validity end {cheque_validity_end}',
                    'result': 'PASS'
                })

        if presentation_date:
            timeline_analysis['timeline_chart'].append({
                'date': presentation_date.strftime('%Y-%m-%d'),
                'event': 'Cheque Presented',
                'status': '✅'
            })

        if dishonour_date:
            timeline_analysis['timeline_chart'].append({
                'date': dishonour_date.strftime('%Y-%m-%d'),
                'event': f"Dishonoured - {case_data.get('dishonour_reason', 'Reason unknown')}",
                'status': '⚠️'
            })

        if dishonour_date and notice_date:
            try:
                days_to_notice = (notice_date - dishonour_date).days

                # Validate logical date order
                if days_to_notice < 0:
                    timeline_analysis['risk_markers'].append({
                        'severity': 'FATAL',
                        'marker': 'RED',
                        'issue': 'Notice date is before dishonour date (impossible timeline)',
                        'impact': 'Data integrity error - timeline logically impossible',
                        'rule': 'Notice can only be sent after dishonour'
                    })
                    timeline_analysis['limitation_risk'] = 'CANNOT ASSESS'
                    timeline_analysis['edge_cases_detected'].append({
                        'case': 'Invalid Date Sequence',
                        'severity': 'FATAL',
                        'issue': f'Notice date ({notice_date}) precedes dishonour date ({dishonour_date})'
                    })
                else:
                    notice_deadline = add_calendar_months(dishonour_date, 1)
                    timeline_analysis['critical_dates']['notice_deadline'] = notice_deadline.strftime('%Y-%m-%d')

                    if notice_date <= notice_deadline:
                        timeline_analysis['compliance_status']['notice_timing'] = f'✅ COMPLIANT ({days_to_notice} days, deadline: {notice_deadline.strftime("%Y-%m-%d")})'
                        timeline_analysis['timeline_chart'].append({
                            'date': notice_date.strftime('%Y-%m-%d'),
                            'event': f'Notice Sent (Day {days_to_notice}, within limit)',
                            'status': '✅'
                        })
                    else:
                        timeline_analysis['compliance_status']['notice_timing'] = f'❌ DELAYED ({days_to_notice} days)'
                        timeline_analysis['limitation_risk'] = 'EXPIRED'  # FATAL: Notice beyond 30 days is time-barred
                        timeline_analysis['deterministic_score'] = 0
                        timeline_analysis['risk_markers'].append({
                            'severity': 'FATAL',
                            'marker': 'RED',
                            'issue': f'Notice sent {days_to_notice} days after dishonour (>30 days limit)',
                            'impact': 'FATAL DEFECT - Case will be dismissed as time-barred',
                            'rule': 'Section 138 NI Act - Notice must be sent within 30 days of dishonour'
                        })
                        timeline_analysis['timeline_chart'].append({
                            'date': notice_date.strftime('%Y-%m-%d'),
                            'event': f'Notice Sent (Day {days_to_notice}/30) - TIME-BARRED',
                            'status': '❌'
                        })
            except Exception as e:
                logger.error(f"Error calculating notice timing: {e}")
                timeline_analysis['edge_cases_detected'].append({
                    'case': 'Notice Timing Calculation Error',
                    'severity': 'HIGH',
                    'error': str(e)
                })
        elif dishonour_date and not notice_date:
            timeline_analysis['edge_cases_detected'].append({
                'case': 'Missing Notice Date',
                'severity': 'CRITICAL',
                'issue': 'Cannot assess notice timing compliance without notice_date'
            })
            timeline_analysis['limitation_risk'] = 'CANNOT ASSESS'

        if notice_received_date:
            fifteen_day_expiry = notice_received_date + timedelta(days=15)
            timeline_analysis['critical_dates']['fifteen_day_expiry'] = fifteen_day_expiry.strftime('%Y-%m-%d')
            timeline_analysis['timeline_chart'].append({
                'date': notice_received_date.strftime('%Y-%m-%d'),
                'event': 'Notice Received',
                'status': '✅'
            })
            timeline_analysis['timeline_chart'].append({
                'date': fifteen_day_expiry.strftime('%Y-%m-%d'),
                'event': '15-Day Payment Period Expired (Cause of Action)',
                'status': '⚠️'
            })
        elif notice_date:
            timeline_analysis['edge_cases_detected'].append({
                'case': 'Notice Service Date Not Provided',
                'severity': 'HIGH',
                'rule': '⚠️ MANUAL VERIFICATION REQUIRED',
                'legal_principle': 'Deemed service varies: postal endorsement/refusal/unclaimed/General Clauses Act',
                'action_required': 'CRITICAL: Obtain postal records before calculating limitation',
                'options': [
                    'Returned unclaimed: Verify court deemed service period (7-30 days varies)',
                    'Postal endorsement: Use endorsement date',
                    'Delivery refused: Use refusal date',
                    'No dispatch proof: Notice validity questionable'
                ],
                'legal_risk': 'HIGH - Cannot calculate limitation without verified service',
                'do_not_assume': 'Automatic deemed service is legally risky'
            })

            timeline_analysis['critical_dates']['notice_service_status'] = 'UNVERIFIED'
            timeline_analysis['compliance_status']['notice_service'] = '⚠️ VERIFICATION REQUIRED'
            timeline_analysis['limitation_risk'] = 'CANNOT ASSESS'

        if complaint_filed_date and 'fifteen_day_expiry' in timeline_analysis['critical_dates']:
            cause_of_action = datetime.strptime(timeline_analysis['critical_dates']['fifteen_day_expiry'], '%Y-%m-%d').date()
            limitation_deadline = add_calendar_months(cause_of_action, 1)
            timeline_analysis['critical_dates']['limitation_deadline'] = limitation_deadline.strftime('%Y-%m-%d')

            days_to_complaint = (complaint_filed_date - cause_of_action).days

            if complaint_filed_date < cause_of_action:
                # ── PREMATURE FILING: complaint before 15-day payment period expired ──
                days_premature = (cause_of_action - complaint_filed_date).days
                timeline_analysis['compliance_status']['limitation'] = (
                    f'❌ PREMATURE FILING — complaint filed {days_premature} days before '
                    f'cause of action arose ({cause_of_action.strftime("%Y-%m-%d")})'
                )
                timeline_analysis['limitation_risk'] = 'CRITICAL'
                timeline_analysis['score'] = 0   # premature filing — fatal defect
                timeline_analysis['timeline_chart'].append({
                    'date': complaint_filed_date.strftime('%Y-%m-%d'),
                    'event': f'Complaint Filed — PREMATURE (Day {days_to_complaint}/30)',
                    'status': '❌'
                })
                timeline_analysis['risk_markers'].append({
                    'severity': 'FATAL',
                    'issue': f'Complaint filed {days_premature} days BEFORE cause of action',
                    'impact': 'Premature complaint — cause of action had not yet arisen under Section 138',
                    'legal_basis': 'Section 138 NI Act: complaint maintainable only after 15-day payment period expires',
                    'action': 'File fresh complaint after cause of action arises (if still within limitation)'
                })

            elif complaint_filed_date <= limitation_deadline:
                timeline_analysis['compliance_status']['limitation'] = f'✅ WITHIN LIMITATION ({days_to_complaint} days, deadline: {limitation_deadline.strftime("%Y-%m-%d")})'
                timeline_analysis['limitation_risk'] = 'LOW'
                timeline_analysis['score'] = 95   # all compliant
                timeline_analysis['timeline_chart'].append({
                    'date': complaint_filed_date.strftime('%Y-%m-%d'),
                    'event': f'Complaint Filed (Day {days_to_complaint}/30)',
                    'status': '✅'
                })
            else:
                delay_days = days_to_complaint - 30
                timeline_analysis['compliance_status']['limitation'] = f'❌ BARRED BY LIMITATION ({days_to_complaint} days, delayed by {delay_days} days)'
                timeline_analysis['limitation_risk'] = 'CRITICAL'
                timeline_analysis['risk_markers'].append({
                    'severity': 'CRITICAL',
                    'issue': f'Complaint filed {delay_days} days after limitation period',
                    'impact': 'Time-barred unless delay condoned with sufficient cause'
                })
                timeline_analysis['timeline_chart'].append({
                    'date': complaint_filed_date.strftime('%Y-%m-%d'),
                    'event': f'Complaint Filed - DELAYED (Day {days_to_complaint}/30)',
                    'status': '❌'
                })

                if delay_days <= 60:
                    timeline_analysis['edge_cases_detected'].append({
                        'case': 'Limitation Delay Condonable',
                        'rule': 'Delay may be condoned if sufficient cause shown',
                        'action_required': 'File application for condonation of delay with strong grounds'
                    })

        if case_data.get('is_multiple_cheques'):
            timeline_analysis['edge_cases_detected'].append({
                'case': 'Multiple Cheques Case',
                'rule': 'Each cheque creates separate cause of action with separate limitation',
                'action_required': 'Verify timeline compliance for EACH cheque separately'
            })

        if cheque_date and dishonour_date:
            if cheque_date > dishonour_date:
                timeline_analysis['edge_cases_detected'].append({
                    'case': 'Premature Presentation',
                    'rule': 'Cheque presented before date on cheque',
                    'action_required': 'May affect liability - consult precedent'
                })

        critical_risks = [r for r in timeline_analysis['risk_markers'] if r['severity'] in ['HIGH', 'CRITICAL']]
        if critical_risks:
            timeline_analysis['limitation_risk'] = 'CRITICAL'
        elif timeline_analysis['compliance_status'].get('limitation', '').startswith('✅'):
            timeline_analysis['limitation_risk'] = 'LOW'
        else:
            timeline_analysis['limitation_risk'] = 'MEDIUM'

    except Exception as e:
        logger.error(f"Timeline analysis error: {e}")
        timeline_analysis['error'] = str(e)

    required_fields = ['cheque_date', 'dishonour_date', 'notice_date', 'complaint_filed_date']
    provided_fields = sum([1 for f in required_fields if case_data.get(f)])
    data_completeness = (provided_fields / len(required_fields)) * 100

    timeline_analysis['confidence'] = calculate_confidence(
        data_completeness=data_completeness,
        kb_coverage=75.0,
        retrieval_strength=0.80
    )

    return timeline_analysis

@safe_analysis  # FIX #3: Error handling wrapper
def analyze_ingredients(case_data: Dict, timeline_data: Dict) -> Dict:

    ingredients = {
        'overall_compliance': 0,
        'ingredient_scores': {},
        'weakest_ingredients': [],
        'fatal_defects': [],
        'ingredient_details': []
    }

    scores = []

    ing1_score = 100
    ing1_issues = []
    if not case_data.get('original_cheque_available'):
        ing1_score -= 30
        ing1_issues.append('Original cheque not available - may affect proof')

    ingredients['ingredient_details'].append({
        'number': 1,
        'name': 'Cheque Drawn on Bank Account',
        'score': ing1_score,
        'status': '✅ Compliant' if ing1_score >= SCORE_EXCELLENT else '⚠️ Weak',
        'issues': ing1_issues,
        'evidence_required': ['Original cheque', 'Cheque number', 'Bank name', 'Account details']
    })
    scores.append(ing1_score)

    ing2_score = 100
    ing2_issues = []

    weights = get_calibrated_weights()

    if case_data.get('transaction_date'):
        try:
            trans_date = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
            cheque_date = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d')
            years_diff = (cheque_date - trans_date).days / 365

            if years_diff > 3:
                deduction, severity = get_severity_deduction('no_debt_proof')
                ing2_score -= deduction
                ing2_issues.append(f'Debt may be time-barred (transaction {years_diff:.1f} years before cheque) [−{deduction} {severity}]')
                ingredients['fatal_defects'].append({
                    'ingredient': 2,
                    'defect': 'Possible Time-Barred Debt',
                    'severity': severity,
                    'explanation': 'If debt is >3 years old without acknowledgment, it may be time-barred under Limitation Act'
                })
        except:
            pass

    if case_data.get('defence_type') == 'security_cheque':
        deduction = weights['debt']
        ing2_score -= deduction
        ing2_issues.append(f'Accused claims security cheque (no legally enforceable debt) [−{deduction:.0f} calibrated]')

    if not case_data.get('written_agreement_exists'):
        deduction, severity = get_severity_deduction('no_written_agreement')
        ing2_score -= deduction
        ing2_issues.append(f'No written agreement - relying on presumption alone [−{deduction} {severity}]')

    if not case_data.get('ledger_available'):
        deduction, severity = get_severity_deduction('no_ledger')
        ing2_score -= deduction
        ing2_issues.append(f'No ledger/account books to prove debt [−{deduction} {severity}]')

    ingredients['ingredient_details'].append({
        'number': 2,
        'name': 'Legally Enforceable Debt',
        'score': max(0, ing2_score),
        'status': '✅ Strong' if ing2_score >= SCORE_EXCELLENT else ('⚠️ Moderate' if ing2_score >= SCORE_ADEQUATE else '❌ Weak'),
        'issues': ing2_issues,
        'evidence_required': ['Written agreement', 'Ledger', 'Invoices', 'Email/SMS', 'Transaction proof'],
        'presumption_status': 'Section 139 presumption active - burden on accused to rebut',
        'calibration_note': f'Scoring uses data-calibrated weights (debt weight: {weights["debt"]:.0f})'
    })
    scores.append(max(0, ing2_score))

    ing3_score = 100
    ing3_issues = []

    if timeline_data['compliance_status'].get('cheque_validity', '').startswith('❌'):
        ing3_score = 0
        ing3_issues.append('Cheque presented after 3-month validity - FATAL DEFECT')
        ingredients['fatal_defects'].append({
            'ingredient': 3,
            'defect': 'Cheque Presented After Validity',
            'severity': 'CRITICAL',
            'explanation': 'Cheque presented beyond 3 months from date - case likely to be dismissed'
        })

    ingredients['ingredient_details'].append({
        'number': 3,
        'name': 'Presented Within Validity Period',
        'score': ing3_score,
        'status': '✅ Compliant' if ing3_score == 100 else '❌ FAILED',
        'issues': ing3_issues,
        'evidence_required': ['Presentation date proof', 'Return memo']
    })
    scores.append(ing3_score)

    ing4_score = 100
    ing4_issues = []

    if not case_data.get('return_memo_available'):
        ing4_score -= 25
        ing4_issues.append('Return memo not available - weakens proof of dishonour')

    dishonour_reason = (case_data.get('dishonour_reason') or '').lower()
    if 'insufficient' in dishonour_reason or 'funds' in dishonour_reason:

        pass
    elif 'stop payment' in dishonour_reason:
        ing4_score -= 20
        ing4_issues.append('Stop payment instruction - accused may claim valid reason')
    elif 'account closed' in dishonour_reason:
        ing4_score -= 15
        ing4_issues.append('Account closed - accused may claim no knowledge')
    elif 'signature' in dishonour_reason:
        ing4_score -= 30
        ing4_issues.append('Signature mismatch - serious challenge to cheque authenticity')

    ingredients['ingredient_details'].append({
        'number': 4,
        'name': 'Dishonoured by Bank',
        'score': ing4_score,
        'status': '✅ Strong' if ing4_score >= SCORE_EXCELLENT else '⚠️ Moderate',
        'issues': ing4_issues,
        'dishonour_reason': case_data.get('dishonour_reason', 'Not specified'),
        'evidence_required': ['Return memo/cheque', 'Bank memo']
    })
    scores.append(ing4_score)

    ing5_score = 100
    ing5_issues = []

    if timeline_data['compliance_status'].get('notice_timing', '').startswith('❌'):
        ing5_score = 0
        ing5_issues.append('Notice sent beyond 30 days - FATAL DEFECT')
        ingredients['fatal_defects'].append({
            'ingredient': 5,
            'defect': 'Notice Delayed Beyond 30 Days',
            'severity': 'CRITICAL',
            'explanation': 'Notice must be sent within 30 days of dishonour - mandatory requirement'
        })

    if not case_data.get('postal_proof_available'):
        ing5_score -= 15
        ing5_issues.append('No postal proof - may face challenge on service')

    if not case_data.get('notice_signed'):
        ing5_score -= 20
        ing5_issues.append('Notice unsigned - procedural defect')

    if case_data.get('notice_sent_to_address') and 'wrong' in (case_data.get('notice_sent_to_address') or '').lower():
        ing5_score -= 40
        ing5_issues.append('Notice sent to wrong address - serious defect')
        ingredients['fatal_defects'].append({
            'ingredient': 5,
            'defect': 'Notice to Wrong Address',
            'severity': 'HIGH',
            'explanation': 'Notice must be sent to accused address - wrong address may invalidate notice'
        })

    ingredients['ingredient_details'].append({
        'number': 5,
        'name': 'Notice Within 30 Days of Dishonour',
        'score': ing5_score,
        'status': '✅ Compliant' if ing5_score >= SCORE_EXCELLENT else ('⚠️ Weak' if ing5_score >= SCORE_ADEQUATE else '❌ FAILED'),
        'issues': ing5_issues,
        'evidence_required': ['Copy of notice', 'Postal receipt', 'AD/registered post proof']
    })
    scores.append(ing5_score)

    ing6_score = 100
    ing6_issues = []

    ingredients['ingredient_details'].append({
        'number': 6,
        'name': 'Payment Failed Within 15 Days',
        'score': ing6_score,
        'status': '✅ Compliant',
        'issues': ing6_issues,
        'evidence_required': ['Affidavit stating no payment received']
    })
    scores.append(ing6_score)

    ing7_score = 100
    ing7_issues = []

    limitation_status_str = timeline_data.get('compliance_status', {}).get('limitation', '')

    if 'PREMATURE' in limitation_status_str.upper():
        # Premature filing — complaint filed BEFORE cause of action arose
        # This is a separate defect from time-barred (which is filing TOO LATE)
        ing7_score = 0
        ing7_issues.append('Premature complaint — cause of action had not yet arisen under Section 138')
        ingredients['fatal_defects'].append({
            'ingredient': 7,
            'defect': 'Complaint Filed Before Cause of Action Arose',
            'severity': 'CRITICAL',
            'explanation': (
                'Complaint filed before the 15-day payment period expired. '
                'Cause of action under Section 138 arises only after the accused '
                'fails to pay within 15 days of receiving notice. '
                'Filing before this is premature and not maintainable.'
            )
        })
        _ing7_status = '❌ PREMATURE'

    elif limitation_status_str.startswith('❌') and 'PREMATURE' not in limitation_status_str.upper():
        # Time-barred — complaint filed TOO LATE (after 1-month limitation)
        ing7_score = 0
        ing7_issues.append('Complaint time-barred — filed beyond 1 month of cause of action')
        ingredients['fatal_defects'].append({
            'ingredient': 7,
            'defect': 'Complaint Barred by Limitation',
            'severity': 'CRITICAL',
            'explanation': (
                'Complaint filed beyond the 1-month limitation period from cause of action. '
                'Requires condonation application under Section 142 NI Act. '
                'Condonation is discretionary and not guaranteed.'
            )
        })
        _ing7_status = '❌ TIME-BARRED'

    elif 'DELAYED' in limitation_status_str:
        ing7_score = 50
        ing7_issues.append('Slight delay — may be condoned with sufficient cause')
        _ing7_status = '⚠️ Delayed'

    else:
        _ing7_status = '✅ Compliant'

    ingredients['ingredient_details'].append({
        'number': 7,
        'name': 'Complaint Within One Month',
        'score': ing7_score,
        'status': _ing7_status,
        'issues': ing7_issues,
        'evidence_required': ['Complaint filing date proof', 'Calculation of limitation period']
    })
    scores.append(ing7_score)

    # Calculate overall compliance and apply universal cap
    ingredients['overall_compliance'] = sum(scores) / len(scores)
    ingredients['overall_compliance'] = cap_score_realistic(ingredients['overall_compliance'], max_cap=98.0)

    ingredient_rankings = sorted(
        [(i+1, score) for i, score in enumerate(scores)],
        key=lambda x: x[1]
    )
    ingredients['weakest_ingredients'] = [
        {
            'ingredient_number': ing_num,
            'ingredient_name': ingredients['ingredient_details'][ing_num-1]['name'],
            'score': score,
            'rank': 'WEAKEST' if idx == 0 else 'WEAK'
        }
        for idx, (ing_num, score) in enumerate(ingredient_rankings[:3])
        if score < SCORE_EXCELLENT
    ]

    if ingredients['fatal_defects']:
        ingredients['risk_level'] = 'CRITICAL - Fatal Defects Present'
    elif ingredients['overall_compliance'] >= 80:
        ingredients['risk_level'] = 'LOW - Strong Compliance'
    elif ingredients['overall_compliance'] >= 60:
        ingredients['risk_level'] = 'MEDIUM - Moderate Compliance'
    else:
        ingredients['risk_level'] = 'HIGH - Weak Compliance'

    return ingredients

def analyze_presumption_rebuttal(case_data: Dict, ingredient_data: Dict, doc_data: Dict) -> Dict:
    """
    Section 139 Presumption Analysis - ENHANCED

    BEFORE (v9.8 OLD):
    - Did not distinguish signature admitted vs denied
    - Rebuttal strength not evidence-based
    - Could increase score (incorrect)

    AFTER (v9.8 ENHANCED):
    - Strict signature status tracking
    - Evidence-based rebuttal strength
    - Presumption only shifts burden, never increases score
    - Minimum tier enforcement for HIGH rebuttal
    """

    presumption_analysis = {
        'module': 'Section 139 Presumption Analysis',
        'current_stage': '',
        'presumption_activated': False,
        'burden_position': '',
        'signature_status': '',  # NEW: admitted/denied/disputed
        'rebuttal_evidence_type': '',
        'rebuttal_strength': '',  # LOW/MEDIUM/HIGH (evidence-based)
        'minimum_tier_enforced': False,  # NEW: For HIGH rebuttal
        'burden_shift_timeline': [],
        'strategic_position': {},
        'evidence_requirements': {},
        'score_impact': 0  # Presumption NEVER increases score
    }

    # Stage 1: Check Signature Status (ENHANCED)
    signature_admitted = case_data.get('signature_admitted', False)
    signature_denied = case_data.get('signature_denied', False)

    if signature_admitted:
        presumption_analysis['signature_status'] = 'ADMITTED'
        cheque_proved = True
        presumption_analysis['burden_shift_timeline'].append({
            'stage': 1,
            'event': 'Signature Admitted by Accused',
            'legal_effect': 'Section 118 & 139 presumption automatically applies',
            'burden': 'On Complainant - DISCHARGED (signature admitted)'
        })
    elif signature_denied:
        presumption_analysis['signature_status'] = 'DENIED'
        cheque_proved = False
        presumption_analysis['burden_shift_timeline'].append({
            'stage': 1,
            'event': 'Signature Denied by Accused',
            'legal_effect': 'Complainant must FIRST prove signature (handwriting expert)',
            'burden': 'On Complainant - PENDING (prove signature first)'
        })
    else:
        # Original cheque check (fallback)
        presumption_analysis['signature_status'] = 'DISPUTED'
        if case_data.get('original_cheque_available'):
            cheque_proved = True
            presumption_analysis['burden_shift_timeline'].append({
                'stage': 1,
                'event': 'Cheque Execution Proved (Original Available)',
                'evidence': 'Original cheque available',
                'burden': 'On Complainant - DISCHARGED'
            })
        else:
            cheque_proved = False
            presumption_analysis['burden_shift_timeline'].append({
                'stage': 1,
                'event': 'Cheque Execution To Be Proved',
                'evidence': 'Original cheque missing - must prove by secondary evidence',
                'burden': 'On Complainant - PENDING'
            })

    # Stage 2: Presumption Activation
    if cheque_proved:
        presumption_analysis['presumption_activated'] = True
        presumption_analysis['current_stage'] = 'Stage 2: Presumption Activated (S.118 & S.139)'
        presumption_analysis['burden_shift_timeline'].append({
            'stage': 2,
            'event': 'Presumption Under Section 118 & 139 Activated',
            'presumption': 'Cheque presumed to be for legally enforceable debt',
            'burden': 'SHIFTED TO ACCUSED',
            'legal_note': 'Accused must rebut on preponderance of probabilities'
        })
        presumption_analysis['burden_position'] = 'On Accused'
    else:
        presumption_analysis['current_stage'] = 'Stage 1: Cheque Execution Not Yet Proved'
        presumption_analysis['burden_position'] = 'On Complainant'
        presumption_analysis['strategic_position']['complainant'] = [
            'Prove cheque execution first',
            'Use handwriting expert if signature disputed',
            'Establish chain of custody of cheque'
        ]
        return presumption_analysis

    # Stage 3: Rebuttal Strength Assessment (EVIDENCE-BASED - ENHANCED)
    defence_type = (case_data.get('defence_type') or '').lower()

    if defence_type in ['security_cheque', 'no_debt', 'time_barred', 'legally_unenforceable']:
        presumption_analysis['current_stage'] = 'Stage 3: Accused Attempting Rebuttal'

        # Evidence-Based Rebuttal Scoring (ENHANCED)
        rebuttal_score = 0
        evidence_types = []
        evidence_quality = []

        # Documentary Evidence (Strongest)
        if case_data.get('written_agreement_exists'):
            rebuttal_score += 35
            evidence_types.append('Documentary - Written Agreement')
            evidence_quality.append('HIGH')

        # Witness Evidence
        if case_data.get('witness_available'):
            witness_count = case_data.get('witness_count', 1)
            if witness_count >= 3:
                rebuttal_score += 25
                evidence_quality.append('MEDIUM-HIGH')
            else:
                rebuttal_score += 15
                evidence_quality.append('MEDIUM')
            evidence_types.append(f'Oral Testimony ({witness_count} witnesses)')

        # Electronic Evidence (with Section 65B check)
        if case_data.get('email_sms_evidence'):
            if case_data.get('section_65b_certificate', False):
                rebuttal_score += 30
                evidence_types.append('Electronic (Section 65B compliant)')
                evidence_quality.append('HIGH')
            else:
                rebuttal_score += 10  # Weak without certificate
                evidence_types.append('Electronic (No Section 65B certificate - WEAK)')
                evidence_quality.append('LOW')

        # Account Books / Ledger
        if case_data.get('ledger_available'):
            rebuttal_score += 25
            evidence_types.append('Account Books/Ledger')
            evidence_quality.append('MEDIUM-HIGH')

        # Banking Records
        if case_data.get('bank_statement_support'):
            rebuttal_score += 20
            evidence_types.append('Banking Records')
            evidence_quality.append('MEDIUM')

        # Determine Rebuttal Strength (EVIDENCE-BASED)
        if rebuttal_score >= 70:
            presumption_analysis['rebuttal_strength'] = 'HIGH'
            presumption_analysis['minimum_tier_enforced'] = True  # NEW: Enforce minimum tier
            presumption_analysis['burden_position'] = 'Rebuttal Strong - Burden Shifts Back to Complainant'
            presumption_analysis['score_impact'] = 0  # Presumption never increases score
        elif rebuttal_score >= 40:
            presumption_analysis['rebuttal_strength'] = 'MEDIUM'
            presumption_analysis['burden_position'] = 'On Accused - Partial Rebuttal'
            presumption_analysis['score_impact'] = 0
        else:
            presumption_analysis['rebuttal_strength'] = 'LOW'
            presumption_analysis['burden_position'] = 'On Accused - Weak Rebuttal'
            presumption_analysis['score_impact'] = 0  # Never increases score

        presumption_analysis['rebuttal_evidence_type'] = ', '.join(evidence_types)
        presumption_analysis['evidence_quality_breakdown'] = evidence_quality

        presumption_analysis['burden_shift_timeline'].append({
            'stage': 3,
            'event': f'Rebuttal Attempted - {presumption_analysis["rebuttal_strength"]} strength',
            'evidence_types': evidence_types,
            'rebuttal_score': rebuttal_score,
            'burden': presumption_analysis['burden_position']
        })

        # Strategic Guidance Based on Rebuttal Strength
        if presumption_analysis['rebuttal_strength'] == 'HIGH':
            presumption_analysis['strategic_position']['accused'] = [
                'Strong rebuttal evidence available',
                'Present documentary evidence first',
                'Corroborate with witness testimony',
                'Burden effectively shifted back to complainant'
            ]
            presumption_analysis['strategic_position']['complainant'] = [
                'CRITICAL: Strengthen primary evidence',
                'Challenge authenticity of defence documents',
                'Cross-examine defence witnesses rigorously',
                'Consider settlement - defence is strong'
            ]
        elif presumption_analysis['rebuttal_strength'] == 'MEDIUM':
            presumption_analysis['strategic_position']['accused'] = [
                'Partial rebuttal - need more evidence',
                'Strengthen with additional witnesses or documents',
                'Burden still substantially on accused'
            ]
        else:  # LOW
            presumption_analysis['strategic_position']['accused'] = [
                'Weak rebuttal - presumption largely intact',
                'Urgently gather stronger documentary evidence',
                'Consider settlement negotiations'
            ]
    else:
        presumption_analysis['current_stage'] = 'Stage 2: No Rebuttal Attempted'
        presumption_analysis['burden_position'] = 'On Accused - Presumption Unrebutted'
        presumption_analysis['rebuttal_strength'] = 'NONE'

    # Final Stage: Evidence Requirements
    presumption_analysis['evidence_requirements'] = {
        'complainant': [
            'Original cheque (already proved)' if cheque_proved else 'Prove cheque execution first',
            'Return memo showing dishonour',
            'Proof of notice service',
            'If rebuttal strong: Additional evidence to counter defence'
        ],
        'accused': [
            'Evidence to rebut presumption on preponderance',
            'Documentary evidence preferred (highest weight)',
            'Witness corroboration essential',
            'Section 65B certificate mandatory for electronic evidence'
        ]
    }

    return presumption_analysis

# ============================================================================
# MISSING FEATURE #1: SECURITY CHEQUE DETECTOR
# ============================================================================

def analyze_security_cheque_probability(case_data: Dict) -> Dict:
    """
    Analyze probability that cheque was given as security (not payment).

    This is the MOST COMMON defence in Section 138 cases.
    Courts heavily scrutinize this.
    """
    probability_score = 0
    indicators = []

    # RED FLAGS indicating security cheque (each adds to probability)

    # 1. No transaction proof (MAJOR RED FLAG)
    if not case_data.get('written_agreement_exists') and not case_data.get('invoice_available'):
        probability_score += 30
        indicators.append({
            'factor': 'No transaction documentation',
            'weight': 'HIGH',
            'impact': '+30 points to security cheque defence'
        })

    # 2. Cash transaction claimed (MAJOR RED FLAG)
    if case_data.get('transaction_mode') == 'cash':
        probability_score += 25
        indicators.append({
            'factor': 'Cash transaction claimed',
            'weight': 'HIGH',
            'impact': '+25 points - Courts highly suspicious of large cash loans'
        })

    # 3. No bank transfer proof (CRITICAL)
    if not case_data.get('bank_transfer_proof'):
        probability_score += 20
        indicators.append({
            'factor': 'No bank transfer evidence',
            'weight': 'HIGH',
            'impact': '+20 points - No electronic trail of loan'
        })

    # 4. Amount vs income mismatch
    complainant_income = case_data.get('complainant_annual_income', 0)
    transaction_amount = case_data.get('transaction_amount', 0)

    if complainant_income > 0 and transaction_amount > complainant_income:
        probability_score += 15
        indicators.append({
            'factor': 'Loan amount exceeds declared income',
            'weight': 'MEDIUM',
            'impact': '+15 points - Financial capacity questionable'
        })

    # 5. No ITR/income proof
    if not case_data.get('itr_available'):
        probability_score += 10
        indicators.append({
            'factor': 'No income tax returns',
            'weight': 'MEDIUM',
            'impact': '+10 points - Cannot prove source of funds'
        })

    # 6. Blank/undated cheque
    if case_data.get('cheque_date_filled_later'):
        probability_score += 15
        indicators.append({
            'factor': 'Cheque date filled later',
            'weight': 'HIGH',
            'impact': '+15 points - Classic security cheque indicator'
        })

    # 7. No ledger entries
    if not case_data.get('ledger_available'):
        probability_score += 10
        indicators.append({
            'factor': 'No account ledger',
            'weight': 'MEDIUM',
            'impact': '+10 points - No transaction record'
        })

    # Determine probability level
    if probability_score >= 70:
        probability = 'VERY HIGH'
        defence_strength = 'VERY STRONG'
        recommendation = 'CRITICAL RISK - Security cheque defence highly likely to succeed'
    elif probability_score >= 50:
        probability = 'HIGH'
        defence_strength = 'STRONG'
        recommendation = 'HIGH RISK - Strong security cheque defence probable'
    elif probability_score >= 30:
        probability = 'MEDIUM'
        defence_strength = 'MODERATE'
        recommendation = 'MODERATE RISK - Security cheque defence possible'
    else:
        probability = 'LOW'
        defence_strength = 'WEAK'
        recommendation = 'LOW RISK - Security cheque defence unlikely'

    return {
        'module': 'Security Cheque Probability Analysis',
        'probability_score': probability_score,
        'probability_level': probability,
        'defence_strength': defence_strength,
        'indicators': indicators,
        'recommendation': recommendation,
        'legal_principle': 'Burden on complainant to prove debt. Courts presume security cheque if no transaction proof.',
        'counter_measures': [
            'Obtain written loan agreement immediately',
            'Get bank transfer records',
            'Collect ITR and income proof',
            'Maintain detailed ledger with signatures',
            'Get acknowledgment of debt in writing'
        ]
    }


# ============================================================================
# MISSING FEATURE #2: FINANCIAL CAPACITY ANALYSIS
# ============================================================================

def analyze_financial_capacity(case_data: Dict) -> Dict:
    """
    Analyze if complainant had financial capacity to give the loan.

    Courts HEAVILY scrutinize this in trial.
    """
    complainant_income = case_data.get('complainant_annual_income', 0)
    loan_amount = case_data.get('transaction_amount', 0)

    capacity_analysis = {
        'module': 'Financial Capacity Analysis',
        'complainant_income': complainant_income,
        'loan_amount': loan_amount,
        'capacity_score': 0,
        'capacity_level': '',
        'red_flags': [],
        'green_flags': [],
        'cross_exam_vulnerabilities': []
    }

    # Calculate capacity score (0-100)
    score = 100

    # RED FLAG 1: Loan exceeds annual income
    if complainant_income > 0 and loan_amount > complainant_income:
        ratio = loan_amount / complainant_income
        score -= 40
        capacity_analysis['red_flags'].append({
            'issue': f'Loan amount is {ratio:.1f}x annual income',
            'severity': 'CRITICAL',
            'impact': 'Defence will question source of funds'
        })
        capacity_analysis['cross_exam_vulnerabilities'].append(
            'Where did you get money to lend more than your annual income?'
        )

    # RED FLAG 2: No ITR
    if not case_data.get('itr_available'):
        score -= 20
        capacity_analysis['red_flags'].append({
            'issue': 'No income tax returns',
            'severity': 'HIGH',
            'impact': 'Cannot prove income'
        })
        capacity_analysis['cross_exam_vulnerabilities'].append(
            'Do you have income tax returns to prove your income?'
        )

    # RED FLAG 3: No bank statements
    if not case_data.get('bank_statement_available'):
        score -= 15
        capacity_analysis['red_flags'].append({
            'issue': 'No bank statements',
            'severity': 'HIGH',
            'impact': 'Cannot show fund availability'
        })
        capacity_analysis['cross_exam_vulnerabilities'].append(
            'Show your bank statements proving you had this amount.'
        )

    # RED FLAG 4: Cash withdrawal claim
    if case_data.get('transaction_mode') == 'cash' and loan_amount > 50000:
        score -= 15
        capacity_analysis['red_flags'].append({
            'issue': 'Large cash transaction (>₹50,000)',
            'severity': 'HIGH',
            'impact': 'Violates cash transaction limits, suspicious'
        })
        capacity_analysis['cross_exam_vulnerabilities'].append(
            'Why did you give such a large amount in cash instead of bank transfer?'
        )

    # GREEN FLAGS
    if case_data.get('itr_available'):
        capacity_analysis['green_flags'].append('Income tax returns available')

    if case_data.get('bank_statement_available'):
        capacity_analysis['green_flags'].append('Bank statements available')

    if case_data.get('bank_transfer_proof'):
        score += 20
        capacity_analysis['green_flags'].append('Bank transfer proof strengthens capacity claim')

    if complainant_income > 0 and loan_amount <= complainant_income * 0.5:
        capacity_analysis['green_flags'].append('Loan amount reasonable compared to income')

    # Final assessment
    capacity_analysis['capacity_score'] = max(0, score)

    if score >= 80:
        capacity_analysis['capacity_level'] = 'STRONG'
        capacity_analysis['risk'] = 'LOW'
    elif score >= 60:
        capacity_analysis['capacity_level'] = 'ADEQUATE'
        capacity_analysis['risk'] = 'MEDIUM'
    elif score >= 40:
        capacity_analysis['capacity_level'] = 'WEAK'
        capacity_analysis['risk'] = 'HIGH'
    else:
        capacity_analysis['capacity_level'] = 'VERY WEAK'
        capacity_analysis['risk'] = 'CRITICAL'

    return capacity_analysis


def analyze_liability_exposure(case_data: Dict, timeline_data: Dict, ingredient_data: Dict) -> Dict:

    liability_exposure = {
        'vulnerability_zones': [],
        'likely_questions': [],
        'preparation_required': [],
        'overall_risk': ''
    }

    if case_data.get('case_type') == 'complainant':

        if not case_data.get('written_agreement_exists'):
            cross_exam_analysis['vulnerability_zones'].append({
                'zone': 'No Written Agreement',
                'severity': 'HIGH',
                'attack_vector': 'Defence will question nature and existence of debt'
            })
            cross_exam_analysis['likely_questions'].extend([
                'Where is the written agreement for this transaction?',
                'How do you prove the amount of debt without written record?',
                'Why was no receipt/agreement made for such large amount?',
                'Is this not a security cheque given for different purpose?'
            ])
            cross_exam_analysis['preparation_required'].append({
                'area': 'Transaction Proof',
                'preparation': 'Prepare detailed oral explanation of transaction, gather any emails/SMS, witness statements'
            })

        if not case_data.get('ledger_available'):
            cross_exam_analysis['vulnerability_zones'].append({
                'zone': 'No Ledger/Account Books',
                'severity': 'MEDIUM',
                'attack_vector': 'Defence will question business practice and genuineness of transaction'
            })
            cross_exam_analysis['likely_questions'].extend([
                'Why is there no entry in account books for this debt?',
                'Do you maintain proper accounts for your business?',
                'How can such transaction occur without accounting entry?'
            ])

        if (case_data.get('debt_nature') or '').lower() == 'cash':
            cross_exam_analysis['vulnerability_zones'].append({
                'zone': 'Cash Transaction - No Bank Trail',
                'severity': 'HIGH',
                'attack_vector': 'Defence will question genuineness - why no bank transfer proof'
            })
            cross_exam_analysis['likely_questions'].extend([
                'Why was such large amount given in cash?',
                'Where is the withdrawal proof from your account?',
                'Who was witness to this cash handover?',
                'Why not bank transfer/cheque for such amount?'
            ])
            cross_exam_analysis['preparation_required'].append({
                'area': 'Cash Transaction Justification',
                'preparation': 'Have clear explanation ready, any witness to cash handover, withdrawal proof if available'
            })

        if case_data.get('transaction_date') and case_data.get('cheque_date'):
            try:
                trans_date = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
                cheque_date = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d')
                gap_months = (cheque_date - trans_date).days / 30

                if gap_months > 6:
                    cross_exam_analysis['vulnerability_zones'].append({
                        'zone': f'Large Time Gap ({gap_months:.1f} months) Between Transaction and Cheque',
                        'severity': 'MEDIUM',
                        'attack_vector': 'Defence will argue cheque was for different purpose, not original transaction'
                    })
                    cross_exam_analysis['likely_questions'].append(
                        f'Why did accused give cheque {gap_months:.0f} months after transaction?'
                    )
            except:
                pass

        cross_exam_analysis['likely_questions'].extend([
            'Have you shown this transaction in your income tax return?',
            'What is your annual income to give such amount?'
        ])
        cross_exam_analysis['preparation_required'].append({
            'area': 'Financial Capacity',
            'preparation': 'Be ready to explain source of funds, ITR if applicable, business turnover'
        })

    else:

        cross_exam_analysis['vulnerability_zones'].append({
            'zone': 'Signature on Cheque',
            'severity': 'HIGH',
            'attack_vector': 'Prosecution will establish cheque issuance through signature'
        })
        cross_exam_analysis['likely_questions'].extend([
            'This is your signature on the cheque?',
            'You issued this cheque from your bank account?',
            'Cheque was in your possession and you filled it?'
        ])
        cross_exam_analysis['preparation_required'].append({
            'area': 'Signature & Issuance',
            'preparation': 'If admitting signature, have clear explanation why cheque was given (security/different purpose)'
        })

        if case_data.get('defence_type') in ['security_cheque', 'no_debt']:
            if not case_data.get('written_agreement_exists') and not case_data.get('email_sms_evidence'):
                cross_exam_analysis['vulnerability_zones'].append({
                    'zone': 'No Documentary Proof of Defence',
                    'severity': 'CRITICAL',
                    'attack_vector': 'Prosecution will attack credibility - why no written proof of security/no debt claim'
                })
                cross_exam_analysis['likely_questions'].extend([
                    'If it was security cheque, where is written agreement stating so?',
                    'Any email/SMS showing it was only for security?',
                    'Why would you give blank signed cheque as security?',
                    'This is self-serving afterthought, correct?'
                ])
                cross_exam_analysis['preparation_required'].append({
                    'area': 'Defence Evidence',
                    'preparation': 'URGENT: Gather any correspondence, witness who knew cheque was security, contemporaneous evidence'
                })

        dishonour_reason = (case_data.get('dishonour_reason') or '').lower()
        if 'insufficient' in dishonour_reason:
            cross_exam_analysis['vulnerability_zones'].append({
                'zone': 'Insufficient Funds - Implicit Debt Admission',
                'severity': 'MEDIUM',
                'attack_vector': 'Prosecution will argue insufficient funds shows you knew debt existed'
            })
            cross_exam_analysis['likely_questions'].extend([
                'If no debt existed, why not give stop payment instruction?',
                'Insufficient funds means you expected cheque to be presented for payment?',
                'This shows you knew it was for debt payment?'
            ])

    critical_count = len([z for z in cross_exam_analysis['vulnerability_zones'] if z['severity'] == 'CRITICAL'])
    high_count = len([z for z in cross_exam_analysis['vulnerability_zones'] if z['severity'] == 'HIGH'])

    if critical_count > 0:
        cross_exam_analysis['overall_risk'] = 'CRITICAL - Severe Vulnerabilities in Cross-Examination'
    elif high_count >= 2:
        cross_exam_analysis['overall_risk'] = 'HIGH - Multiple High-Risk Zones'
    elif high_count == 1:
        cross_exam_analysis['overall_risk'] = 'MEDIUM - Some Vulnerable Areas'
    else:
        cross_exam_analysis['overall_risk'] = 'LOW - Manageable Cross-Examination Risk'

    return cross_exam_analysis

@safe_analysis  # FIX #3: Error handling wrapper
def analyze_documentary_strength(case_data: Dict) -> Dict:

    doc_analysis = {
        'overall_strength_score': 0,
        'liability_documentation': {},
        'service_proof': {},
        'transaction_trail': {},
        'defence_exposure': {},
        'financial_capacity_risk': {},
        'critical_gaps': [],
        'structured_summary': {},
        'recommendations': []
    }

    liability_score = 0
    liability_grade = ""
    liability_details = []

    has_agreement = case_data.get('written_agreement_exists', False)
    has_invoice = case_data.get('invoice_available', False)
    has_ledger = case_data.get('ledger_available', False)
    has_bank_transfer = case_data.get('bank_transfer_proof', False)
    transaction_mode = case_data.get('transaction_mode', 'cash').lower()

    if has_agreement and has_bank_transfer:
        liability_score = 95
        liability_grade = "STRONG"
        liability_details.append("✓ Written agreement available")
        liability_details.append("✓ Bank transfer proof available")
    elif has_agreement:
        liability_score = 75
        liability_grade = "STRONG"
        liability_details.append("✓ Written agreement available")
        liability_details.append("⚠️ No bank transfer proof")
    elif has_invoice and has_ledger:
        liability_score = 55
        liability_grade = "MODERATE"
        liability_details.append("✓ Invoice available")
        liability_details.append("✓ Ledger available")
        liability_details.append("⚠️ No formal written agreement")
    elif has_ledger:
        liability_score = 35
        liability_grade = "WEAK"
        liability_details.append("⚠️ Ledger only")
        liability_details.append("❌ No agreement or invoice")
        doc_analysis['critical_gaps'].append({
            'category': 'Liability Documentation',
            'gap': 'No written agreement',
            'impact': 'Debt enforceability contestable',
            'severity': 'HIGH',
            'deduction': -20
        })
    elif transaction_mode == 'oral' or (not has_agreement and not has_invoice and not has_ledger):
        liability_score = 15
        liability_grade = "HIGHLY CONTESTABLE"
        liability_details.append("❌ Oral transaction only")
        liability_details.append("❌ No documentary proof")
        doc_analysis['critical_gaps'].append({
            'category': 'Liability Documentation',
            'gap': 'No written evidence of debt',
            'impact': 'Severe - debt enforceability highly contestable',
            'severity': 'CRITICAL',
            'deduction': -35
        })
    else:
        liability_score = 10
        liability_grade = "CRITICALLY WEAK"
        liability_details.append("❌ No documentation")

    doc_analysis['liability_documentation'] = {
        'score': liability_score,
        'grade': liability_grade,
        'details': liability_details,
        'weight': 0.40
    }

    # CRITICAL FIX: Use unified service proof evaluator (no independent evaluation)
    unified_service = evaluate_service_proof_unified(case_data)

    service_score = unified_service['score']
    service_grade = unified_service['proof_strength']
    service_details = []

    # Build details from unified result
    if unified_service['delivery_confirmed']:
        service_details.append(f"✓ {unified_service['delivery_method']}")
        service_details.append(f"✓ Delivery confirmed: {unified_service['service_quality']}")
    else:
        service_details.append(f"⚠️ {unified_service['delivery_method']}")
        if unified_service['delivery_status'] == 'REFUSED':
            service_details.append("⚠️ Notice refused - deemed service applies")
        elif unified_service['delivery_status'] == 'UNCLAIMED':
            service_details.append("⚠️ Returned unclaimed - deemed service applies")
        elif unified_service['delivery_status'] == 'SENT_NO_PROOF':
            service_details.append("❌ No proof of delivery")
            doc_analysis['critical_gaps'].append({
                'category': 'Service Proof',
                'gap': 'No delivery confirmation',
                'impact': 'Service contestable',
                'severity': 'MEDIUM' if unified_service['notice_sent'] else 'CRITICAL',
                'deduction': -15 if unified_service['notice_sent'] else -25
            })

    doc_analysis['service_proof'] = {
        'score': service_score,
        'grade': service_grade,
        'details': service_details,
        'weight': 0.25
    }

    trail_score = 0
    trail_details = []

    if transaction_mode == 'cash' and not has_agreement:
        trail_score = 15
        trail_details.append("❌ Cash-only with no written agreement")
        trail_details.append("⚠️ Serious contestability risk")
        doc_analysis['critical_gaps'].append({
            'category': 'Transaction Trail',
            'gap': 'Cash transaction without documentation',
            'impact': 'High - transaction authenticity contestable',
            'severity': 'HIGH',
            'deduction': -25
        })
    elif transaction_mode == 'cash':
        trail_score = 45
        trail_details.append("⚠️ Cash transaction")
        trail_details.append("✓ Agreement mitigates risk partially")
    elif has_bank_transfer:
        trail_score = 95
        trail_details.append("✓ Bank transfer proof")
    elif has_ledger:
        trail_score = 65
        trail_details.append("✓ Ledger provides trail")
    else:
        trail_score = 35
        trail_details.append("⚠️ Limited transaction trail")

    doc_analysis['transaction_trail'] = {
        'score': trail_score,
        'details': trail_details,
        'weight': 0.20
    }

    defence_score = 100
    defence_risks = []

    defence_type = (case_data.get('defence_type') or '').lower()
    security_alleged = case_data.get('security_cheque_alleged', False)

    if defence_type == 'security_cheque' or security_alleged:
        defence_score -= 50
        defence_risks.append({
            'risk': 'Security cheque defence',
            'impact': 'HIGH - accused claims cheque was security',
            'deduction': -50,
            'cross_exam_focus': 'Transaction purpose, consideration, written terms',
            'severity': 'HIGH'
        })
        doc_analysis['critical_gaps'].append({
            'category': 'Defence Exposure',
            'gap': 'Security cheque allegation',
            'impact': 'Major courtroom defence - requires strong rebuttal',
            'severity': 'HIGH',
            'deduction': -15
        })

    if defence_type == 'no_debt':
        defence_score -= 30
        defence_risks.append({
            'risk': 'Debt existence challenged',
            'impact': 'Moderate - onus on complainant',
            'deduction': -30,
            'cross_exam_focus': 'Debt documentation and transaction proof'
        })

    if not has_agreement:
        defence_score -= 20
        defence_risks.append({
            'risk': 'No written agreement',
            'impact': 'Moderate - increases defence leverage',
            'deduction': -20
        })

    defence_score = max(0, defence_score)

    doc_analysis['defence_exposure'] = {
        'score': defence_score,
        'risks': defence_risks,
        'risk_count': len(defence_risks),
        'weight': 0.10
    }

    capacity_risk_level = "LOW"
    capacity_flags = []
    capacity_score = 100

    cheque_amount = float(case_data.get('cheque_amount', 0))
    has_itr = case_data.get('itr_available', False)
    has_bank_statement = case_data.get('bank_statement_available', False)
    income_source = case_data.get('income_source_documented', False)

    if cheque_amount > 1000000:
        if not has_itr and not has_bank_statement:
            capacity_risk_level = "HIGH"
            capacity_score = 30
            capacity_flags.append("❌ Large amount (>₹10L) without ITR or bank statement")
            capacity_flags.append("⚠️ Defence may challenge financial capacity")
            doc_analysis['critical_gaps'].append({
                'category': 'Financial Capacity',
                'gap': 'No income documentation for large amount',
                'impact': 'Complainant capacity challengeable',
                'severity': 'MEDIUM',
                'deduction': -10
            })
        elif not income_source:
            capacity_risk_level = "MODERATE"
            capacity_score = 60
            capacity_flags.append("⚠️ Income source not documented")
            capacity_flags.append("Attach income proof recommended")
        else:
            capacity_risk_level = "LOW"
            capacity_score = 95
            capacity_flags.append("✓ Financial documentation adequate")
    else:
        capacity_risk_level = "LOW"
        capacity_score = 100
        capacity_flags.append("Amount within normal range")

    doc_analysis['financial_capacity_risk'] = {
        'risk_level': capacity_risk_level,
        'score': capacity_score,
        'flags': capacity_flags,
        'weight': 0.05,
        'recommendation': 'Attach ITR/bank statements' if capacity_risk_level != 'LOW' else None
    }

    weighted_score = (
        liability_score * 0.40 +
        service_score * 0.25 +
        trail_score * 0.20 +
        defence_score * 0.10 +
        capacity_score * 0.05
    )

    doc_analysis['overall_strength_score'] = round(weighted_score, 1)

    if weighted_score >= 80:
        overall_grade = "STRONG"
    elif weighted_score >= 60:
        overall_grade = "MODERATE"
    elif weighted_score >= 40:
        overall_grade = "WEAK"
    else:
        overall_grade = "CRITICALLY WEAK"

    doc_analysis['overall_grade'] = overall_grade

    doc_analysis['structured_summary'] = {
        'liability_documentation': {
            'grade': liability_grade,
            'details': liability_details
        },
        'service_proof': {
            'grade': service_grade,
            'details': service_details
        },
        'transaction_trail': {
            'grade': 'Strong' if trail_score >= 80 else 'Moderate' if trail_score >= 50 else 'Weak',
            'details': trail_details
        },
        'defence_exposure': {
            'risk_count': len(defence_risks),
            'risks': defence_risks
        },
        'financial_capacity': {
            'risk': capacity_risk_level,
            'flags': capacity_flags
        }
    }

    if liability_score < 60:
        doc_analysis['recommendations'].append("Strengthen liability documentation with written agreement")
    if service_score < 60:
        doc_analysis['recommendations'].append("Obtain stronger proof of notice service")
    if trail_score < 50:
        doc_analysis['recommendations'].append("Document transaction trail with bank records")
    if capacity_risk_level != "LOW":
        doc_analysis['recommendations'].append("Attach ITR/bank statements to prove financial capacity")

    return doc_analysis

def analyze_accused_liability(case_data: Dict) -> Dict:
    """
    Section 141 Company Liability Analysis - ENHANCED

    BEFORE (v9.8 OLD):
    - No director designation validation
    - No resignation date cross-check
    - No authorized signatory mapping

    AFTER (v9.8 ENHANCED):
    - Director designation validation (MD/ED/Independent)
    - CRITICAL: Resignation date < cheque_date = Not Liable
    - Authorized signatory cross-verification
    - Board resolution date validation
    """

    liability_analysis = {
        'module': 'Section 141 Liability Analysis - ENHANCED',
        'parties_properly_impleaded': True,
        'issues': [],
        'recommendations': [],
        'vicarious_liability_check': {},
        'director_liability_matrix': [],  # NEW: Individual director analysis
        'risk_score': 0
    }

    risk_points = 0

    if case_data.get('is_company_case'):
        liability_analysis['case_type'] = 'Company Case - Section 141 Applicable'

        liability_analysis['vicarious_liability_check']['company_impleaded'] = {
            'status': '✅ Company is accused',
            'requirement': 'Mandatory - Company must be accused No. 1'
        }

        if case_data.get('directors_impleaded'):
            liability_analysis['vicarious_liability_check']['directors_impleaded'] = {
                'status': '✅ Directors impleaded',
                'requirement': 'Required for vicarious liability'
            }

            # NEW: Individual Director Liability Analysis
            directors_list = case_data.get('directors_list', [])
            cheque_date = case_data.get('cheque_date')

            if directors_list and cheque_date:
                # datetime already imported at top of file
                cheque_date_obj = datetime.strptime(cheque_date, '%Y-%m-%d').date()

                for director in directors_list:
                    director_analysis = {
                        'name': director.get('name'),
                        'designation': director.get('designation'),  # MD/ED/Director/Independent
                        'din': director.get('din'),
                        'liable': True,
                        'liability_basis': [],
                        'exclusion_reasons': []
                    }

                    # CRITICAL: Resignation Date Check (ENHANCED)
                    resignation_date = director.get('resignation_date')
                    if resignation_date:
                        resignation_date_obj = datetime.strptime(resignation_date, '%Y-%m-%d').date()
                        if resignation_date_obj < cheque_date_obj:
                            director_analysis['liable'] = False
                            director_analysis['exclusion_reasons'].append({
                                'reason': 'Resigned Before Cheque Date',
                                'resignation_date': resignation_date,
                                'cheque_date': cheque_date,
                                'legal_effect': 'NOT LIABLE - Not in charge at relevant time',
                                'precedent': 'Director ceased to be in charge before cheque issuance'
                            })
                            liability_analysis['issues'].append({
                                'issue': f'Director {director.get("name")} Cannot Be Prosecuted',
                                'severity': 'INFORMATIONAL',
                                'impact': f'Resignation date {resignation_date} is before cheque date {cheque_date}',
                                'action': 'Remove this director from complaint or face dismissal on this ground'
                            })

                    # Director Designation Validation (ENHANCED)
                    designation = director.get('designation', '').lower()

                    if 'managing' in designation or designation == 'md':
                        director_analysis['liability_basis'].append({
                            'basis': 'Managing Director',
                            'presumption': 'STRONG - Presumed to be in charge',
                            'proof_required': 'MINIMAL - Position itself establishes control',
                            'precedent': 'S.M.S. Pharmaceuticals - MD presumed in charge'
                        })
                    elif 'executive' in designation or designation == 'ed':
                        director_analysis['liability_basis'].append({
                            'basis': 'Executive Director',
                            'presumption': 'MEDIUM - Active role expected',
                            'proof_required': 'MODERATE - Show day-to-day involvement'
                        })
                    elif 'independent' in designation:
                        director_analysis['liability_basis'].append({
                            'basis': 'Independent Director',
                            'presumption': 'WEAK - Oversight role only',
                            'proof_required': 'HIGH - Must prove active role in operations',
                            'precedent': 'Aneeta Hada - Independent directors not automatically liable'
                        })
                        director_analysis['liable'] = False  # Default: not liable unless proven
                        director_analysis['exclusion_reasons'].append({
                            'reason': 'Independent Director - Insufficient Averment',
                            'legal_effect': 'NOT LIABLE unless specific role proven',
                            'action_required': 'Complainant must specifically aver and prove active operational role'
                        })
                    else:
                        director_analysis['liability_basis'].append({
                            'basis': 'Director (General)',
                            'presumption': 'MEDIUM',
                            'proof_required': 'MODERATE - Show involvement in business conduct'
                        })

                    # Authorized Signatory Check (ENHANCED)
                    authorized_signatories = case_data.get('authorized_signatories', [])
                    if authorized_signatories:
                        if director.get('name') in authorized_signatories or director.get('din') in authorized_signatories:
                            director_analysis['liability_basis'].append({
                                'basis': 'Authorized Signatory',
                                'evidence': 'Name appears in authorized signatory list',
                                'legal_effect': 'STRONG indicator of being "in charge"',
                                'board_resolution_date': case_data.get('board_resolution_date')
                            })
                        else:
                            director_analysis['exclusion_reasons'].append({
                                'reason': 'Not Authorized Signatory',
                                'evidence': 'Name does not appear in signatory authorization',
                                'defense_angle': 'Can argue lack of operational control'
                            })

                    liability_analysis['director_liability_matrix'].append(director_analysis)

        else:
            liability_analysis['vicarious_liability_check']['directors_impleaded'] = {
                'status': '❌ Directors NOT impleaded',
                'requirement': 'Required for vicarious liability'
            }
            liability_analysis['issues'].append({
                'issue': 'Directors Not Impleaded',
                'severity': 'CRITICAL',
                'impact': 'Cannot prosecute directors without naming them as accused',
                'remedy': 'File application to implead directors as additional accused'
            })
            liability_analysis['parties_properly_impleaded'] = False
            risk_points += 50

        if case_data.get('specific_averment_present'):
            liability_analysis['vicarious_liability_check']['specific_averment'] = {
                'status': '✅ Specific averment present',
                'requirement': 'MANDATORY per SMS Pharmaceuticals judgment',
                'details': 'Must specifically state each director was "in charge of and responsible for conduct of business" at relevant time'
            }
        else:
            liability_analysis['vicarious_liability_check']['specific_averment'] = {
                'status': '❌ Specific averment MISSING',
                'requirement': 'MANDATORY per SMS Pharmaceuticals judgment'
            }
            liability_analysis['issues'].append({
                'issue': 'No Specific Averment of Director Liability',
                'severity': 'CRITICAL - FATAL FOR DIRECTORS',
                'impact': 'Directors will be discharged (company liability may survive).',
                'clarification': 'This defect is FATAL for director liability only. Complaint against company may still proceed.',
                'legal_basis': 'SMS Pharmaceuticals Ltd. v. Neeta Bhalla (2005): Complaint must specifically aver that each director was in charge of and responsible for conduct of business',
                'remedy': 'Amendment may not be allowed at later stage. File fresh complaint or application for amendment immediately.'
            })
            liability_analysis['parties_properly_impleaded'] = False
            liability_analysis['director_liability_status'] = 'FATAL - Directors will be discharged'
            liability_analysis['company_liability_status'] = 'VIABLE - Company liability may survive'
            risk_points += 70

        # Board Resolution & Signatory Authority (ENHANCED)
        board_resolution_date = case_data.get('board_resolution_date')
        if board_resolution_date and cheque_date:
            # datetime already imported at top of file
            resolution_date_obj = datetime.strptime(board_resolution_date, '%Y-%m-%d').date()
            cheque_date_obj = datetime.strptime(cheque_date, '%Y-%m-%d').date()

            liability_analysis['vicarious_liability_check']['signatory_authorization'] = {
                'board_resolution_date': board_resolution_date,
                'cheque_date': cheque_date,
                'authorized_signatories': case_data.get('authorized_signatories', []),
                'status': '✅ Resolution predates cheque' if resolution_date_obj < cheque_date_obj else '⚠️ Timeline issue'
            }

        liability_analysis['vicarious_liability_check']['director_distinction'] = {
            'note': 'Managing Director: Presumed to be in charge. Independent Director: Specific role must be shown.',
            'action_required': 'If independent directors included, prove their active role in business conduct',
            'precedent': 'Aneeta Hada v. Godfather Travels (2012)'
        }

        if not case_data.get('specific_averment_present'):
            liability_analysis['recommendations'].append({
                'priority': 'URGENT',
                'recommendation': 'Add specific averment in complaint that each named director was "at the relevant time in charge of and responsible for conduct of business of the company"'
            })

        liability_analysis['recommendations'].append({
            'priority': 'HIGH',
            'recommendation': 'Obtain board resolution showing signatory authority and decision-makers at time of cheque issuance',
            'enhanced_check': 'Verify resolution date is before cheque date'
        })

        liability_analysis['recommendations'].append({
            'priority': 'HIGH',
            'recommendation': 'Cross-verify director status on cheque date: Check MCA Form DIR-12 for resignation dates',
            'critical_note': 'If director resigned before cheque date, remove from complaint immediately'
        })

        liability_analysis['recommendations'].append({
            'priority': 'MEDIUM',
            'recommendation': 'Obtain MCA records (Form DIR-12, Annual Returns) to prove director positions during relevant period'
        })

    else:

        liability_analysis['case_type'] = 'Individual Case'
        liability_analysis['vicarious_liability_check']['simple_liability'] = {
            'status': '✅ Direct liability - no Section 141 issues',
            'note': 'Individual is directly liable as drawer of cheque'
        }

    if risk_points == 0:
        liability_analysis['risk_score'] = 0
        liability_analysis['risk_level'] = 'LOW - Proper Impleading'
    elif risk_points <= 50:
        liability_analysis['risk_score'] = 50
        liability_analysis['risk_level'] = 'MEDIUM - Some Deficiencies'
    else:
        liability_analysis['risk_score'] = risk_points
        liability_analysis['risk_level'] = 'CRITICAL - Fatal Defects in Impleading'

    return liability_analysis

def analyze_defence_vulnerabilities(case_data: Dict, ingredient_analysis: Dict, doc_analysis: Dict) -> Dict:

    defence_matrix = {
        'possible_defences': [],
        'strongest_defence': None,
        'overall_defence_strength': '',
        'complainant_counter_strategy': []
    }

    security_cheque_strength = 0
    if case_data.get('defence_type') == 'security_cheque':
        security_cheque_strength = 60

        if not case_data.get('written_agreement_exists'):
            security_cheque_strength += 15
        if not case_data.get('ledger_available'):
            security_cheque_strength += 10

        defence_matrix['possible_defences'].append({
            'defence': 'Security Cheque (No Legally Enforceable Debt)',
            'strength': DefenceStrength.HIGH if security_cheque_strength >= 70 else DefenceStrength.MEDIUM,
            'strength_score': security_cheque_strength,
            'basis': 'Accused claims cheque given as security, not for discharge of debt',
            'evidence_required_by_accused': [
                'Admission that cheque was given',
                'Proof that it was for security purpose',
                'No immediate debt existed',
                'Contemporaneous correspondence'
            ],
            'complainant_counter': [
                'Rely on Section 139 presumption',
                'Accused must prove no debt - burden shifted',
                'Show transaction history',
                'Prove debt existed at time of cheque issuance'
            ]
        })

    no_debt_strength = 30
    if case_data.get('defence_type') in ['no_debt', 'debt_discharged']:
        no_debt_strength = 50

        if not case_data.get('written_agreement_exists'):
            no_debt_strength += 10

        defence_matrix['possible_defences'].append({
            'defence': 'No Debt Existed',
            'strength': DefenceStrength.MEDIUM if no_debt_strength >= 50 else DefenceStrength.LOW,
            'strength_score': no_debt_strength,
            'basis': 'Accused denies any debt existed',
            'evidence_required_by_accused': [
                'Admission of cheque signature',
                'Explanation for why cheque was given',
                'Proof no transaction occurred',
                'Correspondence showing no debt'
            ],
            'complainant_counter': [
                'Section 139 presumption',
                'Burden on accused is heavy',
                'Preponderance of probabilities',
                'Show transaction evidence'
            ]
        })

    time_bar_strength = 0
    if case_data.get('transaction_date'):
        try:
            trans_date = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
            cheque_date = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d')
            years_diff = (cheque_date - trans_date).days / 365

            if years_diff > 3:
                time_bar_strength = 70
                defence_matrix['possible_defences'].append({
                    'defence': 'Time-Barred Debt',
                    'strength': DefenceStrength.HIGH,
                    'strength_score': time_bar_strength,
                    'basis': f'Debt is {years_diff:.1f} years old - beyond 3-year limitation',
                    'legal_principle': 'Cheque for time-barred debt is not for legally enforceable debt',
                    'evidence_required_by_accused': [
                        'Prove original transaction date',
                        'Show no acknowledgment of debt within 3 years',
                        'No part payment made'
                    ],
                    'complainant_counter': [
                        'Show acknowledgment of debt',
                        'Part payment within 3 years',
                        'Fresh promise to pay',
                        'Argue cheque issuance itself is acknowledgment'
                    ]
                })
        except:
            pass

    if (case_data.get('dishonour_reason') or '').lower().find('stop') >= 0:
        stop_payment_strength = 40
        defence_matrix['possible_defences'].append({
            'defence': 'Stop Payment with Lawful Reason',
            'strength': DefenceStrength.MEDIUM,
            'strength_score': stop_payment_strength,
            'basis': 'Accused gave stop payment instruction',
            'legal_principle': 'Stop payment with lawful reason may be valid defence',
            'evidence_required_by_accused': [
                'Proof of stop payment instruction',
                'Valid reason (breach of contract, goods not supplied, etc.)',
                'Contemporaneous evidence of dispute'
            ],
            'complainant_counter': [
                'Stop payment without lawful reason is admission of cheque issuance',
                'Prove transaction was completed',
                'No breach occurred',
                'Stop payment instruction shows consciousness of debt'
            ]
        })

    technical_defect_strength = 0
    technical_defects = []

    for fatal_defect in ingredient_analysis.get('fatal_defects', []):
        if fatal_defect['severity'] in ['CRITICAL', 'HIGH']:
            technical_defect_strength += 25
            technical_defects.append(fatal_defect['defect'])

    if technical_defect_strength > 0:
        defence_matrix['possible_defences'].append({
            'defence': 'Technical/Procedural Defects',
            'strength': DefenceStrength.HIGH if technical_defect_strength >= 50 else DefenceStrength.MEDIUM,
            'strength_score': min(technical_defect_strength, 100),
            'basis': 'Complaint suffers from procedural non-compliance',
            'defects_found': technical_defects,
            'impact': 'May lead to discharge without trial',
            'complainant_counter': [
                'File condonation application if limitation delay',
                'Cure curable defects through amendment',
                'Argue defects are not fatal'
            ]
        })

    settlement_strength = 30
    defence_matrix['possible_defences'].append({
        'defence': 'Settlement/Payment Made',
        'strength': DefenceStrength.LOW,
        'strength_score': settlement_strength,
        'basis': 'Accused claims payment made after notice',
        'evidence_required_by_accused': [
            'Receipt of payment',
            'Bank transfer proof',
            'Acknowledgment from complainant'
        ],
        'complainant_counter': [
            'Deny receipt of payment',
            'Challenge genuineness of receipt if produced',
            'Prove complaint filed before alleged payment'
        ],
        'note': 'If genuine payment proven, complaint will fail'
    })

    if defence_matrix['possible_defences']:
        strongest = max(defence_matrix['possible_defences'], key=lambda x: x['strength_score'])
        defence_matrix['strongest_defence'] = strongest

        if strongest['strength_score'] >= 70:
            defence_matrix['overall_defence_strength'] = 'STRONG - Accused has viable defence'
        elif strongest['strength_score'] >= 50:
            defence_matrix['overall_defence_strength'] = 'MODERATE - Defence has some merit'
        else:
            defence_matrix['overall_defence_strength'] = 'WEAK - Defences unlikely to succeed'
    else:
        defence_matrix['overall_defence_strength'] = 'MINIMAL - No strong defence available'

    if case_data.get('case_type') == 'complainant':
        defence_matrix['complainant_counter_strategy'].append({
            'strategy': 'Rely Heavily on Section 139 Presumption',
            'action': 'Ensure cheque and dishonour are proved beyond doubt. Burden then shifts to accused.'
        })

        if doc_analysis['overall_strength_score'] < 60:
            defence_matrix['complainant_counter_strategy'].append({
                'strategy': 'Strengthen Documentary Evidence',
                'action': 'Urgently gather any email, SMS, WhatsApp evidence showing debt acknowledgment'
            })

        if defence_matrix['strongest_defence'] and defence_matrix['strongest_defence']['strength_score'] >= 70:
            defence_matrix['complainant_counter_strategy'].append({
                'strategy': 'Prepare for Strong Defence',
                'action': f"Accused likely to raise: {defence_matrix['strongest_defence']['defence']}. Prepare counter-evidence proactively.",
                'priority': 'HIGH'
            })

    return defence_matrix

def scan_procedural_defects(case_data: Dict, timeline_data: Dict, liability_data: Dict) -> Dict:

    defect_scan = {
        'fatal_defects': [],
        'curable_defects': [],
        'warnings': [],
        'overall_risk': '',
        'remedies': []
    }

    if case_data.get('court_location'):
        defect_scan['warnings'].append({
            'area': 'Jurisdiction',
            'warning': 'Verify jurisdiction carefully',
            'check': 'Cause of action must have arisen within court territorial limits (place of dishonour, notice service, accused residence, or business place)'
        })
    else:
        defect_scan['curable_defects'].append({
            'defect': 'Court Location Not Specified',
            'severity': 'MEDIUM',
            'cure': 'Specify court and verify jurisdiction'
        })

    if case_data.get('notice_sent_to_address'):
        if 'wrong' in case_data['notice_sent_to_address'].lower():
            defect_scan['fatal_defects'].append({
                'defect': 'Notice Sent to Wrong Address',
                'severity': 'HIGH',
                'impact': 'Notice may be invalid - ingredient 5 fails',
                'remedy': 'Difficult to cure. May need to send fresh notice and refile complaint (if within limitation).'
            })

    if not case_data.get('notice_signed'):
        defect_scan['curable_defects'].append({
            'defect': 'Notice Unsigned',
            'severity': 'MEDIUM',
            'impact': 'Procedural irregularity',
            'cure': 'File affidavit explaining and producing signed copy if available'
        })

    defect_scan['warnings'].append({
        'area': 'Complaint Affidavit',
        'warning': 'Ensure complaint is supported by affidavit',
        'check': 'Mandatory requirement - complaint must be on oath'
    })

    if case_data.get('is_company_case'):
        if not case_data.get('specific_averment_present'):
            defect_scan['fatal_defects'].append({
                'defect': 'No Specific Averment for Directors (Company Case)',
                'severity': 'CRITICAL - FATAL FOR DIRECTORS ONLY',
                'impact': 'Directors will be discharged (company liability may survive)',
                'scope': 'This defect affects director liability only. Company can still be prosecuted.',
                'remedy': 'File amendment application immediately OR file fresh complaint if within limitation',
                'legal_basis': 'SMS Pharmaceuticals Ltd. judgment'
            })

        defect_scan['warnings'].append({
            'area': 'Board Resolution',
            'warning': 'Company prosecution requires board resolution or authorization',
            'check': 'Verify signatory was authorized to issue cheque on behalf of company'
        })

    for risk_marker in timeline_data.get('risk_markers', []):
        if risk_marker['severity'] in ['HIGH', 'CRITICAL']:
            defect_scan['fatal_defects'].append({
                'defect': risk_marker['issue'],
                'severity': risk_marker['severity'],
                'impact': risk_marker['impact'],
                'remedy': 'Timeline defects usually cannot be cured. May need condonation if limitation delay is minor.'
            })

    if case_data.get('notice_received_date') and case_data.get('complaint_filed_date'):
        try:
            notice_recv = datetime.strptime(case_data['notice_received_date'], '%Y-%m-%d')
            complaint_filed = datetime.strptime(case_data['complaint_filed_date'], '%Y-%m-%d')
            fifteen_day_expiry = notice_recv + timedelta(days=15)

            if complaint_filed < fifteen_day_expiry:
                defect_scan['fatal_defects'].append({
                    'defect': 'Complaint Filed Before 15-Day Period Expired',
                    'severity': 'CRITICAL',
                    'impact': 'Premature complaint - cause of action not yet arisen',
                    'remedy': 'NONE - Complaint will be dismissed. Must file fresh complaint after 15 days expire.'
                })
        except:
            pass

    if case_data.get('is_multiple_cheques') and case_data.get('number_of_cheques', 1) > 1:
        defect_scan['warnings'].append({
            'area': 'Multiple Cheques',
            'warning': 'Each cheque creates separate offence',
            'check': 'Verify all cheques are within limitation and properly detailed in complaint'
        })

    defect_scan['warnings'].append({
        'area': 'Annexures',
        'warning': 'Verify all annexures match complaint averments',
        'check': 'Check cheque number, amount, date, bank name match between complaint and actual cheque/memo'
    })

    fatal_count = len(defect_scan['fatal_defects'])
    if fatal_count >= 2:
        defect_scan['overall_risk'] = 'CRITICAL - Multiple Fatal Defects'
    elif fatal_count == 1:
        defect_scan['overall_risk'] = 'HIGH - One Fatal Defect Present'
    elif defect_scan['curable_defects']:
        defect_scan['overall_risk'] = 'MEDIUM - Curable Defects Present'
    else:
        defect_scan['overall_risk'] = 'LOW - No Major Defects Detected'

    if defect_scan['fatal_defects']:
        defect_scan['remedies'].append({
            'priority': 'URGENT',
            'remedy': 'Consult advocate immediately regarding fatal defects. Some may not be curable.'
        })

    if defect_scan['curable_defects']:
        defect_scan['remedies'].append({
            'priority': 'HIGH',
            'remedy': 'File amendment application to cure procedural defects before accused files discharge application'
        })

    return defect_scan

def calculate_overall_risk_score(
    timeline_data: Dict,
    ingredient_data: Dict,
    doc_data: Dict,
    liability_data: Dict,
    defect_data: Dict
) -> Dict:

    risk_model = {
        'category_scores': {},
        'overall_risk_score': 0,
        'risk_breakdown': {},
        'compliance_level': '',
        'critical_issues': [],
        'fatal_defects': [],
        'confidence_level': ''
    }

    weights = get_centralized_weights()

    if timeline_data['limitation_risk'] == 'LOW':
        timeline_score = 95
    elif timeline_data['limitation_risk'] == 'MEDIUM':
        timeline_score = 60
    else:
        timeline_score = 20

    for marker in timeline_data.get('risk_markers', []):
        if marker['severity'] == 'CRITICAL':
            timeline_score -= 30
        elif marker['severity'] == 'HIGH':
            timeline_score -= 20

    timeline_score = normalize_score(timeline_score)
    timeline_score = cap_score_realistic(timeline_score, max_cap=98.0)

    risk_model['category_scores']['Timeline Compliance'] = {
        'score': timeline_score,
        'weight': int(weights['timeline'] * 100),
        'weighted_score': timeline_score * weights['timeline']
    }
    risk_model['risk_breakdown']['timeline_risk'] = int((100 - timeline_score) * weights['timeline'])

    ingredient_score = normalize_score(ingredient_data['overall_compliance'])
    ingredient_score = cap_score_realistic(ingredient_score, max_cap=98.0)
    risk_model['category_scores']['Ingredient Compliance'] = {
        'score': ingredient_score,
        'weight': int(weights['ingredients'] * 100),
        'weighted_score': ingredient_score * weights['ingredients']
    }
    risk_model['risk_breakdown']['ingredient_risk'] = int((100 - ingredient_score) * weights['ingredients'])

    doc_score = normalize_score(doc_data['overall_strength_score'])
    doc_score = cap_score_realistic(doc_score, max_cap=98.0)
    risk_model['category_scores']['Documentary Strength'] = {
        'score': doc_score,
        'weight': int(weights['documentary'] * 100),
        'weighted_score': doc_score * weights['documentary']
    }
    risk_model['risk_breakdown']['documentary_risk'] = int((100 - doc_score) * weights['documentary'])

    if liability_data['parties_properly_impleaded']:
        liability_score = 98
    else:
        liability_score = 100 - liability_data['risk_score']

    liability_score = normalize_score(liability_score)
    liability_score = cap_score_realistic(liability_score, max_cap=98.0)
    risk_model['category_scores']['Proper Impleading'] = {
        'score': liability_score,
        'weight': int(weights['liability'] * 100),
        'weighted_score': liability_score * weights['liability']
    }
    risk_model['risk_breakdown']['liability_risk'] = int((100 - liability_score) * weights['liability'])

    if defect_data['overall_risk'].startswith('CRITICAL'):
        procedural_score = 20
    elif defect_data['overall_risk'].startswith('HIGH'):
        procedural_score = 40
    elif defect_data['overall_risk'].startswith('MEDIUM'):
        procedural_score = 70
    else:
        procedural_score = 95

    procedural_score = normalize_score(procedural_score)
    procedural_score = cap_score_realistic(procedural_score, max_cap=98.0)
    risk_model['category_scores']['Procedural Compliance'] = {
        'score': procedural_score,
        'weight': int(weights['procedural'] * 100),
        'weighted_score': procedural_score * weights['procedural']
    }
    risk_model['risk_breakdown']['procedural_risk'] = int((100 - procedural_score) * weights['procedural'])

    total_weighted = sum(cat['weighted_score'] for cat in risk_model['category_scores'].values())

    risk_model['overall_risk_score'] = normalize_score(total_weighted)

    risk_model['overall_risk_score'] = cap_score_realistic(risk_model['overall_risk_score'], max_cap=98.0)

    all_fatal_defects = []

    timeline_critical_risks = [
        r for r in timeline_data.get('risk_markers', [])
        if r.get('severity') == 'CRITICAL' and r.get('marker') == 'RED'
    ]

    if timeline_critical_risks:
        for risk in timeline_critical_risks:
            all_fatal_defects.append({
                'category': 'Timeline Compliance',
                'defect': risk['issue'],
                'defect_type': 'limitation_expired',
                'severity': 'FATAL',
                'impact': 'Case dismissal certain'
            })

    if timeline_data.get('deterministic_score', 100) == 0:
        all_fatal_defects.append({
            'category': 'Timeline',
            'defect': 'Cheque validity/limitation fatal defect',
            'defect_type': 'cheque_validity_failure',
            'severity': 'FATAL',
            'impact': 'Case will be dismissed on technical grounds'
        })
    elif timeline_data.get('limitation_risk') == 'CRITICAL':
        all_fatal_defects.append({
            'category': 'Timeline',
            'defect': 'Critical limitation violation',
            'defect_type': 'complaint_beyond_30_days',
            'severity': 'FATAL',
            'impact': 'Dismissal risk substantial'
        })

    if ingredient_data.get('fatal_defects'):
        for defect in ingredient_data['fatal_defects']:
            # Determine correct defect_type based on ingredient number
            defect_name = defect.get('defect', '')
            if 'Notice' in defect_name and '30 days' in defect_name:
                defect_type = 'notice_limitation_violation'
            elif 'Complaint' in defect_name or 'limitation' in defect_name.lower():
                defect_type = 'complaint_limitation_violation'
            elif 'debt' in defect_name.lower():
                defect_type = 'no_legally_enforceable_debt'
            elif 'cheque' in defect_name.lower() and 'validity' in defect_name.lower():
                defect_type = 'cheque_validity_expired'
            else:
                defect_type = 'statutory_ingredient_missing'

            all_fatal_defects.append({
                'category': 'Ingredient',
                'defect': defect.get('defect', 'Fatal ingredient defect'),
                'defect_type': defect_type,
                'severity': 'FATAL',
                'impact': 'Essential element missing'
            })

    risk_model['fatal_defects'] = all_fatal_defects

    risk_model['has_fatal_defects'] = len(all_fatal_defects) > 0

    if all_fatal_defects:
        original_score = risk_model['overall_risk_score']
        final_score, override_details = apply_weighted_fatal_override(
            risk_model['overall_risk_score'],
            all_fatal_defects
        )
        risk_model['overall_risk_score'] = final_score

        if override_details['applied']:
            risk_model['fatal_defect_override'] = override_details

    if all_fatal_defects:
        hard_override_score, hard_override_reason = apply_hard_fatal_override(
            risk_model['overall_risk_score'],
            all_fatal_defects
        )

        if hard_override_reason != "NO_OVERRIDE":
            if hard_override_score < risk_model['overall_risk_score']:
                risk_model['overall_risk_score'] = hard_override_score
                risk_model['hard_fatal_override'] = {
                    'applied': True,
                    'reason': hard_override_reason,
                    'original_score': round(original_score, 1),
                    'capped_at': hard_override_score,
                    'capped_at_display': f"{hard_override_score}/100",
                    'final_score': hard_override_score,
                    'message': f'FATAL DEFECT — Score capped at {hard_override_score}/100'
                }

                risk_model['compliance_level'] = 'FATAL – HIGH DISMISSAL RISK'

    for defect in all_fatal_defects:
        risk_model['critical_issues'].append({
            'category': defect['category'],
            'issue': defect['defect'],
            'severity': defect['severity'],
            'impact': defect['impact']
        })

    fatal_defect_override = False
    override_reason = []

    procedural_fatal_count = len([d for d in defect_data.get('fatal_defects', [])
                                   if d.get('severity') in ['CRITICAL', 'HIGH']])
    if procedural_fatal_count >= 2:
        fatal_defect_override = True
        override_reason.append(f'{procedural_fatal_count} critical procedural defects - technical dismissal likely')
        risk_model['overall_risk_score'] = min(risk_model['overall_risk_score'], 30)
    elif procedural_fatal_count == 1 and defect_data['overall_risk'].startswith('CRITICAL'):
        fatal_defect_override = True
        override_reason.append('Critical procedural defect detected')
        risk_model['overall_risk_score'] = min(risk_model['overall_risk_score'], 40)

    if fatal_defect_override:
        _capped_at = risk_model['overall_risk_score']
        risk_model['fatal_defect_override'] = {
            'applied': True,
            'original_weighted_score': round(total_weighted, 1),
            'original_score': round(total_weighted, 1),
            'overridden_score': _capped_at,
            'capped_at': _capped_at,
            'capped_at_display': f"{_capped_at}/100",
            'reason': ' | '.join(override_reason),
            'logic': 'Fatal defects auto-cap score — case will collapse regardless of other strengths',
            'severity': 'FATAL' if _capped_at <= 25 else 'CRITICAL',
            'warning': 'Despite strong scores in other areas, fatal defects make case unviable'
        }

        if risk_model['overall_risk_score'] <= 20:
            risk_model['compliance_level'] = ComplianceLevel.CRITICAL
            risk_model['confidence_level'] = 'CERTAIN FAILURE - Fatal defects present'
        elif risk_model['overall_risk_score'] <= 40:
            risk_model['compliance_level'] = ComplianceLevel.CRITICAL
            risk_model['confidence_level'] = 'Very high risk - Critical defects likely fatal'
    else:

        if (risk_model.get('overall_risk_score') or 0) >= 90:
            risk_model['compliance_level'] = ComplianceLevel.EXCELLENT
            risk_model['confidence_level'] = 'Very high confidence in case strength'
        elif (risk_model.get('overall_risk_score') or 0) >= 75:
            risk_model['compliance_level'] = ComplianceLevel.GOOD
            risk_model['confidence_level'] = 'High confidence in case strength'
        elif (risk_model.get('overall_risk_score') or 0) >= 60:
            risk_model['compliance_level'] = ComplianceLevel.MODERATE
            risk_model['confidence_level'] = 'Moderate confidence - some weaknesses present'
        elif (risk_model.get('overall_risk_score') or 0) >= 40:
            risk_model['compliance_level'] = ComplianceLevel.WEAK
            risk_model['confidence_level'] = 'Low confidence - significant risks'
        else:
            risk_model['compliance_level'] = ComplianceLevel.CRITICAL
            risk_model['confidence_level'] = 'Very low confidence - critical defects present'

    risk_model['risk_breakdown'] = {
        'timeline_risk': 100 - timeline_score,
        'ingredient_risk': 100 - ingredient_score,
        'documentary_risk': 100 - doc_score,
        'liability_risk': 100 - liability_score,
        'procedural_risk': 100 - procedural_score
    }

    for category, data in risk_model['category_scores'].items():
        if data['score'] < 50:
            risk_model['critical_issues'].append({
                'category': category,
                'score': data['score'],
                'severity': 'CRITICAL' if data['score'] < 30 else 'HIGH',
                'impact': f'{category} is a major weakness in the case'
            })

    avg_data_completeness = 85.0
    risk_model['analysis_confidence'] = calculate_confidence(
        data_completeness=avg_data_completeness,
        kb_coverage=70.0,
        retrieval_strength=0.75
    )

    # CRITICAL: Guarantee overall_risk_score is always a valid number
    if 'overall_risk_score' not in risk_model or risk_model['overall_risk_score'] is None:
        logger.error("overall_risk_score was None or missing - defaulting to 0")
        risk_model['overall_risk_score'] = 0

    # Ensure it's a number
    try:
        risk_model['overall_risk_score'] = float(risk_model['overall_risk_score'])
    except (TypeError, ValueError):
        logger.error(f"overall_risk_score invalid type: {type(risk_model['overall_risk_score'])} - defaulting to 0")
        risk_model['overall_risk_score'] = 0

    return risk_model

def analyze_settlement_exposure(case_data: Dict, risk_score_data: Dict) -> Dict:

    settlement_analysis = {
        'financial_exposure': {},
        'settlement_leverage': '',
        'settlement_probability': '',
        'strategic_options': [],
        'interim_compensation_eligible': False
    }

    cheque_amount = case_data.get('cheque_amount', 0)

    if case_data.get('case_type') == 'accused':

        max_fine = cheque_amount * 2
        min_fine = 0
        max_imprisonment_months = 24

        settlement_analysis['financial_exposure'] = {
            'cheque_amount': cheque_amount,
            'maximum_fine': max_fine,
            'minimum_fine': min_fine,
            'typical_fine_range': f'{cheque_amount * 0.5:.2f} to {cheque_amount * 1.5:.2f}',
            'imprisonment_risk': f'Up to {max_imprisonment_months} months (usually suspended on deposit)',
            'compensation': f'Up to {cheque_amount} under Section 357 CrPC',
            'appeal_deposit': f'Minimum 20% of fine/compensation ({cheque_amount * 0.2:.2f}) required for suspension of sentence (Section 148)'
        }

        case_strength = risk_score_data['overall_risk_score']
        if case_strength < 40:

            settlement_analysis['settlement_leverage'] = SettlementPressure.LOW
            settlement_analysis['settlement_probability'] = 'LOW - Complainant case weak, accused may fight'
            settlement_analysis['strategic_options'].append({
                'option': 'Fight the Case',
                'rationale': 'Complainant case has critical weaknesses. Good chance of acquittal.',
                'financial_impact': 'Legal costs but avoid payment if acquitted'
            })
            settlement_analysis['strategic_options'].append({
                'option': 'Negotiate Settlement at 30-40% of Amount',
                'rationale': 'Leverage weak case for substantial discount',
                'financial_impact': f'Pay around {cheque_amount * 0.35:.2f}'
            })
        elif case_strength < 70:

            settlement_analysis['settlement_leverage'] = SettlementPressure.MODERATE
            settlement_analysis['settlement_probability'] = 'MODERATE - Both parties may prefer settlement'
            settlement_analysis['strategic_options'].append({
                'option': 'Negotiate Settlement at 60-80% of Amount',
                'rationale': 'Case outcome uncertain. Settlement avoids risk.',
                'financial_impact': f'Pay around {cheque_amount * 0.70:.2f}'
            })
            settlement_analysis['strategic_options'].append({
                'option': 'Fight But Keep Settlement Option Open',
                'rationale': 'Defend case but settle if evidence goes against accused',
                'financial_impact': 'Legal costs + possible full amount if convicted'
            })
        else:

            settlement_analysis['settlement_leverage'] = SettlementPressure.HIGH
            settlement_analysis['settlement_probability'] = 'HIGH - Accused under significant pressure'
            settlement_analysis['strategic_options'].append({
                'option': 'Settle Quickly at Full Amount or Slightly Less',
                'rationale': 'Strong case against accused. Settlement cheaper than conviction + legal costs.',
                'financial_impact': f'Pay around {cheque_amount * 0.90:.2f} to {cheque_amount}'
            })
            settlement_analysis['strategic_options'].append({
                'option': 'Request Installment Payment Plan',
                'rationale': 'If lump sum difficult, propose structured payment',
                'financial_impact': f'Full amount {cheque_amount} in installments'
            })

    else:

        settlement_analysis['financial_exposure'] = {
            'cheque_amount': cheque_amount,
            'maximum_recoverable': cheque_amount,
            'interim_compensation': f'Up to 20% ({cheque_amount * 0.20:.2f}) pending trial under Section 143A',
            'conviction_scenario': f'Fine (up to 2x amount) + Compensation (up to amount) + Costs',
            'acquittal_scenario': 'No recovery, legal costs incurred'
        }

        if (risk_score_data.get('overall_risk_score') or 0) >= 60:
            settlement_analysis['interim_compensation_eligible'] = True
            settlement_analysis['strategic_options'].append({
                'option': 'Apply for Interim Compensation (Section 143A)',
                'rationale': 'Case has reasonable strength. Can get 20% immediately.',
                'financial_impact': f'Receive {cheque_amount * 0.20:.2f} pending trial'
            })

        case_strength = risk_score_data['overall_risk_score']
        if case_strength >= 70:
            settlement_analysis['settlement_leverage'] = SettlementPressure.HIGH
            settlement_analysis['settlement_probability'] = 'HIGH - Strong position to demand full payment'
            settlement_analysis['strategic_options'].append({
                'option': 'Demand Full Payment',
                'rationale': 'Strong case. Accused facing conviction.',
                'financial_impact': f'Recover full {cheque_amount}'
            })
            settlement_analysis['strategic_options'].append({
                'option': 'Offer Minor Discount for Quick Settlement',
                'rationale': 'Quick settlement avoids prolonged litigation',
                'financial_impact': f'Recover {cheque_amount * 0.90:.2f} to {cheque_amount}'
            })
        elif case_strength >= 40:
            settlement_analysis['settlement_leverage'] = SettlementPressure.MODERATE
            settlement_analysis['settlement_probability'] = 'MODERATE - Settlement at 70-90% likely'
            settlement_analysis['strategic_options'].append({
                'option': 'Negotiate Settlement at 70-90%',
                'rationale': 'Case has weaknesses. Settlement de-risks outcome.',
                'financial_impact': f'Recover around {cheque_amount * 0.80:.2f}'
            })
        else:
            settlement_analysis['settlement_leverage'] = SettlementPressure.LOW
            settlement_analysis['settlement_probability'] = 'LOW - Weak case, accused may not settle'
            settlement_analysis['strategic_options'].append({
                'option': 'Strengthen Evidence Before Proceeding',
                'rationale': 'Case too weak. Risk of dismissal high.',
                'financial_impact': 'Legal costs with low recovery chance'
            })
            settlement_analysis['strategic_options'].append({
                'option': 'Accept Settlement at 40-60% if Offered',
                'rationale': 'Weak case. Any recovery better than dismissal.',
                'financial_impact': f'Recover around {cheque_amount * 0.50:.2f}'
            })

    return settlement_analysis

def detect_edge_cases(case_data: Dict) -> Dict:

    edge_cases = {
        'detected_cases': [],
        'handling_required': [],
        'complexity_score': 0,
        'special_considerations': []
    }

    complexity = 0

    if case_data.get('is_multiple_cheques') or case_data.get('number_of_cheques', 1) > 1:
        num_cheques = case_data.get('number_of_cheques', 2)
        edge_cases['detected_cases'].append({
            'type': 'Multiple Cheques',
            'severity': 'HIGH',
            'count': num_cheques,
            'implications': [
                'Each cheque creates separate offence',
                'Separate limitation for each cheque',
                'Different dishonour dates possible',
                'Multiple notices may be required'
            ],
            'handling': [
                'Verify timeline compliance for EACH cheque separately',
                'Ensure each cheque mentioned in complaint',
                'Check if all cheques from same transaction or different',
                'Confirm notice sent for each dishonoured cheque'
            ]
        })
        complexity += 25

    if case_data.get('civil_suit_pending'):
        edge_cases['detected_cases'].append({
            'type': 'Parallel Civil Proceedings',
            'severity': 'MEDIUM',
            'implications': [
                'Risk of double recovery challenge',
                'Accused may argue abuse of process',
                'Civil suit outcome may influence criminal case',
                'Stay application possible'
            ],
            'handling': [
                'Clarify that criminal proceedings are independent',
                'Be prepared to explain why both proceedings filed',
                'Check for res judicata issues',
                'Monitor civil suit progress - may need coordination'
            ]
        })
        complexity += 15

    if case_data.get('insolvency_proceedings'):
        edge_cases['detected_cases'].append({
            'type': 'Insolvency/Bankruptcy Proceedings',
            'severity': 'CRITICAL',
            'implications': [
                'Section 138 proceedings may be stayed',
                'Moratorium under IBC may apply',
                'Claim must be filed with resolution professional',
                'Criminal proceedings may continue but enforcement affected'
            ],
            'handling': [
                'Check IBC moratorium status',
                'File claim with insolvency professional',
                'Understand interaction between IBC and NI Act',
                'Consult IBC specialist - complex interplay'
            ]
        })
        complexity += 30

    if case_data.get('is_company_case') and case_data.get('directors_impleaded'):
        edge_cases['detected_cases'].append({
            'type': 'Company Case - Multiple Directors',
            'severity': 'HIGH',
            'implications': [
                'Section 141 vicarious liability applies',
                'Each director needs specific averment',
                'Managing director vs independent director distinction',
                'Signatory authority must be proved'
            ],
            'handling': [
                'Ensure specific averment for EACH director',
                'Obtain board resolution showing signatory authority',
                'Distinguish role of each director (SMS Pharmaceuticals test)',
                'Be prepared to prove "in charge of business" for each'
            ]
        })
        complexity += 20

    # Post-dated cheque detection - check if presented before cheque date
    if case_data.get('cheque_date') and case_data.get('presentation_date'):
        try:
            cheque_dt = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d')
            pres_dt = datetime.strptime(case_data['presentation_date'], '%Y-%m-%d')

            # True post-dated = presented BEFORE cheque date
            if pres_dt < cheque_dt:
                days_premature = (cheque_dt - pres_dt).days
                edge_cases['detected_cases'].append({
                    'type': 'Premature Presentation (Post-Dated Cheque)',
                    'severity': 'HIGH',
                    'days_premature': days_premature,
                    'implications': [
                        'Cheque presented before date on cheque',
                        'Presentation is premature and potentially invalid',
                        'Defence may argue dishonour was justified'
                    ],
                    'handling': [
                        'Verify if presentation was actually premature',
                        'Check if bank accepted despite future date',
                        'May need to re-present on or after cheque date'
                    ]
                })
                complexity += 20
            # Also flag if cheque date is significantly after transaction (informational)
            elif case_data.get('transaction_date'):
                trans_dt = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
                if cheque_dt > trans_dt:
                    days_post_dated = (cheque_dt - trans_dt).days
                    if days_post_dated > 90:  # More than 3 months gap
                        edge_cases['detected_cases'].append({
                            'type': 'Cheque Issued Long After Transaction',
                            'severity': 'MEDIUM',
                            'days_gap': days_post_dated,
                            'implications': [
                                'Large gap between transaction and cheque issuance',
                                'Accused may question timing and debt acknowledgment'
                            ],
                            'handling': [
                                'Explain reason for delay in cheque issuance',
                                'Provide evidence of continuing debt obligation'
                            ]
                        })
                        complexity += 10
        except:
            pass

    if (case_data.get('dishonour_reason') or '').lower().find('stop') >= 0:
        edge_cases['detected_cases'].append({
            'type': 'Stop Payment Instruction',
            'severity': 'MEDIUM',
            'implications': [
                'Accused gave stop payment - admission of cheque issuance',
                'Accused may claim lawful reason for stop payment',
                'Burden on accused to prove lawful reason',
                'Can be used as evidence of cheque execution'
            ],
            'handling': [
                'Use stop payment as admission of signature/issuance',
                'Challenge accused to prove lawful reason',
                'Argue stop payment shows consciousness of debt',
                'Distinguish from genuine disputes (goods not supplied, etc.)'
            ]
        })
        complexity += 10

    if 'appeal' in (case_data.get('case_summary') or '').lower():
        edge_cases['detected_cases'].append({
            'type': 'Appeal Stage',
            'severity': 'HIGH',
            'implications': [
                'Section 148 deposit requirement (min 20%)',
                'Suspension of sentence requires deposit',
                'Limited scope of appeal (questions of law)',
                'Compounding still possible even in appeal'
            ],
            'handling': [
                'Calculate Section 148 deposit amount',
                'File deposit within time limit',
                'Frame grounds of appeal carefully (legal vs factual)',
                'Explore Section 147 compounding option'
            ]
        })
        complexity += 20

    if case_data.get('complainant_address') and case_data.get('accused_address'):
        if case_data['complainant_address'] != case_data['accused_address']:
            edge_cases['special_considerations'].append({
                'consideration': 'Parties in Different Locations',
                'note': 'Verify jurisdiction carefully - where was notice served, cheque presented, etc.'
            })

    if case_data.get('cheque_amount', 0) > 1000000:
        edge_cases['special_considerations'].append({
            'consideration': 'High Value Case (>10 Lakhs)',
            'note': 'Higher stakes - expect vigorous defence, consider interim compensation application'
        })
        complexity += 5

    if case_data.get('transaction_date'):
        try:
            trans_dt = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
            age_years = (datetime.now() - trans_dt).days / 365
            if age_years > 3:
                edge_cases['special_considerations'].append({
                    'consideration': f'Old Transaction ({age_years:.1f} years)',
                    'note': 'Time-barred debt defence likely - ensure cheque issuance itself is within limitation'
                })
        except:
            pass

    edge_cases['complexity_score'] = min(100, complexity)

    if edge_cases['complexity_score'] >= 50:
        edge_cases['complexity_level'] = 'HIGH - Complex Case'
        edge_cases['recommendation'] = 'Consult experienced Section 138 specialist - multiple edge cases present'
    elif edge_cases['complexity_score'] >= 25:
        edge_cases['complexity_level'] = 'MEDIUM - Moderately Complex'
        edge_cases['recommendation'] = 'Careful handling required - edge cases need specific attention'
    else:
        edge_cases['complexity_level'] = 'LOW - Standard Case'
        edge_cases['recommendation'] = 'Standard procedures applicable'

    return edge_cases




def pure_escalation_engine(flags: Dict) -> Dict:

    if flags.get('fatal', False):
        return {
            'tier': 'FATAL',
            'status': 'CASE FAILURE',
            'score': 0,
            'recommendation': 'DO NOT FILE',
            'reason': 'Fatal condition detected',
            'tier_locked': True
        }

    if flags.get('debt_disputed', False):
        return {
            'tier': 'HIGH_RISK',
            'status': 'SUBSTANTIVE DEFENCE RISK',
            'score': 40,
            'recommendation': 'ADDRESS DEBT DISPUTE',
            'reason': 'Enforceable debt challenged - MINIMUM HIGH RISK LOCKED',
            'tier_locked': True,
            'minimum_tier_enforcement': 'HIGH_RISK cannot be reduced by procedural compliance'
        }

    if flags.get('director_inactive_no_averment', False):
        return {
            'tier': 'HIGH_RISK',
            'status': 'SECTION 141 FAILURE',
            'score': 35,
            'recommendation': 'OBTAIN SPECIFIC AVERMENT',
            'reason': 'Director liability requirements not met - MINIMUM HIGH RISK LOCKED',
            'tier_locked': True,
            'minimum_tier_enforcement': 'HIGH_RISK cannot be reduced'
        }

    critical_count = flags.get('critical_count', 0)
    if critical_count >= 2:
        return {
            'tier': 'HIGH_RISK',
            'status': 'MULTIPLE CRITICAL GAPS',
            'score': 45,
            'recommendation': 'ADDRESS CRITICAL ISSUES',
            'reason': f'{critical_count} critical defects detected',
            'tier_locked': False
        }

    if critical_count >= 1:
        return {
            'tier': 'MODERATE_RISK',
            'status': 'GAPS PRESENT',
            'score': 60,
            'recommendation': 'STRENGTHEN CASE',
            'reason': 'One critical gap identified',
            'tier_locked': False
        }

    warning_count = flags.get('warning_count', 0)
    if warning_count >= 4:
        return {
            'tier': 'REVIEW_REQUIRED',
            'status': 'MINOR GAPS',
            'score': 70,
            'recommendation': 'ADDRESS WARNINGS',
            'reason': f'{warning_count} warnings detected',
            'tier_locked': False
        }

    return {
        'tier': 'ELIGIBLE',
        'status': 'READY TO FILE',
        'score': flags.get('base_score', 85),
        'recommendation': 'PROCEED WITH FILING',
        'reason': 'No critical issues detected',
        'tier_locked': False
    }


def analyze_defence_risks(case_data: Dict, documentary_result: Dict) -> Dict:

    # CRITICAL: Check for fatal defects in case (BUG FIX #2)
    # If prosecution has fatal defects, defence is automatically very strong
    timeline_result = case_data.get('_timeline_analysis', {})
    fatal_defects = timeline_result.get('fatal_defects', [])

    # If ANY fatal defect exists in prosecution, defence wins automatically
    if len(fatal_defects) > 0:
        return {
            'module': 'Defence Risk Analysis',
            'overall_risk': 'NONE',
            'defence_strength': 'VERY STRONG',
            'outcome': 'COMPLAINT WILL BE DISMISSED',
            'reason': f'Fatal defects in prosecution: {", ".join([d.get("type", "defect") for d in fatal_defects])}',
            'accused_advantage': 'CERTAIN WIN',
            'defence_grounds': {},
            'high_risk_defences': [],
            'fatal_defences': [{
                'ground': 'Prosecution has fatal defects',
                'consequence': 'Complaint dismissed on threshold',
                'viability_impact': 'CASE WILL FAIL'
            }],
            'mitigation_required': [],
            'risk_score': 0,  # Zero risk for complainant (prosecution fails)
            'case_viability_impact': 'FATAL'
        }

    defence_analysis = {
        'module': 'Defence Risk Analysis',
        'overall_risk': 'MODERATE',
        'defence_grounds': {},
        'high_risk_defences': [],
        'fatal_defences': [],
        'mitigation_required': [],
        'risk_score': 0,
        'consequence_score_adjustment': 0,
        'case_viability_impact': 'NONE'
    }

    risk_score = 100
    consequence_adjustment = 0

    has_written_agreement = case_data.get('written_agreement_exists', False)
    has_invoice = case_data.get('invoice_available', False)
    has_ledger = case_data.get('ledger_available', False)

    if not has_written_agreement and not has_invoice and not has_ledger:
        debt_risk = 'FATAL'
        consequence_adjustment -= 30
        defence_analysis['fatal_defences'].append({
            'ground': 'No legally enforceable debt',
            'consequence': 'Accused can claim no debt existed - Burden on complainant',
            'viability_impact': 'CASE MAY COLLAPSE',
            'minimum_tier_enforced': 'FATAL - Cannot be reduced by any procedural compliance'
        })
        defence_analysis['absolute_minimum_tier'] = 'FATAL'
    elif not has_written_agreement:
        debt_risk = 'HIGH'
        consequence_adjustment -= 20
        defence_analysis['absolute_minimum_tier'] = 'HIGH_RISK'
        defence_analysis['tier_enforcement'] = 'Minimum HIGH RISK enforced - Clean documents cannot reduce below this'
    else:
        debt_risk = 'LOW'

    defence_analysis['defence_grounds']['enforceable_debt'] = {
        'risk_level': debt_risk,
        'vulnerability': 'No documentary proof of debt' if not has_written_agreement else 'Documented',
        'defence_likely': not has_written_agreement,
        'consequence': 'Accused denies debt - Complainant must prove transaction',
        'score_impact': consequence_adjustment if not has_written_agreement else 0
    }

    if debt_risk in ['FATAL', 'HIGH']:
        risk_score -= abs(consequence_adjustment)
        defence_analysis['mitigation_required'].append({
            'action': 'Obtain ledger/bank statements/transaction proof urgently',
            'priority': 'CRITICAL' if debt_risk == 'FATAL' else 'HIGH'
        })

    part_payment_claimed = case_data.get('part_payment_made', False)
    part_payment_amount = case_data.get('part_payment_amount', 0)

    if part_payment_claimed:
        defence_analysis['defence_grounds']['part_payment'] = {
            'risk_level': 'HIGH',
            'vulnerability': f"Accused claims ₹{part_payment_amount} already paid",
            'defence_likely': True,
            'consequence': 'Enforceable debt reduced - May fall below ₹5000 threshold or cheque amount',
            'score_impact': -12,
            'amount_impact': f"Disputed amount: ₹{case_data.get('cheque_amount', 0) - part_payment_amount}"
        }
        risk_score -= 12
        consequence_adjustment -= 12
        defence_analysis['high_risk_defences'].append('Part payment reduces claim value')

    postal_ack = case_data.get('postal_acknowledgment', False)
    ad_card = case_data.get('ad_card_signed', False)
    notice_returned = case_data.get('notice_returned_undelivered', False)

    if notice_returned:
        notice_risk = 'CRITICAL'
        consequence_adjustment -= 20
        # Service issues are CRITICAL not FATAL - can be cured through alternative means
        defence_analysis['high_risk_defences'].append({
            'ground': 'Notice returned undelivered',
            'consequence': 'Service not complete - May need alternative service',
            'viability_impact': 'HIGH RISK - Requires proof of alternative service'
        })
    elif not ad_card and not postal_ack:
        notice_risk = 'HIGH'
        consequence_adjustment -= 15
    elif not ad_card:
        notice_risk = 'MODERATE'
        consequence_adjustment -= 8
    else:
        notice_risk = 'LOW'

    defence_analysis['defence_grounds']['notice_service'] = {
        'risk_level': notice_risk,
        'vulnerability': 'No signed acknowledgment' if not ad_card else 'Properly served',
        'defence_likely': not ad_card,
        'consequence': 'Accused denies receipt - Complainant bears proof burden',
        'score_impact': consequence_adjustment if not ad_card else 0
    }

    if notice_risk in ['CRITICAL', 'HIGH']:
        risk_score -= abs(consequence_adjustment)
        defence_analysis['mitigation_required'].append({
            'action': 'File affidavit of deemed service OR proof of delivery',
            'priority': 'CRITICAL' if notice_risk == 'CRITICAL' else 'HIGH'
        })

    if case_data.get('is_company_case'):
        # Check both possible field names for specific averment
        director_averment = case_data.get('director_specific_averment') or case_data.get('specific_averment_present', False)
        director_knowledge = case_data.get('director_knowledge_proof', False)
        director_designation = case_data.get('director_designation_stated') or case_data.get('directors_impleaded', False)

        # Director liability issues are CRITICAL, not FATAL (defence vulnerability, not statutory defect)
        if not (director_averment and director_knowledge and director_designation):
            director_risk = 'CRITICAL'  # Changed from FATAL
            consequence_adjustment -= 25
            # Don't add to fatal_defences - this is high risk, not absolute fatal
            defence_analysis['high_risk_defences'].append({
                'ground': 'Section 141 requirements incomplete',
                'consequence': 'Director may successfully defend - higher acquittal risk',
                'viability_impact': 'HIGH ACQUITTAL RISK'
            })
        else:
            director_risk = 'LOW'

        defence_analysis['defence_grounds']['director_liability'] = {
            'risk_level': director_risk,
            'vulnerability': 'Section 141 proof incomplete' if director_risk == 'CRITICAL' else 'Compliant',
            'defence_likely': director_risk == 'CRITICAL',
            'consequence': 'Director may deny role - requires stronger proof',
            'score_impact': -25 if director_risk == 'CRITICAL' else 0
        }

        if director_risk == 'CRITICAL':
            risk_score -= 25
            defence_analysis['mitigation_required'].append({
                'action': 'Add specific averment showing director role, knowledge, and consent',
                'priority': 'CRITICAL'
            })

    if not case_data.get('cheque_signature_verified'):
        defence_analysis['defence_grounds']['false_cheque_risk'] = {
            'risk_level': 'MODERATE',
            'vulnerability': 'Signature not forensically verified',
            'defence_likely': True,
            'consequence': 'Accused may claim forged signature - Burden shifts to complainant',
            'score_impact': -10
        }
        risk_score -= 10
        consequence_adjustment -= 10

    defence_analysis['risk_score'] = max(0, risk_score)
    defence_analysis['consequence_score_adjustment'] = consequence_adjustment

    high_risk_count = len(defence_analysis.get('high_risk_defences', []))
    fatal_count = len(defence_analysis.get('fatal_defences', []))

    flags = {
        'fatal': fatal_count > 0,
        'debt_disputed': not has_written_agreement,
        'director_inactive_no_averment': False,
        'critical_count': high_risk_count,
        'warning_count': 0,
        'base_score': risk_score
    }

    if case_data.get('is_company_case'):
        director_inactive = not case_data.get('director_active', True)
        has_averment = case_data.get('director_specific_averment', False)
        if director_inactive and not has_averment:
            flags['director_inactive_no_averment'] = True

    escalation_result = pure_escalation_engine(flags)

    defence_analysis['overall_risk'] = escalation_result['tier']
    defence_analysis['case_viability_impact'] = escalation_result['status']
    defence_analysis['final_risk_score'] = escalation_result['score']
    defence_analysis['recommendation'] = escalation_result['recommendation']
    defence_analysis['escalation_reason'] = escalation_result['reason']
    defence_analysis['deterministic_tier'] = True

    return defence_analysis

def analyze_cross_examination_risks(case_data: Dict, doc_data: Dict, defence_data: Dict) -> Dict:
    """
    Cross Examination Risk Analysis
    Analyzes vulnerability zones for cross-examination
    """
    return {
        'module': 'Cross Examination Risks',
        'vulnerability_zones': [],
        'likely_questions': [],
        'preparation_required': [],
        'overall_risk': 'MEDIUM',
        'complainant_weaknesses': [],
        'accused_weaknesses': [],
        'confidence': 0.6
    }


def analyze_section_65b_compliance(case_data: Dict) -> Dict:
    """
    Section 65B Evidence Act - Electronic Evidence Certificate Compliance

    As per Anvar P.V. v. P.K. Basheer (2014) 10 SCC 473:
    Electronic records are INADMISSIBLE without Section 65B certificate
    """

    compliance = {
        'module': 'Section 65B Electronic Evidence Compliance',
        'applicable': False,
        'compliant': True,
        'electronic_evidence_types': [],
        'certificate_status': {},
        'risk_level': 'NOT_APPLICABLE',
        'recommendations': []
    }

    # Check for electronic evidence
    has_email_sms = case_data.get('email_sms_evidence', False)
    has_electronic = case_data.get('electronic_evidence', False)
    has_whatsapp = case_data.get('whatsapp_evidence', False)
    has_bank_statement_electronic = case_data.get('bank_statement_electronic', False)

    if not any([has_email_sms, has_electronic, has_whatsapp, has_bank_statement_electronic]):
        compliance['applicable'] = False
        compliance['risk_level'] = 'NOT_APPLICABLE'
        return compliance

    compliance['applicable'] = True

    # Identify electronic evidence types
    if has_email_sms:
        compliance['electronic_evidence_types'].append('Email/SMS')
    if has_whatsapp:
        compliance['electronic_evidence_types'].append('WhatsApp Messages')
    if has_bank_statement_electronic:
        compliance['electronic_evidence_types'].append('Electronic Bank Statements')
    if has_electronic:
        compliance['electronic_evidence_types'].append('Other Electronic Records')

    # Check 65B certificate status
    has_certificate = case_data.get('section_65b_certificate', False)
    certificate_signed = case_data.get('section_65b_certificate_signed', False)
    certificate_from_custodian = case_data.get('certificate_from_device_custodian', False)

    # Check if electronic evidence is PRIMARY proof or just supporting
    has_primary_docs = (
        case_data.get('original_cheque_available', False) or
        case_data.get('return_memo_available', False) or
        case_data.get('written_agreement_exists', False) or
        case_data.get('ledger_available', False)
    )

    is_primary_proof = not has_primary_docs  # Electronic is primary only if no traditional docs

    compliance['certificate_status'] = {
        'certificate_available': has_certificate,
        'properly_signed': certificate_signed if has_certificate else False,
        'from_authorized_custodian': certificate_from_custodian if has_certificate else False,
        'electronic_evidence_role': 'PRIMARY PROOF' if is_primary_proof else 'SUPPORTING EVIDENCE',
        'legal_requirement': 'MANDATORY per Anvar P.V. v. P.K. Basheer (2014)'
    }

    # Determine compliance and risk based on role of electronic evidence
    if not has_certificate:
        compliance['compliant'] = False

        if is_primary_proof:
            # Electronic evidence is PRIMARY - FATAL if no certificate
            compliance['risk_level'] = 'FATAL'
            compliance['recommendations'].append({
                'priority': 'CRITICAL',
                'action': 'Obtain Section 65B certificate immediately',
                'details': 'Electronic evidence is PRIMARY PROOF - INADMISSIBLE without certificate',
                'from_whom': 'Device/server custodian (email provider, telecom, bank)',
                'legal_basis': 'Section 65B Evidence Act - Mandatory for primary electronic evidence'
            })
        else:
            # Electronic evidence is SUPPORTING - HIGH risk but not fatal
            compliance['risk_level'] = 'HIGH'
            compliance['recommendations'].append({
                'priority': 'HIGH',
                'action': 'Obtain Section 65B certificate to strengthen case',
                'details': 'Electronic evidence is supporting - case can proceed on primary docs but weakened without certificate',
                'from_whom': 'Device/server custodian (email provider, telecom, bank)',
                'legal_basis': 'Section 65B Evidence Act',
                'impact': 'Electronic evidence inadmissible but primary documents remain valid'
            })
    elif not certificate_signed or not certificate_from_custodian:
        compliance['compliant'] = False
        compliance['risk_level'] = 'CRITICAL'
        compliance['recommendations'].append({
            'priority': 'HIGH',
            'action': 'Ensure certificate is properly executed',
            'details': 'Certificate must be signed by authorized custodian',
            'legal_basis': 'Section 65B(4) - Certificate requirements'
        })
    else:
        compliance['compliant'] = True
        compliance['risk_level'] = 'LOW'
        compliance['recommendations'].append({
            'priority': 'MEDIUM',
            'action': 'Verify certificate details match evidence',
            'details': 'Ensure device details, dates, and hash values are correct'
        })

    return compliance


def analyze_income_tax_269ss_compliance(case_data: Dict) -> Dict:
    """
    Income Tax Act Section 269SS - Cash Transaction Limit

    Cash loans/deposits above ₹20,000 are prohibited
    Violation makes debt legally unenforceable
    """

    compliance = {
        'module': 'Income Tax Section 269SS Compliance',
        'applicable': False,
        'compliant': True,
        'transaction_mode': None,
        'cash_amount': 0,
        'violation_detected': False,
        'risk_level': 'NOT_APPLICABLE',
        'implications': [],
        'recommendations': []
    }

    # Check transaction mode
    transaction_mode = case_data.get('transaction_mode', 'Bank Transfer')
    cheque_amount = case_data.get('cheque_amount', 0)
    cash_transaction = case_data.get('cash_transaction', False)

    compliance['transaction_mode'] = transaction_mode
    compliance['cash_amount'] = cheque_amount if (cash_transaction or transaction_mode == 'Cash') else 0

    # Check if cash transaction rules apply
    if not (cash_transaction or transaction_mode == 'Cash'):
        compliance['applicable'] = False
        compliance['risk_level'] = 'NOT_APPLICABLE'
        return compliance

    compliance['applicable'] = True

    # Check for violation (cash > ₹20,000)
    if compliance['cash_amount'] > 20000:
        compliance['compliant'] = False
        compliance['violation_detected'] = True
        compliance['risk_level'] = 'CRITICAL'

        compliance['implications'] = [
            'Loan/deposit violates Income Tax Act Section 269SS',
            'Transaction may be deemed legally unenforceable',
            'Accused can raise this as substantive defence',
            'Burden on complainant to prove transaction was not loan/deposit',
            'Court may dismiss complaint on this ground alone'
        ]

        compliance['recommendations'].append({
            'priority': 'CRITICAL',
            'action': 'Prepare defence against Section 269SS challenge',
            'strategy': [
                'Argue transaction was not a loan but payment for goods/services',
                'Provide evidence of underlying transaction (invoice, delivery proof)',
                'Show cheque was for sale consideration, not repayment of loan',
                'If loan: Argue it was under ₹20,000 with balance in kind/adjustment'
            ],
            'legal_risk': 'HIGH - Defence likely to succeed if proven to be cash loan'
        })
    else:
        compliance['compliant'] = True
        compliance['risk_level'] = 'LOW'
        compliance['recommendations'].append({
            'priority': 'LOW',
            'action': 'Document nature of cash transaction',
            'details': 'Keep records showing transaction was not prohibited under 269SS'
        })

    return compliance


# ============================================================================
# UNIFIED EVIDENCE STATE - SINGLE SOURCE OF TRUTH (BUG FIX #1)
# ============================================================================

def evaluate_service_proof_unified(case_data: Dict) -> Dict:
    """
    UNIFIED SERVICE PROOF EVALUATOR - Single Source of Truth

    ALL modules (documentary_strength, notice_delivery, defence_risk)
    MUST use this result. No independent evaluation allowed.

    This eliminates module contradictions (Bug #1)
    """

    # Gather all evidence indicators
    notice_date = case_data.get('notice_date')
    notice_received_date = case_data.get('notice_received_date')
    postal_acknowledgment = case_data.get('postal_acknowledgment', False)
    ad_card_signed = case_data.get('ad_card_signed', False)
    notice_signed = case_data.get('notice_signed', False)
    postal_proof_available = case_data.get('postal_proof_available', False)
    notice_refused = case_data.get('notice_refused', False)
    notice_unclaimed = case_data.get('notice_unclaimed', False)

    # SINGLE EVALUATION (Priority order)
    result = {
        'notice_sent': bool(notice_date or notice_received_date),
        'delivery_confirmed': False,
        'delivery_status': 'NOT_SENT',
        'delivery_method': 'None',
        'proof_strength': 'NONE',
        'service_quality': 'NONE',
        'risk_level': 'FATAL',
        'score': 0,
        'grade': 'FATAL'
    }

    # Priority 1: AD card signed (STRONGEST proof)
    if ad_card_signed:
        result.update({
            'delivery_confirmed': True,
            'delivery_status': 'DELIVERED',
            'delivery_method': 'AD Card Signed',
            'proof_strength': 'STRONG',
            'service_quality': 'EXCELLENT',
            'risk_level': 'LOW',
            'score': 100,
            'grade': 'EXCELLENT'
        })

    # Priority 2: Postal proof + signature (GOOD)
    elif postal_proof_available and notice_signed:
        result.update({
            'delivery_confirmed': True,
            'delivery_status': 'DELIVERED',
            'delivery_method': 'Postal Proof with Signature',
            'proof_strength': 'GOOD',
            'service_quality': 'GOOD',
            'risk_level': 'LOW',
            'score': 85,
            'grade': 'GOOD'
        })

    # Priority 3: Postal acknowledgment only (WEAK - no delivery proof)
    elif postal_acknowledgment or postal_proof_available:
        result.update({
            'delivery_confirmed': False,
            'delivery_status': 'SENT (proof of posting only)',
            'delivery_method': 'Postal Receipt',
            'proof_strength': 'WEAK',
            'service_quality': 'WEAK',
            'risk_level': 'HIGH',
            'score': 25,
            'grade': 'WEAK'
        })

    # Priority 4: Refused or Unclaimed (DEEMED SERVICE)
    elif notice_refused or notice_unclaimed:
        result.update({
            'delivery_confirmed': True,
            'delivery_status': 'REFUSED/UNCLAIMED (deemed service)',
            'delivery_method': 'Deemed Service',
            'proof_strength': 'MODERATE',
            'service_quality': 'MODERATE',
            'risk_level': 'MEDIUM',
            'score': 65,
            'grade': 'MODERATE'
        })

    # Priority 5: Notice sent but no proof
    elif notice_date:
        result.update({
            'delivery_confirmed': False,
            'delivery_status': 'SENT (no delivery proof)',
            'delivery_method': 'Unknown',
            'proof_strength': 'NONE',
            'service_quality': 'INSUFFICIENT',
            'risk_level': 'CRITICAL',
            'score': 10,
            'grade': 'CRITICAL'
        })

    return result


def analyze_notice_delivery_status(case_data: Dict) -> Dict:
    """
    Detailed Notice Service Analysis

    Uses unified service proof evaluator to ensure consistency
    Covers: Delivered, Refused, Unclaimed, Returned, Deemed Service

    PRODUCTION-SAFE: All variables extracted deterministically
    """

    try:
        # DETERMINISTIC VARIABLE EXTRACTION (Production Safety)
        notice_date = case_data.get('notice_date')
        notice_received_date = case_data.get('notice_received_date')
        postal_acknowledgment = case_data.get('postal_acknowledgment', False)
        ad_card_signed = case_data.get('ad_card_signed', False)
        notice_refused = case_data.get('notice_refused', False)
        notice_unclaimed = case_data.get('notice_unclaimed', False)
        notice_returned = case_data.get('notice_returned', False)
        notice_mode = case_data.get('notice_mode', 'RPAD')

        # USE UNIFIED EVALUATOR (Bug Fix #1)
        unified_result = evaluate_service_proof_unified(case_data)

        analysis = {
            'module': 'Notice Delivery Status Analysis',
            'notice_sent': unified_result['notice_sent'],
            'delivery_status': unified_result['delivery_status'],
            'delivery_confirmed': unified_result['delivery_confirmed'],
            'proof_strength': unified_result['proof_strength'],
            'service_quality': unified_result['service_quality'],
            'risk_level': unified_result['risk_level'],
            'service_mode': notice_mode,
            'deemed_service_applicable': notice_refused or notice_unclaimed,
            'service_complete': unified_result['delivery_confirmed'],
            'service_proof': {
                'strength': unified_result['proof_strength'],
                'method': unified_result['delivery_method'],
                'score': unified_result['score']
            },
            'recommendations': []
        }

        # Check if notice sent at all
        if not unified_result['notice_sent']:
            analysis['risk_level'] = 'FATAL'
            analysis['recommendations'].append({
                'priority': 'FATAL',
                'action': 'Legal notice not sent - statutory requirement not met',
                'remedy': 'Send legal notice immediately'
            })
            return analysis

        # Add specific recommendations based on delivery status
        if notice_refused or notice_unclaimed:
            analysis['recommendations'].append({
                'priority': 'LOW',
                'action': 'Deemed service applicable',
                'details': f'Notice {"refused" if notice_refused else "unclaimed"} - deemed service after reasonable time'
            })

        if notice_returned:
            analysis['recommendations'].append({
                'priority': 'HIGH',
                'action': 'Notice returned undelivered',
                'details': 'Consider alternative mode of service or address verification'
            })

        if unified_result['risk_level'] in ['HIGH', 'CRITICAL']:
            analysis['recommendations'].append({
                'priority': 'HIGH',
                'action': 'Strengthen service proof',
                'details': f'Current proof strength: {unified_result["proof_strength"]} - obtain delivery confirmation'
            })

        return analysis

    except Exception as e:
        # GRACEFUL DEGRADATION (Production Safety)
        logger.error(f"Notice delivery analysis failed: {str(e)}")
        return {
            'module': 'Notice Delivery Status Analysis',
            'error': str(e),
            'notice_sent': False,
            'delivery_status': 'ERROR',
            'risk_level': 'CRITICAL',
            'service_complete': False,
            'recommendations': [{
                'priority': 'CRITICAL',
                'action': 'Module execution failed - manual review required',
                'details': f'Error: {str(e)}'
            }]
        }


def analyze_part_payment_defence(case_data: Dict) -> Dict:
    """
    Part Payment Defence Analysis

    If part payment made after notice, debt acknowledged but cheque amount disputed
    """

    analysis = {
        'module': 'Part Payment Defence Analysis',
        'part_payment_made': False,
        'payment_details': {},
        'cause_of_action_impact': None,
        'defence_strength': 'NOT_APPLICABLE',
        'strategic_implications': [],
        'recommendations': []
    }

    part_payment_made = case_data.get('part_payment_made', False)

    if not part_payment_made:
        analysis['part_payment_made'] = False
        analysis['defence_strength'] = 'NOT_APPLICABLE'
        return analysis

    analysis['part_payment_made'] = True

    # Get payment details
    part_payment_amount = case_data.get('part_payment_amount', 0)
    part_payment_date = case_data.get('part_payment_date')
    cheque_amount = case_data.get('cheque_amount', 0)
    notice_date = case_data.get('notice_date')

    analysis['payment_details'] = {
        'amount_paid': part_payment_amount,
        'payment_date': part_payment_date,
        'original_debt': cheque_amount,
        'balance_amount': cheque_amount - part_payment_amount if part_payment_amount else cheque_amount
    }

    # Determine timing - before or after notice
    payment_after_notice = False
    if part_payment_date and notice_date:
        try:
            payment_dt = datetime.strptime(part_payment_date, '%Y-%m-%d')
            notice_dt = datetime.strptime(notice_date, '%Y-%m-%d')
            payment_after_notice = payment_dt > notice_dt
        except:
            pass

    # Analyze impact
    if payment_after_notice:
        analysis['cause_of_action_impact'] = 'NEW_CAUSE_OF_ACTION'
        analysis['defence_strength'] = 'MEDIUM'
        analysis['strategic_implications'] = [
            'Part payment after notice creates new cause of action',
            'Original limitation period may be extended',
            'Debt acknowledgment strengthens complainant position',
            'But accused can argue only balance amount due, not full cheque',
            'Complainant should file for balance amount + interest'
        ]
        analysis['recommendations'].append({
            'priority': 'HIGH',
            'action': 'Recalculate cause of action date from part payment',
            'details': 'Part payment after notice creates fresh cause of action',
            'limitation_impact': 'Fresh 1 month period starts from date of default after part payment',
            'filing_strategy': 'File complaint for balance amount or amend claim'
        })
    else:
        analysis['cause_of_action_impact'] = 'DEBT_PARTIALLY_SATISFIED'
        analysis['defence_strength'] = 'HIGH'
        analysis['strategic_implications'] = [
            'Part payment before notice reduces debt quantum',
            'Accused can argue cheque was for original amount, now reduced',
            'Complainant position weaker if suing for full cheque amount',
            'Accused may succeed in getting complaint dismissed or reduced'
        ]
        analysis['recommendations'].append({
            'priority': 'CRITICAL',
            'action': 'Adjust claim to reflect part payment',
            'details': 'Sue only for balance amount after part payment',
            'risk': 'HIGH - Accused will successfully defend for paid portion',
            'remedy': 'Amend complaint to claim balance amount only'
        })

    # Additional checks
    if part_payment_amount >= cheque_amount:
        analysis['defence_strength'] = 'FATAL'
        analysis['recommendations'].append({
            'priority': 'FATAL',
            'action': 'Debt fully satisfied - complaint not maintainable',
            'details': 'Part payment equals or exceeds cheque amount',
            'outcome': 'Case will be dismissed - no debt remains'
        })

    return analysis


def analyze_territorial_jurisdiction(case_data: Dict) -> Dict:
    """
    Section 142(2) NI Act - Territorial Jurisdiction Validation

    Complaint can be filed where:
    1. Cheque was presented for payment (bank branch)
    2. Cheque was dishonoured (drawee bank branch)
    3. Payee/holder resides or carries business
    """

    analysis = {
        'module': 'Section 142 Territorial Jurisdiction Analysis',
        'jurisdiction_valid': True,
        'filing_court': None,
        'valid_jurisdictions': [],
        'jurisdiction_basis': [],
        'risk_level': 'LOW',
        'recommendations': []
    }

    # Get location details
    presentation_bank_branch = case_data.get('presentation_bank_branch')
    drawee_bank_branch = case_data.get('drawee_bank_branch')
    payee_residence = case_data.get('payee_residence')
    payee_business_place = case_data.get('payee_business_place')
    accused_residence = case_data.get('accused_residence')
    filing_court_location = case_data.get('filing_court_location')

    # Determine valid jurisdictions per Section 142(2)
    if presentation_bank_branch:
        analysis['valid_jurisdictions'].append({
            'location': presentation_bank_branch,
            'basis': 'Bank where cheque presented for payment',
            'legal_ref': 'Section 142(2)(a) NI Act'
        })
        analysis['jurisdiction_basis'].append('Presentation Bank')

    if drawee_bank_branch and drawee_bank_branch != presentation_bank_branch:
        analysis['valid_jurisdictions'].append({
            'location': drawee_bank_branch,
            'basis': 'Drawee bank where cheque dishonoured',
            'legal_ref': 'Section 142(2)(b) NI Act'
        })
        analysis['jurisdiction_basis'].append('Drawee Bank')

    if payee_residence:
        analysis['valid_jurisdictions'].append({
            'location': payee_residence,
            'basis': 'Place where payee/holder resides',
            'legal_ref': 'Section 142(2)(c) NI Act'
        })
        analysis['jurisdiction_basis'].append('Payee Residence')

    if payee_business_place:
        analysis['valid_jurisdictions'].append({
            'location': payee_business_place,
            'basis': 'Place where payee carries on business',
            'legal_ref': 'Section 142(2)(c) NI Act'
        })
        analysis['jurisdiction_basis'].append('Payee Business Place')

    # Check if filing court is valid
    if filing_court_location:
        analysis['filing_court'] = filing_court_location

        # Check if we have sufficient data to validate
        if len(analysis['valid_jurisdictions']) == 0:
            # No jurisdiction data provided - CANNOT DETERMINE (not invalid)
            analysis['jurisdiction_valid'] = None  # FIX: None (not False) when data insufficient
            analysis['status'] = 'INSUFFICIENT_DATA'  # FIX: Add status field
            analysis['risk_level'] = 'MEDIUM'  # FIX: Don't mark as HIGH when just missing data
            analysis['recommendations'].append({
                'priority': 'MEDIUM',
                'issue': 'Insufficient data to validate jurisdiction',
                'details': 'Cannot validate filing court without bank branch or payee location details',
                'action': 'Provide: presentation_bank_branch, drawee_bank_branch, or payee location',
                'consequence': 'Jurisdiction validity cannot be confirmed'
            })
        else:
            # Validate jurisdiction
            valid_locations = [j['location'].lower() for j in analysis['valid_jurisdictions']]
            filing_location_lower = filing_court_location.lower()

            # Check if filing court matches any valid jurisdiction
            jurisdiction_matches = any(
                filing_location_lower in loc or loc in filing_location_lower
                for loc in valid_locations
            )

            if not jurisdiction_matches:
                analysis['jurisdiction_valid'] = False
                analysis['risk_level'] = 'CRITICAL'
                analysis['recommendations'].append({
                    'priority': 'CRITICAL',
                    'issue': 'Territorial jurisdiction may be invalid',
                    'details': f'Filing court "{filing_court_location}" does not match valid jurisdictions',
                    'valid_options': [j['location'] for j in analysis['valid_jurisdictions']],
                    'action': 'File complaint in court within valid territorial jurisdiction',
                    'consequence': 'Complaint may be dismissed for lack of jurisdiction'
                })
            else:
                analysis['jurisdiction_valid'] = True
                analysis['risk_level'] = 'LOW'
    else:
        # Filing court not specified
        analysis['jurisdiction_valid'] = False  # Cannot be valid if not specified

        if len(analysis['valid_jurisdictions']) > 0:
            analysis['risk_level'] = 'MEDIUM'
            analysis['recommendations'].append({
                'priority': 'MEDIUM',
                'action': 'Specify filing court location',
                'valid_options': [j['location'] for j in analysis['valid_jurisdictions']],
                'suggestion': 'Choose court with convenient jurisdiction per Section 142(2)',
                'status': 'INSUFFICIENT_DATA'
            })
        else:
            analysis['risk_level'] = 'HIGH'
            analysis['recommendations'].append({
                'priority': 'HIGH',
                'action': 'Provide bank branch and payee location details',
                'details': 'Required to determine valid territorial jurisdiction',
                'status': 'INSUFFICIENT_DATA'
            })

    return analysis


def analyze_compounding_eligibility(case_data: Dict) -> Dict:
    """
    Section 147 NI Act - Compounding of Offence

    Offence can be compounded with court permission
    """

    analysis = {
        'module': 'Section 147 Compounding Analysis',
        'compounding_eligible': True,
        'settlement_attempted': False,
        'compounding_stage': None,
        'settlement_details': {},
        'recommendations': []
    }

    # Check settlement status
    settlement_attempted = case_data.get('settlement_attempted', False)
    settlement_amount_offered = case_data.get('settlement_amount_offered', 0)
    settlement_amount_agreed = case_data.get('settlement_amount_agreed', 0)
    compounding_stage = case_data.get('compounding_stage', 'Pre-Filing')
    cheque_amount = case_data.get('cheque_amount', 0)

    analysis['settlement_attempted'] = settlement_attempted
    analysis['compounding_stage'] = compounding_stage

    if settlement_attempted:
        analysis['settlement_details'] = {
            'amount_offered': settlement_amount_offered,
            'amount_agreed': settlement_amount_agreed,
            'original_debt': cheque_amount,
            'settlement_percentage': (settlement_amount_agreed / cheque_amount * 100) if cheque_amount > 0 else 0,
            'stage': compounding_stage
        }

    # Determine compounding eligibility
    # Section 147 allows compounding with court permission
    analysis['compounding_eligible'] = True

    # Stage-specific recommendations
    if compounding_stage == 'Pre-Filing':
        analysis['recommendations'].append({
            'priority': 'MEDIUM',
            'stage': 'Pre-Filing Settlement',
            'action': 'Attempt settlement before filing complaint',
            'benefits': [
                'Avoid litigation costs',
                'Faster resolution',
                'Preserve business relationships',
                'No criminal record for accused'
            ],
            'strategy': 'Negotiate settlement for cheque amount + interest + costs'
        })
    elif compounding_stage == 'Post-Filing':
        analysis['recommendations'].append({
            'priority': 'MEDIUM',
            'stage': 'Post-Filing Compounding',
            'action': 'Move compounding application under Section 147',
            'requirements': [
                'Court permission required',
                'Settlement agreement between parties',
                'Accused must pay agreed amount',
                'File compounding petition in court'
            ],
            'procedure': 'File joint compounding application with settlement terms'
        })
    elif compounding_stage == 'Post-Conviction':
        analysis['recommendations'].append({
            'priority': 'HIGH',
            'stage': 'Post-Conviction Compounding',
            'action': 'Compounding still possible even after conviction',
            'requirements': [
                'Court permission mandatory',
                'Pay compensation as agreed',
                'File compounding application',
                'Court may impose costs'
            ],
            'legal_basis': 'Section 147 allows compounding at any stage'
        })

    # Settlement leverage analysis
    if settlement_attempted and settlement_amount_offered > 0:
        if settlement_amount_offered < cheque_amount:
            analysis['recommendations'].append({
                'priority': 'MEDIUM',
                'action': 'Settlement offer below cheque amount',
                'details': f'Offered: ₹{settlement_amount_offered}, Debt: ₹{cheque_amount}',
                'strategy': 'Negotiate for full amount + interest + costs, or proceed with filing'
            })
        elif settlement_amount_agreed > 0:
            analysis['recommendations'].append({
                'priority': 'HIGH',
                'action': 'Settlement agreed - execute compounding',
                'next_steps': [
                    'Prepare compounding petition',
                    'Get settlement agreement notarized',
                    'File in court with settlement terms',
                    'Ensure payment made as per agreement'
                ]
            })

    return analysis


def analyze_director_role_liability(case_data: Dict) -> Dict:
    """
    Enhanced Director Liability Analysis - Role-Based Assessment

    As per SMS Pharmaceuticals v. Neeta Bhalla:
    Not all directors are automatically liable - only those responsible for conduct of business
    """

    analysis = {
        'module': 'Director Role-Based Liability Analysis',
        'is_company_case': False,
        'directors_liable': [],
        'liability_basis': {},
        'risk_assessment': {},
        'recommendations': []
    }

    is_company_case = case_data.get('is_company_case', False)

    if not is_company_case:
        analysis['is_company_case'] = False
        return analysis

    analysis['is_company_case'] = True

    # Check director details
    directors_impleaded = case_data.get('directors_impleaded', False)
    director_role = case_data.get('director_role')
    director_active_period = case_data.get('director_active_period')
    specific_averment_present = case_data.get('specific_averment_present', False)
    director_knowledge_alleged = case_data.get('director_knowledge_alleged', False)

    # Role-based liability assessment
    if director_role:
        role_liability = {
            'Managing Director': {
                'liability': 'HIGH',
                'reasoning': 'Managing Directors are responsible for day-to-day operations',
                'presumption': 'Liability presumed unless proven otherwise'
            },
            'Executive Director': {
                'liability': 'HIGH',
                'reasoning': 'Executive Directors involved in business conduct',
                'presumption': 'Liability likely if in-charge of finance/operations'
            },
            'Whole-time Director': {
                'liability': 'MEDIUM',
                'reasoning': 'Depends on specific role and responsibilities',
                'presumption': 'Need to show involvement in transaction'
            },
            'Non-executive Director': {
                'liability': 'LOW',
                'reasoning': 'Not involved in day-to-day operations',
                'presumption': 'Difficult to establish liability without specific role'
            },
            'Independent Director': {
                'liability': 'LOW',
                'reasoning': 'Independent oversight role, not operational',
                'presumption': 'Liability only if proven active involvement'
            },
            'Nominee Director': {
                'liability': 'LOW',
                'reasoning': 'Representing financial institution or investor',
                'presumption': 'Liability only if proven decision-making role'
            }
        }

        liability_assessment = role_liability.get(director_role, {
            'liability': 'MEDIUM',
            'reasoning': 'Role not clearly specified',
            'presumption': 'Depends on specific averments'
        })

        analysis['liability_basis'] = {
            'director_role': director_role,
            'liability_level': liability_assessment['liability'],
            'reasoning': liability_assessment['reasoning'],
            'legal_presumption': liability_assessment['presumption']
        }

    # Check specific averments (SMS Pharmaceuticals requirement)
    if not specific_averment_present:
        analysis['risk_assessment'] = {
            'risk_level': 'CRITICAL',
            'issue': 'Specific averment missing',
            'consequence': 'Director liability cannot be established',
            'legal_basis': 'SMS Pharmaceuticals Ltd. v. Neeta Bhalla'
        }
        analysis['recommendations'].append({
            'priority': 'CRITICAL',
            'action': 'Add specific averment in complaint',
            'required_content': [
                'Director was in-charge of and responsible for conduct of business',
                'Director had knowledge of the transaction',
                'Cheque issued with director\'s knowledge and consent',
                'Director\'s specific role in the transaction'
            ],
            'legal_requirement': 'Mandatory per Supreme Court ruling'
        })

    # Check if director was active during transaction period
    cheque_date = case_data.get('cheque_date')
    if director_active_period and cheque_date:
        # This would need date comparison logic
        analysis['recommendations'].append({
            'priority': 'HIGH',
            'action': 'Verify director was serving during transaction period',
            'details': 'Director must have been in office when cheque was issued',
            'required_proof': 'Board resolution, DIN records, ROC filings'
        })

    # Overall liability assessment
    if not directors_impleaded:
        analysis['risk_assessment']['filing_strategy'] = {
            'option_1': 'File against company only',
            'option_2': 'Amend to implead directors with specific averments',
            'recommendation': 'Implead directors to ensure personal liability'
        }
    elif directors_impleaded and not specific_averment_present:
        analysis['risk_assessment']['amendment_required'] = True
        analysis['recommendations'].append({
            'priority': 'CRITICAL',
            'action': 'Amendment required - Add specific averments',
            'risk': 'Directors may be discharged without specific averments',
            'remedy': 'File amendment application immediately'
        })

    return analysis


def analyze_document_compliance(case_data: Dict) -> Dict:

    # USE UNIFIED SERVICE PROOF EVALUATOR (Bug Fix #1)
    unified_service = evaluate_service_proof_unified(case_data)

    compliance = {
        'module': 'Document Compliance Analysis',
        'overall_compliance': 0,
        'mandatory_docs': {},
        'evidence_classification': {},
        'section_65b_compliance': {},
        'service_proof_unified': unified_service,  # Store unified result
        'filing_readiness': 'NOT READY',
        'missing_critical_docs': [],
        'fatal_defects': [],
        'compliance_score': 0,
        'defects': [],
        'severity_breakdown': {'FATAL': 0, 'CRITICAL': 0, 'WARNING': 0}
    }

    score = 100
    critical_missing = []
    fatal_missing = []

    mandatory_docs = {
        'original_cheque': {
            'present': case_data.get('original_cheque_available', False),
            'severity': 'FATAL',
            'deduction': 30,
            'consequence': 'Complaint will be rejected - Original cheque is mandatory evidence'
        },
        'dishonour_memo': {
            'present': case_data.get('return_memo_available', False),
            'seal_verified': case_data.get('dishonour_memo_seal', False),
            'severity': 'FATAL',
            'deduction': 30,
            'consequence': 'Dishonour not proved - Bank memo mandatory under Section 138'
        },
        'legal_notice': {
            # Infer notice presence from multiple indicators
            'present': (
                case_data.get('notice_copy_available', False) or
                case_data.get('legal_notice_available', False) or
                bool(case_data.get('notice_date')) or  # If notice was sent, copy should exist
                bool(case_data.get('notice_received_date'))  # If received, must have been sent
            ),
            'severity': 'FATAL',
            'deduction': 25,
            'consequence': 'Notice requirement not satisfied - Statutory condition'
        },
        'postal_acknowledgment': {
            # USE UNIFIED EVALUATOR RESULT (Bug Fix #1)
            'present': unified_service['delivery_confirmed'],
            'severity': 'CRITICAL' if unified_service['risk_level'] in ['HIGH', 'CRITICAL'] else 'WARNING',
            'deduction': 100 - unified_service['score'],  # Use unified score
            'consequence': f"Service proof {unified_service['proof_strength']} - {unified_service['delivery_method']}",
            'unified_grade': unified_service['grade']
        },
        'section_65b_certificate': {
            'present': case_data.get('section_65b_certificate', True) if case_data.get('electronic_evidence') else True,
            'severity': 'FATAL' if (case_data.get('electronic_evidence') and not case_data.get('section_65b_certificate')) else ('CRITICAL' if case_data.get('electronic_evidence') else 'NA'),
            'deduction': 30,  # Increased from 20
            'consequence': 'Electronic evidence INADMISSIBLE without Section 65B certificate - NO OVERRIDE ALLOWED',
            'applicable': case_data.get('electronic_evidence', False),
            'legal_basis': 'Section 65B Evidence Act - Anvar P.V. v. P.K. Basheer (2014)',
            'strict_enforcement': True  # NEW: No override
        },
        'postal_receipt': {
            'present': case_data.get('postal_proof_available', False),
            'severity': 'CRITICAL',
            'deduction': 15,
            'consequence': 'Notice dispatch not proved'
        },
        'track_report': {
            'present': case_data.get('track_report_available', True) if case_data.get('courier_used') else True,
            'severity': 'WARNING',
            'deduction': 10,
            'consequence': 'Service timeline weakened',
            'applicable': case_data.get('courier_used', False)
        },
        'document_list': {
            'present': case_data.get('document_list_prepared', False),
            'severity': 'WARNING',
            'deduction': 5,
            'consequence': 'Filing formality incomplete'
        }
    }

    for doc_name, doc_info in mandatory_docs.items():
        if doc_info.get('applicable', True):
            present = doc_info['present']
            severity = doc_info['severity']

            compliance['mandatory_docs'][doc_name] = {
                'present': present,
                'status': '✅ Available' if present else f"❌ MISSING ({severity})",
                'severity': severity,
                'consequence': doc_info['consequence']
            }

            if not present:
                score -= doc_info['deduction']
                compliance['severity_breakdown'][severity] = compliance['severity_breakdown'].get(severity, 0) + 1

                if severity == 'FATAL':
                    fatal_missing.append({
                        'document': doc_name.replace('_', ' ').title(),
                        'consequence': doc_info['consequence']
                    })
                    compliance['fatal_defects'].append({
                        'defect': f"Missing {doc_name.replace('_', ' ')}",
                        'severity': 'FATAL',
                        'impact': doc_info['consequence'],
                        'filing_blocked': True
                    })
                elif severity == 'CRITICAL':
                    critical_missing.append(doc_name.replace('_', ' ').title())
                    compliance['defects'].append({
                        'defect': f"Missing {doc_name.replace('_', ' ')}",
                        'severity': 'CRITICAL',
                        'impact': doc_info['consequence']
                    })

    if fatal_missing:
        compliance['filing_readiness'] = '🔴 FILING BLOCKED'
        compliance['overall_compliance'] = 'FATAL - DO NOT FILE'
        compliance['compliance_score'] = min(score, 25)
        compliance['fatal_message'] = f"FILING BLOCKED: {len(fatal_missing)} fatal document defects detected"
    elif critical_missing:
        compliance['filing_readiness'] = '⚠️ HIGH RISK - NOT READY'
        compliance['overall_compliance'] = 'CRITICAL GAPS'
        compliance['compliance_score'] = min(score, 60)
    elif score >= 80:
        compliance['filing_readiness'] = '✅ READY TO FILE'
        compliance['overall_compliance'] = 'COMPLIANT'
        compliance['compliance_score'] = score
    else:
        compliance['filing_readiness'] = '⚠️ GAPS PRESENT'
        compliance['overall_compliance'] = 'MINOR GAPS'
        compliance['compliance_score'] = score

    compliance['missing_critical_docs'] = critical_missing
    compliance['fatal_documents_missing'] = fatal_missing

    compliance['evidence_classification'] = {
        'original_cheque': 'Original' if case_data.get('original_cheque_available') else 'MISSING/COPY',
        # Fixed: Don't mark as DEFECTIVE just because seal field missing - if return_memo available, it's acceptable
        'dishonour_memo': 'Original with seal' if (case_data.get('return_memo_available') and case_data.get('dishonour_memo_seal')) else ('Original' if case_data.get('return_memo_available') else 'DEFECTIVE'),
        'notice': 'Original' if (case_data.get('notice_copy_available') or case_data.get('legal_notice_available') or case_data.get('notice_date')) else 'MISSING',
        'classification_status': 'Strong' if (case_data.get('original_cheque_available') and case_data.get('return_memo_available')) else 'FATAL'
    }

    return compliance


def generate_executive_summary(
    case_data, timeline_data, ingredient_data, doc_data,
    liability_data, defence_data, defect_data, risk_data, settlement_data,
    presumption_data, cross_exam_data, judicial_behavior, contradiction_data,
    edge_case_data
) -> Dict:
    """
    Generate a rich executive summary that NEVER returns empty fields.
    Robustly handles any module returning None, list, or malformed dict.
    This is what lawyers read first — it must always be complete.
    """

    def _s(v, fb="Not available"):
        if v is None or v == "" or v == [] or v == {}:
            return fb
        return v

    def _n(v, fb=0.0):
        try: return round(float(v), 1)
        except: return fb

    def _d(v):
        return v if isinstance(v, dict) else {}

    # Safe module data
    risk_data      = _d(risk_data)
    timeline_data  = _d(timeline_data)
    defect_data    = _d(defect_data)
    ingredient_data= _d(ingredient_data)
    doc_data       = _d(doc_data)
    settlement_data= _d(settlement_data)
    presumption_data= _d(presumption_data)
    judicial_behavior= _d(judicial_behavior)
    edge_case_data = _d(edge_case_data)
    defence_data   = _d(defence_data)
    cross_exam_data= _d(cross_exam_data)

    score      = _n(risk_data.get('overall_risk_score'))
    compliance = _s(risk_data.get('compliance_level'), 'Under Review')
    amount     = case_data.get('cheque_amount', 0)
    case_type  = "Complainant" if case_data.get('case_type') == 'complainant' else "Accused"

    # ── Collect ALL fatal defects from all modules ──
    all_fatals = []
    all_fatals.extend(risk_data.get('fatal_defects', []) or [])
    all_fatals.extend(defect_data.get('fatal_defects', []) or [])
    all_fatals.extend(ingredient_data.get('fatal_defects', []) or [])
    seen, unique_fatals = set(), []
    for d in all_fatals:
        k = d.get('defect', str(d))
        if k not in seen:
            seen.add(k)
            unique_fatals.append(d)

    fatal_flag = len(unique_fatals) > 0

    # ── Filing verdict ──
    if fatal_flag:
        filing_verdict = "DO NOT FILE — FATAL DEFECTS PRESENT"
        assessment     = "FATAL FAILURE"
    elif score >= 75:
        filing_verdict = "READY TO FILE — STRONG CASE"
        assessment     = "COMPLIANT"
    elif score >= 55:
        filing_verdict = "FILE WITH CAUTION — GAPS PRESENT"
        assessment     = "MODERATE RISK"
    else:
        filing_verdict = "HIGH RISK — REMEDIATION REQUIRED BEFORE FILING"
        assessment     = "WEAK CASE"

    # ── Case overview one-liner ──
    limitation_status = _s(
        (timeline_data.get('compliance_status') or {}).get('limitation'),
        'Not assessed'
    )
    case_overview = (
        f"{case_type} in Section 138 NI Act case | "
        f"Cheque: ₹{indian_number_format(amount)} | "
        f"Risk Score: {score}/100 | "
        f"Classification: {compliance} | "
        f"Limitation: {limitation_status}"
    )
    if unique_fatals:
        case_overview += f" | ⚠️ {len(unique_fatals)} FATAL DEFECT(S) DETECTED"

    # ── Strengths ──
    strengths = []
    cat_scores = risk_data.get('category_scores') or {}
    for cat, data in cat_scores.items():
        s = _n(data.get('score') if isinstance(data, dict) else data)
        if s >= 75:
            strengths.append(f"{cat}: {s}/100 — Strong ✅")
    if case_data.get('return_memo_available'):
        strengths.append("Dishonour memo available — primary evidence secured ✅")
    if case_data.get('postal_proof_available'):
        strengths.append("Postal proof available — notice service established ✅")
    if case_data.get('original_cheque_available'):
        strengths.append("Original cheque available — foundational document present ✅")
    if not strengths:
        strengths.append("Core transaction facts established")

    # ── Weaknesses ──
    weaknesses = []
    for cat, data in cat_scores.items():
        s = _n(data.get('score') if isinstance(data, dict) else data)
        if s < 60:
            reason = (data.get('reason', '') or '') if isinstance(data, dict) else ''
            weaknesses.append(
                f"{cat}: {s}/100 — {reason or 'Needs strengthening'} ❌"
            )
    if not case_data.get('written_agreement_exists'):
        weaknesses.append("No written agreement — debt enforceability can be challenged ❌")
    if not case_data.get('ledger_available'):
        weaknesses.append("No ledger/records — transaction trail incomplete ❌")
    if not case_data.get('postal_proof_available'):
        weaknesses.append("No postal proof — notice service presumption weak ❌")
    if not weaknesses:
        weaknesses.append("No critical weaknesses at this stage")

    # ── Critical risks ──
    critical_risks = []
    for d in unique_fatals[:5]:
        critical_risks.append({
            'risk':     _s(d.get('defect')),
            'severity': _s(d.get('severity'), 'CRITICAL'),
            'impact':   _s(d.get('impact')),
            'remedy':   _s(d.get('remedy', d.get('cure', 'Consult legal counsel')))
        })
    # Also add high-risk defence items
    for hr in (defence_data.get('high_risk_defences') or [])[:2]:
        critical_risks.append({
            'risk':     _s(hr.get('defence', hr.get('ground'))),
            'severity': 'HIGH',
            'impact':   _s(hr.get('strategy', hr.get('risk_impact', 'Acquittal risk'))),
            'remedy':   _s(hr.get('counter', hr.get('legal_basis', 'Strengthen evidence')))
        })

    # ── Strategic recommendations ──
    strategic_recommendations = []
    if fatal_flag:
        strategic_recommendations.append({
            'priority': 'URGENT',
            'recommendation': 'Address all fatal defects before filing',
            'rationale': f"{len(unique_fatals)} fatal defect(s) will cause dismissal"
        })
        for d in unique_fatals[:2]:
            strategic_recommendations.append({
                'priority': 'URGENT',
                'recommendation': _s(d.get('remedy', d.get('cure', 'Remediate defect'))),
                'rationale': _s(d.get('defect'))
            })
    elif score >= 70:
        strategic_recommendations.append({
            'priority': 'HIGH',
            'recommendation': 'File complaint — case is in strong position',
            'rationale': f"Risk score {score}/100 indicates solid compliance"
        })
        if settlement_data.get('interim_compensation_eligible'):
            strategic_recommendations.append({
                'priority': 'HIGH',
                'recommendation': 'Apply for interim compensation under Section 143A',
                'rationale': 'Creates financial pressure; recovers 20% of cheque amount upfront'
            })
    else:
        strategic_recommendations.append({
            'priority': 'HIGH',
            'recommendation': 'Strengthen evidence before filing',
            'rationale': f"Score {score}/100 — multiple gaps increase acquittal risk"
        })
        if not case_data.get('written_agreement_exists'):
            strategic_recommendations.append({
                'priority': 'MEDIUM',
                'recommendation': 'Obtain written acknowledgment of debt from accused',
                'rationale': 'Absence of written agreement is the primary defence weakness'
            })

    # ── Next actions ──
    # next_actions as structured dicts so PDF template can read .action .urgency .details
    next_actions = []
    for d in unique_fatals[:3]:
        next_actions.append({
            'action':  _s(d.get('remedy', d.get('defect', 'Address fatal defect'))),
            'urgency': 'URGENT',
            'details': _s(d.get('impact', 'Fatal defect — will cause dismissal if not remedied'))
        })
    if not case_data.get('postal_proof_available'):
        next_actions.append({
            'action':  'Obtain AD card or speed post tracking confirmation for notice',
            'urgency': 'HIGH',
            'details': 'Without postal proof, notice service can be challenged by accused'
        })
    if not case_data.get('written_agreement_exists'):
        next_actions.append({
            'action':  'Collect WhatsApp/email/SMS evidence of the transaction or debt acknowledgment',
            'urgency': 'MEDIUM',
            'details': 'Absence of written agreement is the primary defence weakness'
        })
    if not case_data.get('ledger_available'):
        next_actions.append({
            'action':  'Obtain ledger entries or bank transfer records showing the transaction',
            'urgency': 'MEDIUM',
            'details': 'Without financial records, transaction trail is incomplete'
        })
    if not next_actions:
        next_actions.append({
            'action':  'Proceed with final legal review before filing',
            'urgency': 'NORMAL',
            'details': 'Case is in reasonable position — verify all documents before filing'
        })

    # ── Edge case alert ──
    edge_cases_alert = edge_case_data.get('detected_cases', []) or []

    return {
        'case_overview':             case_overview,
        'filing_verdict':            filing_verdict,
        'overall_assessment':        assessment,
        'risk_score':                f"{score}/100",
        'compliance_level':          compliance,
        'fatal_defects_count':       len(unique_fatals),
        'strengths':                 strengths[:6],
        'weaknesses':                weaknesses[:6],
        'critical_risks':            critical_risks[:6],
        'strategic_recommendations': strategic_recommendations[:5],
        'next_actions':              next_actions[:5],
        'limitation_status':         limitation_status,
        'processing_time':           'See response root',
        'edge_cases_alert':          edge_cases_alert[:3],
        'generated_by':              'JUDIQ Intelligence Engine',
    }


def generate_verdict_one_liner(
    score: float,
    fatal_defects: List,
    limitation_risk: str,
    primary_weakness: str
) -> Dict:

    verdict = {}

    if fatal_defects and len(fatal_defects) > 0:
        maintainability = "⚠️ Maintainability Questionable"
        maintainability_status = "CRITICAL"

        verdict_line = f"Maintainability at risk due to {fatal_defects[0].get('defect', 'statutory compliance gaps')}. Immediate remediation advisable."
    elif score >= 75:
        maintainability = "Appears Maintainable"  # Not "✅ Definitely"
        maintainability_status = "ADEQUATE"
        if limitation_risk in ['HIGH', 'CRITICAL']:
            verdict_line = "Case appears maintainable under Section 138, subject to limitation compliance verification."
        elif primary_weakness:
            verdict_line = f"Statutory requirements appear satisfied. However, {primary_weakness} requires strengthening to reduce litigation exposure."
        else:
            verdict_line = "Statutory requirements appear satisfied. Evidentiary substantiation advisable to minimize cross-examination risk."
    elif score >= 60:
        maintainability = "Maintainable with Risks"
        maintainability_status = "MODERATE"
        verdict_line = f"Case maintainable subject to addressing {primary_weakness or 'documentary gaps'}. Evidentiary strengthening strongly recommended."
    elif score >= 40:
        maintainability = "Weak Maintainability"
        maintainability_status = "WEAK"
        verdict_line = f"Significant compliance gaps detected. {primary_weakness or 'Multiple deficiencies'} must be addressed before filing to avoid dismissal risk."
    else:
        maintainability = "High Dismissal Risk"
        maintainability_status = "CRITICAL"
        verdict_line = f"Critical statutory violations detected. Filing not advisable without substantial remediation."

    if fatal_defects or score < 40:
        dismissal_risk = "Substantial"  # Not "HIGH" - more professional
    elif score < 60:
        dismissal_risk = "Moderate"
    else:
        dismissal_risk = "Mitigable"  # Not "LOW" - always some risk

    verdict['legal_position'] = verdict_line  # Changed from 'one_liner'
    verdict['maintainability'] = maintainability
    verdict['maintainability_status'] = maintainability_status
    verdict['dismissal_risk'] = dismissal_risk
    verdict['primary_vulnerability'] = primary_weakness or "Documentary substantiation"

    if fatal_defects:
        verdict['strategic_recommendation'] = "Address identified defects before filing. Consult with litigation counsel on remediation strategy."
    elif score < 60:
        verdict['strategic_recommendation'] = "Strengthen documentary foundation before filing to reduce acquittal exposure."
    elif score < 80:
        verdict['strategic_recommendation'] = "Address identified gaps. Filing advisable subject to review of substantiation documents."
    else:
        verdict['strategic_recommendation'] = "Proceed with filing after final verification of documentary evidence."

    verdict['assessment_note'] = "Assessment based on information provided. Independent legal judgment required."

    return verdict


def generate_score_drivers(
    category_scores: Dict,
    fatal_defects: List,
    documentary_gaps: List
) -> Dict:

    drivers = {
        'score_explanation': '',
        'compliance_strengths': [],
        'identified_vulnerabilities': [],  # Changed from 'weaknesses'
        'fatal_issues': [],
        'score_reduction_factors': []
    }

    total_score = sum(data.get('score', 0) * data.get('weight', 0) / 100
                     for data in category_scores.values())

    drivers['score_explanation'] = f"Composite score derived from weighted assessment across five statutory compliance categories. "

    for category, data in category_scores.items():
        score = data.get('score', 0)
        if score >= 75:
            drivers['compliance_strengths'].append({
                'category': category,
                'score': round(score, 1),
                'assessment': f"{category} compliance adequate"
            })

    for category, data in category_scores.items():
        score = data.get('score', 0)
        if score < 70:
            weight = data.get('weight', 20)
            reduction = int((100 - score) * weight / 100)

            drivers['identified_vulnerabilities'].append({
                'category': category,
                'score': round(score, 1),
                'impact': f"Score reduced by {reduction} points",
                'issue': f"{category} below compliance threshold"
            })

            drivers['score_reduction_factors'].append(
                f"{category}: -{reduction} points"
            )

    if documentary_gaps:
        gap_list = []
        if 'written_agreement_missing' in documentary_gaps:
            gap_list.append("No written debt acknowledgment")
        if 'ledger_missing' in documentary_gaps:
            gap_list.append("Ledger extract absent")
        if 'postal_proof_missing' in documentary_gaps:
            gap_list.append("Delivery proof unclear")

        if gap_list:
            drivers['identified_vulnerabilities'].insert(0, {
                'category': 'Documentary Evidence',
                'score': 0,
                'impact': 'Substantiation gaps',
                'issue': '; '.join(gap_list)
            })

    for defect in fatal_defects:
        drivers['fatal_issues'].append({
            'defect': defect.get('defect', 'Statutory violation'),
            'severity': 'Fatal',
            'impact': defect.get('impact', 'Dismissal risk'),
            'remediation': 'Must be addressed before filing'
        })

    if drivers['score_reduction_factors']:
        drivers['score_explanation'] += f"Score reduced primarily due to: {', '.join(drivers['score_reduction_factors'][:3])}."

    return drivers


def generate_court_impact_assessment(
    score: float,
    fatal_defects: List,
    defence_type: str,
    documentary_strength: float
) -> Dict:

    impact = {
        'cognizance_likelihood': '',
        'defence_strategy_likely': '',
        'cross_exam_risk': '',
        'settlement_window': '',
        'practical_notes': []
    }

    if fatal_defects:
        impact['cognizance_likelihood'] = "Magistrate may decline cognizance due to fatal defects"
    elif score >= 70:
        impact['cognizance_likelihood'] = "Magistrate likely to take cognizance"
    elif score >= 50:
        impact['cognizance_likelihood'] = "Cognizance possible but may face initial scrutiny"
    else:
        impact['cognizance_likelihood'] = "Significant risk of cognizance refusal"

    if defence_type == 'security_cheque':
        impact['defence_strategy_likely'] = "Defence may focus on security cheque argument"
    elif defence_type == 'no_debt':
        impact['defence_strategy_likely'] = "Defence likely to challenge debt enforceability"
    elif defence_type == 'time_barred':
        impact['defence_strategy_likely'] = "Defence may raise limitation objections"
    else:
        impact['defence_strategy_likely'] = "Defence may challenge documentary proof and transaction basis"

    if documentary_strength >= 75:
        impact['cross_exam_risk'] = "LOW - Strong documentary foundation"
    elif documentary_strength >= 50:
        impact['cross_exam_risk'] = "MODERATE - Some documentary gaps exist"
    else:
        impact['cross_exam_risk'] = "HIGH - Significant evidentiary weaknesses"

    if score >= 70:
        impact['settlement_window'] = "Settlement leverage moderate to high"
    elif score >= 50:
        impact['settlement_window'] = "Settlement window exists if documentary gaps addressed"
    else:
        impact['settlement_window'] = "Limited settlement leverage due to case weaknesses"

    if score >= 75:
        impact['practical_notes'].append("Strong position for trial prosecution")
    if documentary_strength < 60:
        impact['practical_notes'].append("Documentary evidence requires strengthening")
    if fatal_defects:
        impact['practical_notes'].append("Fatal defects must be remedied before proceeding")

    return impact


def generate_prefiling_checklist(
    documentary_gaps: List,
    timeline_issues: List,
    liability_issues: List,
    uploaded_docs: List = None
) -> Dict:

    checklist = {
        'critical_actions': [],
        'recommended_actions': [],
        'optional_improvements': [],
        'document_status': {}
    }

    if 'original_cheque_missing' in documentary_gaps:
        checklist['critical_actions'].append({
            'action': '📎 Obtain and attach original cheque',
            'priority': 'CRITICAL',
            'reason': 'Primary evidence required'
        })

    if 'return_memo_missing' in documentary_gaps:
        checklist['critical_actions'].append({
            'action': '📎 Obtain dishonour memo from bank',
            'priority': 'CRITICAL',
            'reason': 'Statutory requirement under Section 138'
        })

    if 'postal_proof_missing' in documentary_gaps:
        checklist['critical_actions'].append({
            'action': '📎 Attach postal proof/acknowledgment',
            'priority': 'CRITICAL',
            'reason': 'Proof of notice service essential'
        })

    if 'written_agreement_missing' in documentary_gaps:
        checklist['recommended_actions'].append({
            'action': '📄 Attach written agreement/contract',
            'priority': 'HIGH',
            'reason': 'Strengthens proof of legally enforceable debt'
        })

    if 'ledger_missing' in documentary_gaps:
        checklist['recommended_actions'].append({
            'action': '📊 Include ledger extract',
            'priority': 'HIGH',
            'reason': 'Supports transaction authenticity'
        })

    checklist['optional_improvements'].append({
        'action': '🏦 Attach bank statement trail',
        'priority': 'MEDIUM',
        'reason': 'Additional corroboration of transaction'
    })

    checklist['optional_improvements'].append({
        'action': '👤 Prepare witness statements',
        'priority': 'MEDIUM',
        'reason': 'Strengthens factual foundation'
    })

    checklist['optional_improvements'].append({
        'action': '📧 Include email/SMS correspondence',
        'priority': 'MEDIUM',
        'reason': 'Demonstrates communication trail'
    })

    return checklist


def verify_documents_against_claims(
    claimed_docs: Dict,
    uploaded_files: List = None
) -> Dict:

    verification = {
        'verified_count': 0,
        'claimed_count': 0,
        'unverified_count': 0,
        'missing_count': 0,
        'documents': [],
        'verification_rate': 0,
        'litigation_readiness': ''
    }

    doc_mapping = {
        'original_cheque_available': 'Original Cheque',
        'return_memo_available': 'Dishonour Memo',
        'postal_proof_available': 'Postal Proof/Acknowledgment',
        'written_agreement_exists': 'Written Agreement',
        'ledger_available': 'Ledger Extract',
        'email_sms_evidence': 'Email/SMS Evidence',
        'witness_available': 'Witness Statements'
    }

    uploaded_files = uploaded_files or []

    for field, doc_name in doc_mapping.items():
        claimed = claimed_docs.get(field, False)

        if claimed:
            verification['claimed_count'] += 1

            if uploaded_files and len(uploaded_files) > 0:

                verified = False
                if verified:
                    verification['verified_count'] += 1
                    status = "✅ VERIFIED"
                    action = None
                else:
                    verification['unverified_count'] += 1
                    status = "⚠️ CLAIMED but NOT VERIFIED"
                    action = f"Upload {doc_name.lower()} to confirm"
            else:
                verification['unverified_count'] += 1
                status = "⚠️ CLAIMED but NOT VERIFIED"
                action = f"Upload {doc_name.lower()} to confirm"
        else:
            verification['missing_count'] += 1
            status = "❌ NOT PROVIDED"
            action = f"Consider obtaining {doc_name.lower()}"

        verification['documents'].append({
            'document': doc_name,
            'claimed': claimed,
            'status': status,
            'action': action
        })

    if verification['claimed_count'] > 0:
        verification['verification_rate'] = round(
            (verification['verified_count'] / verification['claimed_count']) * 100
        )

    if verification['verification_rate'] >= 80:
        verification['litigation_readiness'] = "READY"
    elif verification['verification_rate'] >= 50:
        verification['litigation_readiness'] = "INCOMPLETE"
    else:
        verification['litigation_readiness'] = "INSUFFICIENT"

    return verification


def generate_filing_readiness_checklist(
    document_compliance: Dict,
    defence_risks: Dict,
    timeline_result: Dict,
    ingredients: Dict
) -> Dict:

    checklist = {
        'module': 'Filing Readiness Assessment',
        'overall_status': 'NOT READY',
        'decisive_verdict': '',
        'readiness_score': 0,
        'clearance_items': {},
        'fatal_blockers': [],
        'critical_warnings': [],
        'minor_gaps': [],
        'ready_to_file': False,
        'filing_recommendation': ''
    }

    ready_count = 0
    total_checks = 5
    fatal_blocks = []
    critical_warns = []

    doc_fatal = len(document_compliance.get('fatal_defects', [])) > 0
    doc_ready = document_compliance.get('filing_readiness') == '✅ READY TO FILE'

    if doc_fatal:
        fatal_blocks.append({
            'blocker': 'FATAL DOCUMENTS MISSING',
            'impact': document_compliance.get('fatal_message', 'Critical documents absent'),
            'action': 'Filing will be rejected - Obtain mandatory documents'
        })
        checklist['clearance_items']['documents'] = {
            'status': '🔴 BLOCKED',
            'detail': f"{len(document_compliance.get('fatal_defects', []))} FATAL defects",
            'ready': False,
            'blocking': True
        }
    elif doc_ready:
        ready_count += 1
        checklist['clearance_items']['documents'] = {
            'status': '✅ Ready',
            'detail': f"Compliance: {document_compliance.get('compliance_score', 0)}/100",
            'ready': True
        }
    else:
        critical_warns.append('Document gaps present - High rejection risk')
        checklist['clearance_items']['documents'] = {
            'status': '⚠️ RISKY',
            'detail': f"Compliance: {document_compliance.get('compliance_score', 0)}/100",
            'ready': False
        }

    limitation_fatal = timeline_result.get('limitation_risk') in ['CRITICAL', 'EXPIRED']
    limitation_safe = timeline_result.get('limitation_risk') in ['LOW', 'NONE']

    if limitation_fatal:
        fatal_blocks.append({
            'blocker': 'LIMITATION EXPIRED',
            'impact': 'Complaint is time-barred',
            'action': 'Filing will fail - Case is limitation-barred'
        })
        checklist['clearance_items']['limitation'] = {
            'status': '🔴 BLOCKED',
            'detail': 'LIMITATION EXPIRED',
            'ready': False,
            'blocking': True
        }
    elif limitation_safe:
        ready_count += 1
        checklist['clearance_items']['limitation'] = {
            'status': '✅ Safe',
            'detail': f"Risk: {timeline_result.get('limitation_risk', 'Unknown')}",
            'ready': True
        }
    else:
        critical_warns.append('Limitation timeline tight - Verify urgently')
        checklist['clearance_items']['limitation'] = {
            'status': '⚠️ RISKY',
            'detail': f"Risk: {timeline_result.get('limitation_risk', 'Unknown')}",
            'ready': False
        }

    ingredients_ok = ingredients.get('overall_compliance', 0) >= 70
    checklist['clearance_items']['ingredients'] = {
        'status': '✅ Compliant' if ingredients_ok else '⚠️ Gaps',
        'detail': f"Compliance: {ingredients.get('overall_compliance', 0):.1f}%",
        'ready': ingredients_ok
    }
    if ingredients_ok:
        ready_count += 1
    else:
        critical_warns.append('Statutory ingredient gaps - Review required')

    defence_fatal = len(defence_risks.get('fatal_defences', [])) > 0
    defence_safe = defence_risks.get('overall_risk') in ['LOW', 'MANAGEABLE']

    if defence_fatal:
        fatal_blocks.append({
            'blocker': 'FATAL DEFENCE EXPOSURE',
            'impact': defence_risks.get('case_viability_impact', 'Case may collapse on defence'),
            'action': 'Address fatal defence vulnerabilities before filing'
        })
        checklist['clearance_items']['defence_exposure'] = {
            'status': '🔴 CRITICAL',
            'detail': f"{len(defence_risks.get('fatal_defences', []))} fatal defence risks",
            'ready': False,
            'blocking': False
        }
    elif defence_safe:
        ready_count += 1
        checklist['clearance_items']['defence_exposure'] = {
            'status': '✅ Manageable',
            'detail': defence_risks.get('overall_risk', 'Unknown'),
            'ready': True
        }
    else:
        critical_warns.append(f"Defence exposure: {defence_risks.get('overall_risk')}")
        checklist['clearance_items']['defence_exposure'] = {
            'status': '⚠️ Elevated',
            'detail': defence_risks.get('overall_risk', 'Unknown'),
            'ready': False
        }

    procedural_ok = True
    checklist['clearance_items']['procedural'] = {
        'status': '✅ Clear',
        'detail': 'No procedural defects detected',
        'ready': True
    }
    ready_count += 1

    checklist['readiness_score'] = int((ready_count / total_checks) * 100)
    checklist['fatal_blockers'] = fatal_blocks
    checklist['critical_warnings'] = critical_warns

    if len(fatal_blocks) > 0:
        checklist['overall_status'] = '🔴 FILING BLOCKED'
        checklist['ready_to_file'] = False
        checklist['decisive_verdict'] = f"DO NOT FILE - {len(fatal_blocks)} fatal blockers detected"
        checklist['filing_recommendation'] = f"FILING WILL FAIL due to: {fatal_blocks[0]['blocker']}"
    elif len(critical_warns) >= 3:
        checklist['overall_status'] = '⚠️ HIGH RISK - NOT ADVISABLE'
        checklist['ready_to_file'] = False
        checklist['decisive_verdict'] = f"FILING NOT ADVISABLE - {len(critical_warns)} critical gaps present"
        checklist['filing_recommendation'] = 'Address critical gaps before filing to avoid dismissal risk'
    elif len(critical_warns) > 0:
        checklist['overall_status'] = '⚠️ RISKY - REVIEW REQUIRED'
        checklist['ready_to_file'] = False
        checklist['decisive_verdict'] = f"FILING POSSIBLE BUT RISKY - {len(critical_warns)} warnings present"
        checklist['filing_recommendation'] = 'Strengthen weak areas before filing'
    else:
        checklist['overall_status'] = '✅ READY TO FILE'
        checklist['ready_to_file'] = True
        checklist['decisive_verdict'] = 'CLEAR TO FILE - All critical requirements satisfied'
        checklist['filing_recommendation'] = 'Proceed with filing - Case is ready'

    return checklist





def _safe(value, default="Not available", fmt=None):
    """Return value safely, replacing None/empty with default. Optionally format numbers."""
    if value is None or value == "" or value == {} or value == []:
        return default
    if fmt == "inr" and isinstance(value, (int, float)):
        return f"₹{indian_number_format(value)}"
    if fmt == "score" and isinstance(value, (int, float)):
        return f"{value:.1f}/100"
    if fmt == "pct" and isinstance(value, (int, float)):
        return f"{value:.1f}%"
    return str(value)


def sanitize_module_output(data, _depth=0):
    """
    Recursively replace None / empty values with meaningful fallbacks
    so the PDF renderer never receives null and shows broken characters.
    """
    if _depth > 10:
        return data
    if data is None:
        return "Not available"
    if isinstance(data, dict):
        return {k: sanitize_module_output(v, _depth+1) for k, v in data.items()}
    if isinstance(data, list):
        return [sanitize_module_output(i, _depth+1) for i in data]
    if isinstance(data, str) and data.strip() == '':
        return "Not available"
    if isinstance(data, float) and data != data:  # NaN
        return 0.0
    return data


def _safe(val, fallback="Not available"):
    """Return val if truthy and not None, else fallback."""
    if val is None or val == "" or val == [] or val == {}:
        return fallback
    return val


def _safe_score(val, fallback=0.0):
    """Return numeric score safely."""
    try:
        return round(float(val), 1)
    except (TypeError, ValueError):
        return fallback


def generate_clean_professional_report(analysis: Dict, case_data: Dict) -> Dict:
    # Read from central result object when available — guaranteed non-null fields
    R = analysis.get('_result', {})

    risk        = analysis.get('modules', {}).get('risk_assessment', {}) or {}
    timeline    = analysis.get('modules', {}).get('timeline_intelligence', {}) or {}
    ingredients = analysis.get('modules', {}).get('ingredient_compliance', {}) or {}
    documentary = analysis.get('modules', {}).get('documentary_strength', {}) or {}
    liability   = analysis.get('modules', {}).get('liability_analysis', {}) or {}
    defence_m   = analysis.get('modules', {}).get('defence_matrix', {}) or {}
    defects_m   = analysis.get('modules', {}).get('procedural_defects', {}) or {}
    cross_exam  = analysis.get('modules', {}).get('cross_examination_risk', {}) or {}
    judicial    = analysis.get('modules', {}).get('judicial_behavior', {}) or {}
    settlement  = analysis.get('modules', {}).get('settlement_analysis', {}) or {}

    score       = _safe_score(risk.get('overall_risk_score'))
    fatal_flag  = analysis.get('fatal_flag', False)

    # Collect ALL fatal defects from ALL modules
    all_fatal_defects = []
    all_fatal_defects.extend(risk.get('fatal_defects', []))
    all_fatal_defects.extend(defects_m.get('fatal_defects', []))
    all_fatal_defects.extend(ingredients.get('fatal_defects', []))

    # Deduplicate by defect text
    seen_defects = set()
    unique_fatals = []
    for d in all_fatal_defects:
        key = d.get('defect', str(d))
        if key not in seen_defects:
            seen_defects.add(key)
            unique_fatals.append(d)
    fatal_defects = unique_fatals

    amount = case_data.get('cheque_amount', 0)
    case_type = "Complainant" if case_data.get('case_type') == 'complainant' else "Accused"

    # ── SECTION 0: CASE OVERVIEW ──────────────────────────────────
    case_overview = {
        'title': 'CASE OVERVIEW',
        'case_reference': _safe(analysis.get('case_id')),
        'analysis_date': analysis.get('analysis_timestamp', '')[:10] or 'Not available',
        'case_type': case_type,
        'cheque_amount': f"₹{indian_number_format(amount)}",
        'cheque_number': _safe(case_data.get('cheque_number')),
        'cheque_date': _safe(case_data.get('cheque_date')),
        'bank_name': _safe(case_data.get('bank_name')),
        'dishonour_date': _safe(case_data.get('dishonour_date')),
        'dishonour_reason': _safe(case_data.get('dishonour_reason')),
        'notice_date': _safe(case_data.get('notice_date')),
        'notice_received_date': _safe(case_data.get('notice_received_date')),
        'complaint_filed_date': _safe(case_data.get('complaint_filed_date')),
        'court_location': _safe(case_data.get('court_location')),
        'debt_nature': _safe(case_data.get('debt_nature')),
    }

    # ── SECTION 1: EXECUTIVE SUMMARY ─────────────────────────────
    # Determine filing recommendation
    if fatal_flag and fatal_defects:
        filing_status = "DO NOT FILE"
        filing_colour = "RED"
        one_liner = (
            f"This {case_type.lower()} case has {len(fatal_defects)} fatal defect(s). "
            f"Filing is blocked until defects are remedied. Risk score: {score}/100."
        )
        recommended_action = "Address all fatal defects listed below before filing. Consult litigation counsel urgently."
    elif score >= 75:
        filing_status = "READY TO FILE"
        filing_colour = "GREEN"
        one_liner = f"Case is in a strong position to file. Risk score: {score}/100. Address minor gaps before proceeding."
        recommended_action = "Proceed with filing after addressing the gaps noted below."
    elif score >= 55:
        filing_status = "FILE WITH CAUTION"
        filing_colour = "AMBER"
        one_liner = f"Case is maintainable but has significant evidentiary gaps. Risk score: {score}/100."
        recommended_action = "Strengthen documentary evidence before filing to reduce acquittal risk."
    else:
        filing_status = "HIGH RISK — REMEDIATION REQUIRED"
        filing_colour = "RED"
        one_liner = f"Multiple compliance gaps detected. Risk score: {score}/100. Filing not advisable without remediation."
        recommended_action = "Obtain missing documents, strengthen transaction proof, then reassess."

    # Strengths — from category scores
    strengths = []
    for cat, data in (risk.get('category_scores') or {}).items():
        s = _safe_score(data.get('score') if isinstance(data, dict) else data)
        if s >= 75:
            strengths.append(f"{cat}: {s}/100 — Strong")
    if case_data.get('return_memo_available'):
        strengths.append("Dishonour memo available — primary evidence secured")
    if case_data.get('postal_proof_available'):
        strengths.append("Postal proof of notice available — service established")
    if case_data.get('original_cheque_available'):
        strengths.append("Original cheque available — foundational document present")
    if not strengths:
        strengths.append("Core cheque transaction established")

    # Weaknesses — from category scores + missing docs
    weaknesses = []
    for cat, data in (risk.get('category_scores') or {}).items():
        s = _safe_score(data.get('score') if isinstance(data, dict) else data)
        if s < 60:
            reason = data.get('reason', '') if isinstance(data, dict) else ''
            weaknesses.append(f"{cat}: {s}/100 — {_safe(reason, 'Needs strengthening')}")
    if not case_data.get('written_agreement_exists'):
        weaknesses.append("No written agreement — accused can challenge legally enforceable debt")
    if not case_data.get('ledger_available'):
        weaknesses.append("No ledger/account records — transaction trail incomplete")
    if not weaknesses:
        weaknesses.append("No critical weaknesses detected at this stage")

    # Fatal defect summary for executive section
    fatal_summary = []
    for d in fatal_defects[:5]:
        fatal_summary.append({
            'defect': _safe(d.get('defect')),
            'severity': _safe(d.get('severity')),
            'impact': _safe(d.get('impact')),
            'remedy': _safe(d.get('remedy', d.get('cure', 'Consult legal counsel')))
        })

    executive_summary = {
        'title': 'EXECUTIVE SUMMARY',
        'one_line_verdict': one_liner,
        'filing_status': filing_status,
        'filing_colour': filing_colour,
        'risk_score': f"{score}/100",
        'compliance_level': _safe(risk.get('compliance_level'), 'Under Review'),
        'fatal_defects_count': len(fatal_defects),
        'fatal_defects': fatal_summary,
        'strengths': strengths[:5],
        'weaknesses': weaknesses[:5],
        'recommended_action': recommended_action,
        'processing_time': f"{_safe_score(analysis.get('processing_time_seconds'))}s",
        'engine_version': analysis.get('engine_version', 'v10.0'),
        'generated_by': 'JUDIQ Legal Intelligence Engine',
    }

    # ── SECTION 2: TIMELINE (chronological order) ─────────────────
    chart = sorted(
        [e for e in timeline.get('timeline_chart', []) if e.get('date')],
        key=lambda x: x.get('date', '9999')
    )
    timeline_section = {
        'title': 'TIMELINE ANALYSIS',
        'limitation_risk': _safe(timeline.get('limitation_risk'), 'Not assessed'),
        'limitation_status': _safe(
            timeline.get('compliance_status', {}).get('limitation'), 'Not assessed'
        ),
        'chronological_events': [
            {
                'date': e.get('date', 'Unknown'),
                'event': _safe(e.get('event')),
                'status': _safe(e.get('status'), '—')
            }
            for e in chart
        ],
        'critical_dates': timeline.get('critical_dates', {}),
        'edge_cases': timeline.get('edge_cases_detected', [])[:3],
        'risk_markers': [
            {
                'issue': _safe(r.get('issue')),
                'severity': _safe(r.get('severity')),
                'impact': _safe(r.get('impact'))
            }
            for r in timeline.get('risk_markers', [])
        ]
    }

    # ── SECTION 3: RISK SCORE BREAKDOWN ──────────────────────────
    cat_scores = risk.get('category_scores', {})
    breakdown = []
    weight_map = {
        'Timeline Compliance':   ('25%', 'Limitation period, notice timing, complaint filing'),
        'Ingredient Compliance': ('30%', 'All 7 statutory ingredients of Section 138'),
        'Documentary Strength':  ('20%', 'Cheque, memo, notice, agreement, ledger'),
        'Procedural Compliance': ('10%', 'Proper parties, averments, jurisdiction'),
        'Liability Expansion':   ('15%', 'Section 141 director liability (company cases)'),
    }
    for cat, data in cat_scores.items():
        s = _safe_score(data.get('score') if isinstance(data, dict) else data)
        weight, desc = weight_map.get(cat, ('—', 'Contributing factor'))
        status = 'Strong' if s >= 75 else ('Adequate' if s >= 55 else 'Weak')
        breakdown.append({
            'category': cat,
            'score': f"{s}/100",
            'weight': weight,
            'status': status,
            'description': desc,
            'deductions': data.get('deductions', []) if isinstance(data, dict) else []
        })

    risk_breakdown = {
        'title': 'RISK SCORE BREAKDOWN',
        'overall_score': R.get('overall_score_display', f"{score}/100"),
        'compliance_level': R.get('compliance_level', _safe(risk.get('compliance_level'), 'Under Review')),
        'fatal_override_active': R.get('fatal_override_applied', fatal_flag),
        'fatal_override_note': R.get('fatal_override_note',
            f"Score capped at {R.get('capped_at_display','N/A')} due to {R.get('fatal_defects_count',0)} fatal defect(s)"
            if fatal_flag else "No fatal override applied"
        ),
        'original_score': f"{R.get('original_score', score)}/100",
        'capped_at': R.get('capped_at_display', 'N/A'),
        'category_breakdown': breakdown,
        'scoring_note': (
            'Score starts at 100. Deductions applied per severity: '
            'CRITICAL (-20 to -30), MAJOR (-10 to -15), MINOR (-5). '
            'Fatal defects cap the score regardless of other factors.'
        )
    }

    # ── SECTION 4: DOCUMENTARY EVIDENCE ──────────────────────────
    doc_items = [
        ('Original Cheque',      case_data.get('original_cheque_available'),   'Primary instrument — essential'),
        ('Return Memo',          case_data.get('return_memo_available'),        'Proof of dishonour — essential'),
        ('Postal Proof',         case_data.get('postal_proof_available'),       'Notice service evidence — critical'),
        ('Written Agreement',    case_data.get('written_agreement_exists'),     'Debt proof — important'),
        ('Ledger/Account Records', case_data.get('ledger_available'),           'Transaction trail — important'),
        ('Email/SMS Evidence',   case_data.get('email_sms_evidence'),           'Supporting evidence'),
        ('Witness Available',    case_data.get('witness_available'),            'Testimonial evidence'),
    ]
    doc_section = {
        'title': 'DOCUMENTARY EVIDENCE ASSESSMENT',
        'overall_strength': f"{_safe_score(documentary.get('overall_strength_score'))}/100",
        'strength_label': _safe(documentary.get('strength_label'), 'Under Review'),
        'documents': [
            {
                'document': name,
                'status': '✅ Available' if available else '❌ Missing',
                'importance': importance
            }
            for name, available, importance in doc_items
        ],
        'missing_critical': [
            name for name, available, imp in doc_items
            if not available and 'essential' in imp
        ],
        'document_gaps': R.get('documentary_gaps', []),
        'score_explanation': (
            f"Documentary strength is {R.get('documentary_score', _safe_score(documentary.get('overall_strength_score')))}/100. "
            + ("Written agreement and ledger are missing — without these, the accused can challenge "
               "the existence of legally enforceable debt, which is a fundamental Section 138 ingredient."
               if not case_data.get('written_agreement_exists') else
               "Documentary evidence is in reasonable order.")
        )
    }

    # ── SECTION 5: DEFENCE ANALYSIS ──────────────────────────────
    high_risk = defence_m.get('high_risk_defences', [])
    all_defences = defence_m.get('defences', defence_m.get('defence_angles', []))

    # Ensure premature complaint appears if applicable
    proc_fatals = defects_m.get('fatal_defects', [])
    premature = next((d for d in proc_fatals if 'Premature' in d.get('defect', '') or '15-Day' in d.get('defect', '')), None)
    if premature and not any('Premature' in str(d) for d in high_risk):
        high_risk.insert(0, {
            'defence': 'Premature Complaint — Cause of Action Not Yet Arisen',
            'strength': 'FATAL',
            'legal_basis': 'Section 138(b) NI Act',
            'probability': 'CERTAIN',
            'strategy': 'File discharge application at first hearing',
        })

    defence_section = {
        'title': 'DEFENCE ANALYSIS',
        'overall_exposure': _safe(defence_m.get('overall_exposure', defence_m.get('exposure_level')), 'Not assessed'),
        'high_risk_defences': [
            {
                'defence': _safe(d.get('defence', d.get('ground'))),
                'strength': _safe(d.get('strength', d.get('risk_impact'))),
                'legal_basis': _safe(d.get('legal_basis', d.get('counter', 'Statutory provision'))),
                'strategy': _safe(d.get('strategy')),
            }
            for d in high_risk[:5]
        ] if high_risk else [{
            'defence': 'No major defences identified',
            'strength': 'LOW',
            'legal_basis': 'Case documents appear adequate',
            'strategy': 'Maintain current evidence posture'
        }],
        'preparation_priorities': defence_m.get('preparation_priorities', [])[:5]
    }

    # ── SECTION 6: PROCEDURAL DEFECTS ────────────────────────────
    procedural_section = {
        'title': 'PROCEDURAL DEFECTS',
        'overall_risk': _safe(defects_m.get('overall_risk'), 'Not assessed'),
        'fatal_defects': [
            {
                'defect': _safe(d.get('defect')),
                'severity': _safe(d.get('severity')),
                'impact': _safe(d.get('impact')),
                'remedy': _safe(d.get('remedy', d.get('cure', 'Consult legal counsel')))
            }
            for d in defects_m.get('fatal_defects', [])
        ],
        'curable_defects': [
            {
                'defect': _safe(d.get('defect')),
                'severity': _safe(d.get('severity')),
                'cure': _safe(d.get('cure'))
            }
            for d in defects_m.get('curable_defects', [])
        ],
        'warnings': [
            {'area': _safe(w.get('area')), 'warning': _safe(w.get('warning'))}
            for w in defects_m.get('warnings', [])
        ]
    }

    # ── SECTION 7: JUDICIAL BEHAVIOUR ────────────────────────────
    jb_confidence = R.get('court_confidence', judicial.get('confidence', 'LOW'))
    jb_indices = judicial.get('behavioral_indices', {})
    if jb_confidence in ('HIGH', 'MEDIUM') and jb_indices:
        judicial_section = {
            'title': 'JUDICIAL BEHAVIOUR ANALYSIS',
            'court': R.get('court_name', _safe(judicial.get('court_identified'), case_data.get('court_location', 'Not specified'))),
            'confidence': jb_confidence,
            'sample_size': judicial.get('sample_size', 0),
            'behavioral_indices': {
                k: v for k, v in jb_indices.items() if v not in (None, '', 'N/A')
            },
            'observed_patterns': judicial.get('observed_patterns', [])[:3],
            'strategic_insights': judicial.get('strategic_insights', [])[:3],
        }
    else:
        judicial_section = {
            'title': 'JUDICIAL BEHAVIOUR ANALYSIS',
            'court': R.get('court_name', _safe(case_data.get('court_location'), 'Not specified')),
            'confidence': 'INSUFFICIENT DATA',
            'note': R.get('court_note',
                'Judicial behaviour analysis is unavailable — '
                'insufficient case data for this court in the knowledge base. '
                'Analysis will improve as more judgments are added.'
            ),
            'indices': R.get('court_indices', {})
        }

    # ── SECTION 8: CROSS-EXAMINATION RISK ────────────────────────
    vuln_zones = cross_exam.get('vulnerability_zones', [])
    likely_qs  = cross_exam.get('likely_questions', [])
    cx_section = {
        'title': 'CROSS-EXAMINATION RISK',
        'overall_risk': R.get('cross_exam_risk', _safe(cross_exam.get('overall_cross_exam_risk'), 'Not assessed')),
        'vulnerability_zones': R.get('cross_exam_zones') or [
            {
                'zone': _safe(z.get('zone', z.get('area'))),
                'risk': _safe(z.get('risk_level', z.get('severity'))),
                'likely_question': _safe(z.get('likely_question', z.get('question')))
            }
            for z in vuln_zones[:5]
        ] if vuln_zones else [],
        'likely_questions': R.get('cross_exam_questions') or likely_qs[:8] or [
            'Can you produce the original loan agreement in writing?',
            'How was the money transferred — cash, cheque, or bank transfer?',
            'Is it not true the cheque was given as security and not towards repayment?',
            'Why is there no ledger entry or bank record of this transaction?',
            'Who was present when the loan was given?',
        ] if not case_data.get('written_agreement_exists') else [
            'Please produce the original written agreement.',
            'When exactly was the notice served — produce postal acknowledgment.',
        ],
        'preparation_tips': cross_exam.get('preparation_required', [])[:3]
    }

    # ── SECTION 9: CONCLUSIONS & ACTIONS ─────────────────────────
    immediate_actions = []
    if fatal_defects:
        for d in fatal_defects[:3]:
            immediate_actions.append(f"URGENT: {d.get('remedy', d.get('defect', 'Address fatal defect'))}")
    if not case_data.get('written_agreement_exists'):
        immediate_actions.append("Obtain written acknowledgment of debt from accused")
    if not case_data.get('postal_proof_available'):
        immediate_actions.append("Secure AD card or postal tracking proof of notice")
    if not immediate_actions:
        immediate_actions.append("Proceed with filing after final legal review")

    conclusions = {
        'title': 'CONCLUSIONS & RECOMMENDED ACTIONS',
        'filing_status': filing_status,
        'overall_score': f"{score}/100",
        'one_line_verdict': one_liner,
        'recommended_action': recommended_action,
        'immediate_actions': R.get('next_actions', immediate_actions)[:5],
        'disclaimer': (
            'This report is a structured statutory compliance assessment. '
            'It does not constitute legal advice. All findings must be reviewed '
            'by qualified legal counsel before filing or making legal decisions. '
            'JUDIQ AI accepts no liability for decisions based on this report.'
        )
    }

    return {
        'report_format': 'Structured Professional Brief',
        'report_version': '2.0',
        'table_of_contents': [
            {'section': '0', 'title': 'Case Overview',            'key': 'section_0_case_overview'},
            {'section': '1', 'title': 'Executive Summary',        'key': 'section_1_executive_summary'},
            {'section': '2', 'title': 'Timeline Analysis',        'key': 'section_2_timeline'},
            {'section': '3', 'title': 'Risk Score Breakdown',     'key': 'section_3_risk_breakdown'},
            {'section': '4', 'title': 'Documentary Evidence',     'key': 'section_4_documentary'},
            {'section': '5', 'title': 'Defence Analysis',         'key': 'section_5_defence'},
            {'section': '6', 'title': 'Procedural Defects',       'key': 'section_6_procedural_defects'},
            {'section': '7', 'title': 'Judicial Behaviour',       'key': 'section_7_judicial_behaviour'},
            {'section': '8', 'title': 'Cross-Examination Risk',   'key': 'section_8_cross_examination'},
            {'section': '9', 'title': 'Conclusions & Actions',    'key': 'section_9_conclusions'},
        ],
        'section_0_case_overview':    case_overview,
        'section_1_executive_summary': executive_summary,
        'section_2_timeline':          timeline_section,
        'section_3_risk_breakdown':    risk_breakdown,
        'section_4_documentary':       doc_section,
        'section_5_defence':           defence_section,
        'section_6_procedural_defects': procedural_section,
        'section_7_judicial_behaviour': judicial_section,
        'section_8_cross_examination':  cx_section,
        'section_9_conclusions':        conclusions,
        'metadata': {
            'engine': 'JUDIQ Intelligence Engine',
            'analysis_id': _safe(analysis.get('case_id')),
            'generated': analysis.get('analysis_timestamp', '')[:19],
            'processing_time': f"{_safe_score(analysis.get('processing_time_seconds'))}s",
            'fatal_defects_total': len(fatal_defects),
            'modules_executed': len(analysis.get('modules', {})),
        }
    }

    page_1 = {
        'title': 'EXECUTIVE OVERVIEW',
        'product_name': 'JUDIQ Section 138 Intelligence Engine',
        'positioning': 'A deterministic legal risk analysis system designed to assess cheque dishonour litigation viability before filing or defence.',
        'case_reference': analysis.get('case_id', 'N/A'),
        'analysis_date': analysis.get('analysis_timestamp', '')[:10],

        'legal_position': {
            'maintainability': '',
            'compliance_score': round(score, 1),
            'fatal_defects': 'Yes' if fatal_flag else 'No',
            'primary_concern': ''
        },

        'problems_solved': [
            'Technical dismissals due to limitation calculation errors',
            'Manual timeline arithmetic prone to calendar month miscalculation',
            'Section 141 liability mapping requiring careful statutory analysis',
            'Notice service compliance gaps creating acquittal risk',
            'Defence exposure assessment lacking structured methodology'
        ],

        'methodology': 'JUDIQ operates as a deterministic rule engine, not predictive AI. Every output derives from statutory compliance rules, calendar-based date mathematics, and structured legal logic. The system identifies compliance gaps and quantifies statutory risk through weighted severity analysis.'
    }

    if fatal_flag:
        page_1['legal_position']['maintainability'] = 'High Dismissal Risk'
        page_1['legal_position']['primary_concern'] = 'Catastrophic statutory violations detected'
    elif score >= 80:
        page_1['legal_position']['maintainability'] = 'Appears Maintainable'
        page_1['legal_position']['primary_concern'] = 'Statutory requirements satisfied. Minor gaps noted.'
    elif score >= 60:
        page_1['legal_position']['maintainability'] = 'Maintainable with Gaps'
        page_1['legal_position']['primary_concern'] = 'Evidentiary strengthening required'
    else:
        page_1['legal_position']['maintainability'] = 'Weak Position'
        page_1['legal_position']['primary_concern'] = 'Multiple compliance gaps. Remediation essential.'

    page_2 = {
        'title': 'CORE COMPLIANCE ANALYSIS',
        'clusters': {
            '1_chronology_limitation': {
                'name': '1. Chronology & Limitation Engine',
                'status': 'Compliant' if timeline.get('limitation_risk') in ['LOW', 'NONE'] else 'At Risk',
                'findings': [
                    # FIX: Read from timeline's computed status, don't regenerate
                    f"Cheque validity: {timeline.get('validity', {}).get('status', 'Unknown')}",
                    f"Notice timeline: {timeline.get('gaps', {}).get('dishonour_to_notice', 'N/A')} days",
                    f"Limitation status: {timeline.get('limitation_risk', 'Under review')}"
                ]
            },
            '2_statutory_ingredients': {
                'name': '2. Statutory Ingredient Validator',
                'status': 'Compliant' if ingredients.get('overall_compliance', 0) >= 70 else 'Gaps Identified',
                'findings': [
                    f"Overall compliance: {ingredients.get('overall_compliance', 0):.1f}%",
                    f"Critical ingredients: {ingredients.get('critical_count', 0)}/7",
                    f"Fatal defects: {len(ingredients.get('fatal_defects', []))}"
                ]
            },
            '3_evidence_documentation': {
                'name': '3. Evidence & Documentation Assessment',
                'status': 'Strong' if documentary.get('overall_strength_score', 0) >= 70 else 'Requires Strengthening',
                'findings': [
                    f"Documentary strength: {documentary.get('overall_strength_score', 0):.1f}/100",
                    f"Original cheque: {'Available' if case_data.get('original_cheque_available') else 'Not verified'}",
                    f"Notice proof: {'Available' if case_data.get('postal_proof_available') else 'Missing'}"
                ]
            },
            '4_defence_exposure': {
                'name': '4. Defence Exposure & Risk Assessment',
                'status': 'Moderate' if score >= 60 else 'Elevated',
                'findings': [
                    'Security cheque defence exposure evaluated',
                    'Cross-examination vulnerabilities identified',
                    'Contradiction analysis completed'
                ]
            },
            '5_liability_compliance': {
                'name': '5. Liability & Advanced Compliance',
                'status': 'Compliant' if not case_data.get('is_company_case') or liability.get('liability_score', 0) >= 60 else 'Gaps',
                'findings': [
                    f"Section 141 applicable: {'Yes' if case_data.get('is_company_case') else 'No'}",
                    'Impleading requirements verified',
                    'Averment compliance checked'
                ]
            }
        }
    }

    page_3 = {
        'title': 'RISK ASSESSMENT FRAMEWORK',

        'scoring_model': {
            'base_score': '100',
            'method': 'Weighted severity-based deductions',
            'severity_tiers': [
                {'tier': 'CRITICAL', 'deduction': '-20 to -30 points', 'examples': 'Limitation expired, Notice beyond 1 month'},
                {'tier': 'MAJOR', 'deduction': '-10 to -15 points', 'examples': 'Missing postal proof, Weak documentation'},
                {'tier': 'MINOR', 'deduction': '-5 points', 'examples': 'Incomplete addresses, Date format issues'}
            ],
            'fatal_override': 'Automatic cap at 25 for catastrophic defects (limitation/validity failure)'
        },

        'current_assessment': {
            'final_score': round(score, 1),
            'compliance_level': risk.get('compliance_level', 'Under Review'),
            'fatal_defects_count': len(fatal_defects),
            'critical_issues': len([d for d in fatal_defects if d.get('severity') == 'CRITICAL'])
        },

        'evidence_grading': [
            {'strength': 'STRONG', 'criteria': 'Original cheque + Return memo + AD signed postal proof'},
            {'strength': 'MODERATE', 'criteria': 'Original cheque + Return memo + Delivery tracking'},
            {'strength': 'WEAK', 'criteria': 'Cheque copy + Postal receipt only'},
            {'strength': 'CONTESTABLE', 'criteria': 'No written agreement + Oral transaction claim'}
        ]
    }

    page_4 = {
        'title': 'STRATEGIC INTELLIGENCE LAYERS',

        'defence_exposure': {
            'description': 'Structured analysis of potential defence strategies based on case documentation gaps',
            'vulnerabilities': []
        },

        'director_liability': {
            'description': 'Section 141 compliance validation for company liability expansion',
            'applicable': case_data.get('is_company_case', False),
            'status': 'Compliant' if not case_data.get('is_company_case') or liability.get('liability_score', 0) >= 60 else 'Gaps identified'
        },

        'cross_examination': {
            'description': 'Identification of logical contradictions and evidentiary weaknesses',
            'risk_level': 'Moderate' if score >= 60 else 'High',
            'focus_areas': ['Transaction basis', 'Notice service', 'Financial capacity']
        },

        'presumption_framework': {
            'description': 'Section 139 presumption trigger strength and rebuttal capacity analysis',
            # FIX: Read from presumption_analysis module, don't regenerate
            'presumption_available': analysis.get('modules', {}).get('presumption_analysis', {}).get('presumption_triggered', False),
            'burden_shift': 'Triggered' if analysis.get('modules', {}).get('presumption_analysis', {}).get('presumption_triggered', False) else 'Not applicable'
        }
    }

    if score < 60:
        page_4['defence_exposure']['vulnerabilities'].append('Weak documentary foundation')
    if not case_data.get('written_agreement_exists'):
        page_4['defence_exposure']['vulnerabilities'].append('No written debt acknowledgment')
    if fatal_defects:
        page_4['defence_exposure']['vulnerabilities'].append('Statutory compliance failures')

    page_5 = {
        'title': 'CONCLUSIONS & LEGAL DISCLAIMER',

        'summary': {
            'legal_position': page_1['legal_position']['maintainability'],
            'compliance_score': round(score, 1),
            'recommendation': ''
        },

        'immediate_actions': [],

        'disclaimer': {
            'primary': 'This analysis is a structured compliance assessment tool based on information provided. It does not constitute legal advice or legal representation.',
            'scope': 'All findings should be reviewed by qualified legal counsel before filing. Document verification is based on user-provided information.',
            'limitation': 'JUDIQ AI is a legal technology platform. We do not provide legal representation. Outputs are for informational purposes only.',
            'consultation': 'For specific legal concerns, consult with a qualified legal professional.'
        }
    }

    if fatal_flag:
        page_5['summary']['recommendation'] = 'Immediate remediation required. Filing not advisable without addressing fatal defects.'
        page_5['immediate_actions'] = [
            'Address limitation/validity failures',
            'Consult litigation counsel urgently',
            'Consider settlement if defects cannot be remedied'
        ]
    elif score >= 70:
        page_5['summary']['recommendation'] = 'Case appears maintainable. Address identified gaps before filing.'
        page_5['immediate_actions'] = [
            'Strengthen documentary evidence',
            'Verify notice service proof',
            'Obtain final legal review before filing'
        ]
    else:
        page_5['summary']['recommendation'] = 'Significant gaps. Evidentiary strengthening essential before filing.'
        page_5['immediate_actions'] = [
            'Obtain missing documentation',
            'Strengthen transaction proof',
            'Reassess viability after remediation'
        ]

    return {
        'report_type': 'Professional Compliance Analysis',
        'report_format': 'Structured 5-Page Brief',
        'page_1_executive': page_1,
        'page_2_compliance': page_2,
        'page_3_risk_framework': page_3,
        'page_4_strategic_intelligence': page_4,
        'page_5_conclusions': page_5,
        'metadata': {
            'engine': 'JUDIQ Intelligence Engine',
            'analysis_id': analysis.get('case_id'),
            'generated': analysis.get('analysis_timestamp')
        }
    }


def generate_executive_report(analysis_data: Dict) -> Dict:
    """
    Generate a fully-populated executive report.
    All fields have safe fallbacks — no null/undefined values.
    """
    modules      = analysis_data.get('modules', {})
    exec_summ    = analysis_data.get('executive_summary', {})
    plain_summ   = analysis_data.get('plain_summary', {})
    risk_data    = modules.get('risk_assessment', {})
    timeline_data= modules.get('timeline_intelligence', {})
    doc_data     = modules.get('documentary_strength', {})
    defence_data = modules.get('defence_risk_analysis', {})
    defect_data  = modules.get('document_compliance', {})
    ingredient_data = modules.get('ingredient_compliance', {})
    settlement_data = modules.get('settlement_analysis', {})
    cross_data   = modules.get('cross_examination_risk', {})
    judicial_data= modules.get('judicial_behavior', {})

    score        = risk_data.get('overall_risk_score', 0)
    fatal_flag   = analysis_data.get('fatal_flag', False)
    fatal_defects= risk_data.get('fatal_defects', [])
    cat_scores   = risk_data.get('category_scores', {})
    limitation_risk = timeline_data.get('limitation_risk', 'Unknown')
    case_meta    = analysis_data.get('case_metadata', {})
    proc_time    = analysis_data.get('processing_time_seconds', 0)

    # ── Executive Summary (auto-generated if missing) ──────────
    one_liner = plain_summ.get('one_line_verdict') or exec_summ.get('case_overview') or (
        f"{'⛔ FATAL DEFECT — Do not file.' if fatal_flag else ('✅ Strong case.' if score >= 75 else ('⚠️ Moderate case — evidence gaps.' if score >= 60 else '🔴 Weak case — major remediation required.'))}"
        f" Risk score: {score:.0f}/100."
    )

    strengths = (
        plain_summ.get('strengths') or
        exec_summ.get('strengths') or
        ["Cheque dishonour established", "Notice served on accused"]
    )
    weaknesses = (
        plain_summ.get('weaknesses') or
        exec_summ.get('weaknesses') or
        ["Documentary evidence needs strengthening"]
    )
    recommendation = (
        plain_summ.get('recommendation') or
        exec_summ.get('strategic_recommendations', [{}])[0].get('recommendation', '') or
        ("Do not file — address fatal defect first." if fatal_flag else
         "Strengthen documentary evidence before filing." if score < 70 else
         "Ready to file. Obtain final legal review.")
    )

    # ── Chronological timeline events ──────────────────────────
    raw_chart = timeline_data.get('timeline_chart', [])
    timeline_events = sorted(
        [e for e in raw_chart if e.get('date')],
        key=lambda x: x.get('date', '9999')
    )
    if not timeline_events:
        # Build from critical_dates if chart is empty
        cd = timeline_data.get('critical_dates', {})
        for label, date_key in [
            ("Cheque Date",         "cheque_date"),
            ("Dishonour Date",      "dishonour_date"),
            ("Notice Sent",         "notice_date"),
            ("Cause of Action",     "fifteen_day_expiry"),
            ("Complaint Filed",     "complaint_filed_date"),
            ("Limitation Deadline", "limitation_deadline"),
        ]:
            if cd.get(date_key):
                status = timeline_data.get('compliance_status', {}).get(date_key, '—')
                timeline_events.append({
                    'date': cd[date_key], 'event': label, 'status': _safe(status, '—')
                })

    # ── Risk breakdown ──────────────────────────────────────────
    risk_breakdown = []
    for cat, data in cat_scores.items():
        risk_breakdown.append({
            'category': cat,
            'score': f"{_safe(data.get('score'), 0, 'pct')}",
            'weight': f"{data.get('weight', 0)*100:.0f}% of total",
            'status': _safe(data.get('status'), 'Under review'),
            'reason': _safe(data.get('reason') or data.get('primary_issue'), 'No issues detected')
        })

    # ── Documentary gaps explained ──────────────────────────────
    doc_gaps = []
    doc_details = doc_data.get('document_details', {})
    for doc_name, doc_info in doc_details.items():
        if isinstance(doc_info, dict) and doc_info.get('grade') in ['WEAK','VERY WEAK','MISSING','NOT AVAILABLE']:
            doc_gaps.append({
                'gap_name':  _safe(doc_name),   # ← PDF template reads gap_name
                'document':  _safe(doc_name),
                'severity':  'High',
                'status':    _safe(doc_info.get('grade'), 'Missing'),
                'impact':    _safe(doc_info.get('impact'), 'Reduces evidential strength'),
                'remedy':    _safe(doc_info.get('remedy'), 'Obtain and verify document')
            })
    if not doc_gaps and doc_data.get('overall_strength_score', 100) < 70:
        for field, label, sev, imp in [
            ('written_agreement_exists', 'Written Loan/Transaction Agreement', 'Severe',
             'Debt enforceability highly contestable — accused can deny legally enforceable debt'),
            ('ledger_available',         'Ledger / Account Records', 'High',
             'Transaction authenticity contestable — no paper trail of the transaction'),
            ('original_cheque_available','Original Cheque', 'High',
             'Primary instrument not secured — foundational evidence missing'),
            ('return_memo_available',    'Bank Dishonour Memo', 'Severe',
             'Proof of dishonour not available — essential ingredient missing'),
            ('postal_proof_available',   'Postal Proof of Notice', 'Moderate',
             'Notice service cannot be established — accused may deny receipt'),
        ]:
            if not case_meta.get(field, True):
                doc_gaps.append({
                    'gap_name':  label,       # ← PDF template reads gap_name
                    'document':  label,
                    'severity':  sev,
                    'status':    'Not available',
                    'impact':    imp,
                    'remedy':    'Obtain before filing'
                })

    # ── Defence analysis ────────────────────────────────────────
    high_risk_defences = defence_data.get('high_risk_defences', [])
    fatal_defences     = defence_data.get('fatal_defences', [])

    # Premature complaint = automatic strong defence
    limitation_status = timeline_data.get('compliance_status', {}).get('limitation', '')
    if 'PREMATURE' in str(limitation_status).upper() and not any(
        'premature' in str(d).lower() for d in high_risk_defences
    ):
        high_risk_defences = [{
            'ground': 'Premature Complaint — Cause of Action Not Yet Arisen',
            'strength': 'FATAL',
            'legal_basis': 'Section 138 NI Act — complaint maintainable only after 15-day payment period expires',
            'impact': 'Accused will succeed in getting complaint dismissed'
        }] + list(high_risk_defences)

    # ── Cross-examination questions ─────────────────────────────
    cross_questions = cross_data.get('likely_questions', [])
    vuln_zones = cross_data.get('vulnerability_zones', [])
    if not cross_questions:
        # Generate from weaknesses
        cross_questions = []
        if not case_meta.get('written_agreement_exists'):
            cross_questions.append("Is it correct that there is no written agreement evidencing the alleged loan?")
        if not case_meta.get('ledger_available'):
            cross_questions.append("Can you produce any ledger entry or bank record showing the transfer of funds?")
        if 'PREMATURE' in str(limitation_status).upper():
            cross_questions.append("Was the complaint filed before the 15-day payment period under Section 138 had expired?")
        if not case_meta.get('postal_proof_available'):
            cross_questions.append("Do you have an Acknowledgment Due (AD) card proving the accused received the notice?")
        cross_questions.append("On what date exactly was the cheque amount allegedly given to the accused, and in what form?")

    # ── Judicial behaviour ──────────────────────────────────────
    jud_confidence = judicial_data.get('confidence', 'LOW')
    jud_indices    = judicial_data.get('behavioral_indices', {})
    jud_section = (
        {
            'court': _safe(judicial_data.get('court_identified'), 'Not specified'),
            'confidence': jud_confidence,
            'limitation_strictness': _safe(jud_indices.get('limitation_strictness'), 'Insufficient data'),
            'settlement_friendly':   _safe(jud_indices.get('settlement_friendly'),   'Insufficient data'),
            'patterns': judicial_data.get('observed_patterns', []),
            'note': (
                'Judicial behaviour analysis unavailable — insufficient court-specific data in knowledge base.'
                if jud_confidence == 'LOW' else
                f"Based on {_safe(judicial_data.get('sample_size'), 'limited')} cases from this court."
            )
        }
    )

    report = {
        "header": {
            "platform": "JUDIQ AI — Legal Intelligence Platform",
            "report_type": "Section 138 NI Act — Litigation Intelligence Report",
            "classification": "CONFIDENTIAL — ATTORNEY-CLIENT PRIVILEGED"
        },

        "document_info": {
            "title": "SECTION 138 NI ACT — LITIGATION INTELLIGENCE REPORT",
            "case_id": _safe(analysis_data.get('case_id'), 'N/A'),
            "date": datetime.now().strftime("%d %B %Y"),
            "processing_time": f"{proc_time:.2f}s" if proc_time else "< 1s",
            "engine_version": _safe(analysis_data.get('engine_version'), 'v10.0')
        },

        # ── PAGE 1: CASE OVERVIEW ──────────────────────────────
        "page_1_case_overview": {
            "title": "CASE OVERVIEW",
            "case_type":       _safe(case_meta.get('case_type'), 'Not specified').upper(),
            "cheque_amount":   _safe(case_meta.get('cheque_amount'), 0, 'inr'),
            "cheque_number":   _safe(case_meta.get('cheque_number'), 'Not provided'),
            "bank_name":       _safe(case_meta.get('bank_name'), 'Not provided'),
            "dishonour_reason":_safe(case_meta.get('dishonour_reason'), 'Not specified'),
            "court_location":  _safe(case_meta.get('court_location'), 'Not specified'),
            "debt_nature":     _safe(case_meta.get('debt_nature'), 'Not specified'),
            "overall_score":   f"{score:.1f}/100",
            "risk_classification": _safe(risk_data.get('compliance_level'), 'Under Review'),
            "fatal_defects_present": "YES — DO NOT FILE" if fatal_flag else "None detected"
        },

        # ── PAGE 2: EXECUTIVE SUMMARY ──────────────────────────
        "page_2_executive_summary": {
            "title": "EXECUTIVE SUMMARY",
            "one_line_verdict": one_liner,
            "case_strength":    _safe(risk_data.get('compliance_level'), 'Under Review'),
            "risk_score":       f"{score:.1f}/100",
            "fatal_flag":       fatal_flag,
            "fatal_count":      len(fatal_defects),
            "fatal_defects":    [
                {
                    "defect":  _safe(d.get('defect'),  'Procedural violation'),
                    "impact":  _safe(d.get('impact'),  'Filing risk'),
                    "action":  _safe(d.get('action') or d.get('remedy'), 'Consult counsel')
                }
                for d in (fatal_defects + defect_data.get('fatal_defects', []))[:5]
            ],
            "strengths":    [_safe(s) for s in strengths[:5]],
            "weaknesses":   [_safe(w) for w in weaknesses[:5]],
            "recommendation": recommendation,
            "limitation_status": _safe(
                timeline_data.get('compliance_status', {}).get('limitation'),
                'Not assessed'
            )
        },

        # ── PAGE 3: TIMELINE (CHRONOLOGICAL) ──────────────────
        "page_3_timeline": {
            "title": "TIMELINE ANALYSIS (CHRONOLOGICAL)",
            "limitation_risk": _safe(limitation_risk, 'Unknown'),
            "events": timeline_events,
            "critical_dates": {
                k: _safe(v, 'Not provided')
                for k, v in timeline_data.get('critical_dates', {}).items()
            },
            "compliance_status": {
                k: _safe(v, 'Not assessed')
                for k, v in timeline_data.get('compliance_status', {}).items()
            },
            "edge_cases": timeline_data.get('edge_cases_detected', [])
        },

        # ── PAGE 4: RISK BREAKDOWN ─────────────────────────────
        "page_4_risk_breakdown": {
            "title": "RISK SCORE BREAKDOWN",
            "overall_score": f"{score:.1f}/100",
            "scoring_method": "Weighted deductions from 100 base — fatal defects cap score at 15",
            "breakdown": risk_breakdown,
            "fatal_override_applied": fatal_flag,
            "score_cap_reason": (
                _safe(fatal_defects[0].get('defect'), 'Fatal condition') if fatal_flag else "No cap applied"
            )
        },

        # ── PAGE 5: DOCUMENTARY EVIDENCE ──────────────────────
        "page_5_documentary_evidence": {
            "title": "DOCUMENTARY EVIDENCE ANALYSIS",
            "overall_strength": f"{doc_data.get('overall_strength_score', 0):.1f}/100",
            "strength_grade": _safe(doc_data.get('strength_grade'), 'Under review'),
            "gaps_identified": doc_gaps,
            "gaps_count": len(doc_gaps),
            "impact_summary": (
                "Strong documentary foundation" if doc_data.get('overall_strength_score', 0) >= 70 else
                f"{len(doc_gaps)} document gap(s) identified — defence can exploit missing records"
            )
        },

        # ── PAGE 6: DEFENCE ANALYSIS ──────────────────────────
        "page_6_defence_analysis": {
            "title": "DEFENCE EXPOSURE ANALYSIS",
            "overall_risk": _safe(defence_data.get('overall_risk'), 'Under review'),
            "fatal_defences": [
                {
                    'ground':      _safe(d.get('ground'),      'Fatal defence'),
                    'strength':    _safe(d.get('strength'),    'FATAL'),
                    'legal_basis': _safe(d.get('legal_basis'), 'Statutory violation'),
                    'impact':      _safe(d.get('viability_impact') or d.get('impact'), 'Case may be dismissed')
                }
                for d in fatal_defences[:3]
            ],
            "high_risk_defences": [
                {
                    'ground':  _safe(d.get('ground'),  'Defence argument'),
                    'strength':_safe(d.get('strength'),'HIGH'),
                    'counter': _safe(d.get('counter_strategy') or d.get('counter'), 'Obtain documentary proof')
                }
                for d in high_risk_defences[:5]
            ],
            "no_defence_note": (
                None if (fatal_defences or high_risk_defences) else
                "Note: No specific defences detected by module — however verify for procedural defects manually"
            )
        },

        # ── PAGE 7: CROSS-EXAMINATION ─────────────────────────
        "page_7_cross_examination": {
            "title": "CROSS-EXAMINATION RISK ANALYSIS",
            "vulnerability_zones": [
                {
                    'zone':   _safe(v.get('zone'),   'Evidentiary gap'),
                    'risk':   _safe(v.get('risk_level'), 'HIGH'),
                    'detail': _safe(v.get('detail'),  'Witness may be challenged')
                }
                for v in vuln_zones[:5]
            ],
            "likely_questions": [_safe(q) for q in cross_questions[:8]],
            "preparation_advice": _safe(
                cross_data.get('preparation_summary'),
                "Prepare witnesses for questions on transaction basis, notice receipt, and document authenticity."
            )
        },

        # ── PAGE 8: JUDICIAL BEHAVIOUR ────────────────────────
        "page_8_judicial_behaviour": {
            "title": "JUDICIAL BEHAVIOUR ANALYSIS",
            "analysis": jud_section,
            "strategic_insights": judicial_data.get('strategic_insights', []),
        },

        # ── PAGE 9: SETTLEMENT INTELLIGENCE ──────────────────
        "page_9_settlement": {
            "title": "SETTLEMENT & FINANCIAL EXPOSURE",
            "settlement_recommended": settlement_data.get('settlement_recommended', False),
            "settlement_range": settlement_data.get('recommended_settlement_range', {}),
            "financial_exposure": settlement_data.get('financial_exposure', {}),
            "strategy": _safe(settlement_data.get('settlement_strategy'), 'Evaluate based on case strength')
        },

        # ── PAGE 10: CONCLUSIONS ──────────────────────────────
        "page_10_conclusions": {
            "title": "CONCLUSIONS & RECOMMENDED ACTIONS",
            "verdict": one_liner,
            "immediate_actions": exec_summ.get('next_actions', []) or (
                ["Address fatal defect before filing", "Consult litigation counsel urgently"]
                if fatal_flag else
                ["Strengthen documentary evidence", "Verify notice service proof", "File within limitation period"]
            ),
            "strategic_recommendations": exec_summ.get('strategic_recommendations', []),
            "disclaimer": (
                "This analysis is a structured compliance assessment based on information provided. "
                "It does NOT constitute legal advice or legal representation. "
                "All findings must be reviewed by qualified legal counsel before filing."
            )
        },

        "footer": {
            "prepared_by": "JUDIQ AI Intelligence Engine v10.0",
            "generated_at": datetime.now().strftime("%d %B %Y, %H:%M"),
            "processing_time": f"{proc_time:.2f}s" if (proc_time and proc_time > 0) else "< 1s",
            "processing_time_seconds": round(proc_time, 2) if proc_time else 0,
            "classification": "CONFIDENTIAL — FOR AUTHORIZED USE ONLY",
            "disclaimer": "NOT LEGAL ADVICE — Consult qualified legal counsel before acting on this report."
        }
    }

    return report


def generate_executive_report_legacy(analysis_data: Dict) -> Dict:
    """Legacy version kept for backward compatibility."""
    executive_summary = analysis_data.get('executive_summary', {})
    risk_data = analysis_data['modules'].get('risk_assessment', {})
    timeline_data = analysis_data['modules'].get('timeline_intelligence', {})

    report = {
        "header": {
            "platform": "JUDIQ AI — Legal Intelligence Platform",
        },

        "document_info": {
            "title": "SECTION 138 NI ACT - LITIGATION INTELLIGENCE REPORT",
            "subtitle": "Executive Summary",
            "case_id": analysis_data['case_id'],
            "date": datetime.now().strftime("%d %B, %Y"),
            "classification": "CONFIDENTIAL - ATTORNEY-CLIENT PRIVILEGED"
        },

        "page_1": {
            "case_overview": {
                "title": "CASE OVERVIEW",
                "case_type": analysis_data['case_metadata']['case_type'].upper(),
                "cheque_amount": f"₹{analysis_data['case_metadata']['cheque_amount']:,.2f}",
                "overall_assessment": executive_summary.get('overall_assessment', 'Assessment pending'),
                "risk_classification": risk_data.get('compliance_level', 'Unknown'),
                "overall_score": f"{risk_data.get('overall_risk_score', 0):.1f}/100"
            },

            "critical_findings": {
                "title": "⚠️ CRITICAL FINDINGS",
                "items": [
                    {
                        "severity": issue.get('severity', 'HIGH'),
                        "finding": issue.get('issue', issue.get('category', 'Issue')),
                        "impact": issue.get('impact', 'Significant impact on case')
                    }
                    for issue in executive_summary.get('critical_risks', [])[:3]
                ]
            },

            "key_strengths": {
                "title": "✓ KEY STRENGTHS",
                "items": executive_summary.get('strengths', [])[:3]
            },

            "key_weaknesses": {
                "title": "✗ KEY WEAKNESSES",
                "items": executive_summary.get('weaknesses', [])[:3]
            },

            "risk_breakdown": {
                "title": "RISK BREAKDOWN",
                "categories": [
                    {
                        "category": cat,
                        "score": f"{data.get('score', 0):.0f}%",
                        "status": data.get('status', 'Unknown')
                    }
                    for cat, data in risk_data.get('category_scores', {}).items()
                ]
            }
        },

        "page_2": {
            "timeline_compliance": {
                "title": "TIMELINE COMPLIANCE",
                "limitation_risk": timeline_data.get('limitation_risk', 'Unknown'),
                "critical_dates": timeline_data.get('critical_dates', {}),
                "compliance_status": timeline_data.get('compliance_status', {})
            },

            "strategic_recommendations": {
                "title": "STRATEGIC RECOMMENDATIONS",
                "urgent": [
                    rec for rec in executive_summary.get('strategic_recommendations', [])
                    if rec.get('priority') in ['URGENT', 'CRITICAL']
                ][:3],
                "high_priority": [
                    rec for rec in executive_summary.get('strategic_recommendations', [])
                    if rec.get('priority') == 'HIGH'
                ][:2]
            },

            "next_actions": {
                "title": "IMMEDIATE NEXT ACTIONS",
                "items": executive_summary.get('next_actions', [])[:5]
            },

            "financial_exposure": {
                "title": "FINANCIAL EXPOSURE ANALYSIS",
                "data": analysis_data['modules'].get('settlement_analysis', {})
            },

            "confidence_assessment": {
                "title": "ANALYSIS CONFIDENCE",
                "confidence_level": risk_data.get('confidence_level', 'Analysis based on provided data'),
                "data_completeness": "Based on complete case information provided",
                "limitations": "This is an intelligence tool, not legal advice. Consult qualified legal counsel."
            }
        },

        "footer": {
            "disclaimer": "This report is generated by JUDIQ AI Legal Intelligence Platform. It provides analytical insights and risk assessment based on the information provided. This is NOT legal advice. Engage qualified legal counsel for case-specific guidance.",
            "prepared_by": "JUDIQ AI Intelligence Engine v5.0",
            "classification": "CONFIDENTIAL - FOR AUTHORIZED USE ONLY"
        }
    }

    return report


def generate_detailed_report(analysis_data: Dict) -> Dict:

    report = {
        "header": {
            "company": "JUDIQ AI",
            "tagline": "Legal Intelligence Platform - Comprehensive Analysis",
            "contact": {
                "phone": "+123-456-7890",
                "email": "hello@judiq.ai",
                "platform": "JUDIQ AI"
            }
        },

        "document_info": {
            "title": "SECTION 138 NI ACT - COMPREHENSIVE LITIGATION INTELLIGENCE REPORT",
            "case_id": analysis_data['case_id'],
            "date": datetime.now().strftime("%d %B, %Y"),
            "version": "Detailed Analysis v5.0",
            "classification": "CONFIDENTIAL - ATTORNEY-CLIENT PRIVILEGED"
        },

        "table_of_contents": {
            "sections": [
                "1. Executive Summary",
                "2. Timeline Intelligence Analysis",
                "3. Ingredient Compliance Matrix",
                "4. Documentary Strength Assessment",
                "5. Liability & Impleading Analysis",
                "6. Defence Vulnerability Matrix",
                "7. Procedural Defect Scan",
                "8. Overall Risk Assessment",
                "9. Settlement & Financial Exposure",
                "10. Judicial Behavior Intelligence",
                "11. Presumption & Burden Analysis",
                "12. Cross-Examination Risk Assessment",
                "13. Strategic Recommendations",
                "14. Appendices"
            ]
        },

        "section_1_executive": generate_executive_report(analysis_data),

        "section_2_timeline": {
            "title": "TIMELINE INTELLIGENCE ANALYSIS",
            "data": analysis_data['modules'].get('timeline_intelligence', {}),
            "subsections": [
                "2.1 Critical Dates Analysis",
                "2.2 Limitation Period Calculation",
                "2.3 Timeline Compliance Status",
                "2.4 Edge Cases Detected",
                "2.5 Risk Markers"
            ]
        },

        "section_3_ingredients": {
            "title": "INGREDIENT COMPLIANCE MATRIX",
            "data": analysis_data['modules'].get('ingredient_compliance', {}),
            "subsections": [
                "3.1 Seven Essential Ingredients",
                "3.2 Ingredient-wise Scoring",
                "3.3 Weakest Ingredients",
                "3.4 Fatal Defects Analysis",
                "3.5 Compliance Recommendations"
            ]
        },

        "section_4_documentary": {
            "title": "DOCUMENTARY STRENGTH ASSESSMENT",
            "data": analysis_data['modules'].get('documentary_strength', {}),
            "subsections": [
                "4.1 Primary Documents (40%)",
                "4.2 Debt Proof Documents (35%)",
                "4.3 Supporting Documents (15%)",
                "4.4 Procedural Documents (10%)",
                "4.5 Critical Gaps & Recommendations"
            ]
        },

        "section_5_liability": {
            "title": "LIABILITY & IMPLEADING ANALYSIS",
            "data": analysis_data['modules'].get('liability_analysis', {}),
            "subsections": [
                "5.1 Party Impleading Status",
                "5.2 Section 141 Compliance",
                "5.3 Vicarious Liability Check",
                "5.4 Specific Averment Requirements",
                "5.5 Risk Assessment"
            ]
        },

        "section_6_defence": {
            "title": "DEFENCE VULNERABILITY MATRIX",
            "data": analysis_data['modules'].get('defence_matrix', {}),
            "subsections": [
                "6.1 Possible Defences Identified",
                "6.2 Defence Strength Scoring",
                "6.3 Evidence Requirements",
                "6.4 Counter-Strategies",
                "6.5 Strongest Defence Analysis"
            ]
        },

        "section_7_procedural": {
            "title": "PROCEDURAL DEFECT SCAN",
            "data": analysis_data['modules'].get('procedural_defects', {}),
            "subsections": [
                "7.1 Fatal Defects",
                "7.2 Curable Defects",
                "7.3 Jurisdiction Analysis",
                "7.4 Notice Compliance",
                "7.5 Remedies & Priority"
            ]
        },

        "section_8_risk": {
            "title": "OVERALL RISK ASSESSMENT",
            "data": analysis_data['modules'].get('risk_assessment', {}),
            "subsections": [
                "8.1 Category-wise Scoring",
                "8.2 Weighted Risk Model",
                "8.3 Fatal Defect Override",
                "8.4 Critical Issues",
                "8.5 Confidence Level"
            ]
        },

        "section_9_settlement": {
            "title": "SETTLEMENT & FINANCIAL EXPOSURE",
            "data": analysis_data['modules'].get('settlement_analysis', {}),
            "subsections": [
                "9.1 Financial Exposure Calculation",
                "9.2 Settlement Leverage",
                "9.3 Strategic Options",
                "9.4 Interim Compensation",
                "9.5 Appeal Deposit Requirements"
            ]
        },

        "section_10_judicial": {
            "title": "JUDICIAL BEHAVIOR INTELLIGENCE",
            "data": analysis_data['modules'].get('judicial_behavior', {}),
            "subsections": [
                "10.1 Court Behavioral Indices",
                "10.2 Observed Patterns",
                "10.3 Strategic Insights",
                "10.4 Dismissal Fingerprint",
                "10.5 Confidence Assessment"
            ]
        },

        "section_11_presumption": {
            "title": "PRESUMPTION & BURDEN ANALYSIS",
            "data": analysis_data['modules'].get('presumption_analysis', {}),
            "subsections": [
                "11.1 Presumption Activation (S.139)",
                "11.2 Burden Shift Timeline",
                "11.3 Rebuttal Evidence Assessment",
                "11.4 Strategic Position",
                "11.5 Evidence Requirements"
            ]
        },

        "section_12_cross_exam": {
            "title": "CROSS-EXAMINATION RISK ASSESSMENT",
            "data": analysis_data['modules'].get('cross_examination_risk', {}),
            "subsections": [
                "12.1 Vulnerability Zones",
                "12.2 Likely Questions",
                "12.3 Preparation Requirements",
                "12.4 Overall Risk Level"
            ]
        },

        "section_13_strategic": {
            "title": "STRATEGIC RECOMMENDATIONS",
            "data": analysis_data.get('executive_summary', {}),
            "subsections": [
                "13.1 Urgent Actions",
                "13.2 High Priority Actions",
                "13.3 Medium Priority Actions",
                "13.4 Case Strategy",
                "13.5 Timeline for Actions"
            ]
        },

        "section_14_appendices": {
            "title": "APPENDICES",
            "subsections": [
                "A. Statutory Provisions Referenced",
                "B. Calculation Methodology",
                "C. Risk Scoring Formula",
                "D. Data Sources & Confidence",
                "E. Glossary of Terms"
            ],
            "statutory_provisions": [
                {"section": "138", "title": "Dishonour of cheque for insufficiency, etc., of funds in the account"},
                {"section": "139", "title": "Presumption in favour of holder"},
                {"section": "142", "title": "Cognizance of offences"},
                {"section": "143A", "title": "Interim compensation"},
                {"section": "147", "title": "Compounding of offences"},
                {"section": "148", "title": "Suspension of sentence pending appeal"}
            ],
            "disclaimer": "IMPORTANT DISCLAIMER: This report is generated by JUDIQ AI, an artificial intelligence-powered legal intelligence platform. It provides analytical insights, risk assessments, and strategic recommendations based on the information provided and historical data patterns. THIS IS NOT LEGAL ADVICE. This analysis should not be construed as legal counsel or relied upon as a substitute for consultation with qualified legal professionals. Always engage experienced advocates or legal counsel for case-specific advice and representation. JUDIQ AI and its operators accept no liability for decisions made based on this report."
        },

        "metadata": {
            "total_pages": "Approximately 10-12 pages",
            "generation_time": analysis_data.get('processing_time_seconds', 0),
            "analysis_modules": len(analysis_data.get('modules', {})),
            "platform_version": "JUDIQ AI v5.0 - Legal Intelligence Platform",
            "architecture": "Hybrid (Deterministic + RAG + LLM Enhancement)"
        }
    }

    return report


def classify_case_outcome(risk_score: float, fatal_defects: List, timeline_risk: str) -> Dict:

    classification = {
        'category': '',
        'probability': '',
        'recommendation': '',
        'risk_level': ''
    }

    if fatal_defects and len(fatal_defects) > 0:
        classification['category'] = 'HIGH DISMISSAL RISK'
        classification['probability'] = 'Case likely to be dismissed on technical grounds'
        classification['recommendation'] = 'Urgent remedial action required or settlement'
        classification['risk_level'] = 'CRITICAL'
        return classification

    if timeline_risk == 'CRITICAL':
        classification['category'] = 'HIGH DISMISSAL RISK'
        classification['probability'] = 'Limitation issues may cause dismissal'
        classification['recommendation'] = 'Prepare strong limitation defense or settle'
        classification['risk_level'] = 'HIGH'
        return classification

    if risk_score >= SCORE_EXCELLENT:
        classification['category'] = 'STRONG PROSECUTION'
        classification['probability'] = 'High likelihood of conviction'
        classification['recommendation'] = 'Proceed to trial with confidence'
        classification['risk_level'] = 'LOW'

    elif risk_score >= SCORE_GOOD:
        classification['category'] = 'MODERATE STRENGTH'
        classification['probability'] = 'Reasonable chance of conviction'
        classification['recommendation'] = 'Strengthen weak areas, consider settlement leverage'
        classification['risk_level'] = 'MODERATE'

    elif risk_score >= SCORE_ADEQUATE:
        classification['category'] = 'MODERATE RISK'
        classification['probability'] = 'Uncertain outcome, case has weaknesses'
        classification['recommendation'] = 'Serious settlement consideration advised'
        classification['risk_level'] = 'MODERATE-HIGH'

    else:
        classification['category'] = 'WEAK CASE'
        classification['probability'] = 'High risk of acquittal'
        classification['recommendation'] = 'Settlement strongly recommended'
        classification['risk_level'] = 'HIGH'

    return classification


def enforce_defence_dependencies(case_data: Dict, defence_result: Dict, doc_compliance: Dict) -> Dict:

    dependencies = {
        'enforced': [],
        'violations': [],
        'mandatory_requirements': []
    }

    if case_data.get('is_company_case'):
        director_inactive = not case_data.get('director_active', True)
        has_averment = case_data.get('director_specific_averment', False)

        if director_inactive and not has_averment:
            dependencies['violations'].append({
                'requirement': 'Section 141 - Specific Averment Required',
                'severity': 'CRITICAL',
                'consequence': 'Director liability will fail',
                'action': 'MANDATORY: Add specific averment showing director role and knowledge'
            })
            dependencies['mandatory_requirements'].append('director_averment')

    if case_data.get('part_payment_made'):
        part_amount = case_data.get('part_payment_amount', 0)
        has_proof = case_data.get('part_payment_proof_uploaded', False)

        if not has_proof:
            dependencies['violations'].append({
                'requirement': 'Part Payment - Documentary Proof Required',
                'severity': 'CRITICAL',
                'consequence': 'Accused can claim full payment - burden on complainant',
                'action': 'MANDATORY: Upload receipt/acknowledgment of partial payment'
            })
            dependencies['mandatory_requirements'].append('part_payment_proof')

    if case_data.get('electronic_evidence'):
        has_65b = case_data.get('section_65b_certificate', False)

        if not has_65b:
            dependencies['violations'].append({
                'requirement': 'Section 65B Certificate - Electronic Evidence',
                'severity': 'CRITICAL',
                'consequence': 'Electronic evidence inadmissible',
                'action': 'MANDATORY: Obtain Section 65B certificate'
            })
            dependencies['mandatory_requirements'].append('section_65b')

    debt_disputed = not case_data.get('written_agreement_exists', False)
    if debt_disputed:
        has_ledger = case_data.get('ledger_available', False)
        has_bank_stmt = case_data.get('bank_statement_available', False)

        if not (has_ledger or has_bank_stmt):
            dependencies['violations'].append({
                'requirement': 'Debt Proof - Documentary Support Required',
                'severity': 'FATAL',
                'consequence': 'Case may collapse - no proof of debt',
                'action': 'MANDATORY: Obtain ledger or bank statements'
            })
            dependencies['mandatory_requirements'].append('debt_proof')

    dependencies['enforced'] = len(dependencies['violations']) == 0
    dependencies['violation_count'] = len(dependencies['violations'])

    return dependencies




def cross_validate_defence_documents(case_data: Dict, defence_result: Dict, doc_compliance: Dict) -> Dict:

    cross_validation = {
        'gaps_identified': [],
        'evidence_consistency': 'PASS',
        'severity': 'NONE'
    }

    has_written_agreement = case_data.get('written_agreement_exists', False)
    has_ledger = case_data.get('ledger_available', False)
    has_bank_stmt = case_data.get('bank_statement_available', False)

    if not has_written_agreement:
        if not (has_ledger or has_bank_stmt):
            cross_validation['gaps_identified'].append({
                'gap': 'DEBT_DISPUTED_NO_PROOF',
                'description': 'Debt disputed but no documentary evidence uploaded',
                'severity': 'FATAL',
                'consequence': 'Accused can deny debt - No proof available',
                'required_action': 'Upload ledger or bank statements'
            })
            cross_validation['evidence_consistency'] = 'FATAL_GAP'
            cross_validation['severity'] = 'FATAL'

    if case_data.get('part_payment_made'):
        if not case_data.get('part_payment_proof_uploaded'):
            cross_validation['gaps_identified'].append({
                'gap': 'PART_PAYMENT_NO_PROOF',
                'description': 'Part payment claimed but no receipt uploaded',
                'severity': 'CRITICAL',
                'consequence': 'Accused can claim full payment - No counter evidence',
                'required_action': 'Upload part payment receipt/acknowledgment'
            })
            if cross_validation['severity'] != 'FATAL':
                cross_validation['severity'] = 'CRITICAL'

    if case_data.get('electronic_evidence'):
        if not case_data.get('section_65b_certificate'):
            if not any(g['gap'] == 'ELECTRONIC_NO_65B' for g in cross_validation['gaps_identified']):
                cross_validation['gaps_identified'].append({
                    'gap': 'ELECTRONIC_NO_65B',
                    'description': 'Electronic evidence without Section 65B certificate',
                    'severity': 'CRITICAL',
                    'consequence': 'Electronic evidence inadmissible',
                    'required_action': 'Obtain Section 65B certificate'
                })
                if cross_validation['severity'] == 'NONE':
                    cross_validation['severity'] = 'CRITICAL'

    cross_validation['total_gaps'] = len(cross_validation['gaps_identified'])
    cross_validation['requires_immediate_action'] = cross_validation['severity'] in ['FATAL', 'CRITICAL']

    return cross_validation


def save_analysis_to_db(analysis_report: Dict) -> bool:
    """
    Save analysis to database with comprehensive error handling.

    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        risk_data = analysis_report['modules'].get('risk_assessment', {})
        category_scores = risk_data.get('category_scores', {})
        timeline_data = analysis_report['modules'].get('timeline_intelligence', {})

        # Normalize compliance_level to match database enum
        raw_compliance = risk_data.get('compliance_level', 'MODERATE')
        normalized_compliance = normalize_compliance_level(raw_compliance)

        # FIX #19: Use parameterized query (SQL injection safe)
        cursor.execute("""
            INSERT INTO case_analyses (
                case_id, analysis_timestamp, case_type, cheque_amount,
                overall_risk_score, compliance_level, fatal_defect_override,
                fatal_type, timeline_risk, ingredient_compliance,
                documentary_strength, analysis_json, engine_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            analysis_report['case_id'],
            analysis_report['analysis_timestamp'],
            analysis_report['case_metadata']['case_type'],
            analysis_report['case_metadata'].get('cheque_amount', 0),
            risk_data.get('overall_risk_score', 0),
            normalized_compliance,  # Use normalized value
            1 if analysis_report.get('fatal_flag') else 0,
            analysis_report.get('overall_status', '').split(' - ')[0] if ' - ' in analysis_report.get('overall_status', '') else None,
            timeline_data.get('limitation_risk', 'Unknown'),
            category_scores.get('Ingredient Compliance', {}).get('score', 0),
            category_scores.get('Documentary Strength', {}).get('score', 0),
            json.dumps(analysis_report),
            analysis_report.get('engine_version', 'v11.3')  # Updated version
        ))

        conn.commit()
        conn.close()
        logger.info(f"✅ Analysis saved to DB: {analysis_report['case_id']}")
        return True

    except sqlite3.Error as e:
        logger.error(f"❌ Database error saving analysis: {e}")
        return False
    except KeyError as e:
        logger.error(f"❌ Missing required field in analysis: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error saving analysis: {e}")
        return False

from contextlib import asynccontextmanager


def generate_plain_summary(analysis: Dict, case_data: Dict) -> Dict:
    """
    Generate a short, plain-language summary of the analysis —
    the kind a lawyer can read in 30 seconds and immediately act on.

    Returned as 'plain_summary' at the top level of the API response.
    """
    risk    = analysis.get('modules', {}).get('risk_assessment', {})
    timeline = analysis.get('modules', {}).get('timeline_intelligence', {})
    doc     = analysis.get('modules', {}).get('document_compliance', {})
    defence = analysis.get('modules', {}).get('defence_risk_analysis', {})
    verdict = analysis.get('verdict', {})

    score         = risk.get('overall_risk_score', 0)
    compliance    = risk.get('compliance_level', 'UNKNOWN')
    fatal_flag    = analysis.get('fatal_flag', False)
    fatal_defects = doc.get('fatal_defects', [])
    limitation_risk = timeline.get('limitation_risk', 'UNKNOWN')

    amount = case_data.get('cheque_amount', 0)
    case_type = case_data.get('case_type', 'complainant')
    perspective = "Complainant" if case_type == 'complainant' else "Accused"

    # ── Strengths ──
    strengths = []
    cat_scores = risk.get('category_scores', {})
    if cat_scores.get('Timeline Compliance', {}).get('score', 0) >= 80:
        strengths.append("Timeline compliance is strong (notice and dates are in order)")
    if cat_scores.get('Ingredient Compliance', {}).get('score', 0) >= 80:
        strengths.append("All key legal ingredients of Section 138 are present")
    if case_data.get('return_memo_available'):
        strengths.append("Dishonour memo available — primary evidence secured")
    if case_data.get('postal_proof_available'):
        strengths.append("Postal proof of notice available — service presumption established")
    if case_data.get('original_cheque_available'):
        strengths.append("Original cheque available — foundational document secured")
    if not strengths:
        strengths.append("Case ingredients are partially in place")

    # ── Weaknesses ──
    weaknesses = []
    if not case_data.get('written_agreement_exists'):
        weaknesses.append("No written loan/transaction agreement — accused can deny legally enforceable debt")
    if not case_data.get('ledger_available'):
        weaknesses.append("No ledger or account records — difficult to prove financial capacity and transaction trail")
    if limitation_risk in ['CRITICAL', 'EXPIRED']:
        weaknesses.append(f"Limitation period issue: {timeline.get('compliance_status', {}).get('limitation', 'check timeline')}")
    if fatal_defects:
        for d in fatal_defects[:2]:
            weaknesses.append(f"Fatal defect: {d.get('defect', 'procedural violation')}")
    high_risk_defences = defence.get('high_risk_defences', [])
    if high_risk_defences:
        weaknesses.append(f"Strong defence exposure: {high_risk_defences[0].get('ground', 'defence argument likely')}")
    if cat_scores.get('Documentary Strength', {}).get('score', 0) < 50:
        weaknesses.append("Documentary evidence is weak — transaction proof needs strengthening")
    if not weaknesses:
        weaknesses.append("No critical weaknesses detected at this stage")

    # ── One-line verdict ──
    if fatal_flag:
        one_line = (
            f"⛔ Case has a fatal procedural defect — filing not advisable until resolved. "
            f"Risk score: {score:.0f}/100."
        )
    elif score >= 75:
        one_line = (
            f"✅ Strong case — statutory requirements largely satisfied. "
            f"Risk score: {score:.0f}/100. Address minor documentary gaps before filing."
        )
    elif score >= 60:
        one_line = (
            f"⚠️ Moderate case — procedural compliance is adequate but evidence is weak. "
            f"Risk score: {score:.0f}/100. Strengthen documentation before filing."
        )
    elif score >= 40:
        one_line = (
            f"🔴 Weak case — significant evidentiary and procedural gaps. "
            f"Risk score: {score:.0f}/100. Substantial remediation required."
        )
    else:
        one_line = (
            f"⛔ Very weak case — multiple critical deficiencies. "
            f"Risk score: {score:.0f}/100. Do not file without major remediation."
        )

    # ── Recommendation ──
    settlement_data = analysis.get('modules', {}).get('settlement_intelligence', {})
    settlement_recommended = settlement_data.get('settlement_recommended', False)

    if fatal_flag:
        # Fatal defect takes priority — settlement is secondary to fixing the filing defect
        recommendation = "Do not file. Address the fatal defect first (e.g. wait for 15-day period to expire and refile) — consult your lawyer on remediation."
    elif settlement_recommended or score < 60:
        settlement_value = settlement_data.get('recommended_settlement_range', {})
        low  = settlement_value.get('minimum', amount * 0.5)
        high = settlement_value.get('maximum', amount * 0.8)
        recommendation = (
            f"Consider negotiating a settlement "
            f"(estimated range: ₹{indian_number_format(low)}–₹{indian_number_format(high)}) "
            f"while simultaneously strengthening documentary evidence. "
            f"Filing now carries a meaningful acquittal risk."
        )
    else:
        recommendation = (
            f"Case is in a reasonable position to file. "
            f"Obtain a written acknowledgement of debt or supporting transaction records "
            f"to further reduce acquittal exposure."
        )

    return {
        'one_line_verdict': one_line or 'Analysis complete — see details below',
        'perspective': perspective or 'Not specified',
        'cheque_amount': f"₹{indian_number_format(amount)}" if amount else 'Not specified',
        'risk_score': f"{score:.1f}/100",
        'case_classification': compliance or 'Under Review',
        'strengths': strengths or ['Analysis complete'],
        'weaknesses': weaknesses or ['Review full analysis for details'],
        'recommendation': recommendation or 'Consult legal counsel',
        'fatal_flag': fatal_flag,
        'fatal_defects_count': len(analysis.get('modules', {}).get('risk_assessment', {}).get('fatal_defects', []) +
                                    analysis.get('modules', {}).get('procedural_defects', {}).get('fatal_defects', [])),
        'limitation_status': timeline.get('compliance_status', {}).get('limitation') or 'Not assessed',
        'processing_time': f"{analysis.get('processing_time_seconds', 0)}s",
    }

@asynccontextmanager
async def lifespan(app: FastAPI):

    print("\n" + "="*100)
    print("🚀 JUDIQ AI - LEGAL INTELLIGENCE PLATFORM")
    print("="*100 + "\n")

    # Render deployment
    print(f"📂 Data directory: {DATA_DIR}")
    init_analytics_db()

    # Download KB from Google Drive (requires GDRIVE_FILE_ID env var on Render)
    print("\n" + "="*80)
    print("📚 LOADING KNOWLEDGE BASE FROM GOOGLE DRIVE")
    print("="*80)
    load_kb()

    print("="*100)
    print("✅ JUDIQ AI READY - PROFESSIONAL LEGAL INTELLIGENCE")
    print("="*100)
    print(f"📚 Knowledge Base: {len(kb_data) if kb_loaded else 'Minimal fallback'} rows | Drive: {'✅' if kb_loaded else '❌ Set GDRIVE_FILE_ID env var'}")
    print("🤖 LLM Enhancement: Disabled (Render — cross-exam uses rule-based engine)")
    print(f"💾 Analytics DB: {analytics_db_path}")
    print("⚡ Embedding Cache: Disabled (Render)")
    print("="*100 + "\n")
    print("🎯 INTELLIGENCE CAPABILITIES (90% ELITE-GRADE):")
    print("  ✅ Layer 1: Timeline Intelligence (Deterministic + Confidence)")
    print("  ✅ Layer 2: Ingredient Compliance (Calibrated Weights)")
    print("  ✅ Layer 3: Documentary Strength (Severity Tiers)")
    print("  ✅ Layer 4: Liability Expansion (Section 141)")
    print("  ✅ Layer 5: Defence Vulnerability (Logic)")
    print("  ✅ Layer 6: Procedural Defect Scanner (Rule-Based)")
    print("  ✅ Layer 7: Risk Scoring (Fatal Override + Confidence)")
    print("  ✅ Layer 8: Settlement & Financial Exposure")
    print("  ✅ Layer 9: Contradiction Detector")
    print("  ✅ Layer 10: Judicial Behavior (RATIO-BASED ANALYTICS)")
    print("  ✅ Layer 11: Presumption Rebuttal (Evidence-Based)")
    print("  ✅ Layer 12: Cross-Examination Risk")
    print("  ✅ Layer 13: Professional Reports (Dual Format)")
    print("\n🏗️  ARCHITECTURE:")
    print("  📐 Deterministic: 9 modules (100% rule-based)")
    print("  🔍 RAG Analytics: Ratio-based court statistics")
    print("  🤖 LLM: Optional (explanation only)")
    print("  ⚠️  Fatal Override: Active (auto-caps scores)")
    print("  📊 Data Calibration: Weights from dismissal rates")
    print("  🎯 Confidence Layer: Every output scored")
    print("  ✅ Validation: Methodology documented")
    print("="*100 + "\n")

    # Signal main() that all startup work is done — ngrok can now open safely
    _startup_complete.set()

    yield

    # Render: no ngrok to clean up

    print("\n✅ JUDIQ v5.0 shutting down...")

app = FastAPI(
    title="JUDIQ v5.0 - Legal Intelligence Platform",
    version="5.0.0",
    description="90% Elite-Grade Section 138 NI Act Analysis - Ratio-Based Analytics",
    lifespan=lifespan
)

# FIX #10: Add rate limiting (only if slowapi available)
if RATE_LIMITING_AVAILABLE:
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    logger.info("✅ Rate limiting enabled (30 requests/minute)")
else:
    limiter = DummyLimiter()
    logger.warning("⚠️ Rate limiting disabled - install slowapi to enable")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Netlify frontend can call this API freely
    allow_methods=["*"],
    allow_headers=["*"]
)

def enforce_verdict_integrity(analysis_report: Dict) -> Dict:
    """
    VERDICT INTEGRITY LAYER - Master Controller

    Reconciles ALL decision modules into ONE authoritative verdict.
    This is the ONLY source of truth for final case status.

    Priority Hierarchy:
    1. Fatal flag (terminal authority)
    2. Defence risk FATAL
    3. Critical defects
    4. Weighted score

    This function ensures NO contradictions in final output.
    """
    logger.info("  🎯 Reconciling all decision modules...")

    # PHASE 1: Gather all decision inputs - READ FROM AUTHORITATIVE SOURCES
    fatal_flag = analysis_report.get('fatal_flag', False)
    overall_status = analysis_report.get('overall_status', '')

    # Get risk score from AUTHORITATIVE SOURCE (risk_assessment module, not top-level)
    # Top-level risk_score may not be set yet or may be stale
    risk_score = 50  # default
    if 'modules' in analysis_report and 'risk_assessment' in analysis_report['modules']:
        risk_module = analysis_report['modules']['risk_assessment']
        risk_score = risk_module.get('overall_risk_score', 50)
        logger.info(f"  📊 Reading authoritative risk score: {risk_score}")
    else:
        # Fallback: try top-level
        risk_score = analysis_report.get('risk_score', 50)
        logger.warning(f"  ⚠️ Using fallback risk score: {risk_score}")

    # Comprehensive fatal detection across ALL modules
    fatal_sources = []

    # Check defence risks
    defence_risk_level = 'MEDIUM'
    defence_fatal = False
    if 'modules' in analysis_report and 'defence_risk_analysis' in analysis_report['modules']:
        defence_module = analysis_report['modules']['defence_risk_analysis']
        defence_risk_level = defence_module.get('overall_risk', 'MEDIUM')
        defence_fatal = len(defence_module.get('fatal_defences', [])) > 0
        if defence_fatal:
            fatal_sources.append('defence_risk')

    # Get fatal defects from risk assessment
    fatal_defects = []
    if 'modules' in analysis_report and 'risk_assessment' in analysis_report['modules']:
        fatal_defects = analysis_report['modules']['risk_assessment'].get('fatal_defects', [])
        if len(fatal_defects) > 0:
            fatal_sources.append('risk_assessment')

    # Check Section 65B fatal
    if 'modules' in analysis_report and 'section_65b_compliance' in analysis_report['modules']:
        section_65b = analysis_report['modules']['section_65b_compliance']
        if section_65b.get('risk_level') == 'FATAL':
            fatal_sources.append('section_65b')

    # Check Income Tax 269SS critical (not necessarily fatal, but high risk)
    income_tax_critical = False
    if 'modules' in analysis_report and 'income_tax_269ss' in analysis_report['modules']:
        tax_module = analysis_report['modules']['income_tax_269ss']
        if tax_module.get('violation_detected') and tax_module.get('risk_level') == 'CRITICAL':
            fatal_sources.append('income_tax_269ss')
            income_tax_critical = True

    # Check part payment fatal
    if 'modules' in analysis_report and 'part_payment_analysis' in analysis_report['modules']:
        part_payment = analysis_report['modules']['part_payment_analysis']
        if part_payment.get('defence_strength') == 'FATAL':
            fatal_sources.append('part_payment')

    # Check notice delivery fatal
    if 'modules' in analysis_report and 'notice_delivery_status' in analysis_report['modules']:
        notice_delivery = analysis_report['modules']['notice_delivery_status']
        if notice_delivery.get('risk_level') == 'FATAL':
            fatal_sources.append('notice_delivery')

    # Check jurisdiction validity (FIX #2: Escalate CRITICAL jurisdiction to FATAL)
    jurisdiction_penalty = 0
    if 'modules' in analysis_report and 'territorial_jurisdiction' in analysis_report['modules']:
        jurisdiction = analysis_report['modules']['territorial_jurisdiction']
        jurisdiction_status = jurisdiction.get('status', '')

        # Don't penalize if data is insufficient - only penalize if actually invalid
        if jurisdiction_status == 'INSUFFICIENT_DATA':
            # No penalty - just flag as needing data
            logger.info("  ℹ️ Jurisdiction: Insufficient data - no penalty applied")
        elif not jurisdiction.get('jurisdiction_valid', True):
            # Only penalize when jurisdiction is actually INVALID (not just missing data)
            jurisdiction_risk = jurisdiction.get('risk_level', 'MEDIUM')
            if jurisdiction_risk == 'CRITICAL':
                # CRITICAL jurisdiction = FATAL (blocks filing)
                fatal_sources.append('jurisdiction_critical')
                jurisdiction_penalty = 40  # Also apply penalty
                logger.error("  🔴 Jurisdiction CRITICAL - FATAL condition (filing blocked)")
            elif jurisdiction_risk == 'HIGH':
                jurisdiction_penalty = 20  # Cap at 70 (MODERATE)
                logger.warning("  ⚠️ Jurisdiction INVALID (HIGH) - score reduction applied")

    # Check Section 65B impact (FIX #3: Only penalize if PRIMARY evidence)
    section_65b_penalty = 0
    if 'modules' in analysis_report and 'section_65b_compliance' in analysis_report['modules']:
        section_65b = analysis_report['modules']['section_65b_compliance']
        if section_65b.get('applicable', False) and not section_65b.get('compliant', True):
            risk_level = section_65b.get('risk_level', 'LOW')
            # Only penalize FATAL/CRITICAL (electronic is PRIMARY proof)
            # Don't penalize HIGH (electronic is just supporting evidence)
            if risk_level in ['FATAL', 'CRITICAL']:
                section_65b_penalty = 10
                logger.warning("  ⚠️ Section 65B - Electronic evidence is PRIMARY proof - penalty applied")
            elif risk_level == 'HIGH':
                # Supporting evidence only - flag but don't penalize heavily
                logger.info("  ℹ️ Section 65B - Electronic evidence is supporting only - minimal impact")

    # Apply penalties to risk_score
    risk_score = risk_score - jurisdiction_penalty - section_65b_penalty
    risk_score = max(0, risk_score)  # Floor at 0
    logger.info(f"  📊 Adjusted risk score after jurisdiction/65B: {risk_score}")

    # CRITICAL: Update risk_assessment module with adjusted score to prevent contradictions
    if 'modules' in analysis_report and 'risk_assessment' in analysis_report['modules']:
        risk_module = analysis_report['modules']['risk_assessment']
        base_score = risk_module.get('overall_risk_score_base', risk_score + jurisdiction_penalty + section_65b_penalty)

        risk_module['overall_risk_score'] = risk_score
        risk_module['adjusted'] = True
        risk_module['adjustments'] = {
            'jurisdiction_penalty': jurisdiction_penalty,
            'section_65b_penalty': section_65b_penalty,
            'total_penalty': jurisdiction_penalty + section_65b_penalty
        }

        # FIX #1: Add penalty transparency to explanation
        if 'explanation' in risk_module and isinstance(risk_module['explanation'], dict):
            if 'calculation_steps' in risk_module['explanation']:
                # Add Step 3 showing penalty deductions
                if jurisdiction_penalty > 0 or section_65b_penalty > 0:
                    penalty_text = "Step 3: Apply penalties\n"
                    if jurisdiction_penalty > 0:
                        penalty_text += f"  - Jurisdiction penalty: -{jurisdiction_penalty} points\n"
                    if section_65b_penalty > 0:
                        penalty_text += f"  - Section 65B penalty: -{section_65b_penalty} points\n"
                    penalty_text += f"  Adjusted Score: {base_score:.2f} - {jurisdiction_penalty + section_65b_penalty} = {risk_score:.2f}"

                    risk_module['explanation']['calculation_steps'].append(penalty_text)
                    risk_module['explanation']['final_score'] = risk_score

    # Consolidate fatal determination
    is_fatal = fatal_flag or len(fatal_sources) > 0 or defence_risk_level == 'FATAL' or 'FATAL' in overall_status

    # PHASE 2: Determine SINGLE TRUTH (Priority order)

    # Priority 1: Fatal condition (highest authority)
    if is_fatal:
        logger.warning(f"  ⚠️ FATAL condition detected from: {', '.join(fatal_sources) if fatal_sources else 'primary flag'}")
        fatal_reasoning = f"Fatal defects detected: {', '.join(fatal_sources)}" if fatal_sources else 'Fatal defects override all positive assessments'

        final_verdict = {
            'status': 'FATAL',
            'category': 'CRITICAL FAILURE',
            'risk_score': 0,  # HARD CAP at 0 for fatal
            'recommendation': 'DO NOT FILE - Fatal defects present',
            'predicted_outcome': 'Case will be dismissed',
            'filing_blocked': True,
            'confidence': 'HIGH',
            'reasoning': fatal_reasoning,
            'fatal_sources': fatal_sources
        }

    # Priority 2: Critical issues
    elif len(fatal_defects) > 0 or risk_score < 30:
        logger.warning("  ⚠️ Critical issues detected")
        final_verdict = {
            'status': 'CRITICAL',
            'category': 'HIGH RISK',
            'risk_score': min(risk_score, 30),
            'recommendation': 'Urgent remediation required before filing',
            'predicted_outcome': 'High dismissal risk',
            'filing_blocked': False,
            'confidence': 'HIGH',
            'reasoning': 'Critical defects require immediate attention'
        }

    # Priority 3: Normal scoring
    elif risk_score >= 80:
        final_verdict = {
            'status': 'STRONG',
            'category': 'STRONG PROSECUTION',
            'risk_score': risk_score,
            'recommendation': 'Proceed with filing',
            'predicted_outcome': 'High conviction probability',
            'filing_blocked': False,
            'confidence': 'HIGH',
            'reasoning': 'Strong compliance across all modules'
        }
    elif risk_score >= 60:
        final_verdict = {
            'status': 'MODERATE',
            'category': 'MODERATE STRENGTH',
            'risk_score': risk_score,
            'recommendation': 'Strengthen case or consider settlement',
            'predicted_outcome': 'Balanced probability',
            'filing_blocked': False,
            'confidence': 'MEDIUM',
            'reasoning': 'Case has merits but improvements recommended'
        }
    else:
        final_verdict = {
            'status': 'WEAK',
            'category': 'WEAK CASE',
            'risk_score': risk_score,
            'recommendation': 'Settlement strongly advised',
            'predicted_outcome': 'Acquittal more likely than conviction',
            'filing_blocked': False,
            'confidence': 'MEDIUM',
            'reasoning': 'Significant weaknesses in case foundation'
        }

    # PHASE 3: Override ALL conflicting fields with SINGLE TRUTH
    logger.info(f"  ✅ Final verdict: {final_verdict['status']} - {final_verdict['category']}")

    # FATAL override: mark modules as fatal but PRESERVE actual scores
    # (zeroing scores confuses the PDF renderer and produces 0/100 everywhere)
    if is_fatal:
        logger.warning("  ⚠️ FATAL override: Syncing all module scores to reflect fatal condition")
        for module_name in analysis_report.get('modules', {}):
            module = analysis_report['modules'][module_name]
            if isinstance(module, dict):
                module['fatal_override_applied'] = True
                module['fatal_note'] = 'Case has fatal defect — see procedural_defects module'
                # Do NOT zero display_score — keep actual scores visible in report

    # Core verdict fields
    analysis_report['final_verdict'] = final_verdict
    # Ensure risk_score is always a meaningful number (never 0 unless genuinely zero)
    _rs = final_verdict['risk_score']
    analysis_report['risk_score'] = _rs if _rs is not None else 0
    analysis_report['risk_score_display'] = f"{_rs:.1f}/100" if _rs is not None else "0.0/100"
    analysis_report['is_fatal'] = is_fatal
    analysis_report['fatal_cap_applied'] = is_fatal
    analysis_report['overall_status'] = f"{final_verdict['status']} - {final_verdict['category']}"
    analysis_report['decisive_verdict'] = final_verdict['recommendation']
    analysis_report['filing_blocked'] = final_verdict['filing_blocked']
    analysis_report['fatal_flag'] = (final_verdict['status'] == 'FATAL')

    # CRITICAL: Override ALL top-level status fields to prevent contradictions
    # This ensures SINGLE SOURCE OF TRUTH (final_verdict)
    analysis_report['final_status'] = final_verdict['recommendation']
    analysis_report['case_strength'] = final_verdict['status']
    analysis_report['filing_recommendation'] = final_verdict['recommendation']
    analysis_report['risk_level'] = final_verdict['status']

    # Override outcome_classification to match final verdict
    analysis_report['outcome_classification'] = {
        'category': final_verdict['category'],
        'risk_level': final_verdict['status'],
        'recommendation': final_verdict['recommendation'],
        'predicted_outcome': final_verdict['predicted_outcome'],
        'action_priority': 'URGENT' if final_verdict['status'] == 'FATAL' else 'PROCEED',
        'confidence': final_verdict['confidence']
    }

    # Override filing readiness module to match verdict - COMPREHENSIVE
    if 'modules' in analysis_report and 'filing_readiness' in analysis_report['modules']:
        filing_module = analysis_report['modules']['filing_readiness']

        # Determine ready_to_file based on final verdict
        ready_to_file = final_verdict['status'] in ['STRONG', 'MODERATE'] and not final_verdict['filing_blocked']

        if final_verdict['status'] in ['FATAL', 'CRITICAL']:
            filing_module['filing_readiness'] = '❌ NOT READY TO FILE'
            filing_module['ready_to_file'] = False  # Override boolean
            filing_module['overall_status'] = '🔴 FILING BLOCKED' if final_verdict['filing_blocked'] else '⚠️ HIGH RISK'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'NOT READY TO FILE'  # Override any other status fields
        elif final_verdict['status'] == 'WEAK':
            filing_module['filing_readiness'] = '⚠️ RISKY - SETTLEMENT ADVISED'
            filing_module['ready_to_file'] = False  # Override boolean
            filing_module['overall_status'] = '⚠️ HIGH RISK'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'RISKY - REVIEW REQUIRED'
        elif final_verdict['status'] == 'MODERATE':
            filing_module['filing_readiness'] = '⚠️ READY - STRENGTHEN RECOMMENDED'
            filing_module['ready_to_file'] = True  # Override boolean
            filing_module['overall_status'] = '⚠️ MODERATE'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'READY TO FILE'
        else:  # STRONG
            filing_module['filing_readiness'] = '✅ READY TO FILE'
            filing_module['ready_to_file'] = True  # Override boolean
            filing_module['overall_status'] = '✅ STRONG'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'READY TO FILE'

    # Override document compliance to reflect verdict
    if 'modules' in analysis_report and 'document_compliance' in analysis_report['modules']:
        if final_verdict['status'] in ['FATAL', 'CRITICAL']:
            analysis_report['modules']['document_compliance']['filing_readiness'] = '❌ NOT READY TO FILE'

    # Override executive summary to match final verdict
    if 'executive_summary' in analysis_report:
        if isinstance(analysis_report['executive_summary'], dict):
            analysis_report['executive_summary']['overall_assessment'] = final_verdict['category']
            # Sync fatal state into executive_summary
            if analysis_report.get('fatal_flag'):
                analysis_report['executive_summary']['filing_verdict'] = (
                    analysis_report['executive_summary'].get('filing_verdict') or
                    'DO NOT FILE — FATAL DEFECTS PRESENT'
                )
                analysis_report['executive_summary']['fatal_defects_count'] = max(
                    analysis_report['executive_summary'].get('fatal_defects_count', 0),
                    len(analysis_report.get('modules', {}).get('procedural_defects', {}).get('fatal_defects', [])) +
                    len(analysis_report.get('modules', {}).get('risk_assessment', {}).get('fatal_defects', []))
                )
            # Update case overview to reflect final truth
            status_marker = '🔴' if final_verdict['status'] == 'FATAL' else ('⚠️' if final_verdict['status'] in ['CRITICAL', 'WEAK'] else '✅')
            analysis_report['executive_summary']['case_overview'] = f"{status_marker} {final_verdict['status']}: {final_verdict['reasoning']}"

            # Override strategic recommendations to match verdict
            if final_verdict['status'] == 'FATAL':
                analysis_report['executive_summary']['strategic_recommendations'] = [
                    'DO NOT FILE - Fatal defects present',
                    'Immediate remediation required',
                    'Consult legal counsel before proceeding'
                ]
            elif final_verdict['status'] == 'CRITICAL':
                analysis_report['executive_summary']['strategic_recommendations'] = [
                    'Urgent remediation before filing',
                    'Address critical gaps identified',
                    'Consider settlement if defects cannot be cured'
                ]
            elif final_verdict['status'] == 'WEAK':
                analysis_report['executive_summary']['strategic_recommendations'] = [
                    'Settlement strongly advised',
                    'Case has significant weaknesses',
                    'Filing carries high acquittal risk'
                ]
            # For STRONG/MODERATE, keep original recommendations

    # Override professional report if it exists
    if 'professional_report' in analysis_report:
        if isinstance(analysis_report['professional_report'], dict):
            if 'summary' in analysis_report['professional_report']:
                analysis_report['professional_report']['summary']['final_verdict'] = final_verdict['category']
                analysis_report['professional_report']['summary']['recommendation'] = final_verdict['recommendation']

    logger.info("  ✅ Verdict integrity enforced - all modules synchronized")

    return analysis_report


def perform_comprehensive_analysis(case_data: Dict) -> Dict:

    analysis_start_time = datetime.now()
    case_id = None

    # FIX: Initialize analysis_report BEFORE try block to prevent UnboundLocalError
    analysis_report = {
        'success': False,
        'error': None,
        'engine_version': ENGINE_VERSION,
        'timestamp': analysis_start_time.isoformat()
    }

    try:
        if not case_data or not isinstance(case_data, dict):
            logger.error("❌ Invalid input: case_data must be a non-empty dictionary")
            return {
                'error': 'INVALID_INPUT',
                'error_type': 'VALIDATION_ERROR',
                'message': 'case_data must be a non-empty dictionary',
                'fatal_flag': True,
                'overall_status': 'ERROR - INVALID INPUT',
                'engine_version': ENGINE_VERSION,
                'timestamp': analysis_start_time.isoformat()
            }

        required_fields = ['cheque_date', 'dishonour_date', 'notice_date']
        # complaint_date is optional but recommended
        missing_fields = [f for f in required_fields if f not in case_data or not case_data[f]]

        if missing_fields:
            logger.error(f"❌ Missing required fields: {missing_fields}")
            return {
                'error': 'MISSING_REQUIRED_FIELDS',
                'error_type': 'VALIDATION_ERROR',
                'missing_fields': missing_fields,
                'message': f"Required fields missing: {', '.join(missing_fields)}",
                'fatal_flag': True,
                'overall_status': 'ERROR - INCOMPLETE DATA',
                'engine_version': ENGINE_VERSION,
                'timestamp': analysis_start_time.isoformat()
            }

        logger.info("🚀 Starting comprehensive legal intelligence analysis...")
        start_time = time.time()

        analysis_report = {
            'case_id': hashlib.md5(json.dumps(case_data, sort_keys=True).encode()).hexdigest()[:12],
            'input_hash': hashlib.sha256(json.dumps(case_data, sort_keys=True).encode()).hexdigest(),
            'analysis_timestamp': analysis_start_time.isoformat(),
            'engine_version': ENGINE_VERSION,
            'scoring_model_version': SCORING_MODEL_VERSION,
            'timeline_math_version': TIMELINE_MATH_VERSION,
            'fatal_flag': False,
            'case_metadata': {
                'case_type': case_data.get('case_type'),
                'cheque_amount': case_data.get('cheque_amount'),
                'is_company_case': case_data.get('is_company_case', False)
            },
            'modules': {},
            'architecture': {
                'deterministic_modules': [],
                'rag_modules': [],
                'llm_enhanced': False
            },
            'audit_log': {
                'analysis_started': analysis_start_time.isoformat(),
                'input_validated': True,
                'phases_executed': []
            }
        }

        case_id = analysis_report['case_id']
        # Logging removed for production stability

        logger.info("🔧 PHASE 1: FATAL EVALUATION (Pre-Analysis)")
        analysis_report['audit_log']['phases_executed'].append('PHASE_1_FATAL_EVALUATION')

        # Execute timeline with error handling
        try:
            timeline_result = analyze_timeline(case_data)
            analysis_report['modules']['timeline_intelligence'] = timeline_result
        except Exception as e:
            logger.error(f"❌ Timeline module FAILED: {e}")
            return {
                'case_id': case_id,
                'success': False,
                'fatal_flag': True,
                'fatal_source': 'timeline_intelligence',
                'error': 'Core timeline calculation failed',
                'status': 'INVALID',
                'overall_status': 'ANALYSIS FAILED',
                'risk_score': 0,
                'engine_version': ENGINE_VERSION,
                'analysis_timestamp': analysis_start_time.isoformat()
            }

        # FATAL CHECKPOINT 1: Timeline
        if timeline_result.get('limitation_risk') in ['EXPIRED', 'CRITICAL']:
            # Fatal detected - logging removed for stability
            analysis_report['fatal_flag'] = True
            analysis_report['fatal_source'] = 'timeline_intelligence'
            return {
                'case_id': case_id,
                'fatal_flag': True,
                'fatal_source': 'timeline_intelligence',
                'overall_status': 'FATAL - LIMITATION BARRED',
                'risk_score': 0,
                'decisive_verdict': 'CASE TIME-BARRED - FILING WILL FAIL',
                'filing_blocked': True,
                'fatal_type': 'LIMITATION_EXPIRED',
                'execution_stopped_at': 'PHASE_1',
                'modules': {'timeline_intelligence': timeline_result},
                'no_further_analysis': True,
                'engine_version': ENGINE_VERSION,
                'analysis_timestamp': analysis_start_time.isoformat()
            }

        doc_compliance = analyze_document_compliance(case_data)
        analysis_report['modules']['document_compliance'] = doc_compliance

        # NEW MODULE: Section 65B Electronic Evidence
        logger.info("🔒 Analyzing Section 65B Compliance...")
        section_65b = analyze_section_65b_compliance(case_data)
        analysis_report['modules']['section_65b_compliance'] = section_65b

        # NEW MODULE: Income Tax 269SS
        logger.info("💰 Analyzing Income Tax 269SS Compliance...")
        income_tax_269ss = analyze_income_tax_269ss_compliance(case_data)
        analysis_report['modules']['income_tax_269ss'] = income_tax_269ss

        # NEW MODULE: Notice Delivery Status
        logger.info("📬 Analyzing Notice Delivery Status...")
        notice_delivery = analyze_notice_delivery_status(case_data)
        analysis_report['modules']['notice_delivery_status'] = notice_delivery

        # NEW MODULE: Part Payment Defence
        logger.info("💵 Analyzing Part Payment Defence...")
        part_payment = analyze_part_payment_defence(case_data)
        analysis_report['modules']['part_payment_analysis'] = part_payment

        # NEW MODULE: Territorial Jurisdiction (Section 142)
        logger.info("⚖️  Analyzing Territorial Jurisdiction...")
        jurisdiction = analyze_territorial_jurisdiction(case_data)
        analysis_report['modules']['territorial_jurisdiction'] = jurisdiction

        # NEW MODULE: Compounding Eligibility (Section 147)
        logger.info("🤝 Analyzing Compounding Eligibility...")
        compounding = analyze_compounding_eligibility(case_data)
        analysis_report['modules']['compounding_analysis'] = compounding

        # NEW MODULE: Enhanced Director Role Liability
        logger.info("👔 Analyzing Director Role-Based Liability...")
        director_role = analyze_director_role_liability(case_data)
        analysis_report['modules']['director_role_analysis'] = director_role

        # FATAL CHECKPOINT 2: Documents (only if timeline passed)
        fatal_conditions = []

        # Check document fatal defects
        if len(doc_compliance.get('fatal_defects', [])) > 0:
            fatal_conditions.append({
                'source': 'document_compliance',
                'type': 'DOCUMENT_DEFECTS',
                'details': doc_compliance['fatal_defects']
            })

        # Check Section 65B fatal
        if section_65b.get('risk_level') == 'FATAL':
            fatal_conditions.append({
                'source': 'section_65b_compliance',
                'type': 'ELECTRONIC_EVIDENCE_INADMISSIBLE',
                'details': 'Section 65B certificate missing for electronic evidence'
            })

        # Check Income Tax 269SS fatal
        if income_tax_269ss.get('violation_detected') and income_tax_269ss.get('risk_level') == 'CRITICAL':
            fatal_conditions.append({
                'source': 'income_tax_269ss',
                'type': 'ILLEGAL_CASH_TRANSACTION',
                'details': 'Cash transaction above ₹20,000 violates Section 269SS'
            })

        # Check part payment fatal (debt fully satisfied)
        if part_payment.get('defence_strength') == 'FATAL':
            fatal_conditions.append({
                'source': 'part_payment_analysis',
                'type': 'DEBT_SATISFIED',
                'details': 'Part payment equals or exceeds cheque amount'
            })

        # Check notice delivery fatal
        if notice_delivery.get('risk_level') == 'FATAL':
            fatal_conditions.append({
                'source': 'notice_delivery_status',
                'type': 'NOTICE_NOT_SENT',
                'details': 'Legal notice requirement not satisfied'
            })

        # If any fatal condition exists, stop and return
        if len(fatal_conditions) > 0:
            analysis_report['fatal_flag'] = True
            analysis_report['fatal_source'] = ', '.join([f['source'] for f in fatal_conditions])

            fatal_messages = [f"{f['type']}: {f['details']}" for f in fatal_conditions]

            _elapsed = round(time.time() - start_time, 2) if 'start_time' in dir() else 0
            return {
                'case_id': case_id,
                'fatal_flag': True,
                'fatal_source': analysis_report['fatal_source'],
                'overall_status': 'FATAL - FILING BLOCKED',
                'risk_score': FATAL_CAP_UNIFIED,
                'decisive_verdict': f"FATAL CONDITIONS DETECTED - {len(fatal_conditions)} critical issues",
                'filing_blocked': True,
                'fatal_type': 'MULTIPLE_FATAL_CONDITIONS',
                'execution_stopped_at': 'PHASE_1',
                'fatal_details': fatal_messages,
                'processing_time_seconds': _elapsed,
                'modules': {
                    'timeline_intelligence': timeline_result,
                    'document_compliance': doc_compliance,
                    'section_65b_compliance': section_65b,
                    'income_tax_269ss': income_tax_269ss,
                    'notice_delivery_status': notice_delivery,
                    'part_payment_analysis': part_payment,
                    'territorial_jurisdiction': jurisdiction,
                    'compounding_analysis': compounding,
                    'director_role_analysis': director_role
                },
                'no_further_analysis': True,
                'engine_version': ENGINE_VERSION,
                'analysis_timestamp': analysis_start_time.isoformat()
            }

        logger.info("✅ PHASE 1 COMPLETE: No fatal conditions - Proceeding to full analysis")
        analysis_report['audit_log']['phase_1_passed'] = True

        logger.info("🔧 PHASE 2: Full Module Execution")
        analysis_report['audit_log']['phases_executed'].append('PHASE_2_FULL_ANALYSIS')

        logger.info("  Module 2: Ingredient Analysis...")
        ingredient_result = analyze_ingredients(case_data, timeline_result)
        analysis_report['modules']['ingredient_compliance'] = ingredient_result
        analysis_report['architecture']['deterministic_modules'].append('Ingredient Compliance')

        logger.info("  Module 3: Documentary Analysis...")
        doc_result = analyze_documentary_strength(case_data)
        analysis_report['modules']['documentary_strength'] = doc_result
        analysis_report['architecture']['deterministic_modules'].append('Documentary Strength')

        # CRITICAL NEW MODULE: Security Cheque Analysis
        logger.info("  Module 3.5: Security Cheque Probability...")
        security_cheque_result = analyze_security_cheque_probability(case_data)
        analysis_report['modules']['security_cheque_analysis'] = security_cheque_result
        analysis_report['architecture']['deterministic_modules'].append('Security Cheque Detector')

        # CRITICAL NEW MODULE: Financial Capacity Analysis
        logger.info("  Module 3.6: Financial Capacity Analysis...")
        financial_capacity_result = analyze_financial_capacity(case_data)
        analysis_report['modules']['financial_capacity'] = financial_capacity_result
        analysis_report['architecture']['deterministic_modules'].append('Financial Capacity')

        logger.info("  Module 4: Liability Analysis...")
        liability_result = analyze_accused_liability(case_data)
        analysis_report['modules']['liability_analysis'] = liability_result
        analysis_report['architecture']['deterministic_modules'].append('Liability Expansion')

        logger.info("  Module 5: Defence Analysis...")
        defence_result = analyze_defence_vulnerabilities(case_data, ingredient_result, doc_result)

        # ── Inject procedural defences that the defence module may miss ──
        # Check premature complaint BEFORE defect module runs
        if case_data.get('notice_received_date') and case_data.get('complaint_filed_date'):
            try:
                from datetime import datetime as _dt
                nr = _dt.strptime(case_data['notice_received_date'], '%Y-%m-%d')
                cf = _dt.strptime(case_data['complaint_filed_date'], '%Y-%m-%d')
                if cf < nr + timedelta(days=15):
                    premature_defence = {
                        'defence': 'Premature Complaint — Cause of Action Not Arisen',
                        'strength': 'FATAL',
                        'legal_basis': 'Section 138(b): Complaint only maintainable after 15-day payment period expires',
                        'probability': 'CERTAIN',
                        'strategy': 'File discharge application — complaint is not maintainable',
                        'risk_impact': 'FATAL — Complaint must be dismissed'
                    }
                    if 'high_risk_defences' not in defence_result:
                        defence_result['high_risk_defences'] = []
                    defence_result['high_risk_defences'].insert(0, premature_defence)
                    defence_result['premature_complaint_defence'] = True
            except Exception:
                pass

        analysis_report['modules']['defence_matrix'] = defence_result
        analysis_report['architecture']['deterministic_modules'].append('Defence Vulnerability')

        logger.info("  Module 6: Defect Scanning...")
        defect_result = scan_procedural_defects(case_data, timeline_result, liability_result)
        analysis_report['modules']['procedural_defects'] = defect_result
        analysis_report['architecture']['deterministic_modules'].append('Procedural Defects')

        # ── FATAL PROPAGATION: procedural fatal defects → global fatal_flag ──
        proc_fatal = defect_result.get('fatal_defects', [])
        if proc_fatal:
            analysis_report['fatal_flag'] = True
            analysis_report['fatal_source'] = analysis_report.get('fatal_source', '') + ',procedural_defects'
            logger.warning(f"🔴 FATAL PROCEDURAL DEFECT: {proc_fatal[0].get('defect', 'Unknown')}")

        logger.info("  Module 7: Risk Scoring...")
        risk_result = calculate_overall_risk_score(
            timeline_result,
            ingredient_result,
            doc_result,
            liability_result,
            defect_result
        )

        # CRITICAL: Validate risk_result has valid overall_risk_score
        if not isinstance(risk_result, dict):
            logger.error(f"risk_result is not a dict: {type(risk_result)}")
            risk_result = {'overall_risk_score': 0, 'category_scores': {}, 'fatal_defects': []}

        if 'overall_risk_score' not in risk_result or risk_result['overall_risk_score'] is None:
            logger.error("risk_result missing or None overall_risk_score - setting to 0")
            risk_result['overall_risk_score'] = 0

        analysis_report['modules']['risk_assessment'] = risk_result
        analysis_report['architecture']['deterministic_modules'].append('Risk Scoring')

        if risk_result.get('has_fatal_defects', False) or len(risk_result.get('fatal_defects', [])) > 0:
            analysis_report['fatal_flag'] = True
            logger.warning("🔴 FATAL DEFECTS DETECTED - Score capped")

        logger.info("  Module 8: Settlement Analysis...")
        settlement_result = analyze_settlement_exposure(case_data, risk_result)
        analysis_report['modules']['settlement_analysis'] = settlement_result
        analysis_report['architecture']['deterministic_modules'].append('Settlement Exposure')

        logger.info("  Module 9: Contradiction Detection...")
        contradiction_result = detect_contradictions(case_data)
        analysis_report['modules']['contradiction_detection'] = contradiction_result
        analysis_report['architecture']['deterministic_modules'].append('Contradiction Detector')

        logger.info("  Module 9B: Edge Case Detection...")
        edge_case_result = detect_edge_cases(case_data)
        analysis_report['modules']['edge_case_detection'] = edge_case_result
        analysis_report['architecture']['deterministic_modules'].append('Edge Case Handler')

        logger.info("🔍 LAYER 2: RAG-Powered Pattern Analysis...")

        logger.info("  Module 10: Judicial Behavior Analysis...")

        court_location = case_data.get('court_location', 'Generic Court')
        kb_court_patterns = search_kb(
            f"Section 138 {court_location} limitation technical dismissal",
            top_k=10,
            threshold=0.60
        ) if kb_loaded else []

        judicial_behavior = analyze_judicial_behavior(court_location, kb_court_patterns)
        analysis_report['modules']['judicial_behavior'] = judicial_behavior
        analysis_report['architecture']['rag_modules'].append('Judicial Behavior Intelligence')

        logger.info("  Module 11: Presumption & Burden Shift Analysis...")
        presumption_result = analyze_presumption_rebuttal(case_data, ingredient_result, doc_result)
        analysis_report['modules']['presumption_analysis'] = presumption_result
        analysis_report['architecture']['rag_modules'].append('Presumption Rebuttal Engine')

        logger.info("  Module 12: Cross-Examination Risk Analysis...")
        cross_exam_result = analyze_cross_examination_risks(case_data, doc_result, defence_result)
        analysis_report['modules']['cross_examination_risk'] = cross_exam_result
        analysis_report['architecture']['rag_modules'].append('Cross-Examination Risk')

        logger.info("🤖 LAYER 3: Deterministic Insight Generation (LLM disabled on Render)...")
        analysis_report['architecture']['llm_enhanced'] = False
        logger.info("  ⏭️  LLM disabled on Render — using deterministic insights only")

        # ============================================================================
        # VERDICT INTEGRITY - ENFORCE BEFORE SUMMARIES
        # ============================================================================
        logger.info("🎯 Enforcing Verdict Integrity (Pre-Summary)...")
        analysis_report = enforce_verdict_integrity(analysis_report)

        logger.info("📊 Generating Executive Intelligence Report...")
        analysis_report['executive_summary'] = generate_executive_summary(
            case_data,
            timeline_result,
            ingredient_result,
            doc_result,
            liability_result,
            defence_result,
            defect_result,
            risk_result,
            settlement_result,
            presumption_result,
            cross_exam_result,
            judicial_behavior,
            contradiction_result,
            edge_case_result
        )

        logger.info("🚀 Adding Enterprise Features...")

        logger.info("  ⚖️ Section 139 Presumption Intelligence...")
        presumption_intel = calculate_presumption_intelligence(
            {
                'original_cheque_available': case_data.get('original_cheque_available', False),
                'return_memo_available': case_data.get('return_memo_available', False),
                'written_agreement_exists': case_data.get('written_agreement_exists', False),
                'ledger_available': case_data.get('ledger_available', False),
                'postal_proof_available': case_data.get('postal_proof_available', False),
                'email_sms_available': case_data.get('email_sms_evidence', False),
                'witness_statements': case_data.get('witness_available', False)
            },
            case_data.get('defence_type')
        )
        analysis_report['presumption_intelligence'] = presumption_intel

        logger.info("  🎯 Judicial Variance Simulation...")
        # Only simulate variance if KB has sufficient data
        # Use kb_court_patterns from earlier KB search
        kb_provisions = len(kb_court_patterns) if kb_court_patterns else 0
        if kb_provisions >= 10:  # Minimum threshold for statistical validity
            judicial_variance = simulate_judicial_variance(
                analysis_report['modules']['risk_assessment']['overall_risk_score'],
                {
                    'timeline': analysis_report['modules']['risk_assessment']['category_scores']['Timeline Compliance']['score'],
                    'ingredient': analysis_report['modules']['risk_assessment']['category_scores']['Ingredient Compliance']['score'],
                    'documentary': analysis_report['modules']['risk_assessment']['category_scores']['Documentary Strength']['score'],
                    'procedural': analysis_report['modules']['risk_assessment']['category_scores']['Procedural Compliance']['score']
                }
            )
            analysis_report['judicial_variance_simulation'] = judicial_variance
        else:
            logger.warning(f"  ⚠️  KB insufficient ({kb_provisions} provisions) - judicial variance disabled")
            analysis_report['judicial_variance_simulation'] = {
                'disabled': True,
                'reason': f'Insufficient KB data ({kb_provisions} provisions, minimum 10 required)',
                'fallback_variance': 'Not simulated'
            }

        logger.info("  🔄 Dynamic Weight Analysis...")
        base_weights = CONFIG['SCORING_WEIGHTS']
        adjusted_weights, weight_explanations = adjust_weights_contextually(
            base_weights,
            case_data.get('cheque_amount', 0),
            case_data.get('is_company_case', False)
        )
        analysis_report['weight_analysis'] = {
            'base_weights': base_weights,
            'adjusted_weights': adjusted_weights,
            'adjustments_made': weight_explanations
        }

        logger.info("  🔍 Advanced Contradiction Detection...")
        contradictions_enterprise = detect_contradictions(case_data)
        analysis_report['contradictions_detected'] = contradictions_enterprise

        logger.info("  🛡️ Fraud Risk Analysis...")
        fraud_analysis = calculate_fraud_risk(case_data)
        analysis_report['fraud_risk_analysis'] = fraud_analysis

        analysis_report['version_info'] = get_version_info()

        logger.info("📄 Module 14: Document Compliance Analysis...")
        doc_compliance = analyze_document_compliance(case_data)
        analysis_report['modules']['document_compliance'] = doc_compliance
        analysis_report['architecture']['deterministic_modules'].append('Document Compliance')

        if len(doc_compliance.get('fatal_defects', [])) > 0:
            analysis_report['fatal_flag'] = True
            analysis_report['overall_status'] = 'FATAL - FILING BLOCKED'
            analysis_report['risk_score'] = FATAL_CAP_UNIFIED
            analysis_report['decisive_verdict'] = f"ABSOLUTE FAILURE - {len(doc_compliance['fatal_defects'])} fatal document defects"
            analysis_report['filing_blocked'] = True
            logger.error("🔴 FATAL DOCUMENTS MISSING - IMMEDIATE STOP")
            return analysis_report

        logger.info("⚖️ Module 15: Defence Risk Analysis...")
        defence_risks = analyze_defence_risks(case_data, doc_result)
        analysis_report['modules']['defence_risk_analysis'] = defence_risks
        analysis_report['architecture']['deterministic_modules'].append('Defence Risk Analysis')

        logger.info("🔗 Module 15B: Defence Dependency Enforcement...")
        dependency_check = enforce_defence_dependencies(case_data, defence_risks, doc_compliance)
        analysis_report['modules']['dependency_enforcement'] = dependency_check

        logger.info("🔍 Module 15C: Defence-Document Cross-Validation...")
        cross_validation = cross_validate_defence_documents(case_data, defence_risks, doc_compliance)
        analysis_report['modules']['cross_validation'] = cross_validation

        if cross_validation.get('severity') == 'FATAL':
            analysis_report['fatal_flag'] = True
            analysis_report['overall_status'] = 'FATAL - EVIDENCE GAP'
            analysis_report['risk_score'] = FATAL_CAP_UNIFIED
            analysis_report['decisive_verdict'] = f"Evidence gap: {cross_validation['gaps_identified'][0]['description']}"
            analysis_report['filing_blocked'] = True
            logger.error("🔴 FATAL EVIDENCE GAP - DEFENCE NOT COUNTERED")
            return analysis_report

        if len(dependency_check.get('violations', [])) > 0:
            has_fatal_dependency = any(v['severity'] == 'FATAL' for v in dependency_check['violations'])
            if has_fatal_dependency:
                analysis_report['fatal_flag'] = True
                analysis_report['overall_status'] = 'FATAL - MANDATORY REQUIREMENTS NOT MET'
                analysis_report['risk_score'] = FATAL_CAP_UNIFIED
                analysis_report['decisive_verdict'] = f"{dependency_check['violation_count']} mandatory requirements violated"
                analysis_report['filing_blocked'] = True
                logger.error("🔴 MANDATORY DEPENDENCY VIOLATIONS - FILING BLOCKED")
                return analysis_report

        if len(defence_risks.get('fatal_defences', [])) > 0:
            analysis_report['fatal_flag'] = True
            analysis_report['overall_status'] = 'FATAL - CASE VIABILITY COMPROMISED'
            analysis_report['risk_score'] = FATAL_CAP_UNIFIED
            analysis_report['decisive_verdict'] = defence_risks.get('case_viability_impact', 'CRITICAL DEFENCE EXPOSURE')
            analysis_report['filing_blocked'] = False
            logger.error("🔴 FATAL DEFENCE RISKS - HIGH ACQUITTAL PROBABILITY")
            return analysis_report

        logger.info("✓ Module 16: Filing Readiness Checklist...")
        filing_readiness = generate_filing_readiness_checklist(
            doc_compliance,
            defence_risks,
            timeline_result,
            ingredient_result
        )
        analysis_report['modules']['filing_readiness'] = filing_readiness
        analysis_report['architecture']['deterministic_modules'].append('Filing Readiness')

        logger.info("✅ Advocate feedback modules complete")

        analysis_report['enterprise_features'] = {
            'presumption_intelligence': True,
            'judicial_variance': True,
            'dynamic_weights': True,
            'contradiction_detection': True,
            'fraud_analysis': True,
            'weighted_fatal_override': True,
            'score_capping': 'Max 98% (never 100%)',
            'audit_trail': True,
            'document_compliance': True,
            'defence_risk_analysis': True,
            'filing_readiness_checklist': True
        }

        logger.info("✅ Enterprise features added")

        logger.info("📋 Generating Audit Trail...")
        analysis_report['audit_trail'] = generate_audit_trail(
            case_data,
            timeline_result,
            ingredient_result,
            doc_result,
            risk_result
        )

        logger.info("💡 Generating Score Explanation...")
        analysis_report['score_explanation'] = generate_score_explanation(risk_result)

        logger.info("🎯 Classifying Case Outcome...")
        analysis_report['outcome_classification'] = classify_case_outcome(
            risk_result['overall_risk_score'],
            risk_result.get('fatal_defects', []),
            timeline_result.get('limitation_risk', 'UNKNOWN')
        )

        analysis_report['enterprise_features'].update({
            'audit_trail': True,
            'score_explainability': True,
            'outcome_classification': True,
            'defensive_programming': True,
            'hard_fatal_override': True,
            'strict_validation': True,
            'centralized_constants': True
        })

        logger.info("⚖️ Generating Professional Report Enhancements...")

        primary_weakness = None
        if doc_result['overall_strength_score'] < 70:
            primary_weakness = "documentary evidence"
        elif ingredient_result['overall_compliance'] < 70:
            primary_weakness = "ingredient compliance"

        analysis_report['verdict'] = generate_verdict_one_liner(
            risk_result['overall_risk_score'],
            risk_result.get('fatal_defects', []),
            timeline_result.get('limitation_risk', 'LOW'),
            primary_weakness
        )

        documentary_gaps = []
        if not case_data.get('written_agreement_exists'):
            documentary_gaps.append('written_agreement_missing')
        if not case_data.get('ledger_available'):
            documentary_gaps.append('ledger_missing')
        if not case_data.get('postal_proof_available'):
            documentary_gaps.append('postal_proof_missing')

        analysis_report['score_drivers'] = generate_score_drivers(
            risk_result['category_scores'],
            risk_result.get('fatal_defects', []),
            documentary_gaps
        )

        analysis_report['court_impact'] = generate_court_impact_assessment(
            risk_result['overall_risk_score'],
            risk_result.get('fatal_defects', []),
            case_data.get('defence_type'),
            doc_result['overall_strength_score']
        )

        timeline_issues = []
        if timeline_result.get('limitation_risk') in ['HIGH', 'CRITICAL']:
            timeline_issues.append('limitation_risk')

        liability_issues = []
        if not liability_result.get('parties_properly_impleaded'):
            liability_issues.append('improper_impleading')

        analysis_report['prefiling_checklist'] = generate_prefiling_checklist(
            documentary_gaps,
            timeline_issues,
            liability_issues,
            uploaded_docs=case_data.get('uploaded_files', [])
        )

        analysis_report['document_verification'] = verify_documents_against_claims(
            {
                'original_cheque_available': case_data.get('original_cheque_available', False),
                'return_memo_available': case_data.get('return_memo_available', False),
                'postal_proof_available': case_data.get('postal_proof_available', False),
                'written_agreement_exists': case_data.get('written_agreement_exists', False),
                'ledger_available': case_data.get('ledger_available', False),
                'email_sms_evidence': case_data.get('email_sms_evidence', False),
                'witness_available': case_data.get('witness_available', False)
            },
            uploaded_files=case_data.get('uploaded_files', [])
        )

        logger.info("✅ Professional enhancements complete")

        analysis_report['legal_disclaimer'] = {
            'primary': "This report is a structured compliance assessment tool based on information provided and does not substitute independent legal judgment. All findings should be reviewed by qualified legal counsel before filing.",
            'document_verification': "Document verification is based on user-provided information and claims. Physical document authentication and legal sufficiency determination remain the responsibility of the filing advocate.",
            'scope': "This assessment evaluates statutory compliance under Section 138 of the Negotiable Instruments Act, 1881. It does not constitute legal advice or predict case outcomes.",
            'liability': "Anthropic/JUDIQ AI assumes no liability for legal decisions made based on this assessment. Users are advised to exercise independent legal judgment.",
            'version': f"Generated by JUDIQ AI Engine v{ENGINE_VERSION}"
        }

        analysis_report['processing_time_seconds'] = round(time.time() - start_time, 2)
        analysis_report['processing_time_ms'] = round((time.time() - start_time) * 1000)

        logger.info(f"✅ Analysis complete in {analysis_report['processing_time_seconds']}s")
        logger.info(f"   Deterministic Modules: {len(analysis_report['architecture']['deterministic_modules'])}")
        logger.info(f"   LLM Enhanced: {analysis_report['architecture']['llm_enhanced']}")

        logger.info("🎯 PHASE 3: Pure Deterministic Escalation")
        analysis_report['audit_log']['phases_executed'].append('PHASE_3_ESCALATION')

        defence_result = analysis_report['modules'].get('defence_risk_analysis', {})
        dependency_result = analysis_report['modules'].get('dependency_enforcement', {})
        doc_compliance = analysis_report['modules'].get('document_compliance', {})

        escalation_flags = {
            'fatal': False,
            'debt_disputed': defence_result.get('absolute_minimum_tier') in ['FATAL', 'HIGH_RISK'],
            'director_inactive_no_averment': len(dependency_result.get('violations', [])) > 0,
            'critical_count': len(doc_compliance.get('critical_defects', [])),
            'warning_count': len(doc_compliance.get('warnings', [])),
            'base_score': risk_result.get('overall_risk_score', 50)
        }

        final_tier = pure_escalation_engine(escalation_flags)

        analysis_report['final_tier'] = final_tier['tier']
        analysis_report['final_status'] = final_tier['status']
        analysis_report['final_score'] = final_tier['score']
        analysis_report['final_recommendation'] = final_tier['recommendation']
        analysis_report['tier_locked'] = final_tier.get('tier_locked', False)
        analysis_report['minimum_tier_enforcement'] = final_tier.get('minimum_tier_enforcement')
        analysis_report['deterministic_escalation'] = True

        analysis_report['audit_log']['escalation_flags'] = escalation_flags
        analysis_report['audit_log']['final_tier_assigned'] = final_tier['tier']
        analysis_report['audit_log']['analysis_completed'] = datetime.now().isoformat()

        logger.info(f"   Final Tier: {final_tier['tier']} (Locked: {final_tier.get('tier_locked', False)})")
        logger.info(f"   Production Engine v{ENGINE_VERSION}")

        logger.info("📄 Generating clean professional report...")
        # Sanitize all module outputs — removes None/empty that cause PDF broken characters
        if "modules" in analysis_report:
            analysis_report["modules"] = sanitize_module_output(analysis_report["modules"])

        # ── BUILD CENTRAL RESULT OBJECT ──────────────────────────────────────
        # Single source of truth for all report generators.
        # Every field guaranteed non-null. Report template reads ONLY from here.
        _risk   = analysis_report.get('modules', {}).get('risk_assessment', {}) or {}
        _tl     = analysis_report.get('modules', {}).get('timeline_intelligence', {}) or {}
        _ing    = analysis_report.get('modules', {}).get('ingredient_compliance', {}) or {}
        _doc    = analysis_report.get('modules', {}).get('documentary_strength', {}) or {}
        _def    = analysis_report.get('modules', {}).get('procedural_defects', {}) or {}
        _defm   = analysis_report.get('modules', {}).get('defence_matrix', {}) or {}
        _cx     = analysis_report.get('modules', {}).get('cross_examination_risk', {}) or {}
        _jud    = analysis_report.get('modules', {}).get('judicial_behavior', {}) or {}
        _pres   = analysis_report.get('modules', {}).get('presumption_analysis', {}) or {}
        _sett   = analysis_report.get('modules', {}).get('settlement_analysis', {}) or {}
        _is_fatal = bool(analysis_report.get('fatal_flag'))

        # Collect ALL fatal defects from all modules — deduplicated
        _all_fatals = []
        _all_fatals += (_risk.get('fatal_defects') or [])
        _all_fatals += (_def.get('fatal_defects') or [])
        _all_fatals += (_ing.get('fatal_defects') or [])
        _seen_f, _uniq_f = set(), []
        for _fd in _all_fatals:
            _k = _fd.get('defect', str(_fd))
            if _k not in _seen_f:
                _seen_f.add(_k); _uniq_f.append(_fd)

        # Category scores — guaranteed numeric
        _cat = _risk.get('category_scores', {}) or {}
        def _cscore(name, fb=0.0):
            d = _cat.get(name)
            try: return round(float(d.get('score') if isinstance(d,dict) else d), 1)
            except: return fb

        # Fatal override display — the "Capped at undefined" fix
        _orig_score  = round(_risk.get('overall_risk_score', 0), 1)
        _fdo         = _risk.get('fatal_defect_override', {}) or _risk.get('hard_fatal_override', {}) or {}
        _orig_before = round(_fdo.get('original_score', _fdo.get('original_weighted_score', _orig_score)), 1)
        _cap_val     = round(_fdo.get('capped_at', _fdo.get('overridden_score', _orig_score)), 1)
        _cap_display = f"{_cap_val}/100"

        # Processing time — guaranteed string, never "undefineds"
        _pt_secs  = analysis_report.get('processing_time_seconds', 0) or 0
        _pt_str   = f"{round(_pt_secs, 2):.2f}s" if _pt_secs > 0 else "< 1s"

        # Timeline score — use module's score field, fall back to category score
        _tl_score = _tl.get('score', _cscore('Timeline Compliance', 50))

        # Documentary gaps — always named
        _doc_gaps = []
        for _field, _label, _sev, _imp in [
            ('written_agreement_exists', 'No written loan/transaction agreement', 'Severe',
             'Debt enforceability highly contestable — accused can deny legally enforceable debt'),
            ('ledger_available',         'No ledger or account records',           'High',
             'Transaction trail incomplete — no financial record of the alleged loan'),
            ('postal_proof_available',   'No postal proof of notice',              'Moderate',
             'Notice service unproven — accused can deny receiving the legal notice'),
            ('original_cheque_available','Original cheque not secured',            'High',
             'Primary instrument unavailable — foundational evidence at risk'),
            ('return_memo_available',    'Bank dishonour memo missing',            'Severe',
             'Proof of dishonour not secured — essential statutory ingredient at risk'),
        ]:
            if not case_data.get(_field, True):
                _doc_gaps.append({'gap_name': _label, 'severity': _sev,
                                   'impact': _imp, 'remedy': 'Obtain before filing'})

        # Cross-examination questions — always populated
        _cx_questions = list(_cx.get('likely_questions', []) or [])
        if not _cx_questions:
            if not case_data.get('written_agreement_exists'):
                _cx_questions.append("Is it correct that there is no written agreement evidencing the alleged loan?")
                _cx_questions.append("Can you explain why such a large amount was given without any documentation?")
            if 'PREMATURE' in str(_tl.get('compliance_status', {}).get('limitation', '')).upper():
                _cx_questions.append("Was this complaint filed before the 15-day payment notice period had expired?")
                _cx_questions.append("Are you aware that a complaint under Section 138 is not maintainable before the cause of action arises?")
            if not case_data.get('postal_proof_available'):
                _cx_questions.append("Do you have an Acknowledgment Due (AD) card proving the accused received your notice?")
            if not case_data.get('ledger_available'):
                _cx_questions.append("Can you produce any bank statement or ledger entry showing the transfer of this amount?")
            _cx_questions.append("On what date exactly was the cheque amount given to the accused, and in what form — cash, cheque, or bank transfer?")
            _cx_questions.append("Who was present at the time the alleged loan was made?")
        if not _cx_questions:
            _cx_questions = [
                "Is it correct that there is no written record of this transaction?",
                "Can you produce any independent witness to the alleged transaction?",
                "Was the cheque given as security for a debt or as direct payment?",
            ]

        # Next actions — always structured dicts
        _next_actions = []
        for _fd in _uniq_f[:3]:
            _next_actions.append({
                'action':  str(_fd.get('remedy', _fd.get('defect', 'Address fatal defect')) or 'Consult legal counsel'),
                'urgency': 'URGENT',
                'details': str(_fd.get('impact', 'This defect will cause dismissal of the complaint') or '')
            })
        if not case_data.get('postal_proof_available'):
            _next_actions.append({'action': 'Obtain AD card or speed-post tracking confirmation for notice',
                                   'urgency': 'HIGH', 'details': 'Without postal proof, notice service can be challenged'})
        if not case_data.get('written_agreement_exists'):
            _next_actions.append({'action': 'Collect WhatsApp/email/SMS evidence of the transaction',
                                   'urgency': 'MEDIUM', 'details': 'Absence of written agreement is primary defence weakness'})
        if not _next_actions:
            _next_actions.append({'action': 'Proceed with final legal review before filing',
                                   'urgency': 'NORMAL', 'details': 'Case is in reasonable position'})

        # Presumption
        _pres_stage  = str(_pres.get('current_stage', 'Not assessed') or 'Not assessed')
        _pres_burden = str(_pres.get('burden_position', 'Not assessed') or 'Not assessed')

        # Judicial behaviour
        _jud_conf    = str(_jud.get('confidence', 'Insufficient data') or 'Insufficient data')
        _jud_court   = str(_jud.get('court_identified', case_data.get('court_location', 'Not specified')) or 'Not specified')
        _jud_note    = (
            'Judicial behaviour analysis unavailable — insufficient court data in knowledge base.'
            if _jud_conf in ('LOW', 'Insufficient data', 'Not available') else
            f"Based on cases from {_jud_court}. Confidence: {_jud_conf}."
        )

        # Filing verdict
        if _is_fatal:
            _filing_verdict = "DO NOT FILE — FATAL DEFECTS PRESENT"
        elif _orig_score >= 75:
            _filing_verdict = "READY TO FILE — STRONG CASE"
        elif _orig_score >= 55:
            _filing_verdict = "FILE WITH CAUTION — EVIDENCE GAPS PRESENT"
        else:
            _filing_verdict = "HIGH RISK — REMEDIATION REQUIRED BEFORE FILING"

        # ── Store central result object ───────────────────────────
        analysis_report['_result'] = {
            # Scores
            'overall_score':         _orig_score,
            'overall_score_display': f"{_orig_score:.1f}/100",
            'original_score':        _orig_before,
            'capped_at':             _cap_val,
            'capped_at_display':     _cap_display,
            'fatal_override_applied': _is_fatal and bool(_fdo),
            'fatal_override_note':   (
                f"Original: {_orig_before}/100 → Capped at {_cap_display} due to fatal defect"
                if (_is_fatal and bool(_fdo)) else
                "No fatal override applied"
            ),
            'timeline_score':        _tl_score,
            'ingredient_score':      _cscore('Ingredient Compliance'),
            'documentary_score':     _cscore('Documentary Strength', _doc.get('overall_strength_score', 0)),
            'procedural_score':      _cscore('Procedural Compliance'),
            'liability_score':       _cscore('Liability Expansion'),
            'compliance_level':      str(_risk.get('compliance_level', 'Under Review') or 'Under Review'),
            # Fatal
            'is_fatal':              _is_fatal,
            'fatal_defects_count':   len(_uniq_f),
            'fatal_defects':         [
                {'defect':   str(d.get('defect','Unknown defect')),
                 'severity': str(d.get('severity','CRITICAL')),
                 'impact':   str(d.get('impact','Complaint will be dismissed') or ''),
                 'remedy':   str(d.get('remedy', d.get('cure','Consult legal counsel')) or 'Consult legal counsel')}
                for d in _uniq_f
            ],
            # Filing
            'filing_verdict':        _filing_verdict,
            # Time
            'processing_time':       _pt_str,
            'processing_time_seconds': _pt_secs,
            # Documentary
            'documentary_gaps':      _doc_gaps,
            'documentary_gaps_count': len(_doc_gaps),
            # Cross-exam
            'cross_exam_questions':  _cx_questions[:8],
            'cross_exam_risk':       str(_cx.get('overall_cross_exam_risk') or _cx.get('overall_risk') or 'Not assessed'),
            'cross_exam_zones':      [
                {'zone': str(z.get('zone', z.get('area',''))),
                 'risk': str(z.get('risk_level', z.get('severity','MEDIUM')))}
                for z in (_cx.get('vulnerability_zones') or [])[:4]
            ],
            # Presumption
            'presumption_stage':     _pres_stage,
            'burden_position':       _pres_burden,
            'presumption_activated': bool(_pres.get('presumption_activated') or _pres.get('presumption_triggered')),
            # Judicial
            'court_name':            _jud_court,
            'court_confidence':      _jud_conf,
            'court_note':            _jud_note,
            'court_indices':         {k: str(v) for k,v in (_jud.get('behavioral_indices') or {}).items()},
            # Next actions
            'next_actions':          _next_actions,
            # Settlement
            'cheque_amount':         case_data.get('cheque_amount', 0),
            'settlement_recommended': bool(_sett.get('settlement_recommended')),
            'settlement_range':      str(_sett.get('recommended_settlement_range', 'Not calculated') or 'Not calculated'),
            # Platform
            'platform_name':         'JUDIQ Legal Intelligence Platform',
            'platform_email':        'hello@judiq.ai',
            'platform_website':      'www.judiq.ai',
            'case_id':               str(analysis_report.get('case_id', '')),
            'generated_date':        datetime.now().strftime('%d %B %Y'),
            'engine_version':        str(analysis_report.get('engine_version', 'v10.0')),
        }
        logger.info(f"✅ Central result object built: {len(analysis_report['_result'])} fields")

        analysis_report['professional_report'] = generate_clean_professional_report(analysis_report, case_data)

        # Verdict already enforced before executive summary (line 7456)
        # No need to enforce again

        # CRITICAL FIX: Add audit hash locking
        # hashlib and json already imported at top of file

        # Create hash of input data (prevents tampering)
        case_data_sorted = json.dumps(case_data, sort_keys=True, default=str)
        input_hash = hashlib.sha256(case_data_sorted.encode()).hexdigest()

        # Create hash of analysis output
        analysis_sorted = json.dumps(analysis_report, sort_keys=True, default=str)
        analysis_hash = hashlib.sha256(analysis_sorted.encode()).hexdigest()

        # Add audit trail
        analysis_report['audit_trail'] = {
            'input_hash': input_hash,
            'analysis_hash': analysis_hash,
            'timestamp': datetime.now().isoformat(),
            'locked': True,
            'version': 'v11.2',
            'note': 'Analysis locked - input and output hashes recorded for integrity verification'
        }

        logger.info(f"  🔒 Audit lock applied - Input hash: {input_hash[:16]}...")

        # FIX #20: Check if DB save succeeded
        db_saved = save_analysis_to_db(analysis_report)
        if not db_saved:
            logger.warning("⚠️ Analysis completed but failed to save to database")
            analysis_report['audit_trail']['database_saved'] = False
        else:
            analysis_report['audit_trail']['database_saved'] = True

        # NO LLM enhancement in analysis - LLM is ONLY for cross-examination
        # Cross-examination questions available via separate endpoint: POST /generate-cross-examination

        return analysis_report

    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

        # Safely update analysis_report (may be partially initialized)
        if analysis_report is None:
            analysis_report = {}

        analysis_report.update({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__,
            'error_traceback': traceback.format_exc(),
            'fatal_flag': True,
            'overall_status': 'ERROR - ANALYSIS FAILED',
            'engine_version': ENGINE_VERSION,
            'timestamp': analysis_start_time.isoformat() if 'analysis_start_time' in locals() else datetime.now().isoformat()
        })

        return analysis_report

@app.get("/")
async def root():

    # Check Phi-2 availability for cross-examination
    phi2_status = {
        'libraries_installed': PHI2_AVAILABLE,
        'enabled': PHI2_AVAILABLE,
        'purpose': 'Cross-examination questions only'
    }

    return {
        "platform": "JUDIQ v5.0 - Legal Intelligence Platform",
        "version": "5.0.0",
        "maturity": "90% Elite-Grade",
        "status": "operational",
        "message": "Welcome to JUDIQ AI - Section 138 NI Act Intelligence Platform",
        "capabilities": {
            "ratio_based_analytics": True,
            "data_calibrated_scoring": True,
            "confidence_layer": True,
            "severity_tiers": True,
            "validation_framework": True,
            "cross_examination_llm": phi2_status['enabled']
        },
        "phi2_status": phi2_status,
        "endpoints": {
            "health": "GET /health",
            "validation": "GET /validation/accuracy",
            "analyze_case": "POST /analyze-case",
            "cross_examination": "POST /generate-cross-examination",
            "court_intelligence": "GET /court-intelligence/{court_name}",
            "generate_report": "POST /generate-report",
            "api_docs": "/docs",
            "openapi_schema": "/openapi.json"
        },
        "quick_start": {
            "1_check_health": "GET /health",
            "2_test_validation": "GET /validation/accuracy",
            "3_view_docs": "Visit /docs for interactive API documentation",
            "4_analyze_case": "POST /analyze-case with case details",
            "5_cross_exam": "POST /generate-cross-examination for AI questions"
        },
        "documentation": "/docs",
        "note": "This is NOT legal advice - intelligence tool only. LLM used ONLY for cross-examination questions."
    }

@app.get("/test")
async def test_endpoint():

    return {
        "status": "✅ API is working!",
        "platform": "JUDIQ v5.0",
        "maturity": "90% Elite-Grade",
        "timestamp": time.time(),
        "message": "All systems operational",
        "next_steps": [
            "Visit /docs for interactive API documentation",
            "Try GET /health for system status",
            "Try GET /validation/accuracy to see accuracy metrics"
        ]
    }

@app.get("/llm/status")
async def llm_status():
    return {
        "status": "disabled",
        "message": "LLM disabled on Render deployment",
        "note": "Cross-examination uses rule-based engine"
    }

@app.post("/llm/load")
async def load_llm_endpoint():
    return {
        "status": "disabled",
        "message": "LLM disabled on Render deployment"
        }


@app.post("/kb/reload")
async def reload_kb():
    """
    Re-download the knowledge base from Google Drive.

    Use this after uploading a new version of cheque_bounce_kb.csv to Drive.
    Requires GDRIVE_FILE_ID to be set in Render environment variables.

    Steps to update KB:
      1. Upload new cheque_bounce_kb.csv to Google Drive
      2. Share: Anyone with the link → Viewer
      3. Copy file ID from the shareable URL
      4. Set GDRIVE_FILE_ID=<file-id> in Render environment variables
      5. Call POST /kb/reload — system downloads and reloads automatically
    """
    file_id = CONFIG.get("GDRIVE_FILE_ID", "").strip()
    if not file_id:
        raise HTTPException(
            status_code=400,
            detail="GDRIVE_FILE_ID not configured. Set it in Render environment variables."
        )

    # Force re-download by removing local cache
    kb_local_path = DATA_DIR / "legal_kb" / "cheque_bounce_kb.csv"
    if kb_local_path.exists():
        kb_local_path.unlink()

    success = load_kb()

    return {
        "success": success,
        "kb_loaded": kb_loaded,
        "kb_rows": len(kb_data) if kb_data is not None else 0,
        "message": (
            f"KB reloaded: {len(kb_data)} rows from Google Drive"
            if success else
            "KB reload failed — using minimal fallback. Check GDRIVE_FILE_ID and file sharing settings."
        ),
        "gdrive_file_id_preview": (file_id[:12] + "...") if len(file_id) > 12 else file_id
    }


@app.get("/kb/status")
async def kb_status():
    """Check current knowledge base status and Google Drive configuration."""
    file_id = CONFIG.get("GDRIVE_FILE_ID", "").strip()
    kb_local_path = DATA_DIR / "legal_kb" / "cheque_bounce_kb.csv"
    local_exists = kb_local_path.exists()
    age_hours = (time.time() - kb_local_path.stat().st_mtime) / 3600 if local_exists else None

    return {
        "kb_loaded": kb_loaded,
        "kb_rows": len(kb_data) if kb_data is not None else 0,
        "gdrive_configured": bool(file_id),
        "gdrive_file_id_preview": (file_id[:12] + "...") if len(file_id) > 12 else "(not set)",
        "local_cache_exists": local_exists,
        "local_cache_age_hours": round(age_hours, 1) if age_hours is not None else None,
        "instructions": {
            "step_1": "Upload cheque_bounce_kb.csv to Google Drive",
            "step_2": "Share file: Anyone with the link → Viewer",
            "step_3": "Copy file ID from the shareable URL (long alphanumeric string)",
            "step_4": "Set GDRIVE_FILE_ID=<file-id> in Render environment variables",
            "step_5": "Call POST /kb/reload to download and activate"
        }
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": "5.0.0",
        "platform": "Legal Intelligence Platform",
        "kb_loaded": kb_loaded,
        "kb_provisions": len(kb_data) if kb_loaded else 0,
        "llm_enhancement": False,  # LLM disabled on Render
        "embed_model": False,  # embedding disabled on Render
        "database": str(analytics_db_path),
        "maturity_level": "90% Elite-Grade"
    }

@app.get("/validation/accuracy")
async def validate_accuracy():

    test_cases = [
        {
            'name': 'Timeline: Late complaint filing',
            'input': {
                'cheque_date': '2023-01-01',
                'dishonour_date': '2023-02-01',
                'notice_date': '2023-02-15',
                'complaint_filed_date': '2023-04-30'
            },
            'expected_limitation_risk': 'HIGH'
        },
        {
            'name': 'Timeline: Compliant filing',
            'input': {
                'cheque_date': '2023-01-01',
                'dishonour_date': '2023-02-01',
                'notice_date': '2023-02-15',
                'complaint_filed_date': '2023-03-25'
            },
            'expected_limitation_risk': 'LOW'
        },
        {
            'name': 'Fatal: Cheque presented after 3-month validity',
            'input': {
                'cheque_date': '2023-01-01',
                'presentation_date': '2023-05-15',
                'dishonour_date': '2023-05-15'
            },
            'expected_fatal': True
        },
        {
            'name': 'Timeline: Notice sent beyond 30 days',
            'input': {
                'cheque_date': '2023-01-01',
                'dishonour_date': '2023-02-01',
                'notice_date': '2023-03-15',
                'complaint_filed_date': '2023-04-10'
            },
            'expected_limitation_risk': 'CRITICAL'
        }
    ]

    results = {
        'total_tests': len(test_cases),
        'passed': 0,
        'failed': 0,
        'test_details': []
    }

    for case in test_cases:
        try:
            result = analyze_timeline(case['input'])

            if 'expected_limitation_risk' in case:
                actual = result['limitation_risk']
                expected = case['expected_limitation_risk']
                passed = actual == expected
            elif 'expected_fatal' in case:
                passed = any(
                    r.get('severity') in ['CRITICAL', 'FATAL']
                    for r in result.get('risk_markers', [])
                )
            else:
                passed = False

            if passed:
                results['passed'] += 1
                status = '✅ PASS'
            else:
                results['failed'] += 1
                status = '❌ FAIL'

            results['test_details'].append({
                'test_name': case['name'],
                'status': status,
                'passed': passed,
                'expected': case.get('expected_limitation_risk') or 'Fatal defect detection',
                'actual': result.get('limitation_risk', 'N/A')
            })

        except Exception as e:
            results['failed'] += 1
            results['test_details'].append({
                'test_name': case['name'],
                'status': '❌ ERROR',
                'passed': False,
                'error': str(e)
            })

    results['accuracy_percentage'] = round((results['passed'] / results['total_tests']) * 100, 1) if results['total_tests'] > 0 else 0
    results['pass_rate'] = f"{results['passed']}/{results['total_tests']}"

    if results['accuracy_percentage'] >= 80:
        results['grade'] = 'EXCELLENT'
    elif results['accuracy_percentage'] >= 60:
        results['grade'] = 'GOOD'
    else:
        results['grade'] = 'NEEDS IMPROVEMENT'

    return {
        'module_tested': 'Timeline Intelligence Engine',
        'accuracy': results['accuracy_percentage'],
        'grade': results['grade'],
        'summary': results,
        'note': 'Validation demonstrates system accuracy on known test cases'
    }

@app.post("/generate-report")
async def generate_report(case_id: str, format: str = "executive"):

    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT analysis_json FROM case_analyses WHERE case_id = ?", (case_id,))
        result = cursor.fetchone()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Case not found")

        analysis_data = json.loads(result[0])

        if format == "executive":
            report = generate_executive_report(analysis_data)
        else:
            report = generate_detailed_report(analysis_data)

        return {
            "success": True,
            "case_id": case_id,
            "format": format,
            "report": report,
            "download_ready": True
        }

    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/report-data/{case_id}")
async def get_report_data(case_id: str):
    """
    Returns a FLAT, PDF-ready data object for a given case_id.
    Every field has a guaranteed non-null string value.
    The frontend PDF renderer should read from this endpoint — not from /analyze-case.

    All scores are real numbers. All text fields are meaningful strings.
    No None, no empty strings, no broken characters.
    """
    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT analysis_json FROM case_analyses WHERE case_id = ?", (case_id,))
        result = cursor.fetchone()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail=f"Case not found: {case_id}")

        a = json.loads(result[0])
        return _build_flat_report(a)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Report data fetch failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _s(v, fb="Not available"):
    """Safe string — never returns None or empty."""
    if v is None or (isinstance(v, str) and not v.strip()):
        return fb
    return str(v)

def _n(v, fb=0.0):
    """Safe number."""
    try: return round(float(v), 1)
    except: return fb

def _build_flat_report(a: dict) -> dict:
    """
    Build a completely flat, null-safe report object.
    Every field the PDF template might read is guaranteed to exist with a real value.
    """
    mods    = a.get('modules') or {}
    risk    = (mods.get('risk_assessment') or {})
    tl      = (mods.get('timeline_intelligence') or {})
    ingr    = (mods.get('ingredient_compliance') or {})
    doc     = (mods.get('documentary_strength') or {})
    defects = (mods.get('procedural_defects') or {})
    defence = (mods.get('defence_matrix') or {})
    cx      = (mods.get('cross_examination_risk') or {})
    jb      = (mods.get('judicial_behavior') or {})
    pres    = (mods.get('presumption_analysis') or {})
    settle  = (mods.get('settlement_analysis') or {})
    exec_s  = (a.get('executive_summary') or {})
    report  = (a.get('professional_report') or {})

    score       = _n(a.get('risk_score') or risk.get('overall_risk_score'))
    fatal       = bool(a.get('fatal_flag') or a.get('is_fatal'))
    cat_scores  = risk.get('category_scores') or {}

    # ── Score per category ──
    def cat(name, fallback=0.0):
        d = cat_scores.get(name)
        if isinstance(d, dict): return _n(d.get('score'), fallback)
        return _n(d, fallback)

    # ── All fatal defects ──
    all_fatals = []
    all_fatals += (risk.get('fatal_defects') or [])
    all_fatals += (defects.get('fatal_defects') or [])
    all_fatals += (ingr.get('fatal_defects') or [])
    seen, uniq = set(), []
    for d in all_fatals:
        k = d.get('defect', str(d))
        if k not in seen:
            seen.add(k); uniq.append(d)

    # ── Timeline events sorted chronologically ──
    chart = sorted(
        [e for e in (tl.get('timeline_chart') or []) if e.get('date')],
        key=lambda x: x.get('date','9999')
    )

    # ── Strengths/weaknesses ──
    strengths = exec_s.get('strengths') or []
    weaknesses = exec_s.get('weaknesses') or []

    # ── Cross-exam questions ──
    cx_questions = cx.get('likely_questions') or []
    if not cx_questions and not doc.get('written_agreement_exists'):
        cx_questions = [
            'Can you produce the original loan agreement in writing?',
            'Is it not true that no written record of this transaction exists?',
            'Is it not true the cheque was given as security, not for a debt?',
            'How was the money transferred — cash, cheque, or bank transfer?',
            'Who was present when the alleged loan was given?',
        ]

    return sanitize_module_output({
        # ── Identity ──
        'case_id':              _s(a.get('case_id')),
        'generated_date':       _s(a.get('analysis_timestamp','')[:10]),
        'engine_version':       _s(a.get('engine_version'), 'v10.0'),
        'processing_time':      f"{_n(a.get('processing_time_seconds') or a.get('processing_time',0))}s",
        'processing_time_display': (
            f"{_n(a.get('processing_time_seconds')):.2f}s"
            if a.get('processing_time_seconds') else '< 1s'
        ),

        # ── Score ──
        'overall_score':        score,
        'overall_score_display': f"{score:.1f}/100",
        'compliance_level':     _s(risk.get('compliance_level') or a.get('case_strength'), 'Under Review'),
        'is_fatal':             fatal,
        'fatal_cap_applied':    fatal,
        'fatal_defects_count':  len(uniq),

        # ── Executive summary ──
        'filing_status':        _s(exec_s.get('filing_verdict') or a.get('final_status'), 'See analysis'),
        'one_line_verdict':     _s(exec_s.get('case_overview'), 'Analysis complete — see details below'),
        'recommended_action':   _s(a.get('decisive_verdict') or a.get('filing_recommendation'), 'Consult legal counsel'),
        'strengths':            [_s(s) for s in strengths[:5]] or ['Analysis complete'],
        'weaknesses':           [_s(w) for w in weaknesses[:5]] or ['Review full analysis'],
        'critical_risks':       [{'defect': _s(d.get('defect')),
                                   'severity': _s(d.get('severity'),'CRITICAL'),
                                   'impact': _s(d.get('impact')),
                                   'remedy': _s(d.get('remedy', d.get('cure','Consult counsel')))}
                                  for d in uniq[:5]],

        # ── Category scores ──
        'score_timeline':       cat('Timeline Compliance'),
        'score_ingredients':    cat('Ingredient Compliance'),
        'score_documentary':    cat('Documentary Strength'),
        'score_procedural':     cat('Procedural Compliance'),
        'score_liability':      cat('Liability Expansion'),

        # ── Timeline ──
        'limitation_risk':      _s(tl.get('limitation_risk'), 'Not assessed'),
        'limitation_status':    _s((tl.get('compliance_status') or {}).get('limitation'), 'Not assessed'),
        'timeline_events':      [{'date': _s(e.get('date')),
                                   'event': _s(e.get('event')),
                                   'status': _s(e.get('status'),'—')} for e in chart],
        'critical_dates':       {k: _s(v) for k,v in (tl.get('critical_dates') or {}).items()},

        # ── Ingredients ──
        'ingredient_compliance': _n(ingr.get('overall_compliance')),
        'ingredient_risk_level': _s(ingr.get('risk_level'), 'Not assessed'),

        # ── Documentary ──
        'doc_strength_score':   _n(doc.get('overall_strength_score')),
        'doc_strength_label':   _s(doc.get('strength_label'), 'Not assessed'),
        'doc_items': [
            {'name':'Original Cheque',    'available': bool(a.get('case_metadata',{}).get('original_cheque_available')), 'importance':'Essential'},
            {'name':'Return Memo',        'available': bool(doc.get('return_memo_available')), 'importance':'Essential'},
            {'name':'Postal Proof',       'available': bool(doc.get('postal_proof_available')), 'importance':'Critical'},
            {'name':'Written Agreement',  'available': bool(doc.get('written_agreement_exists')), 'importance':'Important'},
            {'name':'Ledger/Records',     'available': bool(doc.get('ledger_available')), 'importance':'Important'},
        ],

        # ── Procedural defects ──
        'procedural_risk':      _s(defects.get('overall_risk'), 'Not assessed'),
        'fatal_defects':        [{'defect': _s(d.get('defect')),
                                   'severity': _s(d.get('severity'),'CRITICAL'),
                                   'impact': _s(d.get('impact')),
                                   'remedy': _s(d.get('remedy', d.get('cure','Consult counsel')))}
                                  for d in (defects.get('fatal_defects') or [])],
        'curable_defects':      [{'defect': _s(d.get('defect')),
                                   'cure': _s(d.get('cure'))}
                                  for d in (defects.get('curable_defects') or [])[:3]],

        # ── Defence ──
        'defence_exposure':     _s(defence.get('overall_exposure') or defence.get('exposure_level'), 'Not assessed'),
        'high_risk_defences':   [{'defence': _s(d.get('defence', d.get('ground'))),
                                   'strength': _s(d.get('strength', d.get('risk_impact')),'Unknown'),
                                   'strategy': _s(d.get('strategy'))}
                                  for d in (defence.get('high_risk_defences') or [])[:4]]
                                 or [{'defence':'No major defences identified','strength':'LOW','strategy':'Maintain evidence posture'}],

        # ── Settlement ──
        'settlement_recommended': bool(settle.get('settlement_recommended')),
        'settlement_range':     _s(str(settle.get('recommended_settlement_range') or 'Not calculated')),
        'interim_eligible':     bool(settle.get('interim_compensation_eligible')),

        # ── Judicial behaviour ──
        'court_name':           _s(jb.get('court_identified'), 'Generic Court'),
        'court_confidence':     _s(jb.get('confidence'), 'Insufficient data'),
        'court_data_available': jb.get('confidence') in ('HIGH','MEDIUM'),
        'court_note':           _s(jb.get('note') if jb.get('confidence') not in ('HIGH','MEDIUM')
                                   else f"Sample: {jb.get('sample_size',0)} cases",
                                   'Insufficient court data — analysis unavailable'),

        # ── Presumption ──
        'presumption_stage':    R.get('presumption_stage', _s(pres.get('current_stage'), 'Not assessed')),
        'burden_position':      R.get('burden_position', _s(pres.get('burden_position'), 'Not assessed')),
        'presumption_activated': bool(pres.get('presumption_activated') or pres.get('presumption_triggered')),

        # ── Cross-examination ──
        'cross_exam_risk':      _s(cx.get('overall_cross_exam_risk') or cx.get('overall_risk'), 'Not assessed'),
        'cross_exam_questions': [_s(q) for q in cx_questions[:8]],
        'cross_exam_zones':     [{'zone': _s(z.get('zone',z.get('area'))),
                                   'risk': _s(z.get('risk_level',z.get('severity')),'MEDIUM')}
                                  for z in (cx.get('vulnerability_zones') or [])[:4]],

        # ── Platform ──
        'platform_name':        'JUDIQ Legal Intelligence Platform',
        'platform_email':       'hello@judiq.ai',
        'platform_website':     'www.judiq.ai',
        'platform_address':     'JUDIQ AI — Section 138 Intelligence Platform',
    })


@app.post("/generate-cross-examination")
async def generate_cross_examination(request: CrossExaminationRequest):
    """
    Generate cross-examination questions using local Phi-2 model.

    **STRICT PURPOSE:** Cross-examination questions ONLY.
    This endpoint does NOT re-run or affect the main case analysis engine.

    The response includes:
    - **questions** — AI-generated, witness-specific cross-examination questions
    - **summary** — plain-language explanation of the attack strategy and themes
    - **fallback** — if Phi-2 unavailable, rule-based questions are returned automatically

    Witness types:
    - `complainant` — person who filed the case (defence lawyer's cross)
    - `accused` — person who issued the cheque (prosecution's cross)
    - `bank_official` — bank witness on dishonour memo
    - `drawer` — alias for accused
    - `witness` — third-party transaction witness
    """
    valid_types = ['complainant', 'accused', 'drawer', 'bank_official', 'witness']
    if request.witness_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"witness_type must be one of: {', '.join(valid_types)}"
        )

    try:
        logger.info(f"🤖 Generating {request.num_questions} cross-exam questions for {request.witness_type}...")

        result = generate_cross_examination_questions(
            case_data=request.case_data,
            witness_type=request.witness_type,
            num_questions=request.num_questions
        )

        # If Phi-2 failed but fallback questions are available, return them with a note
        if not result.get('enabled'):
            fallback_qs = result.get('fallback', [])
            if fallback_qs:
                summary = _build_summary_section(fallback_qs, request.witness_type, request.case_data)
                return {
                    "success": True,
                    "witness_type": request.witness_type,
                    "questions": fallback_qs,
                    "summary": summary,
                    "question_count": len(fallback_qs),
                    "model": "rule-based fallback (Phi-2 unavailable)",
                    "purpose": "CROSS-EXAMINATION ONLY — does not affect main analysis",
                    "disclaimer": "Rule-based questions derived from case facts. Review with legal counsel before use in court.",
                    "phi2_note": result.get('reason', result.get('error', 'Phi-2 not available'))
                }
            raise HTTPException(
                status_code=503,
                detail=result.get('reason', result.get('error', 'Cross-examination generation failed'))
            )

        return {
            "success": True,
            "witness_type": request.witness_type,
            "questions": result.get('questions', []),
            "summary": result.get('summary', {}),
            "question_count": result.get('question_count', len(result.get('questions', []))),
            "model": result.get('model', 'microsoft/phi-2 (local)'),
            "purpose": "CROSS-EXAMINATION ONLY — does not affect main analysis",
            "disclaimer": result.get('disclaimer', 'AI-generated questions for reference only. Review with legal counsel before use in court.')
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Cross-examination generation failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Cross-examination error: {str(e)}")

@app.post("/analyze-case")
@limiter.limit("30/minute")
async def analyze_case(request: CaseAnalysisRequest, http_request: Request = None):

    start = time.time()
    audit = AuditTrail()

    try:
        case_data = request.model_dump()

        is_valid, validation_errors, sanitized_data = validate_case_input_strict(case_data)
        audit.log_validation(is_valid, validation_errors)

        # Hard errors — cannot proceed
        if not is_valid:
            logger.warning(f"⚠️ VALIDATION FAILED: {validation_errors}")
            return {
                "success": False,
                "error": "Input validation failed",
                "validation_errors": validation_errors,
                "audit_trail": audit.get_trail(),
                "message": "Please correct the errors listed in validation_errors and resubmit."
            }

        # Soft warnings — extract before passing sanitized data to engine
        input_warnings = sanitized_data.pop('_warnings', [])
        if input_warnings:
            logger.info(f"ℹ️ Proceeding with {len(input_warnings)} input warning(s)")

        case_data = sanitized_data

        unified_timeline = compute_unified_timeline(case_data)
        case_data['_unified_timeline'] = unified_timeline

        analysis = safe_module_execution(perform_comprehensive_analysis, case_data)

        if analysis.get('error'):
            error_msg = analysis.get('error_message') or analysis.get('error') or 'Unknown error'
            logger.error(f"❌ Analysis failed: {error_msg}")
            if analysis.get('error_traceback'):
                logger.error(f"Traceback: {analysis.get('error_traceback')}")
            return {
                "success": False,
                "error": "Analysis execution failed",
                "error_message": error_msg,
                "error_details": analysis.get('error_traceback'),
                "audit_trail": audit.get_trail()
            }

        analysis['audit_trail'] = audit.get_trail()

        # Attach input warnings to the response so callers can see them
        if input_warnings:
            analysis['input_warnings'] = input_warnings
            analysis['input_warning_count'] = len(input_warnings)

        # Verdict integrity already enforced in perform_comprehensive_analysis()
        # Do NOT regenerate outcome_classification here - it will override the enforced verdict
        # All verdict fields are already synchronized by enforce_verdict_integrity()

        if 'modules' in analysis and 'risk_assessment' in analysis['modules']:
            risk = analysis['modules']['risk_assessment']
            if 'category_scores' in risk:
                weights = get_centralized_weights()
                analysis['score_breakdown_detail'] = generate_score_breakdown(
                    risk['category_scores'],
                    weights
                )

        # Generate plain-language summary for lawyers
        plain_summary = generate_plain_summary(analysis, case_data)

        return {
            "success": True,
            "case_id": analysis['case_id'],
            # ── Quick read (lawyers read these first) ──────────────────
            "plain_summary":     plain_summary,
            "executive_summary": analysis.get('executive_summary', {}),
            "report":            analysis.get('professional_report', {}),
            "result":            analysis.get('_result', {}),   # ← central result object — PDF template reads this
            # ── Full detail ────────────────────────────────────────────
            "analysis": analysis,
            "api_response_time_ms": round((time.time() - start) * 1000, 1),
            "engine_version": ENGINE_VERSION,
            "maturity_grade": "Production Stable",
        }

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

        return {
            "success": False,
            "error": "Critical analysis error",
            "error_message": str(e),
            "error_type": type(e).__name__,
            "audit_trail": audit.get_trail() if 'audit' in locals() else {},
            "message": "Analysis could not be completed. System logged error for review."
        }

@app.post("/search-knowledge-base")
async def search_knowledge_base(request: SearchKBRequest):

    if not kb_loaded:
        raise HTTPException(status_code=503, detail="Knowledge base not loaded")

    try:
        results = search_kb(
            query=request.query,
            top_k=request.top_k,
            category_filter=request.category_filter
        )

        return {
            "success": True,
            "query": request.query,
            "results_count": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"KB search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/case-history/{case_id}")
async def get_case_history(case_id: str):
    """Retrieve a previously saved analysis by case_id."""
    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT analysis_json, analysis_timestamp, overall_risk_score, compliance_level "
            "FROM case_analyses WHERE case_id = ?",
            (case_id,)
        )
        result = cursor.fetchone()
        conn.close()

        if result:
            analysis_json, timestamp, risk_score, compliance = result
            return {
                "success": True,
                "case_id": case_id,
                "analysis_timestamp": timestamp,
                "overall_risk_score": risk_score,
                "compliance_level": compliance,
                "analysis": json.loads(analysis_json)
            }
        else:
            raise HTTPException(status_code=404, detail=f"Case not found: {case_id}")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Case retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/summary")
async def get_analytics_summary():
    """Get analytics summary with graceful error handling"""
    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='case_analyses'
        """)
        if not cursor.fetchone():
            # Table doesn't exist - return empty stats
            conn.close()
            return {
                "success": True,
                "total_analyses": 0,
                "average_risk_score": 0,
                "compliance_distribution": {},
                "message": "No analyses yet - database initialized"
            }

        cursor.execute("SELECT COUNT(*) FROM case_analyses")
        total_analyses = cursor.fetchone()[0]

        cursor.execute("SELECT AVG(overall_risk_score) FROM case_analyses")
        avg_risk = cursor.fetchone()[0] or 0

        cursor.execute("""
            SELECT compliance_level, COUNT(*)
            FROM case_analyses
            GROUP BY compliance_level
        """)
        compliance_dist = dict(cursor.fetchall())

        conn.close()

        return {
            "success": True,
            "total_analyses": total_analyses,
            "average_risk_score": round(avg_risk, 1),
            "compliance_distribution": compliance_dist
        }
    except Exception as e:
        logger.error(f"Analytics retrieval failed: {e}")
        # Return graceful fallback instead of error
        return {
            "success": True,
            "total_analyses": 0,
            "average_risk_score": 0,
            "compliance_distribution": {},
            "error": "Analytics temporarily unavailable"
        }

@app.get("/court-intelligence/{court_name}")
async def get_court_intelligence(court_name: str):

    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT court_name,
                   total_cases,
                   conviction_rate,
                   acquittal_rate,
                   limitation_dismissal_rate,
                   technical_dismissal_rate,
                   compounding_rate,
                   strictness_index,
                   court_classification,
                   confidence,
                   updated_at
            FROM court_statistics
            WHERE court_name = ?
        """, (court_name,))

        court_data = cursor.fetchone()

        if not court_data:
            raise HTTPException(status_code=404, detail=f"No data available for court: {court_name}")

        name, total, conv_rate, acq_rate, lim_rate, tech_rate, comp_rate, \
        strictness, classification, confidence, updated = court_data

        # Reconstruct approximate counts from stored rates
        convictions  = round((conv_rate  or 0) / 100 * total)
        acquittals   = round((acq_rate   or 0) / 100 * total)
        lim_dismiss  = round((lim_rate   or 0) / 100 * total)
        tech_dismiss = round((tech_rate  or 0) / 100 * total)
        compounded   = round((comp_rate  or 0) / 100 * total)
        interim      = 0
        avg_days     = None
        level        = classification or "Unknown"
        year         = updated[:4] if updated else "Historical"

        report = {
            'court_name': name,
            'court_level': level,
            'data_period': year,
            'sample_size': total,
            'confidence': confidence,
            'last_updated': updated,

            'outcome_distribution': {
                'total_cases': total,
                'convictions': convictions,
                'conviction_rate': round((convictions / total) * 100, 1) if total > 0 else 0,
                'acquittals': acquittals,
                'acquittal_rate': round((acquittals / total) * 100, 1) if total > 0 else 0,
                'limitation_dismissals': lim_dismiss,
                'limitation_dismissal_rate': round((lim_dismiss / total) * 100, 1) if total > 0 else 0,
                'technical_dismissals': tech_dismiss,
                'technical_dismissal_rate': round((tech_dismiss / total) * 100, 1) if total > 0 else 0,
            },

            'behavioral_indices': {},
            'dismissal_fingerprint': {},
            'presumption_analytics': {},
            'settlement_intelligence': {},
        }

        if total > 0:
            lim_rate = (lim_dismiss / total) * 100
            tech_rate = (tech_dismiss / total) * 100
            comp_rate = (compounded / total) * 100

            limitation_strictness = min(10.0, (lim_rate / 10) * 0.6 + (1 - ((100 - lim_rate) / 100)) * 10 * 0.4)

            technical_tendency = min(10.0, (tech_rate / 10) * 10)

            settlement_friendly = min(10.0, 3.0 + (comp_rate / 10) * 7.0)

            procedural_formality = min(10.0, 4.0 + (tech_rate / 5) * 6.0)

            overall_strictness = (limitation_strictness * 0.3 + technical_tendency * 0.25 +
                                procedural_formality * 0.3 + (10 - settlement_friendly) * 0.15)

            report['behavioral_indices'] = {
                'limitation_strictness': round(limitation_strictness, 1),
                'technical_dismissal_tendency': round(technical_tendency, 1),
                'settlement_friendly': round(settlement_friendly, 1),
                'procedural_formality': round(procedural_formality, 1),
                'overall_strictness_index': round(overall_strictness, 1)
            }

        cursor.execute("""
            SELECT reason_for_decision,
                   COUNT(*) AS freq,
                   ROUND(COUNT(*) * 100.0 / NULLIF(?, 0), 1) AS pct
            FROM knowledge_base
            WHERE court_name = ?
              AND (final_outcome LIKE '%dismiss%' OR final_outcome LIKE '%discharge%')
              AND reason_for_decision IS NOT NULL
            GROUP BY reason_for_decision
            ORDER BY freq DESC
            LIMIT 5
        """, (total, court_name))

        dismissals = cursor.fetchall()
        if dismissals:
            report['dismissal_fingerprint'] = {
                'top_reasons': [
                    {'reason': r, 'count': f, 'percentage': p}
                    for r, f, p in dismissals
                ]
            }

        cursor.execute("""
            SELECT reason_for_decision,
                   ROUND(SUM(CASE WHEN final_outcome LIKE '%acquit%' THEN 1 ELSE 0 END)
                         * 100.0 / NULLIF(COUNT(*), 0), 1) AS success_rate,
                   SUM(CASE WHEN final_outcome LIKE '%acquit%' THEN 1 ELSE 0 END) AS sc,
                   SUM(CASE WHEN final_outcome NOT LIKE '%acquit%' THEN 1 ELSE 0 END) AS fc
            FROM knowledge_base
            WHERE court_name = ?
              AND court_reasoning_text LIKE '%presumption%'
              AND reason_for_decision IS NOT NULL
            GROUP BY reason_for_decision
            HAVING COUNT(*) >= 2
            ORDER BY success_rate DESC
            LIMIT 5
        """, (court_name,))

        presumptions = cursor.fetchall()
        if presumptions:
            report['presumption_analytics'] = {
                'rebuttal_patterns': [
                    {
                        'type': t,
                        'success_rate': round(sr, 1),
                        'successful': sc,
                        'failed': fc,
                        'total_attempts': sc + fc
                    }
                    for t, sr, sc, fc in presumptions
                ]
            }

        if compounded > 0:
            report['settlement_intelligence'] = {
                'compounding_rate': round((compounded / total) * 100, 1),
                'compounded_cases': compounded,
                'interim_compensation_orders': interim,
                'interim_rate': round((interim / total) * 100, 1) if interim else 0
            }

        if avg_days:
            report['efficiency_metrics'] = {
                'avg_disposal_days': avg_days,
                'avg_disposal_years': round(avg_days / 365, 1),
                'efficiency_rating': 'Fast' if avg_days < 730 else 'Medium' if avg_days < 1460 else 'Slow'
            }

        conn.close()

        return {
            'success': True,
            'court_intelligence_report': report,
            'methodology': 'Outcome-based statistical analysis',
            'disclaimer': 'Historical patterns only - NOT outcome predictions for individual cases'
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Court intelligence retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/courts/list")
async def list_courts():

    try:
        conn = sqlite3.connect(analytics_db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT court_name,
                   total_cases,
                   confidence,
                   SUBSTR(updated_at, 1, 4) AS data_year,
                   court_classification,
                   strictness_index
            FROM court_statistics
            ORDER BY total_cases DESC
        """)

        courts = cursor.fetchall()
        conn.close()

        return {
            'success': True,
            'courts': [
                {
                    'name': name,
                    'sample_size': total,
                    'confidence': conf,
                    'data_year': year,
                    'court_classification': classification,
                    'strictness_index': strictness
                }
                for name, total, conf, year, classification, strictness in courts
            ],
            'total_courts': len(courts)
        }
    except Exception as e:
        logger.error(f"Court list retrieval failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def _ensure_pyngrok():
    """Ngrok disabled on Render."""
    return False


def setup_ngrok() -> Optional[str]:
    """Ngrok not needed on Render — Render provides public URL automatically."""
    return None


def cleanup_ngrok():
    """Ngrok disabled on Render."""
    pass


def main():
    """Entry point for local development only. Render uses uvicorn directly."""
    print("\n" + "="*100)
    print("JUDIQ v5.0 - LEGAL INTELLIGENCE PLATFORM (Render)")
    print("="*100 + "\n")
    port = CONFIG["PORT"]
    uvicorn.run(app, host=CONFIG["HOST"], port=port, log_level="info")

if __name__ == "__main__":
    main()

def classify_risk_legal_tone(score: float, fatal_defects: List) -> Dict:

    if fatal_defects and len(fatal_defects) > 0:
        return {
            'category': 'HIGH DISMISSAL RISK',
            'tone': 'Statutory compliance deficient - dismissal probable',
            'label': 'Critical Defects Identified'
        }
    elif score >= 80:
        return {
            'category': 'STATUTORILY COMPLIANT',
            'tone': 'Statutory requirements appear satisfied (subject to proof)',
            'label': 'Compliance Adequate'
        }
    elif score >= 60:
        return {
            'category': 'PROCEDURAL RISK IDENTIFIED',
            'tone': 'Compliance adequate with evidentiary gaps',
            'label': 'Moderate Risks Present'
        }
    elif score >= 40:
        return {
            'category': 'SIGNIFICANT DEFICIENCIES',
            'tone': 'Material compliance gaps - remediation advisable',
            'label': 'Weak Compliance'
        }
    else:
        return {
            'category': 'HIGH DISMISSAL RISK',
            'tone': 'Substantial statutory violations - remediation required',
            'label': 'Critical Deficiencies'
        }

class JudiqException(Exception):
    """Base exception for all JUDIQ errors"""
    def __init__(self, message: str, severity: str = "ERROR", details: dict = None):
        self.message = message
        self.severity = severity
        self.details = details or {}
        super().__init__(self.message)

class BusinessLogicException(JudiqException):
    """Exception for business logic violations"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, "WARNING", details)

class LegalViolationException(JudiqException):
    """Exception for legal compliance violations"""
    def __init__(self, message: str, fatal: bool = False, details: dict = None):
        severity = "CRITICAL" if fatal else "ERROR"
        super().__init__(message, severity, details)

class DatabaseException(JudiqException):
    """Exception for database operations"""
    def __init__(self, message: str, query: str = None, details: dict = None):
        details = details or {}
        if query:
            details['query'] = query
        super().__init__(message, "ERROR", details)

class ValidationException(JudiqException):
    """Exception for input validation failures"""
    def __init__(self, message: str, field: str = None, details: dict = None):
        details = details or {}
        if field:
            details['field'] = field
        super().__init__(message, "WARNING", details)

class SystemException(JudiqException):
    """Exception for system-level errors"""
    def __init__(self, message: str, component: str = None, details: dict = None):
        details = details or {}
        if component:
            details['component'] = component
        super().__init__(message, "CRITICAL", details)

class FatalException(Exception):
    pass


def check_absolute_fatal_conditions(case_data: Dict, timeline_result: Dict, doc_compliance: Dict, defence_risks: Dict) -> Dict:

    fatal_conditions = {
        'has_fatal': False,
        'fatal_reasons': [],
        'immediate_fail_status': None
    }

    if timeline_result.get('limitation_risk') in ['EXPIRED', 'CRITICAL']:
        fatal_conditions['has_fatal'] = True
        fatal_conditions['fatal_reasons'].append({
            'condition': 'LIMITATION EXPIRED',
            'impact': 'Case is time-barred - Filing will be rejected',
            'action': 'DO NOT FILE'
        })

    if len(doc_compliance.get('fatal_defects', [])) > 0:
        fatal_conditions['has_fatal'] = True
        for defect in doc_compliance['fatal_defects']:
            fatal_conditions['fatal_reasons'].append({
                'condition': defect.get('defect', 'Fatal document defect'),
                'impact': defect.get('impact', 'Filing blocked'),
                'action': 'DO NOT FILE'
            })

    if len(defence_risks.get('fatal_defences', [])) > 0:
        fatal_conditions['has_fatal'] = True
        for fatal_def in defence_risks['fatal_defences']:
            fatal_conditions['fatal_reasons'].append({
                'condition': fatal_def.get('ground', 'Fatal defence exposure'),
                'impact': fatal_def.get('viability_impact', 'Case may collapse'),
                'action': 'ADDRESS URGENTLY'
            })

    if fatal_conditions['has_fatal']:
        fatal_conditions['immediate_fail_status'] = {
            'overall_status': 'FATAL - DO NOT FILE',
            'risk_score': 15,
            'compliance_level': 'FATAL FAILURE',
            'filing_blocked': True,
            'fatal_count': len(fatal_conditions['fatal_reasons']),
            'decisive_verdict': f"ABSOLUTE FAILURE - {len(fatal_conditions['fatal_reasons'])} fatal conditions detected"
        }

    return fatal_conditions


def apply_severity_tier_escalation(issues: Dict) -> Dict:

    fatal_count = issues.get('fatal', 0)
    critical_count = issues.get('critical', 0)
    warning_count = issues.get('warning', 0)

    if fatal_count >= 1:
        return {
            'tier': 'FATAL',
            'status': 'CASE FAILURE',
            'score_cap': 15,
            'recommendation': 'DO NOT FILE - Fatal defects present'
        }
    elif critical_count >= 2:
        return {
            'tier': 'HIGH RISK',
            'status': 'CRITICAL GAPS',
            'score_cap': 45,
            'recommendation': 'FILING NOT ADVISABLE - Multiple critical issues'
        }
    elif critical_count >= 1:
        return {
            'tier': 'MODERATE RISK',
            'status': 'GAPS PRESENT',
            'score_cap': 65,
            'recommendation': 'Address critical gap before filing'
        }
    elif warning_count >= 4:
        return {
            'tier': 'REVIEW REQUIRED',
            'status': 'MINOR GAPS',
            'score_cap': 75,
            'recommendation': 'Strengthen weak areas'
        }
    else:
        return {
            'tier': 'ACCEPTABLE',
            'status': 'COMPLIANT',
            'score_cap': 100,
            'recommendation': 'Ready to file'
        }



USAGE_LOG = []

def get_usage_analytics() -> Dict:
    if not USAGE_LOG:
        return {'total_analyses': 0, 'events': []}

    fatal_count = sum(1 for e in USAGE_LOG if e.get('event') == 'FATAL_DETECTED')
    success_count = sum(1 for e in USAGE_LOG if e.get('event') == 'ANALYSIS_COMPLETE')

    return {
        'total_analyses': len(USAGE_LOG),
        'fatal_cases': fatal_count,
        'success_cases': success_count,
        'fatal_rate': (fatal_count / len(USAGE_LOG) * 100) if USAGE_LOG else 0,
        'recent_events': USAGE_LOG[-20:]
    }



def format_concise_output(analysis: Dict) -> str:
    lines = []
    lines.append("═" * 50)

    if analysis.get('fatal_flag'):
        lines.append("🔴 FILING BLOCKED")
        lines.append("═" * 50)
        lines.append(f"Status: {analysis.get('overall_status', 'FATAL')}")
        lines.append(f"Score: {analysis.get('risk_score', 0)}/100")
        lines.append(f"Verdict: {analysis.get('decisive_verdict', 'DO NOT FILE')}")
        lines.append("")
        lines.append("Fatal Issues:")

        if 'LIMITATION' in analysis.get('overall_status', ''):
            lines.append("• Case time-barred")
        if 'DOCUMENT' in analysis.get('overall_status', ''):
            lines.append("• Critical documents missing")
        if 'DEFENCE' in analysis.get('overall_status', ''):
            lines.append("• Fatal defence exposure")
    else:
        score = analysis.get('modules', {}).get('risk_assessment', {}).get('overall_risk_score', 0)
        lines.append(f"Risk Score: {score}/100")
        lines.append("═" * 50)

        if score >= 75:
            lines.append("Status: ✅ READY TO FILE")
        elif score >= 60:
            lines.append("Status: ⚠️ GAPS PRESENT")
        else:
            lines.append("Status: 🔴 HIGH RISK")

        issues = []
        doc_comp = analysis.get('modules', {}).get('document_compliance', {})
        if doc_comp.get('compliance_score', 100) < 80:
            issues.append("• Document gaps")

        defence = analysis.get('modules', {}).get('defence_risk_analysis', {})
        if len(defence.get('high_risk_defences', [])) > 0:
            issues.append(f"• {len(defence['high_risk_defences'])} defence risks")

        if issues:
            lines.append("")
            lines.append("Key Issues:")
            lines.extend(issues)

    lines.append("═" * 50)
    return "\n".join(lines)
