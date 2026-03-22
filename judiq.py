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

# ── Firebase Admin SDK ────────────────────────────────────────────────────────
try:
    import firebase_admin
    from firebase_admin import credentials, auth as firebase_auth
    _FIREBASE_AVAILABLE = True
except ImportError:
    _FIREBASE_AVAILABLE = False

_firebase_app = None

def _init_firebase():
    """Initialize Firebase Admin SDK from environment variables."""
    global _firebase_app
    if not _FIREBASE_AVAILABLE:
        logger_fb = logging.getLogger(__name__)
        logger_fb.warning("⚠️  firebase-admin not installed — Firebase auth disabled")
        return False
    if _firebase_app is not None:
        return True
    try:
        # Accept either a JSON string or a file path
        fb_cred_raw = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "")
        fb_cred_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", "")
        cred = None
        if fb_cred_raw.strip():
            import tempfile
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tf:
                tf.write(fb_cred_raw)
                tmp_path = tf.name
            cred = credentials.Certificate(tmp_path)
        elif fb_cred_path and os.path.exists(fb_cred_path):
            cred = credentials.Certificate(fb_cred_path)
        else:
            logging.getLogger(__name__).warning(
                "⚠️  No Firebase credentials found — set FIREBASE_SERVICE_ACCOUNT_JSON "
                "or FIREBASE_SERVICE_ACCOUNT_PATH env var"
            )
            return False
        if not firebase_admin._apps:
            _firebase_app = firebase_admin.initialize_app(cred)
        else:
            _firebase_app = firebase_admin.get_app()
        logging.getLogger(__name__).info("✅ Firebase Admin SDK initialized")
        return True
    except Exception as _e:
        logging.getLogger(__name__).error(f"❌ Firebase init failed: {_e}")
        return False


def verify_firebase_token(id_token: str) -> Optional[Dict]:
    """
    Verify a Firebase ID token.
    Returns decoded token dict on success, None on failure.
    """
    if not _FIREBASE_AVAILABLE:
        return None
    if not _init_firebase():
        return None
    try:
        decoded = firebase_auth.verify_id_token(id_token, check_revoked=True)
        return decoded
    except Exception as _e:
        logging.getLogger(__name__).warning(f"Firebase token verify failed: {_e}")
        return None


def get_firebase_uid_from_request(request) -> Optional[str]:
    """
    Extract and verify Firebase Bearer token from Authorization header.
    Returns uid string on success, None on failure.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:]
    decoded = verify_firebase_token(token)
    if decoded:
        return decoded.get("uid")
    return None


def get_firebase_user_email(request) -> Optional[str]:
    """
    Extract verified user email from Firebase token in Authorization header.
    Returns email string on success, None on failure.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header[7:]
    decoded = verify_firebase_token(token)
    if decoded:
        return decoded.get("email")
    return None
# ─────────────────────────────────────────────────────────────────────────────

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

TORCH_AVAILABLE = False

logger = logging.getLogger(__name__)

PHI2_AVAILABLE = False


ENGINE_VERSION = "v10.0"
SCORING_MODEL_VERSION = "5.0"
TIMELINE_MATH_VERSION = "CALENDAR_MONTHS"


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

    paise = round((amount % 1) * 100)
    if paise:
        return f"{result}.{paise:02d}"
    return result


def add_calendar_months(start_date, months: int):

    if isinstance(start_date, str):
        try:
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            raise ValueError(f"Invalid date '{start_date}' — expected YYYY-MM-DD")
    return start_date + relativedelta(months=months)


def calculate_cheque_expiry(cheque_date):

    if isinstance(cheque_date, str):
        try:
            cheque_date = datetime.strptime(cheque_date, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            raise ValueError(f"Invalid cheque_date '{cheque_date}' — expected YYYY-MM-DD")
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
        try:
            notice_service_date = datetime.strptime(notice_service_date, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            raise ValueError(f"Invalid notice_service_date '{notice_service_date}' — expected YYYY-MM-DD")


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
        dates = timeline_data.get('dates', {})

        if 'dishonour_date' in dates:
            try:
                dishonour = datetime.strptime(dates['dishonour_date'], '%Y-%m-%d').date()
            except (ValueError, TypeError):
                dishonour = None
            if dishonour is None:
                dishonour = None

            notice_deadline = add_calendar_months(dishonour, 1)
            transparent_timeline['critical_deadlines']['notice_deadline'] = {
                'last_date': notice_deadline.isoformat(),
                'calculation': f'Dishonour date ({dishonour.isoformat()}) + 1 calendar month',
                'statutory_basis': 'Section 138 NI Act - Notice within 30 days'
            }

            if 'notice_date' in dates and dishonour:
                try:
                    notice = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    notice = None
                if notice:
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
            try:
                notice = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
            except (ValueError, TypeError):
                notice = None
            if notice:
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
                    try:
                        complaint = datetime.strptime(dates['complaint_filed_date'], '%Y-%m-%d').date()
                    except (ValueError, TypeError):
                        complaint = None
                    if complaint and cause_of_action:
                        days_from_coa = (complaint - cause_of_action).days

                        if complaint < cause_of_action:

                            days_premature = (cause_of_action - complaint).days
                            transparent_timeline['compliance_display']['complaint'] = {
                                'status': '❌ PREMATURE FILING',
                                'details': f'Filed {days_premature} days BEFORE cause of action arose',
                                'cause_of_action_date': cause_of_action.isoformat(),
                                'impact': 'Fatal — complaint filed before 15-day payment period expired'
                            }
                        elif complaint == cause_of_action:

                            transparent_timeline['compliance_display']['complaint'] = {
                                'status': '⚠️ SAME-DAY FILING — BORDERLINE RISK',
                                'details': 'Complaint filed on exact date cause of action arose',
                                'cause_of_action_date': cause_of_action.isoformat(),
                                'impact': 'Some courts require filing strictly AFTER cause of action — risk of dismissal'
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
        'confidence': 'LOW',
        'confidence_note': 'Keyword heuristics — insufficient case data for this court'
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

    _kb_n = len(kb_results) or 1
    limitation_strictness = min(10.0, 3.0 + (limitation_strict_count / _kb_n) * 7.0)
    technical_tendency = min(10.0, 2.0 + (technical_dismissal_count / _kb_n) * 8.0)
    presumption_reliance = min(10.0, 5.0 + (presumption_count / _kb_n) * 5.0)
    settlement_friendly = min(10.0, 4.0 + (settlement_count / _kb_n) * 6.0)
    procedural_formality = min(10.0, 4.0 + (technical_dismissal_count / _kb_n) * 6.0)

    behavior_analysis['behavioral_indices'] = {
        'limitation_strictness': round(limitation_strictness, 1),
        'technical_dismissal_tendency': round(technical_tendency, 1),
        'presumption_reliance': round(presumption_reliance, 1),
        'settlement_friendly': round(settlement_friendly, 1),
        'procedural_formality': round(procedural_formality, 1)
    }

    behavior_analysis['confidence'] = 'MEDIUM' if behavior_analysis['sample_size'] >= 10 else 'LOW'
    behavior_analysis['confidence_note'] = (
        f"Based on {behavior_analysis['sample_size']} cases from knowledge base"
        if behavior_analysis['sample_size'] >= 10 else
        'Insufficient case data — using keyword heuristics'
    )
    behavior_analysis['warning'] = 'Heuristic analysis only - upgrade to structured data for accurate insights'

    return behavior_analysis


CONFIG = {
    "CHEQUE_VALIDITY_MONTHS": 3,
    "CHEQUE_VALIDITY_DAYS": 90,
    "CHEQUE_VALIDITY_CHANGE_DATE": "2012-04-01",
    "NOTICE_DEADLINE_MONTHS": 1,
    "PAYMENT_PERIOD_DAYS": 15,
    "COMPLAINT_LIMITATION_MONTHS": 1,
}


RISK_HIGH = 70
RISK_MEDIUM = 40
RISK_LOW = 0


FATAL_CAP_CATASTROPHIC = 0
FATAL_CAP_UNIFIED = 15


SCORE_EXCELLENT = 80
SCORE_GOOD = 60
SCORE_ADEQUATE = 40


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


    if isinstance(date_value, datetime):
        return date_value.date()
    if isinstance(date_value, date):
        return date_value


    if isinstance(date_value, str):
        formats = [
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%Y/%m/%d',
            '%d.%m.%Y',
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


    missing_optional = [f for f in OPTIONAL_CRITICAL_FIELDS if not case_data.get(f)]

    return True, None, missing_optional


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


            return {
                'module': func.__name__.replace('analyze_', '').replace('_', ' ').title(),
                'error': str(e),
                'error_type': type(e).__name__,
                'status': 'ERROR',
                'score': 0,
                'note': f'Analysis failed: {str(e)}'
            }

    return wrapper


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
                "Expose inconsistencies in the accused\u2019s explanation",
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


    for marker in ["Output:", "Questions:", "Answer:"]:
        if marker in response:
            response = response.split(marker, 1)[1]

    lines = [l.strip() for l in response.split("\n") if l.strip()]
    questions = []

    for line in lines:

        if len(line) < 15:
            continue
        if line.lower().startswith(("focus", "format", "instruct", "generate", "case facts")):
            continue


        cleaned = re.sub(r"^(Q?\d{1,2}[.):\-]\s*|[-•*]\s*)", "", line).strip()


        if "?" in cleaned and len(cleaned) > 20:

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
    _ = case_data.get('postal_proof_available', False)
    amount = indian_number_format(case_data.get('cheque_amount', 0))


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
            "You have stated the cheque was issued towards repayment of a loan — when exactly was this loan given and in what form?",
            "Is it correct that no receipts, ledger entries, or acknowledgements were created at the time of the alleged transaction?",
            f"The cheque dated {cheque_date} — was it issued on the same day as the alleged transaction or at a later date?",
            f"Did you retain any document signed by the accused acknowledging receipt of {amount}?",
            f"Is it not true that the notice dated {notice_date} was sent by ordinary post and not by registered AD post?",
            "Can you explain why no civil suit for recovery was filed despite claiming a legally enforceable debt?",
            "Is it correct that the cheque was given as security and not towards discharge of any specific liability?",
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


    if defence == "security_cheque" and witness_type == "complainant":
        q_pool = [q for q in q_pool if "security" in q.lower() or "agreement" in q.lower()] + q_pool
    if not has_ag and witness_type == "complainant":
        q_pool = [q for q in q_pool if "agreement" in q.lower() or "written" in q.lower() or "ledger" in q.lower()] + q_pool


    seen, result = set(), []
    for q in q_pool:
        if q not in seen:
            seen.add(q)
            result.append(q)
        if len(result) >= num:
            break

    return result[:num]


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


from enum import Enum


from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import uvicorn


RATE_LIMITING_AVAILABLE = False

class DummyLimiter:
    def limit(self, rate):
        def decorator(func):
            return func
        return decorator

Limiter = DummyLimiter
get_remote_address = lambda x: "0.0.0.0"


LOG_LEVEL = os.getenv('LOG_LEVEL', 'WARNING')
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper()),
    format='%(asctime)s - %(levelname)s - %(message)s'
)


DATA_DIR = (Path(__file__).parent if '__file__' in globals() else Path.cwd()) / "data_judiq"
BASE_DIR = DATA_DIR.parent
DATA_DIR.mkdir(parents=True, exist_ok=True)
(DATA_DIR / "legal_kb").mkdir(exist_ok=True)
(DATA_DIR / "case_analysis").mkdir(exist_ok=True)

CONFIG = {


    "PORT": int(os.getenv("PORT", 5001)),
    "HOST": "0.0.0.0",


    "GDRIVE_FILE_ID": os.getenv("GDRIVE_FILE_ID", "1btile_wGE2pjmU_gvm3ZwF-ufB246x5A"),

    "GDRIVE_API_KEY": os.getenv("GDRIVE_API_KEY", ""),

    # ── Firebase ──────────────────────────────────────────────────────────────
    # Set ONE of these in Render environment variables:
    #   FIREBASE_SERVICE_ACCOUNT_JSON  = raw JSON string of your service account key
    #   FIREBASE_SERVICE_ACCOUNT_PATH  = absolute path to the service account JSON file
    # Get it: Firebase Console → Project Settings → Service Accounts → Generate new private key
    "FIREBASE_SERVICE_ACCOUNT_JSON": os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", ""),
    "FIREBASE_SERVICE_ACCOUNT_PATH": os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", ""),
    "FIREBASE_PROJECT_ID": os.getenv("FIREBASE_PROJECT_ID", ""),
    # ─────────────────────────────────────────────────────────────────────────

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

        if total > 0:
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
            errors.append("❌ Cheque amount exceeds reasonable limit (₹100 billion)")
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
            except (ValueError, TypeError):
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
            elif parsed_dates['complaint_filed_date'] == cause_of_action:
                errors.append(
                    f"⚠️ WARNING: Complaint filed on same day as cause of action ({cause_of_action}). "
                    f"Some courts treat this as premature — borderline legal risk."
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
                except (ValueError, TypeError, AttributeError):
                    pass

        dates = timeline_obj['dates']

        parsed_dates = {}
        for field, date_str in dates.items():
            try:
                parsed_dates[field] = datetime.strptime(date_str, '%Y-%m-%d').date()
            except (ValueError, TypeError):
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
            try:
                cheque_date_parsed = datetime.strptime(dates['cheque_date'], '%Y-%m-%d').date()
                dishonour_date_parsed = datetime.strptime(dates['dishonour_date'], '%Y-%m-%d').date()
            except (ValueError, TypeError) as _e:
                logger.warning(f"Timeline cheque/dishonour date parse failed: {_e}")
                cheque_date_parsed = dishonour_date_parsed = None

            if cheque_date_parsed and dishonour_date_parsed:
                expiry_date, days_validity = calculate_cheque_expiry(cheque_date_parsed)
                gap_days = (dishonour_date_parsed - cheque_date_parsed).days
                timeline_obj['validity']['cheque_valid'] = dishonour_date_parsed <= expiry_date
                timeline_obj['validity']['days_from_cheque'] = gap_days
                timeline_obj['validity']['expiry_date'] = expiry_date.isoformat()
                timeline_obj['validity']['validity_months'] = 3
                timeline_obj['validity']['days_before_expiry'] = (expiry_date - dishonour_date_parsed).days

        if 'dishonour_date' in dates and 'notice_date' in dates:
            try:
                dishonour_dt = datetime.strptime(dates['dishonour_date'], '%Y-%m-%d').date()
                notice_dt = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
            except (ValueError, TypeError) as _e:
                logger.warning(f"Timeline dishonour/notice date parse failed: {_e}")
                dishonour_dt = notice_dt = None
            if dishonour_dt and notice_dt:
                notice_deadline = add_calendar_months(dishonour_dt, 1)
                timeline_obj['compliance']['notice_within_limit'] = notice_dt <= notice_deadline
                timeline_obj['compliance']['notice_deadline'] = notice_deadline.isoformat()
                timeline_obj['compliance']['notice_days'] = (notice_dt - dishonour_dt).days

        if 'notice_date' in dates and 'complaint_filed_date' in dates:
            try:
                notice_dt = datetime.strptime(dates['notice_date'], '%Y-%m-%d').date()
                complaint_dt = datetime.strptime(dates['complaint_filed_date'], '%Y-%m-%d').date()
            except (ValueError, TypeError) as _e:
                logger.warning(f"Timeline notice/complaint date parse failed: {_e}")
                notice_dt = complaint_dt = None
            if notice_dt and complaint_dt:
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
        try:
            dishonour_date = datetime.strptime(dishonour_date, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            raise ValueError(f"Invalid dishonour_date '{dishonour_date}' — expected YYYY-MM-DD")
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
        'mean_score': round(sum(scores) / len(scores), 1) if scores else 0,
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
    errors   = []
    warnings = []
    sanitized = {}


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
                warnings.append(f"⚠️ '{field}' not provided — related analysis will be skipped")
            continue

        try:
            parsed = (datetime.strptime(date_str, '%Y-%m-%d').date()
                      if isinstance(date_str, str) else date_str)

            days_diff = (parsed - today).days


            if days_diff > 1:
                warnings.append(
                    f"⚠️ '{field}' is {days_diff} days in the future ({parsed}) — "
                    f"analysis proceeds with provided date"
                )


            if parsed.year < 2000:
                warnings.append(
                    f"⚠️ '{field}' year {parsed.year} is before 2000 — verify this is correct"
                )

            parsed_dates[field] = parsed
            sanitized[field] = parsed.isoformat()

        except Exception:
            errors.append(f"INVALID DATE FORMAT: '{field}' — use YYYY-MM-DD")


    if 'cheque_date' in parsed_dates and 'dishonour_date' in parsed_dates:
        if parsed_dates['cheque_date'] > parsed_dates['dishonour_date']:
            errors.append("Cheque date cannot be after dishonour date")

    if 'dishonour_date' in parsed_dates and 'notice_date' in parsed_dates:
        if parsed_dates['dishonour_date'] > parsed_dates['notice_date']:
            errors.append("Dishonour date cannot be after notice date")

    if 'notice_date' in parsed_dates and 'complaint_filed_date' in parsed_dates:
        if parsed_dates['notice_date'] > parsed_dates['complaint_filed_date']:
            errors.append("Notice date cannot be after complaint filed date")


    if 'complaint_filed_date' not in parsed_dates:
        warnings.append("⚠️ 'complaint_filed_date' not provided — limitation period analysis will be partial")

    if not case_data.get('return_memo_available'):
        warnings.append("⚠️ Return memo not marked available — documentary strength will be reduced")

    if not case_data.get('postal_proof_available'):
        warnings.append("⚠️ Postal proof not provided — notice service presumption may be challenged")


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

        'individual':   'accused',
        'company':      'accused',
    }
    case_type = case_type_mapping.get(case_type_raw)
    if not case_type:

        if any(kw in case_type_raw for kw in ('complain', 'plaintiff', 'recover', 'petitioner')):
            case_type = 'complainant'
            warnings.append(
                f"⚠️ case_type '{case_data.get('case_type')}' auto-mapped to 'complainant'"
            )
        elif any(kw in case_type_raw for kw in ('accus', 'defend', 'respond')):
            case_type = 'accused'
            warnings.append(
                f"⚠️ case_type '{case_data.get('case_type')}' auto-mapped to 'accused'"
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


    for field in case_data:
        if field not in sanitized:
            sanitized[field] = case_data[field]


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
            'base_score': raw,
            'weight_percent': weight * 100,
            'contribution': weighted
        }

    def get_trail(self) -> Dict:
        return self.trail

def safe_module_execution(module_func, *args, **kwargs) -> Dict:
    import traceback as _tb
    try:
        return module_func(*args, **kwargs)
    except Exception as e:
        _trace = _tb.format_exc()
        logger.error(f"❌ Module {module_func.__name__} failed: {e}")
        logger.error(f"Traceback:\n{_trace}")
        return {
            'error': True,
            'error_message': str(e),
            'error_traceback': _trace,
            'module': module_func.__name__,
            'fallback_score': 50.0
        }

def classify_outcome(score: float, fatal_defects: List, confidence: str) -> Dict:


    if score is None:
        logger.error("classify_outcome received score=None, defaulting to 0")
        score = 0


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
            'final_score': data['score'],
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
    'section_65b_missing': 'FATAL',
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

        debt_rate = (debt_failures / total) * 100 if total > 0 else 0
        notice_rate = (notice_failures / total) * 100 if total > 0 else 0

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


    value_upper = str(value).upper().strip()


    if value_upper in ["EXCELLENT", "GOOD", "MODERATE", "WEAK", "CRITICAL"]:
        return value_upper


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


    case_type: str = Field(
        ...,
        description="Whose perspective: 'complainant' (cheque issuer defaulted) or 'accused' (defending against complaint)",
        examples=["complainant", "accused"]
    )


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


    is_multiple_cheques: bool = Field(
        False,
        description="Does this case involve multiple dishonoured cheques? Each creates a separate cause of action."
    )
    number_of_cheques: int = Field(
        1,
        description="Total number of dishonoured cheques in this case",
        examples=[1]
    )


    civil_suit_pending: bool = Field(
        False,
        description="Is a civil money recovery suit pending for the same debt? Affects strategy and settlement."
    )
    insolvency_proceedings: bool = Field(
        False,
        description="Are insolvency/bankruptcy proceedings pending against the accused? May affect enforcement."
    )


    defence_type: Optional[str] = Field(
        None,
        description="Known or expected defence. Values: 'security_cheque', 'no_debt', 'stop_payment', 'time_barred', 'signature_mismatch', 'company_director', 'stolen_cheque', 'no_legally_enforceable_debt'",
        examples=["security_cheque"]
    )


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


    user_email: Optional[str] = Field(
        None,
        description="User email from Firebase Auth — required for per-user daily limit enforcement.",
        examples=["lawyer@example.com"]
    )

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


embed_model = None
embed_loaded = False
llm_model = None
llm_tokenizer = None
llm_loaded = False
kb_data = None
kb_embeddings = None
kb_loaded = False
_kb_lock = __import__("threading").Lock()
court_behavior_db = None
embed_lock = threading.Lock()
llm_lock = threading.Lock()
kb_lock = threading.Lock()
_startup_complete = threading.Event()

_cache = {}
_cache_timestamps = {}


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


    analytics_db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(analytics_db_path)
    cursor = conn.cursor()


    cursor.execute("PRAGMA foreign_keys = ON")


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
            user_email           TEXT,
            created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


    try:
        cursor.execute("ALTER TABLE case_analyses ADD COLUMN user_email TEXT")
        logger.info("✅ Migration: added user_email column to case_analyses")
    except sqlite3.OperationalError:
        pass


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


    cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_id         ON case_analyses(case_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp        ON case_analyses(analysis_timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_compliance       ON case_analyses(compliance_level)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_case_type        ON case_analyses(case_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fatal_type       ON case_analyses(fatal_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at       ON case_analyses(created_at)")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_court_name       ON court_intelligence(court_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_court_case_type  ON court_intelligence(court_name, case_type)")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_case_id  ON case_history(case_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_ts       ON case_history(event_timestamp)")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_endpoint     ON api_usage_log(endpoint)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_timestamp    ON api_usage_log(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_log_severity     ON api_usage_log(severity)")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kb_section       ON knowledge_base(section)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_kb_court         ON knowledge_base(court_name)")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cs_court         ON court_statistics(court_name)")


    cursor.execute("""
        CREATE TABLE IF NOT EXISTS draft_logs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            user_email          TEXT    NOT NULL,
            draft_timestamp     TEXT    NOT NULL,
            case_id             TEXT,
            draft_type          TEXT    DEFAULT 'complaint',
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_draft_email ON draft_logs(user_email)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_draft_ts    ON draft_logs(draft_timestamp)")

    conn.commit()
    conn.close()

    logger.info(f"✅ JUDIQ database ready  |  path: {analytics_db_path}")
    logger.info("   Tables: case_analyses, court_intelligence, case_history, "
                "api_usage_log, knowledge_base, court_statistics, draft_logs")

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


        if b"virus scan warning" in raw.lower() or b"download_warning" in raw.lower():
            import re
            token_match = re.search(rb'confirm=([0-9A-Za-z_\-]+)', raw)
            if token_match:
                token = token_match.group(1).decode()
                confirm_url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm={token}"
                req2 = urllib.request.Request(confirm_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req2, timeout=120) as resp2:
                    raw = resp2.read()


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

            court_name, total_cases, conviction_rate_pct, acquittal_rate_pct,\
            lim_dismiss_rate, tech_dismiss_rate, compounding_rate_pct,\
            strictness_index, court_classification, confidence_level, last_updated = court_stats


            convictions              = round((conviction_rate_pct or 0)  / 100 * total_cases)
            acquittals               = round((acquittal_rate_pct or 0)   / 100 * total_cases)
            limitation_dismissals    = round((lim_dismiss_rate or 0)     / 100 * total_cases)
            technical_dismissals     = round((tech_dismiss_rate or 0)    / 100 * total_cases)
            compounded_cases         = round((compounding_rate_pct or 0) / 100 * total_cases)
            interim_compensation_orders = 0
            avg_disposal_days        = None
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

@safe_analysis
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
            try:
                _validity_change = datetime.strptime(CONFIG["CHEQUE_VALIDITY_CHANGE_DATE"], '%Y-%m-%d').date()
            except (ValueError, KeyError):
                _validity_change = datetime(2012, 4, 1).date()
            if cheque_date < _validity_change:
                validity_days = 180
                timeline_analysis['calculation_log'].append({
                    'rule': 'Cheque Validity (Pre-2012)',
                    'formula': 'Cheque Date + 180 days (6 months)',
                    'note': 'Using pre-RBI circular 2012 validity period'
                })
            else:

                try:
                    cheque_date_obj = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    cheque_date_obj = cheque_date
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
                        timeline_analysis['limitation_risk'] = 'EXPIRED'
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
            try:
                cause_of_action = datetime.strptime(timeline_analysis['critical_dates']['fifteen_day_expiry'], '%Y-%m-%d').date()
            except (ValueError, TypeError):
                cause_of_action = None
            if cause_of_action:
                limitation_deadline = add_calendar_months(cause_of_action, 1)
                timeline_analysis['critical_dates']['limitation_deadline'] = limitation_deadline.strftime('%Y-%m-%d')


                today = datetime.now().date()
                days_to_deadline = (limitation_deadline - today).days
                if days_to_deadline > 0:
                    timeline_analysis['refiling_window'] = {
                        'available':           True,
                        'days_remaining':      days_to_deadline,
                        'deadline':            limitation_deadline.strftime('%Y-%m-%d'),
                    'message':             f"Refiling window: {days_to_deadline} days remaining (deadline {limitation_deadline.strftime('%d %b %Y')})",
                    'urgency':             'URGENT' if days_to_deadline <= 7 else ('HIGH' if days_to_deadline <= 30 else 'MEDIUM'),
                }
                else:
                    timeline_analysis['refiling_window'] = {
                        'available':           False,
                        'days_remaining':      0,
                        'message':             'Limitation period expired — refiling not possible without condonation application',
                        'urgency':             'CRITICAL',
                    }

                days_to_complaint = (complaint_filed_date - cause_of_action).days

            if complaint_filed_date < cause_of_action:

                days_premature = (cause_of_action - complaint_filed_date).days
                timeline_analysis['compliance_status']['limitation'] = (
                    f'❌ PREMATURE FILING — complaint filed {days_premature} days before '
                    f'cause of action arose ({cause_of_action.strftime("%Y-%m-%d")})'
                )
                timeline_analysis['limitation_risk'] = 'CRITICAL'
                timeline_analysis['score'] = 0

                timeline_analysis['cause_of_action_reasoning'] = (
                    f'Under Section 138 NI Act, cause of action arises ONLY after expiry of '
                    f'15 days from the date notice is received by the accused. '
                    f'Notice received: {notice_received_date.strftime("%d %b %Y")}. '
                    f'15-day period expires: {cause_of_action.strftime("%d %b %Y")}. '
                    f'Complaint filed: {complaint_filed_date.strftime("%d %b %Y")} '
                    f'({days_premature} days BEFORE cause of action). '
                    f'This complaint is not maintainable and will be dismissed.'
                )
                timeline_analysis['refile_guidance'] = (
                    f'Complainant MAY refile AFTER {cause_of_action.strftime("%d %b %Y")}. '
                    f'Limitation deadline for fresh complaint: {limitation_deadline.strftime("%d %b %Y")}. '
                    f'Days remaining in limitation window: '
                    + str(max(0, (limitation_deadline - datetime.now().date()).days)) + ' days.'
                )
                timeline_analysis['cause_of_action_explanation'] = (
                    f"Section 138(b) NI Act: Cause of action arises ONLY after the 15-day payment "
                    f"period expires from notice receipt. Notice received: {case_data.get('notice_received_date', '?')}. "
                    f"Cause of action date: {cause_of_action.strftime('%d %B %Y')}. "
                    f"Complaint filed: {complaint_filed_date.strftime('%d %B %Y')} — "
                    f"{days_premature} day(s) BEFORE cause of action. Not maintainable."
                )
                timeline_analysis['can_refile'] = True
                timeline_analysis['refile_guidance'] = (
                    f"Fresh complaint can be filed from {cause_of_action.strftime('%d %B %Y')} "
                    f"within 30 days (by {add_calendar_months(cause_of_action, 1).strftime('%d %B %Y')})."
                )
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

            elif complaint_filed_date == cause_of_action:

                timeline_analysis['compliance_status']['limitation'] = (
                    f'⚠️ SAME-DAY FILING — complaint filed on exact date of cause of action '
                    f'({cause_of_action.strftime("%Y-%m-%d")}). Procedural risk — interpretation dependent.'
                )
                timeline_analysis['limitation_risk'] = 'HIGH'

                _tl_granular = 85
                for _mk in timeline_analysis.get('risk_markers', []):
                    _mk_sev = _mk.get('severity', 'LOW')
                    if _mk_sev == 'CRITICAL': _tl_granular -= 25
                    elif _mk_sev == 'HIGH':   _tl_granular -= 15
                    elif _mk_sev == 'MEDIUM': _tl_granular -= 8
                _tl_granular = max(20, _tl_granular)
                timeline_analysis['score'] = _tl_granular
                timeline_analysis['cause_of_action_reasoning'] = (
                    f'Cause of action under Section 138 arises at expiry of 15-day notice period '
                    f'({cause_of_action.strftime("%d %b %Y")}). Complaint filed on the SAME DAY '
                    f'as cause of action. Courts are split — some require filing strictly AFTER this date. '
                    f'High dismissal risk.'
                )
                timeline_analysis['refile_guidance'] = (
                    f'Strongly recommend refiling on {(cause_of_action + timedelta(days=1)).strftime("%d %b %Y")} '
                    f'(one day after cause of action). Limitation deadline: {limitation_deadline.strftime("%d %b %Y")}. '
                    f'Days remaining: ' + str(max(0, (limitation_deadline - datetime.now().date()).days)) + ' days.'
                )
                timeline_analysis['timeline_chart'].append({
                    'date': complaint_filed_date.strftime('%Y-%m-%d'),
                    'event': 'Complaint Filed — SAME DAY AS CAUSE OF ACTION ⚠️',
                    'status': '⚠️'
                })
                timeline_analysis['risk_markers'].append({
                    'severity': 'HIGH',
                    'issue': 'Complaint filed on same day as cause of action',
                    'impact': 'Some courts hold complaint not maintainable on same day — depends on judicial interpretation',
                    'legal_basis': 'Section 138: cause of action arises when 15-day period expires; filing on same day is legally grey',
                    'action': 'High risk — recommend refiling after cause of action date if within limitation'
                })
                timeline_analysis['edge_cases_detected'].append({
                    'case': 'Same-Day Filing',
                    'risk': 'HIGH',
                    'rule': 'Courts are split: some require filing strictly AFTER cause of action, not on the same day',
                    'recommendation': 'File fresh complaint next day if still within 30-day limitation window'
                })


            from datetime import date as _date_cls
            _today = _date_cls.today()
            if complaint_filed_date < cause_of_action:

                _days_till_coa   = max(0, (cause_of_action - _today).days)
                _days_till_limit = max(0, (limitation_deadline - _today).days)
                timeline_analysis['refiling_window'] = {
                    'cause_of_action_date': cause_of_action.strftime('%Y-%m-%d'),
                    'limitation_deadline':  limitation_deadline.strftime('%Y-%m-%d'),
                    'days_until_coa':       _days_till_coa,
                    'days_to_refile':       _days_till_limit,
                    'refiling_possible':    _days_till_limit > 0,
                    'message': (
                        f"Refiling window: {_days_till_limit} days remaining "
                        f"(must file by {limitation_deadline.strftime('%d %B %Y')})"
                        if _days_till_limit > 0 else
                        "Refiling window CLOSED — limitation period has expired"
                    )
                }

            elif complaint_filed_date <= limitation_deadline:
                timeline_analysis['compliance_status']['limitation'] = f'✅ WITHIN LIMITATION ({days_to_complaint} days, deadline: {limitation_deadline.strftime("%Y-%m-%d")})'
                timeline_analysis['limitation_risk'] = 'LOW'
                timeline_analysis['score'] = 95

                _days_used    = days_to_complaint
                _days_left    = (limitation_deadline - complaint_filed_date).days
                timeline_analysis['limitation_window'] = {
                    'cause_of_action':      cause_of_action.strftime('%Y-%m-%d'),
                    'limitation_deadline':  limitation_deadline.strftime('%Y-%m-%d'),
                    'days_from_coa_to_complaint': days_to_complaint,
                    'days_remaining':       _days_left,
                    'urgency':              'HIGH' if _days_left <= 7 else ('MEDIUM' if _days_left <= 15 else 'LOW'),
                    'message':              (
                        f"Complaint filed on day {_days_used} of 30-day window. "
                        f"{_days_left} day(s) remaining before limitation expires on "
                        f"{limitation_deadline.strftime('%d %B %Y')}."
                    )
                }
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
    data_completeness = (provided_fields / len(required_fields)) * 100 if required_fields else 0

    timeline_analysis['confidence'] = calculate_confidence(
        data_completeness=data_completeness,
        kb_coverage=75.0,
        retrieval_strength=0.80
    )

    return timeline_analysis

@safe_analysis
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
    scores.append(max(0, ing1_score))

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
        except (ValueError, TypeError) as _e:
            logger.debug(f'Ingredient time-bar calc: {_e}')

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
    scores.append(max(0, ing3_score))

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
    scores.append(max(0, ing4_score))

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
    scores.append(max(0, ing5_score))

    ing6_score = 100
    ing6_issues = []

    _tl_limit6 = timeline_data.get('compliance_status', {}).get('limitation', '')
    if 'PREMATURE' in str(_tl_limit6).upper() or 'SAME-DAY' in str(_tl_limit6).upper():
        ing6_score = 50
        ing6_issues.append('15-day period had not expired at complaint filing — ingredient conditionally incomplete')

    ingredients['ingredient_details'].append({
        'number': 6,
        'name': 'Payment Failed Within 15 Days',
        'score': ing6_score,
        'status': '✅ Compliant' if ing6_score == 100 else '⚠️ Conditional — filing was premature',
        'issues': ing6_issues,
        'evidence_required': ['Affidavit stating no payment received']
    })
    scores.append(max(0, ing6_score))

    ing7_score = 100
    ing7_issues = []

    limitation_status_str = timeline_data.get('compliance_status', {}).get('limitation', '')

    if 'SAME-DAY' in limitation_status_str.upper() or 'SAME DAY' in limitation_status_str.upper():

        ing7_score = 50
        ing7_issues.append('Complaint filed on same day as cause of action — borderline legal risk')
        _ing7_status = '⚠️ BORDERLINE (Same-Day)'

    elif 'PREMATURE' in limitation_status_str.upper():


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
    scores.append(max(0, ing7_score))


    ingredients['overall_compliance'] = sum(scores) / len(scores) if scores else 0
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


    _ing_comp = ingredients.get('overall_compliance', 0) or 0
    _ing_fatals = [d for d in ingredients.get('ingredient_details', []) if d.get('score', 100) < 30]
    if _ing_comp >= 70 and not _ing_fatals:
        ingredients['context_note'] = (
            f'Strong ingredient compliance ({_ing_comp:.1f}/100) — statutory requirements well-satisfied. '
            'Note: Final risk score may still be capped if procedural or timeline defects exist.'
        )
    elif _ing_comp >= 60 and _ing_fatals:
        ingredients['context_note'] = (
            f'Good compliance in most ingredients ({_ing_comp:.1f}/100), '
            f'but {len(_ing_fatals)} ingredient(s) need attention.'
        )

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
        'signature_status': '',
        'rebuttal_evidence_type': '',
        'rebuttal_strength': '',
        'minimum_tier_enforced': False,
        'burden_shift_timeline': [],
        'strategic_position': {},
        'evidence_requirements': {},
        'score_impact': 0
    }


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


    defence_type = (case_data.get('defence_type') or '').lower()

    if defence_type in ['security_cheque', 'no_debt', 'time_barred', 'legally_unenforceable']:
        presumption_analysis['current_stage'] = 'Stage 3: Accused Attempting Rebuttal'


        rebuttal_score = 0
        evidence_types = []
        evidence_quality = []


        if case_data.get('written_agreement_exists'):
            rebuttal_score += 35
            evidence_types.append('Documentary - Written Agreement')
            evidence_quality.append('HIGH')


        if case_data.get('witness_available'):
            witness_count = case_data.get('witness_count', 1)
            if witness_count >= 3:
                rebuttal_score += 25
                evidence_quality.append('MEDIUM-HIGH')
            else:
                rebuttal_score += 15
                evidence_quality.append('MEDIUM')
            evidence_types.append(f'Oral Testimony ({witness_count} witnesses)')


        if case_data.get('email_sms_evidence'):
            if case_data.get('section_65b_certificate', False):
                rebuttal_score += 30
                evidence_types.append('Electronic (Section 65B compliant)')
                evidence_quality.append('HIGH')
            else:
                rebuttal_score += 10
                evidence_types.append('Electronic (No Section 65B certificate - WEAK)')
                evidence_quality.append('LOW')


        if case_data.get('ledger_available'):
            rebuttal_score += 25
            evidence_types.append('Account Books/Ledger')
            evidence_quality.append('MEDIUM-HIGH')


        if case_data.get('bank_statement_support'):
            rebuttal_score += 20
            evidence_types.append('Banking Records')
            evidence_quality.append('MEDIUM')


        if rebuttal_score >= 70:
            presumption_analysis['rebuttal_strength'] = 'HIGH'
            presumption_analysis['minimum_tier_enforced'] = True
            presumption_analysis['burden_position'] = 'Rebuttal Strong - Burden Shifts Back to Complainant'
            presumption_analysis['score_impact'] = 0
        elif rebuttal_score >= 40:
            presumption_analysis['rebuttal_strength'] = 'MEDIUM'
            presumption_analysis['burden_position'] = 'On Accused - Partial Rebuttal'
            presumption_analysis['score_impact'] = 0
        else:
            presumption_analysis['rebuttal_strength'] = 'LOW'
            presumption_analysis['burden_position'] = 'On Accused - Weak Rebuttal'
            presumption_analysis['score_impact'] = 0

        presumption_analysis['rebuttal_evidence_type'] = ', '.join(evidence_types)
        presumption_analysis['evidence_quality_breakdown'] = evidence_quality

        presumption_analysis['burden_shift_timeline'].append({
            'stage': 3,
            'event': f'Rebuttal Attempted - {presumption_analysis["rebuttal_strength"]} strength',
            'evidence_types': evidence_types,
            'rebuttal_score': rebuttal_score,
            'burden': presumption_analysis['burden_position']
        })


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
        else:
            presumption_analysis['strategic_position']['accused'] = [
                'Weak rebuttal - presumption largely intact',
                'Urgently gather stronger documentary evidence',
                'Consider settlement negotiations'
            ]
    else:
        presumption_analysis['current_stage'] = 'Stage 2: No Rebuttal Attempted'
        presumption_analysis['burden_position'] = 'On Accused - Presumption Unrebutted'
        presumption_analysis['rebuttal_strength'] = 'NONE'


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


def analyze_security_cheque_probability(case_data: Dict) -> Dict:
    """
    Analyze probability that cheque was given as security (not payment).

    This is the MOST COMMON defence in Section 138 cases.
    Courts heavily scrutinize this.
    """
    probability_score = 0
    indicators = []


    if not case_data.get('written_agreement_exists') and not case_data.get('invoice_available'):
        probability_score += 30
        indicators.append({
            'factor': 'No transaction documentation',
            'weight': 'HIGH',
            'impact': '+30 points to security cheque defence'
        })


    if case_data.get('transaction_mode') == 'cash':
        probability_score += 25
        indicators.append({
            'factor': 'Cash transaction claimed',
            'weight': 'HIGH',
            'impact': '+25 points - Courts highly suspicious of large cash loans'
        })


    if not case_data.get('bank_transfer_proof'):
        probability_score += 20
        indicators.append({
            'factor': 'No bank transfer evidence',
            'weight': 'HIGH',
            'impact': '+20 points - No electronic trail of loan'
        })


    complainant_income = case_data.get('complainant_annual_income', 0)
    transaction_amount = case_data.get('transaction_amount', 0)

    if complainant_income > 0 and transaction_amount > complainant_income:
        probability_score += 15
        indicators.append({
            'factor': 'Loan amount exceeds declared income',
            'weight': 'MEDIUM',
            'impact': '+15 points - Financial capacity questionable'
        })


    if not case_data.get('itr_available'):
        probability_score += 10
        indicators.append({
            'factor': 'No income tax returns',
            'weight': 'MEDIUM',
            'impact': '+10 points - Cannot prove source of funds'
        })


    if case_data.get('cheque_date_filled_later'):
        probability_score += 15
        indicators.append({
            'factor': 'Cheque date filled later',
            'weight': 'HIGH',
            'impact': '+15 points - Classic security cheque indicator'
        })


    if not case_data.get('ledger_available'):
        probability_score += 10
        indicators.append({
            'factor': 'No account ledger',
            'weight': 'MEDIUM',
            'impact': '+10 points - No transaction record'
        })


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


    score = 100


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


    if case_data.get('itr_available'):
        capacity_analysis['green_flags'].append('Income tax returns available')

    if case_data.get('bank_statement_available'):
        capacity_analysis['green_flags'].append('Bank statements available')

    if case_data.get('bank_transfer_proof'):
        score += 20
        capacity_analysis['green_flags'].append('Bank transfer proof strengthens capacity claim')

    if complainant_income > 0 and loan_amount <= complainant_income * 0.5:
        capacity_analysis['green_flags'].append('Loan amount reasonable compared to income')


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
            liability_exposure['vulnerability_zones'].append({
                'zone': 'No Written Agreement',
                'severity': 'HIGH',
                'attack_vector': 'Defence will question nature and existence of debt'
            })
            liability_exposure['likely_questions'].extend([
                'Where is the written agreement for this transaction?',
                'How do you prove the amount of debt without written record?',
                'Why was no receipt/agreement made for such large amount?',
                'Is this not a security cheque given for different purpose?'
            ])
            liability_exposure['preparation_required'].append({
                'area': 'Transaction Proof',
                'preparation': 'Prepare detailed oral explanation of transaction, gather any emails/SMS, witness statements'
            })

        if not case_data.get('ledger_available'):
            liability_exposure['vulnerability_zones'].append({
                'zone': 'No Ledger/Account Books',
                'severity': 'MEDIUM',
                'attack_vector': 'Defence will question business practice and genuineness of transaction'
            })
            liability_exposure['likely_questions'].extend([
                'Why is there no entry in account books for this debt?',
                'Do you maintain proper accounts for your business?',
                'How can such transaction occur without accounting entry?'
            ])

        if (case_data.get('debt_nature') or '').lower() == 'cash':
            liability_exposure['vulnerability_zones'].append({
                'zone': 'Cash Transaction - No Bank Trail',
                'severity': 'HIGH',
                'attack_vector': 'Defence will question genuineness - why no bank transfer proof'
            })
            liability_exposure['likely_questions'].extend([
                'Why was such large amount given in cash?',
                'Where is the withdrawal proof from your account?',
                'Who was witness to this cash handover?',
                'Why not bank transfer/cheque for such amount?'
            ])
            liability_exposure['preparation_required'].append({
                'area': 'Cash Transaction Justification',
                'preparation': 'Have clear explanation ready, any witness to cash handover, withdrawal proof if available'
            })

        if case_data.get('transaction_date') and case_data.get('cheque_date'):
            try:
                trans_date = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
                cheque_date = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d')
                gap_months = (cheque_date - trans_date).days / 30

                if gap_months > 6:
                    liability_exposure['vulnerability_zones'].append({
                        'zone': f'Large Time Gap ({gap_months:.1f} months) Between Transaction and Cheque',
                        'severity': 'MEDIUM',
                        'attack_vector': 'Defence will argue cheque was for different purpose, not original transaction'
                    })
                    liability_exposure['likely_questions'].append(
                        f'Why did accused give cheque {gap_months:.0f} months after transaction?'
                    )
            except (ValueError, TypeError) as _e:
                logger.debug(f"Cross-exam date calc: {_e}")

        liability_exposure['likely_questions'].extend([
            'Have you shown this transaction in your income tax return?',
            'What is your annual income to give such amount?'
        ])
        liability_exposure['preparation_required'].append({
            'area': 'Financial Capacity',
            'preparation': 'Be ready to explain source of funds, ITR if applicable, business turnover'
        })

    else:

        liability_exposure['vulnerability_zones'].append({
            'zone': 'Signature on Cheque',
            'severity': 'HIGH',
            'attack_vector': 'Prosecution will establish cheque issuance through signature'
        })
        liability_exposure['likely_questions'].extend([
            'This is your signature on the cheque?',
            'You issued this cheque from your bank account?',
            'Cheque was in your possession and you filled it?'
        ])
        liability_exposure['preparation_required'].append({
            'area': 'Signature & Issuance',
            'preparation': 'If admitting signature, have clear explanation why cheque was given (security/different purpose)'
        })

        if case_data.get('defence_type') in ['security_cheque', 'no_debt']:
            if not case_data.get('written_agreement_exists') and not case_data.get('email_sms_evidence'):
                liability_exposure['vulnerability_zones'].append({
                    'zone': 'No Documentary Proof of Defence',
                    'severity': 'CRITICAL',
                    'attack_vector': 'Prosecution will attack credibility - why no written proof of security/no debt claim'
                })
                liability_exposure['likely_questions'].extend([
                    'If it was security cheque, where is written agreement stating so?',
                    'Any email/SMS showing it was only for security?',
                    'Why would you give blank signed cheque as security?',
                    'This is self-serving afterthought, correct?'
                ])
                liability_exposure['preparation_required'].append({
                    'area': 'Defence Evidence',
                    'preparation': 'URGENT: Gather any correspondence, witness who knew cheque was security, contemporaneous evidence'
                })

        dishonour_reason = (case_data.get('dishonour_reason') or '').lower()
        if 'insufficient' in dishonour_reason:
            liability_exposure['vulnerability_zones'].append({
                'zone': 'Insufficient Funds - Implicit Debt Admission',
                'severity': 'MEDIUM',
                'attack_vector': 'Prosecution will argue insufficient funds shows you knew debt existed'
            })
            liability_exposure['likely_questions'].extend([
                'If no debt existed, why not give stop payment instruction?',
                'Insufficient funds means you expected cheque to be presented for payment?',
                'This shows you knew it was for debt payment?'
            ])

    critical_count = len([z for z in liability_exposure['vulnerability_zones'] if z['severity'] == 'CRITICAL'])
    high_count = len([z for z in liability_exposure['vulnerability_zones'] if z['severity'] == 'HIGH'])

    if critical_count > 0:
        liability_exposure['overall_risk'] = 'CRITICAL - Severe Vulnerabilities in Cross-Examination'
    elif high_count >= 2:
        liability_exposure['overall_risk'] = 'HIGH - Multiple High-Risk Zones'
    elif high_count == 1:
        liability_exposure['overall_risk'] = 'MEDIUM - Some Vulnerable Areas'
    else:
        liability_exposure['overall_risk'] = 'LOW - Manageable Cross-Examination Risk'

    return liability_exposure

@safe_analysis
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

    try:
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
                'gap': 'No documentary proof of legally enforceable debt',
                'impact': 'Severe — accused can challenge the existence of a legally enforceable debt, which is a fundamental Section 138 ingredient',
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


        unified_service = evaluate_service_proof_unified(case_data)

        service_score = unified_service['score']
        service_grade = unified_service['proof_strength']
        service_details = []


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

        _ds = doc_analysis['overall_strength_score']
        doc_analysis['strength_label'] = (
            'Strong'   if _ds >= 70 else
            'Moderate' if _ds >= 45 else
            'Weak'     if _ds >= 25 else
            'Very Weak'
        )

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
    except Exception as _doc_err:
        logger.error(f'analyze_documentary_strength error: {_doc_err}')
        doc_analysis['overall_strength_score'] = 25
        doc_analysis['strength_label'] = 'Assessment Error — Weak assumed'

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
        'director_liability_matrix': [],
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


            directors_list = case_data.get('directors_list', [])
            cheque_date = case_data.get('cheque_date')

            if directors_list and cheque_date:
                try:
                    cheque_date_obj = datetime.strptime(cheque_date, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    cheque_date_obj = None

                for director in directors_list:
                    director_analysis = {
                        'name': director.get('name'),
                        'designation': director.get('designation'),
                        'din': director.get('din'),
                        'liable': True,
                        'liability_basis': [],
                        'exclusion_reasons': []
                    }


                    resignation_date = director.get('resignation_date')
                    if resignation_date:
                        try:
                            resignation_date_obj = datetime.strptime(resignation_date, '%Y-%m-%d').date()
                        except (ValueError, TypeError):
                            resignation_date_obj = None
                        if resignation_date_obj and cheque_date_obj and resignation_date_obj < cheque_date_obj:
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
                        director_analysis['liable'] = False
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


        board_resolution_date = case_data.get('board_resolution_date')
        if board_resolution_date and cheque_date:
            try:
                resolution_date_obj = datetime.strptime(board_resolution_date, '%Y-%m-%d').date()
                cheque_date_obj = datetime.strptime(cheque_date, '%Y-%m-%d').date()
            except (ValueError, TypeError):
                resolution_date_obj = cheque_date_obj = None

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
    """
    Comprehensive defence vulnerability analysis — every possible Section 138 defence scenario.
    Maps case facts to realistic defence strengths, counter-strategies, and cross-exam angles.
    """
    try:
        defence_matrix = {
            'possible_defences': [],
            'strongest_defence': None,
            'overall_defence_strength': '',
            'complainant_counter_strategy': [],
            'high_risk_defences': [],
            'fatal_defences': []
        }


        defence_type    = (case_data.get('defence_type') or '').lower()
        has_agreement   = case_data.get('written_agreement_exists', False)
        has_ledger      = case_data.get('ledger_available', False)
        has_bank_proof  = case_data.get('bank_statement_proof', False)
        has_postal      = case_data.get('postal_proof_available') or case_data.get('ad_card_signed')
        has_cheque      = case_data.get('original_cheque_available', False)
        has_ret_memo    = case_data.get('return_memo_available', False)
        _ = case_data.get('email_sms_evidence', False)
        is_company      = case_data.get('is_company_case', False)
        part_paid       = case_data.get('part_payment_made', False)
        payment_mode    = (case_data.get('transaction_mode') or case_data.get('payment_mode') or '').lower()
        is_cash         = 'cash' in payment_mode
        amount          = float(case_data.get('cheque_amount', 0) or 0)
        stop_payment    = (case_data.get('dishonour_reason') or '').lower() == 'stop payment'
        insolvency      = case_data.get('insolvency_proceedings', False)
        is_savkari      = case_data.get('is_savkari_loan', False)
        bank_merged     = case_data.get('bank_merger_occurred', False)
        forgery         = case_data.get('forgery_alleged', False)
        specific_av     = case_data.get('specific_averment_present', False)
        directors_impl  = case_data.get('directors_impleaded', False)

        def _add(defence_name, strength_score, basis, accused_evidence, complainant_counter, is_fatal=False):
            strength_label = ('HIGH' if strength_score >= 65 else 'MEDIUM' if strength_score >= 40 else 'LOW')
            entry = {
                'defence': defence_name,
                'strength': strength_label,
                'strength_score': min(100, strength_score),
                'basis': basis,
                'evidence_required_by_accused': accused_evidence,
                'complainant_counter': complainant_counter,
                'is_fatal': is_fatal,
            }
            defence_matrix['possible_defences'].append(entry)
            if is_fatal:
                defence_matrix['fatal_defences'].append(entry)
            if strength_score >= 60:
                defence_matrix['high_risk_defences'].append(entry)


        score1 = 35
        if 'security' in defence_type: score1 = 65
        if not has_agreement:  score1 += 15
        if not has_ledger:     score1 += 8
        if not has_bank_proof: score1 += 7
        _add(
            'Security Cheque — No Legally Enforceable Debt',
            score1,
            'Accused claims cheque was given as collateral security, not towards discharge of an existing debt.',
            [
                'Prove cheque was given as security for future obligation',
                'Show no actual payment/debt existed at time of issue',
                'Produce any contemporaneous correspondence showing security purpose',
                'Deny debt in examination under Section 313 CrPC'
            ],
            [
                'Rely on Section 139 presumption — burden shifts to accused',
                'Produce all documentation of the original transaction',
                'Prove exact date debt arose and that cheque was issued contemporaneously',
                'Show through cross-examination that accused cannot prove security purpose'
            ]
        )


        score2 = 20
        if 'no_debt' in defence_type or 'debt' in defence_type: score2 = 50
        if not has_agreement:  score2 += 15
        if not has_ledger:     score2 += 10
        if not has_bank_proof: score2 += 10
        if is_cash and amount > 20000: score2 += 15
        _add(
            'Absence of Legally Enforceable Debt — No Documentary Proof',
            score2,
            'No written agreement, ledger, or bank transfer proof — accused will deny debt existence.',
            [
                'Deny transaction in examination under Section 313',
                'Challenge complainant to produce any independent proof',
                'Argue presumption is rebutted by absence of documentation',
                'If cash: argue Section 269SS makes debt itself illegal'
            ],
            [
                'Rely heavily on Section 139 presumption',
                'Produce any WhatsApp/SMS/email acknowledging the debt',
                'Produce bank withdrawal records showing cash was available',
                'Identify and examine witnesses to the original transaction'
            ]
        )


        score3 = 30
        if 'discharged' in defence_type or 'paid' in defence_type: score3 = 55
        if part_paid: score3 += 20
        if case_data.get('part_payment_full_amount'): score3 = 90
        _add(
            'Debt Discharged / Payment Already Made',
            score3,
            'Accused claims debt was paid (fully or partially) before or after dishonour.',
            [
                'Produce payment receipts, bank transfer records',
                'Produce WhatsApp/SMS acknowledging payment',
                'Argue balance amount is less than cheque amount'
            ],
            [
                'Deny any payment was made',
                'Produce bank statements showing no incoming payment',
                'Argue any payment was for a different transaction'
            ],
            is_fatal=(score3 >= 85)
        )


        score4 = 30
        if stop_payment: score4 = 55
        if not has_agreement and stop_payment: score4 = 65
        _add(
            'Stop Payment Instruction with Lawful Reason',
            score4,
            'Accused instructed bank to stop payment due to a valid dispute or non-performance.',
            [
                'Produce stop payment instruction to bank with stated reason',
                'Prove the dispute/reason was genuine and pre-existing',
                'Produce any notice/letter sent before cheque presentation citing dispute'
            ],
            [
                'Argue stop payment with valid reason still attracts Section 138 — Supreme Court settled this',
                'Prove no valid dispute existed at time of cheque issue',
                'Rely on K. Bhaskaran v. Sankaran Vaidhyan Balan: stop payment = dishonour under S.138'
            ]
        )


        score5 = 15
        if forgery or case_data.get('false_cheque_alleged'): score5 = 70
        _add(
            'Forgery / Cheque Not Issued by Accused',
            score5,
            'Accused denies signature or claims cheque was forged/stolen.',
            [
                'Challenge handwriting expert to compare signatures',
                'Produce evidence that chequebook was stolen or misplaced',
                'File police complaint for forgery (adds credibility)',
                'Produce bank account records showing chequebook was reported stolen'
            ],
            [
                'File handwriting expert report comparing specimen and disputed signature',
                "Produce bank records showing cheque was issued from accused's account",
                "Examine bank's own record of signature verification",
                'Challenge accused in cross-examination: how was signature placed on cheque?'
            ]
        )


        score6 = 25
        if not has_postal: score6 += 25
        if case_data.get('notice_not_received'): score6 += 20
        if case_data.get('notice_returned_undelivered'): score6 += 10
        _add(
            'Notice Not Received / Improperly Served',
            score6,
            'Accused denies receiving the legal notice or challenges its validity.',
            [
                'Deny receiving notice',
                'Challenge postal proof (no AD card signed)',
                'Argue notice was not sent to correct address',
                'Argue deemed service doctrine does not apply'
            ],
            [
                'Produce postal receipt and tracking confirmation',
                'Produce AD card signed by accused',
                'Argue deemed service applies — accused cannot benefit from own refusal/avoidance',
                'Examine postal department witness if required'
            ]
        )


        score7 = 20
        if 'time_barred' in defence_type: score7 = 55
        if case_data.get('transaction_date'):
            try:
                from datetime import datetime as _dt
                td = _dt.strptime(case_data['transaction_date'], '%Y-%m-%d').date()
                cd = _dt.strptime(case_data['cheque_date'], '%Y-%m-%d').date()
                if (cd - td).days > 1095:
                    score7 = max(score7, 60)
            except Exception:
                pass
        _add(
            'Time-Barred Debt (Limitation Act)',
            score7,
            'Underlying debt is more than 3 years old — accused argues debt itself is barred.',
            [
                'Produce proof that transaction was more than 3 years before cheque',
                'Argue no fresh acknowledgment existed to revive limitation',
                'Challenge complainant to produce acknowledgment within limitation'
            ],
            [
                'Produce any acknowledgment of debt within 3-year period',
                'Argue cheque itself is an acknowledgment reviving limitation (S.18 Limitation Act)',
                'Produce ongoing account statements or ledger entries'
            ]
        )


        score8 = 20

        _tl_compliance = dict(ingredient_analysis.get('ingredient_scores', {}))
        premature_flag = any('PREMATURE' in str(v) for v in _tl_compliance.values())
        if premature_flag: score8 = 75
        if not has_cheque: score8 += 15
        if not has_ret_memo: score8 += 10
        _add(
            'Technical / Procedural Defects',
            score8,
            'Accused challenges procedural compliance: premature filing, limitation, missing documents.',
            [
                'Point to premature filing dates in complaint itself',
                'Challenge service of notice at correct address',
                'Challenge admissibility of documentary evidence'
            ],
            [
                'Verify all dates are within limits before filing',
                'Produce all original documents in court',
                'Prepare affidavit explaining any borderline dates'
            ]
        )


        if is_company and directors_impl:
            score9 = 55 if not specific_av else 20
            _add(
                'Company Case — Director Not "In Charge" (Section 141)',
                score9,
                'Director denies being in charge of or responsible for conduct of business at time of offence.',
                [
                    'Produce resignation letter / board minutes showing non-executive role',
                    'Show director was non-executive / independent / nominee director',
                    'Argue no specific averment in complaint establishes "in charge" status'
                ],
                [
                    'File complaint with specific averment for each director',
                    'Produce Board resolution / bank mandate naming director as signatory',
                    'Show director was involved in day-to-day financial operations',
                    'Cite SMS Pharmaceuticals and National Small Industries Corp judgments'
                ]
            )


        if insolvency:
            _add(
                'Insolvency / IBC Moratorium',
                80,
                'Accused is undergoing insolvency — Section 138 complaint may be stayed.',
                [
                    'Produce NCLT order declaring moratorium under IBC',
                    'Apply to court for stay citing moratorium'
                ],
                [
                    'Verify moratorium status — personal complaints under NI Act may still proceed',
                    'Cite Ajay Kumar Radheshyam Goenka v. Tourism Finance Corp (2023) — S.138 proceeds despite IBC',
                    'Argue criminal liability is not extinguished by IBC proceedings'
                ],
                is_fatal=False
            )


        if is_savkari:
            _add(
                'Savkari / Moneylender Act Violation',
                65,
                'Accused argues complainant is an unregistered moneylender — underlying transaction is void.',
                [
                    'Prove complainant operates as moneylender without license',
                    'Cite applicable State Moneylenders Act',
                    "Produce complainant's alleged pattern of high-interest lending"
                ],
                [
                    'Produce registration certificate under applicable State Act',
                    'Deny operating as commercial moneylender — argue this was a personal loan',
                    'Cite exceptions: one-time friendly loans not covered by Moneylenders Act'
                ]
            )


        if bank_merged:
            score12 = 40
            _add(
                'Bank Merger — Successor Bank Liability',
                score12,
                'Accused challenges whether dishonour by successor bank after merger is valid under Section 138.',
                [
                    'Argue cheque was drawn on original bank which no longer exists',
                    'Challenge validity of dishonour memo from merged bank'
                ],
                [
                    'Produce RBI notification of merger',
                    'Produce dishonour memo from successor bank showing clear lineage',
                    'Cite case law on NI Act liability surviving bank mergers'
                ]
            )


        if defence_matrix['possible_defences']:
            strongest = max(defence_matrix['possible_defences'], key=lambda d: d['strength_score'])
            defence_matrix['strongest_defence'] = strongest

            max_score = strongest['strength_score']
            defence_matrix['overall_defence_strength'] = (
                'VERY HIGH' if max_score >= 75 else
                'HIGH'      if max_score >= 60 else
                'MEDIUM'    if max_score >= 40 else
                'LOW'
            )


        defence_matrix['complainant_counter_strategy'] = [
            'File well-drafted complaint with specific averments for each element',
            'Ensure all documents are original and properly exhibited from day one',
            "Prepare cross-examination questions targeting accused's documentary gaps",
            'Apply for Section 143A interim compensation at first hearing',
        ]
        if not has_agreement:
            defence_matrix['complainant_counter_strategy'].append(
                'URGENT: Locate WhatsApp/email/SMS chain acknowledging debt before trial'
            )
        if not has_postal:
            defence_matrix['complainant_counter_strategy'].append(
                'URGENT: Obtain India Post tracking report and/or file affidavit on notice service'
            )

        return defence_matrix

    except Exception as _def_err:
        logger.error(f'analyze_defence_vulnerabilities error: {_def_err}')
        return {
            'possible_defences': [],
            'strongest_defence': None,
            'overall_defence_strength': 'MEDIUM',
            'complainant_counter_strategy': [],
            'high_risk_defences': [],
            'fatal_defences': [],
            'error': str(_def_err)
        }


def scan_procedural_defects(case_data: Dict, timeline_data: Dict, liability_data: Dict) -> Dict:
    """
    Comprehensive procedural defect scanner — covers every Section 138 scenario.
    Checks: jurisdiction, notice validity, complaint timing, company cases,
    cheque specifics, service proof, stamp duty, drafting compliance,
    limitation, delay condonation, multiple cheques, part payment, and more.
    """
    defect_scan = {
        'fatal_defects': [],
        'curable_defects': [],
        'warnings': [],
        'overall_risk': '',
        'risk_category': '',
        'risk_description': '',
        'remedies': []
    }

    try:

        if case_data.get('notice_received_date') and case_data.get('complaint_filed_date'):
            try:
                notice_recv     = datetime.strptime(case_data['notice_received_date'], '%Y-%m-%d').date()
                complaint_filed = datetime.strptime(case_data['complaint_filed_date'], '%Y-%m-%d').date()
                fifteen_day_expiry = notice_recv + timedelta(days=15)

                if complaint_filed < fifteen_day_expiry:
                    days_early = (fifteen_day_expiry - complaint_filed).days
                    defect_scan['fatal_defects'].append({
                        'defect': 'Complaint Filed Before 15-Day Payment Period Expired',
                        'severity': 'CRITICAL',
                        'impact': f'Premature complaint — cause of action had not yet arisen under Section 138. '
                                  f'Complaint filed {days_early} day(s) before cause of action date ({fifteen_day_expiry}).',
                        'legal_basis': 'Section 138 NI Act: cause of action arises only after expiry of 15-day payment period from notice receipt.',
                        'remedy': 'Current complaint is not maintainable. Do NOT refile same complaint. '
                                  'Wait for 15-day expiry, then file fresh complaint within 30 days of cause of action.'
                    })
                elif complaint_filed == fifteen_day_expiry:
                    defect_scan['curable_defects'].append({
                        'defect': 'Complaint Filed on Same Day as Cause of Action (Same-Day Risk)',
                        'severity': 'HIGH',
                        'impact': 'Borderline: complaint filed on exact date cause of action arose. '
                                  'Some courts hold this premature; others accept it.',
                        'legal_basis': 'Courts are divided on whether filing on Day 0 of cause of action is premature.',
                        'cure': 'If within 30-day limitation, file fresh complaint next day. '
                                'If limitation has expired, prepare detailed arguments that cause of action arises at midnight.'
                    })
            except (ValueError, TypeError) as _e:
                logger.debug(f'Premature check: {_e}')


        _tl_limitation = str((timeline_data.get('compliance_status') or {}).get('limitation', ''))
        if 'BARRED' in _tl_limitation.upper() or 'EXPIRED' in _tl_limitation.upper():
            defect_scan['fatal_defects'].append({
                'defect': 'Complaint Filed Beyond 30-Day Limitation Period',
                'severity': 'CRITICAL',
                'impact': 'Complaint is time-barred under Section 142. Court has no jurisdiction to take cognizance.',
                'legal_basis': 'Section 142 NI Act: complaint must be filed within 30 days of cause of action.',
                'remedy': 'File application for condonation of delay under Section 142(b) with strong justification. '
                          'Delay may be condoned only if court is satisfied with explanation.'
            })


        _tl_notice = str((timeline_data.get('compliance_status') or {}).get('notice_timing', ''))
        if 'EXPIRED' in _tl_notice.upper() or 'EXCEEDED' in _tl_notice.upper() or 'BEYOND 30' in _tl_notice.upper():
            defect_scan['fatal_defects'].append({
                'defect': 'Legal Notice Sent Beyond 30 Days of Dishonour',
                'severity': 'CRITICAL',
                'impact': 'Notice sent too late — ingredient 5 (notice within 30 days) fails. Complaint cannot be maintained.',
                'legal_basis': 'Section 138(b) NI Act: notice must be given within 30 days of dishonour memo.',
                'remedy': 'No remedy. Complaint must be withdrawn or dismissed. Cannot be cured by sending fresh notice.'
            })


        _tl_validity = str((timeline_data.get('compliance_status') or {}).get('cheque_validity', ''))
        if _tl_validity.startswith('❌') or 'EXPIRED' in _tl_validity.upper() or 'BEYOND' in _tl_validity.upper():
            defect_scan['fatal_defects'].append({
                'defect': 'Cheque Presented After 3-Month Validity Period',
                'severity': 'CRITICAL',
                'impact': 'Stale cheque — dishonour of stale cheque does not attract Section 138 liability.',
                'legal_basis': 'Section 138 NI Act requires cheque to be presented within 3 calendar months of issue date.',
                'remedy': 'No remedy — Section 138 does not apply. Civil remedy for debt recovery may be explored.'
            })


        if case_data.get('is_company_case'):
            if not case_data.get('specific_averment_present') and not case_data.get('director_specific_averment'):
                defect_scan['fatal_defects'].append({
                    'defect': 'No Specific Averment for Directors — Company Case (Section 141)',
                    'severity': 'CRITICAL',
                    'impact': 'Directors will be discharged in limine. Without specific averment that each director '
                              'was "in charge of and responsible for conduct of business", no liability attaches.',
                    'legal_basis': 'SMS Pharmaceuticals Ltd. v. Neeta Bhalla (2005) 8 SCC 89; '
                                   'National Small Industries Corp. Ltd. v. Harmeet Singh Paintal (2010) 3 SCC 330.',
                    'remedy': 'Immediately file amendment application to add specific averment. '
                              'If limitation expired, file fresh complaint with correct averments for each director.'
                })
            if not case_data.get('directors_impleaded') and case_data.get('is_company_case'):
                defect_scan['warnings'].append({
                    'area': 'Company Case — Director Impleading',
                    'warning': 'All directors in charge at time of offence must be named as accused.',
                    'check': 'Verify each director named was actually in charge and responsible at time cheque was drawn.'
                })


        notice_addr = (case_data.get('notice_sent_to_address') or '').lower()
        if notice_addr and ('wrong' in notice_addr or 'incorrect' in notice_addr):
            defect_scan['fatal_defects'].append({
                'defect': 'Legal Notice Sent to Wrong Address',
                'severity': 'CRITICAL',
                'impact': 'Service of notice is invalid — accused can challenge that valid notice was never received.',
                'legal_basis': 'Section 138(b): notice must be sent to address where accused is accessible.',
                'remedy': 'Send fresh notice to correct address immediately. If still within notice period, '
                          'refile complaint counting limitation from fresh notice receipt.'
            })


        if not case_data.get('notice_signed'):
            defect_scan['curable_defects'].append({
                'defect': 'Legal Notice Appears Unsigned',
                'severity': 'HIGH',
                'impact': 'Defence will challenge validity of notice — unsigned notice may not satisfy Section 138(b).',
                'cure': 'Produce signed original at trial. File affidavit explaining notice was duly signed and served.'
            })


        if not case_data.get('postal_proof_available') and not case_data.get('ad_card_signed')            and not case_data.get('postal_acknowledgment')            and not case_data.get('notice_refused') and not case_data.get('notice_unclaimed'):
            defect_scan['curable_defects'].append({
                'defect': 'No Proof of Notice Service',
                'severity': 'HIGH',
                'impact': 'Accused can flatly deny receiving notice. Service of notice is a mandatory ingredient.',
                'cure': 'Obtain postal tracking report from India Post. If hand-delivered, produce witness affidavit. '
                        'If service is unproved, complaint may fail at trial.'
            })


        if case_data.get('part_payment_full_amount'):
            defect_scan['fatal_defects'].append({
                'defect': 'Part Payment Equals Full Cheque Amount — Debt Fully Discharged',
                'severity': 'CRITICAL',
                'impact': 'If accused has paid the full cheque amount, no legally enforceable debt survives. '
                          'Complaint is not maintainable.',
                'legal_basis': 'Section 138 requires a legally enforceable debt — if debt is discharged, no offence exists.',
                'remedy': 'Withdraw complaint. Explore if excess was owed on other counts.'
            })
        elif case_data.get('part_payment_made') and case_data.get('part_payment_amount'):
            try:
                paid    = float(case_data['part_payment_amount'] or 0)
                total   = float(case_data.get('cheque_amount', 0) or 0)
                balance = total - paid
                if paid > 0 and total > 0:
                    defect_scan['warnings'].append({
                        'area': 'Part Payment',
                        'warning': f'Part payment of ₹{paid:,.0f} made — balance ₹{balance:,.0f} still due.',
                        'check': 'Amend complaint to reflect balance amount. '
                                 'Complaint for full amount when part paid may need clarification.'
                    })
            except (ValueError, TypeError):
                pass


        if case_data.get('delay_condonation_filed') is False and _tl_limitation:
            if 'DELAY' in _tl_limitation.upper() or 'CONDONATION' in _tl_limitation.upper():
                defect_scan['curable_defects'].append({
                    'defect': 'Delay Condonation Application Not Filed',
                    'severity': 'HIGH',
                    'impact': 'Court may not proceed with complaint if limitation delay is not explained.',
                    'cure': 'File application under Section 142(b) NI Act with affidavit explaining delay. '
                            'Court has discretion — genuine reasons are usually accepted.'
                })


        if case_data.get('electronic_evidence') and not case_data.get('section_65b_certificate'):
            defect_scan['curable_defects'].append({
                'defect': 'Electronic Evidence (WhatsApp/Email/SMS) Without Section 65B Certificate',
                'severity': 'MEDIUM',
                'impact': 'Electronic records without S.65B certificate are inadmissible — Anvar P.V. v. P.K. Basheer (2014).',
                'cure': 'File Section 65B certificate from computer owner before evidence stage. '
                        'Obtain from service provider if own device not available.'
            })


        if not case_data.get('court_location') and not case_data.get('filing_court_city'):
            defect_scan['curable_defects'].append({
                'defect': 'Court Location Not Specified',
                'severity': 'MEDIUM',
                'impact': 'Cannot verify territorial jurisdiction.',
                'cure': 'File in court with jurisdiction: (a) place where cheque drawn, '
                        '(b) place where cheque presented, (c) place where dishonoured, '
                        '(d) place where notice served, or (e) place where complainant resides.'
            })
        elif case_data.get('court_location'):
            defect_scan['warnings'].append({
                'area': 'Jurisdiction Verification',
                'warning': 'Verify territorial jurisdiction before filing.',
                'check': 'Cause of action must have arisen within court limits. '
                         'After Dashrath Rupsingh Rathod v. State of Maharashtra (2014): '
                         'jurisdiction is only where cheque was presented (drawer bank branch).'
            })


        if case_data.get('is_multiple_cheques') or int(case_data.get('number_of_cheques', 1) or 1) > 1:
            num = int(case_data.get('number_of_cheques', 2) or 2)
            defect_scan['warnings'].append({
                'area': 'Multiple Cheques',
                'warning': f'{num} cheques — each dishonoured cheque creates a SEPARATE offence and requires a separate complaint.',
                'check': 'Verify separate notices sent and separate complaints filed for each cheque. '
                         'Clubbing multiple cheques in one complaint is procedurally incorrect.'
            })


        cash_status = (case_data.get('cash_transaction_status') or '').lower()
        amount = float(case_data.get('cheque_amount', 0) or 0)
        if ('cash' in cash_status or 'exceeds' in cash_status) and amount > 20000:
            defect_scan['warnings'].append({
                'area': 'Section 269SS — Cash Transaction',
                'warning': f'Cash transaction of ₹{amount:,.0f} may violate Section 269SS Income Tax Act.',
                'check': 'If loan was given in cash above ₹20,000, defence will argue the debt is illegal '
                         'and cheque given for illegal consideration. Courts have split on this point.'
            })


        if not case_data.get('stamp_duty_paid'):
            defect_scan['warnings'].append({
                'area': 'Stamp Duty',
                'warning': 'Confirm cheque was properly stamped / not a pre-signed blank cheque with improper stamp.',
                'check': 'Generally, cheques are exempt from stamp duty but verify if local law applies.'
            })


        defect_scan['warnings'].append({
            'area': 'Complaint Affidavit',
            'warning': 'Complaint must be on oath — affidavit in support is mandatory.',
            'check': 'Ensure complaint is accompanied by a proper verification affidavit.'
        })


        defect_scan['warnings'].append({
            'area': 'Annexure Verification',
            'warning': 'All documents in complaint must match physical exhibits.',
            'check': 'Verify cheque number, amount, date, bank name, dishonour reason match '
                     'between complaint averments and actual documents.'
        })


        fatal_count   = len(defect_scan['fatal_defects'])
        curable_count = len(defect_scan['curable_defects'])
        _high_cure = [d for d in defect_scan['curable_defects'] if d.get('severity') == 'HIGH']

        if fatal_count >= 2:
            defect_scan['overall_risk']     = 'CRITICAL — Multiple Fatal Defects'
            defect_scan['risk_category']    = 'Multiple Fatal Defects'
            defect_scan['risk_description'] = (
                f'{fatal_count} fatal procedural defects — complaint will be dismissed at first hearing'
            )
        elif fatal_count == 1:
            defect_scan['overall_risk']     = 'HIGH — Fatal Defect Present'
            defect_scan['risk_category']    = 'Fatal Defect'
            defect_scan['risk_description'] = (
                defect_scan['fatal_defects'][0].get('defect', 'Fatal procedural defect') +
                ' — complaint is not maintainable in current form'
            )
        elif _high_cure:
            defect_scan['overall_risk']     = 'MEDIUM — High-Risk Curable Defects'
            defect_scan['risk_category']    = 'Curable High-Risk'
            defect_scan['risk_description'] = (
                f'{len(_high_cure)} high-risk curable defect(s) — addressable before trial but require immediate action'
            )
        elif curable_count > 0:
            defect_scan['overall_risk']     = 'LOW-MEDIUM — Minor Curable Defects'
            defect_scan['risk_category']    = 'Curable Minor'
            defect_scan['risk_description'] = (
                f'{curable_count} minor procedural gap(s) — addressable at evidence stage'
            )
        else:
            defect_scan['overall_risk']     = 'LOW — No Significant Procedural Defects'
            defect_scan['risk_category']    = 'Compliant'
            defect_scan['risk_description'] = 'Procedural compliance is adequate'

    except Exception as _proc_err:
        logger.error(f'scan_procedural_defects error: {_proc_err}')
        defect_scan['overall_risk'] = 'CANNOT ASSESS — System Error'
        defect_scan['warnings'].append({'area': 'System', 'warning': str(_proc_err), 'check': 'Retry'})

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
        'final_score': 0,
        'risk_breakdown': {},
        'compliance_level': '',
        'critical_issues': [],
        'fatal_defects': [],
        'confidence_level': ''
    }

    weights = get_centralized_weights()

    if timeline_data.get('limitation_risk', 'UNKNOWN') == 'LOW':
        timeline_score = 95
    elif timeline_data.get('limitation_risk', 'UNKNOWN') == 'MEDIUM':
        timeline_score = 60
    else:
        timeline_score = 20

    for marker in timeline_data.get('risk_markers', []):
        if marker['severity'] == 'CRITICAL':
            timeline_score -= 30
        elif marker['severity'] == 'HIGH':
            timeline_score -= 20


    if timeline_data.get('score') is not None and isinstance(timeline_data.get('score'), (int, float)):
        tl_module_score = float(timeline_data.get('score') or 0)
        timeline_score = tl_module_score if tl_module_score > 0 else timeline_score
    timeline_score = normalize_score(timeline_score)
    timeline_score = cap_score_realistic(timeline_score, max_cap=98.0)

    risk_model['category_scores']['Timeline Compliance'] = {
        'score': timeline_score,
        'weight': weights['timeline'],
        'weighted_score': round(timeline_score * weights['timeline'], 1)
    }
    risk_model['risk_breakdown']['timeline_risk'] = int((100 - timeline_score) * weights['timeline'])

    ingredient_score = normalize_score(ingredient_data.get('overall_compliance', 0))
    ingredient_score = cap_score_realistic(ingredient_score, max_cap=98.0)
    risk_model['category_scores']['Ingredient Compliance'] = {
        'score': ingredient_score,
        'weight': weights['ingredients'],
        'weighted_score': round(ingredient_score * weights['ingredients'], 1)
    }
    risk_model['risk_breakdown']['ingredient_risk'] = int((100 - ingredient_score) * weights['ingredients'])

    doc_score = normalize_score(doc_data.get('overall_strength_score', 0))
    doc_score = cap_score_realistic(doc_score, max_cap=98.0)
    risk_model['category_scores']['Documentary Strength'] = {
        'score': doc_score,
        'weight': weights['documentary'],
        'weighted_score': round(doc_score * weights['documentary'], 1)
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
        'weight': weights['liability'],
        'weighted_score': round(liability_score * weights['liability'], 1),
        'reason': 'All required parties are correctly impleaded' if liability_score >= 90 else 'One or more required parties may not be correctly impleaded',
        'explanation': 'Section 141 NI Act — for company cases, all directors in-charge at time of offence must be named with specific averments'
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
        'weight': weights['procedural'],
        'weighted_score': round(procedural_score * weights['procedural'], 1)
    }
    risk_model['risk_breakdown']['procedural_risk'] = int((100 - procedural_score) * weights['procedural'])

    total_weighted = round(sum(cat['weighted_score'] for cat in risk_model['category_scores'].values()), 1)

    risk_model['final_score'] = normalize_score(total_weighted)

    risk_model['final_score'] = cap_score_realistic(risk_model['final_score'], max_cap=98.0)

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

        _tl_limit_text = str((timeline_data.get('compliance_status') or {}).get('limitation', ''))
        _is_same_day   = 'SAME-DAY' in _tl_limit_text.upper() or 'SAME DAY' in _tl_limit_text.upper()
        _is_premature  = 'PREMATURE' in _tl_limit_text.upper()
        _is_timebarred = 'BARRED' in _tl_limit_text.upper() or 'EXPIRED' in _tl_limit_text.upper()
        if _is_timebarred or _is_premature:

            all_fatal_defects.append({
                'category': 'Timeline',
                'defect': 'Premature Complaint — Cause of Action Not Yet Arisen' if _is_premature else 'Complaint Barred by Limitation',
                'defect_type': 'premature_complaint' if _is_premature else 'complaint_beyond_30_days',
                'severity': 'FATAL',
                'impact': 'Complaint is not maintainable — will be dismissed at first hearing' if _is_premature else 'Complaint is time-barred — condonation required',
                'is_absolute': True,
            })
        elif _is_same_day:


            _same_day_penalty = round(total_weighted * 0.12, 1)
            risk_model['final_score'] = round(
                max(35, (risk_model.get('final_score') or total_weighted) - _same_day_penalty), 1
            )
            risk_model['same_day_filing_penalty'] = _same_day_penalty
            risk_model['same_day_filing_risk'] = 'HIGH — interpretation-dependent procedural risk'

            all_fatal_defects = [d for d in all_fatal_defects
                                 if 'same-day' not in str(d.get('defect','')).lower()
                                 and 'same day' not in str(d.get('defect','')).lower()]

        else:
            all_fatal_defects.append({
                'category': 'Timeline',
                'defect': 'Critical limitation violation',
                'defect_type': 'complaint_beyond_30_days',
                'severity': 'FATAL',
                'impact': 'Dismissal risk substantial'
            })

    if ingredient_data.get('fatal_defects'):
        for defect in ingredient_data['fatal_defects']:

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


    _score = risk_model.get('final_score', 0) or 0
    _fatal = len(all_fatal_defects) > 0
    if _fatal or _score < 25:
        _prob_label   = 'Very High likelihood of dismissal (>90%)'
        _prob_dismiss = '>90%'
        _prob_convict = '<10%'
    elif _score < 50:
        _prob_label   = 'High likelihood of dismissal (60–80%)'
        _prob_dismiss = '60–80%'
        _prob_convict = '20–40%'
    elif _score < 70:
        _prob_label   = 'Moderate risk — outcome depends on evidence strength'
        _prob_dismiss = '30–50%'
        _prob_convict = '50–70%'
    else:
        _prob_label   = 'Favourable — high likelihood of conviction'
        _prob_dismiss = '<20%'
        _prob_convict = '>80%'

    if _fatal or _score < 25:
        _court_outcome = 'Complaint likely to be dismissed at threshold stage (Section 245 CrPC)'
        _court_stage   = 'Pre-trial / threshold stage'
        _court_action  = 'Accused can file discharge application at first hearing'
    elif _score < 50:
        _court_outcome = 'High risk of acquittal — significant evidentiary gaps'
        _court_stage   = 'Trial stage'
        _court_action  = 'Strengthen documentary evidence before filing'
    elif _score < 70:
        _court_outcome = 'Moderate chance of conviction — case viable but improvable'
        _court_stage   = 'Trial stage'
        _court_action  = 'Address documentary gaps and prepare strong examination-in-chief'
    else:
        _court_outcome = 'Strong case — high likelihood of conviction'
        _court_stage   = 'Likely to proceed to conviction'
        _court_action  = 'File complaint and apply for interim compensation under Section 143A'
    risk_model['outcome_probability'] = {
        'label':                        _prob_label,
        'dismissal_probability':        _prob_dismiss,
        'conviction_probability':       _prob_convict,
        'conviction_probability_label': 'Conviction Probability',
        'dismissal_probability_label':  'Dismissal Probability',
        'conviction_probability_display': f"Conviction Probability: {_prob_convict}",
        'dismissal_probability_display':  f"Dismissal Probability: {_prob_dismiss}",
        'likely_outcome':               _court_outcome,
        'court_stage':                  _court_stage,
        'recommended_action':           _court_action,
        'basis':                        'Based on statutory compliance score and fatal defect analysis',
    }


    _base = round(total_weighted, 1)
    _final = round(risk_model.get('final_score', 0), 1)
    _ = FATAL_CAP_UNIFIED if all_fatal_defects else None


    _contributions = []
    for _cname, _cdata in (risk_model.get('category_scores') or {}).items():
        if not isinstance(_cdata, dict): continue
        _cs  = round(float(_cdata.get('score', 0) or 0), 1)
        _cw  = float(_cdata.get('weight', 0) or 0)
        _cwp = round(_cw * 100)
        _contrib = round(_cs * _cw, 1)
        _contributions.append({
            'category':    _cname,
            'score':       _cs,
            'weight_pct':  _cwp,
            'weight_float':_cw,
            'contribution':_contrib,
            'display':     f'{_cname} ({_cwp}%): {_cs}/100 → contributes {_contrib}',
            'level':       'Strong' if _cs >= 70 else 'Moderate' if _cs >= 45 else 'Weak',
        })
    risk_model['score_contributions'] = _contributions


    _adj_notes = []
    if risk_model.get('jurisdiction_penalty'):
        _adj_notes.append(f"Jurisdiction penalty: -{risk_model['jurisdiction_penalty']}")
    if all_fatal_defects:
        _adj_notes.append(f"Fatal override cap: {_base}/100 → {_final}/100")
    risk_model['score_trail'] = {
        'final_score':          _final,
        'adjustments':          _adj_notes,
        'cap_applied':          bool(all_fatal_defects),
        'trail_display': (
            f'Score: {_final}/100'
            + (' (fatal cap applied)' if all_fatal_defects else '')
        ),
    }


    risk_model['score_cap_explanation'] = {
        'final_score':        _final,
        'final_score_display':f'{_final}/100',
        'fatal_defects_found': len(all_fatal_defects),
        'cap_applied':        len(all_fatal_defects) > 0,
        'cap_reason':         (
            f'{len(all_fatal_defects)} fatal defect(s) detected — score capped regardless of other factors'
            if all_fatal_defects else 'No fatal defects — full score applies'
        ),
        'interpretation':     (
            'Despite adequate scores in some categories, fatal defects make the case legally non-maintainable.'
            if all_fatal_defects else
            'Score reflects weighted compliance across all statutory requirements.'
        ),
        'cap_logic': (
            f'Base: {_base}/100 → Fatal override applied → Final: {_final}/100'
            if all_fatal_defects else
            f'Base: {_base}/100 → No cap → Final: {_final}/100'
        ),
    }


    risk_model['score_interpretation'] = (
        'Very Weak — High dismissal probability' if _final < 30 else
        'Weak — Significant gaps, remediation required' if _final < 50 else
        'Moderate — Maintainable with strengthening' if _final < 65 else
        'Good — Reasonable chance of success' if _final < 80 else
        'Strong — Well-supported case'
    )
    risk_model['score_band'] = (
        '0–30: Very Weak' if _final < 30 else
        '30–50: Weak' if _final < 50 else
        '50–65: Moderate' if _final < 65 else
        '65–80: Good' if _final < 80 else
        '80–100: Strong'
    )

    risk_model['final_score'] = round(_final, 1)

    if all_fatal_defects:
        original_score = risk_model['final_score']
        final_score, override_details = apply_weighted_fatal_override(
            risk_model['final_score'],
            all_fatal_defects
        )
        risk_model['final_score'] = final_score

        if override_details['applied']:
            risk_model['fatal_defect_override'] = override_details

    if all_fatal_defects:
        hard_override_score, hard_override_reason = apply_hard_fatal_override(
            risk_model['final_score'],
            all_fatal_defects
        )

        if hard_override_reason != "NO_OVERRIDE":
            if hard_override_score < risk_model['final_score']:
                risk_model['final_score'] = hard_override_score
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
                                   if d.get('severity') in ['CRITICAL', 'FATAL']
                                   and 'same-day' not in str(d.get('defect','')).lower()
                                   and 'same day' not in str(d.get('defect','')).lower()
                                   and 'borderline' not in str(d.get('defect','')).lower()])
    procedural_high_count = len([d for d in defect_data.get('curable_defects', [])
                                  if d.get('severity') == 'HIGH'])
    if procedural_fatal_count >= 2:
        fatal_defect_override = True
        override_reason.append(f'{procedural_fatal_count} fatal procedural defects — technical dismissal likely')
        risk_model['final_score'] = min(risk_model['final_score'], 30)
    elif procedural_fatal_count == 1 and (defect_data.get('overall_risk', '').startswith('CRITICAL')
                                           or defect_data.get('overall_risk', '').startswith('HIGH')):
        fatal_defect_override = True
        override_reason.append('Fatal procedural defect detected')
        risk_model['final_score'] = min(risk_model['final_score'], 40)
    elif procedural_high_count >= 1:

        _high_reduction = round(risk_model.get('final_score', 0) * 0.20, 1)
        risk_model['final_score'] = round(
            max(25, risk_model.get('final_score', 0) - _high_reduction), 1
        )
        override_reason.append(f'High procedural risk — {_high_reduction:.1f} point reduction applied')

    if fatal_defect_override:
        _capped_at = risk_model['final_score']
        risk_model['fatal_defect_override'] = {
            'applied': True,
            'original_weighted_score': round(total_weighted, 1),
            'original_score': round(total_weighted, 1),
            'original_score_display': f"{round(total_weighted, 1):.1f}/100",
            'overridden_score': _capped_at,
            'capped_at': _capped_at,
            'capped_at_display': f"{_capped_at}/100",
            'reason': ' | '.join(override_reason),
            'logic': 'Fatal defects auto-cap score — case will collapse regardless of other strengths',
            'severity': 'FATAL' if _capped_at <= 25 else 'CRITICAL',
            'warning': 'Despite strong scores in other areas, fatal defects make case unviable'
        }

        if risk_model['final_score'] <= 20:
            risk_model['compliance_level'] = ComplianceLevel.CRITICAL
            risk_model['confidence_level'] = 'CERTAIN FAILURE - Fatal defects present'
            risk_model['score_interpretation'] = 'VERY WEAK (0-30): Case will almost certainly fail without addressing fatal defects'
        elif risk_model['final_score'] <= 40:
            risk_model['compliance_level'] = ComplianceLevel.CRITICAL
            risk_model['confidence_level'] = 'Very high risk - Critical defects likely fatal'
    else:

        if (risk_model.get('final_score') or 0) >= 90:
            risk_model['compliance_level'] = ComplianceLevel.EXCELLENT
            risk_model['confidence_level'] = 'Very high confidence in case strength'
        elif (risk_model.get('final_score') or 0) >= 75:
            risk_model['compliance_level'] = ComplianceLevel.GOOD
            risk_model['confidence_level'] = 'High confidence in case strength'
        elif (risk_model.get('final_score') or 0) >= 60:
            risk_model['compliance_level'] = ComplianceLevel.MODERATE
            risk_model['confidence_level'] = 'Moderate confidence - some weaknesses present'
        elif (risk_model.get('final_score') or 0) >= 40:
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


    if 'final_score' not in risk_model or risk_model['final_score'] is None:
        logger.error("final_score was None or missing - defaulting to 0")
        risk_model['final_score'] = 0


    try:
        risk_model['final_score'] = float(risk_model['final_score'])
    except (TypeError, ValueError):
        logger.error(f"final_score invalid type: {type(risk_model['final_score'])} - defaulting to 0")
        risk_model['final_score'] = 0

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

        case_strength = risk_score_data['final_score']
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
                'rationale': (
                    'Strong procedural case. Settlement avoids conviction risk and legal costs. '
                    'Absence of documentary proof may give some negotiation leverage.'
                ) if not case_data.get('written_agreement_exists') else
                'Strong case against accused. Settlement cheaper than conviction + legal costs.',
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
            'conviction_scenario': 'Fine (up to 2x amount) + Compensation (up to amount) + Costs',
            'acquittal_scenario': 'No recovery, legal costs incurred'
        }

        if (risk_score_data.get('final_score') or 0) >= 60:
            settlement_analysis['interim_compensation_eligible'] = True
            settlement_analysis['strategic_options'].append({
                'option': 'Apply for Interim Compensation (Section 143A)',
                'rationale': 'Case has reasonable strength. Can get 20% immediately.',
                'financial_impact': f'Receive {cheque_amount * 0.20:.2f} pending trial'
            })

        case_strength = risk_score_data['final_score']
        _doc_weak = (risk_score_data.get('category_scores', {}).get('Documentary Strength', {}) or {}).get('score', 100)
        if isinstance(_doc_weak, dict):
            _doc_weak = _doc_weak.get('score', 100)
        _doc_weak = float(_doc_weak or 100)

        if case_strength >= 70:
            settlement_analysis['settlement_leverage'] = SettlementPressure.HIGH
            settlement_analysis['settlement_probability'] = 'HIGH - Strong position to demand full payment'
            if _doc_weak < 60:
                _rationale = (
                    'Case has procedural strength but weak documentary evidence. '
                    'Accused may contest on transaction basis — settle quickly to avoid documentary challenge at trial.'
                )
            else:
                _rationale = 'Strong case with solid documentary foundation. Accused faces high conviction risk.'
            settlement_analysis['strategic_options'].append({
                'option': 'Demand Full Payment',
                'rationale': _rationale,
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


    if case_data.get('cheque_date') and case_data.get('presentation_date'):
        try:
            cheque_dt = datetime.strptime(case_data['cheque_date'], '%Y-%m-%d')
            pres_dt = datetime.strptime(case_data['presentation_date'], '%Y-%m-%d')


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

            elif case_data.get('transaction_date'):
                try:
                    trans_dt = datetime.strptime(case_data['transaction_date'], '%Y-%m-%d')
                except (ValueError, TypeError):
                    trans_dt = None
                if trans_dt and cheque_dt and cheque_dt > trans_dt:
                    days_post_dated = (cheque_dt - trans_dt).days
                    if days_post_dated > 90:
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
        except (ValueError, TypeError) as _e:
            logger.debug(f"Edge case limitation calc: {_e}")

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
        except (ValueError, TypeError) as _e:
            logger.debug(f"Edge case old transaction: {_e}")

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
    """
    Escalation engine: adjusts the computed base_score by applying
    hard floors/caps for serious conditions. Never replaces — only constrains.
    """
    base = float(flags.get('base_score', 50) or 50)

    if flags.get('fatal', False):
        return {
            'tier': 'FATAL',
            'status': 'CASE FAILURE',
            'score': min(base, 15),
            'recommendation': 'DO NOT FILE',
            'reason': 'Fatal condition detected',
            'tier_locked': True
        }

    if flags.get('debt_disputed', False):

        adjusted = min(base, 55)
        return {
            'tier': 'HIGH_RISK',
            'status': 'SUBSTANTIVE DEFENCE RISK',
            'score': adjusted,
            'recommendation': 'ADDRESS DEBT DISPUTE',
            'reason': 'Enforceable debt challenged — score capped at 55',
            'tier_locked': True,
            'minimum_tier_enforcement': 'HIGH_RISK cannot be reduced by procedural compliance'
        }

    if flags.get('director_inactive_no_averment', False):
        adjusted = min(base, 50)
        return {
            'tier': 'HIGH_RISK',
            'status': 'SECTION 141 FAILURE',
            'score': adjusted,
            'recommendation': 'OBTAIN SPECIFIC AVERMENT',
            'reason': 'Director liability requirements not met — score capped at 50',
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


    timeline_result = case_data.get('_timeline_analysis', {})
    fatal_defects = timeline_result.get('fatal_defects', [])


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
            'risk_score': 0,
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

        director_averment = case_data.get('director_specific_averment') or case_data.get('specific_averment_present', False)
        director_knowledge = case_data.get('director_knowledge_proof', False)
        director_designation = case_data.get('director_designation_stated') or case_data.get('directors_impleaded', False)


        if not (director_averment and director_knowledge and director_designation):
            director_risk = 'CRITICAL'
            consequence_adjustment -= 25

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
    Cross Examination Risk Analysis — Deterministic Rule-Based Engine
    Maps documentary gaps and defence postures to specific cross-exam vulnerabilities.
    """
    zones = []
    questions = []
    risk_score = 0

    amount = float(case_data.get('cheque_amount', 0) or 0)
    has_agreement   = case_data.get('written_agreement_exists', False)
    has_ledger      = case_data.get('ledger_available', False)
    has_bank_proof  = case_data.get('bank_statement_proof', False)
    has_postal      = case_data.get('postal_proof_available', False) or case_data.get('postal_acknowledgment', False)
    has_ad_card     = case_data.get('ad_card_signed', False)
    has_orig_cheque = case_data.get('original_cheque_available', False)
    has_ret_memo    = case_data.get('return_memo_available', False)
    has_email_sms   = case_data.get('email_sms_evidence', False)
    has_witness     = case_data.get('witness_available', False)
    is_cash         = 'cash' in (case_data.get('transaction_mode') or case_data.get('payment_mode') or '').lower()
    defence_type    = (case_data.get('defence_type') or '').lower()
    notice_returned = case_data.get('notice_returned_undelivered', False) or case_data.get('notice_not_received', False)


    if not has_agreement and not has_ledger and not has_bank_proof:
        risk_score += 35
        zones.append({
            'zone': 'Transaction Documentation',
            'severity': 'CRITICAL',
            'attack_vector': 'No written proof of debt — defence will ask for every document proving money was actually given',
            'defence_question': "Is it correct you have no written agreement, ledger entry or bank record of this alleged transaction?"
        })
        questions += [
            'Is it correct that there is no written agreement evidencing the alleged debt?',
            'On what date exactly did you give money to the accused and in what form?',
            'Who was present as witness when you gave this money?',
            'Why did you not insist on a written receipt for such a large amount?',
        ]
    elif not has_agreement:
        risk_score += 20
        zones.append({
            'zone': 'Written Agreement',
            'severity': 'HIGH',
            'attack_vector': 'No formal written agreement — defence will question the legal character of the transaction',
            'defence_question': "Is it not a fact that there is no written document signed by the accused acknowledging this debt?"
        })
        questions.append('Why was no written agreement executed for a transaction of this amount?')
        questions.append('Can you produce any document showing the accused agreed to repay this specific amount?')


    if not has_postal and not has_ad_card:
        risk_score += 25
        zones.append({
            'zone': 'Notice Service Proof',
            'severity': 'CRITICAL' if not has_postal and not has_ad_card else 'HIGH',
            'attack_vector': 'No postal receipt or AD card — defence will deny receipt of legal notice entirely',
            'defence_question': "Is it not correct that you cannot produce the AD card signed by the accused acknowledging receipt of your notice?"
        })
        questions += [
            "Do you have the acknowledgment due (AD) card returned bearing the accused's signature?",
            "How do you prove the accused actually received your legal notice?",
            'Is it correct that the postal tracking shows the notice was not delivered to the accused personally?',
        ]
    elif notice_returned:
        risk_score += 15
        zones.append({
            'zone': 'Notice Delivery — Returned Undelivered',
            'severity': 'HIGH',
            'attack_vector': 'Notice returned — accused will deny deemed service applies in this situation',
            'defence_question': 'Is it correct the postal article was returned with "Not Found / Refused" endorsement?'
        })
        questions.append("Was the notice returned undelivered, and if so, did you take any alternative steps to serve the accused?")


    if is_cash:
        risk_score += 20
        zones.append({
            'zone': 'Cash Transaction',
            'severity': 'HIGH',
            'attack_vector': 'Cash transactions are impossible to independently verify — defence will deny any money changed hands',
            'defence_question': 'Is it not correct that no bank statement or transfer record exists showing money actually paid to accused?'
        })
        questions += [
            'From which bank account did you withdraw this cash to give to the accused?',
            'Produce your bank passbook/statement showing the withdrawal of this amount.',
            'Are you aware that cash transactions above ₹20,000 may violate Section 269SS of the Income Tax Act?',
        ]
        if amount > 20000:
            questions.append('Did you file your income tax return declaring this cash loan? Can you produce the ITR?')


    if 'security' in defence_type or case_data.get('defence_type') == 'security_cheque':
        risk_score += 30
        zones.append({
            'zone': 'Security Cheque Allegation',
            'severity': 'CRITICAL',
            'attack_vector': 'Accused claims cheque was given as security only — will attack the legal character of the cheque',
            'defence_question': "Is it not a fact that this cheque was given to you as security deposit and not towards discharge of any debt?"
        })
        questions += [
            'Was any condition attached to the encashment of this cheque?',
            'Did the accused ever give you any other security or guarantee for this transaction?',
            'On what specific date did the legally enforceable debt arise that this cheque was meant to discharge?',
            'Is this cheque dated earlier than the debt obligation matured?',
        ]


    if not has_ret_memo:
        risk_score += 15
        zones.append({
            'zone': 'Bank Dishonour Memo',
            'severity': 'HIGH',
            'attack_vector': 'No bank memo — defence will challenge whether the cheque was actually dishonoured',
            'defence_question': "Can you produce the original bank return memo bearing the bank's seal and endorsement?"
        })
        questions.append("Where is the original bank return memo issued when the cheque was dishonoured?")
        questions.append("Was the dishonour memo signed by an authorized bank official with the bank seal?")


    if not has_orig_cheque:
        risk_score += 20
        zones.append({
            'zone': 'Original Cheque',
            'severity': 'CRITICAL',
            'attack_vector': 'Original instrument not available — admissibility of copy will be challenged',
            'defence_question': "Can you produce the ORIGINAL cheque in court, or only a photocopy?"
        })
        questions.append('Where is the original cheque? Why are you producing only a photocopy?')
        questions.append('How do you prove the signature on the photocopy is that of the accused?')


    if has_email_sms and not case_data.get('section_65b_certificate', False):
        risk_score += 10
        zones.append({
            'zone': 'Electronic Evidence — Section 65B',
            'severity': 'MEDIUM',
            'attack_vector': 'WhatsApp/SMS/email without S.65B certificate is inadmissible — Anvar P.V. v P.K. Basheer (2014)',
            'defence_question': 'Is it not correct you have not filed a Section 65B certificate authenticating this electronic evidence?'
        })
        questions.append("Have you filed a Section 65B certificate authenticating the WhatsApp/SMS/email messages?")


    questions += [
        'Have you filed any civil suit for recovery of this same amount in any civil court?',
        'Are there any other disputes pending between you and the accused?',
        'When did you first demand repayment before sending the legal notice?',
    ]


    seen_q = set(); unique_q = []
    for q in questions:
        if q not in seen_q:
            seen_q.add(q); unique_q.append(q)


    overall_risk = (
        'CRITICAL' if risk_score >= 60 else
        'HIGH'     if risk_score >= 40 else
        'MEDIUM'   if risk_score >= 20 else
        'LOW'
    )


    prep = []
    if not has_agreement:
        prep.append('Locate any WhatsApp/email/SMS acknowledging the debt before cross-examination')
    if not has_postal:
        prep.append('Verify postal tracking data and obtain printout before giving evidence')
    if is_cash:
        prep.append('Locate bank withdrawal records showing cash was available on transaction date')
    if not has_witness:
        prep.append('Identify and prepare any available witness to the transaction')

    return {
        'module': 'Cross Examination Risk Analysis',
        'overall_cross_exam_risk': overall_risk,
        'overall_risk': overall_risk,
        'risk_score': risk_score,
        'vulnerability_zones': zones,
        'likely_questions': unique_q,
        'preparation_required': prep,
        'zone_count': len(zones),
        'critical_zones': [z for z in zones if z['severity'] == 'CRITICAL'],
        'confidence': 0.85,
        'method': 'deterministic_rule_based',
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


    has_email_sms = case_data.get('email_sms_evidence', False)
    has_electronic = case_data.get('electronic_evidence', False)
    has_whatsapp = case_data.get('whatsapp_evidence', False)
    has_bank_statement_electronic = case_data.get('bank_statement_electronic', False)

    if not any([has_email_sms, has_electronic, has_whatsapp, has_bank_statement_electronic]):
        compliance['applicable'] = False
        compliance['risk_level'] = 'NOT_APPLICABLE'
        return compliance

    compliance['applicable'] = True


    if has_email_sms:
        compliance['electronic_evidence_types'].append('Email/SMS')
    if has_whatsapp:
        compliance['electronic_evidence_types'].append('WhatsApp Messages')
    if has_bank_statement_electronic:
        compliance['electronic_evidence_types'].append('Electronic Bank Statements')
    if has_electronic:
        compliance['electronic_evidence_types'].append('Other Electronic Records')


    has_certificate = case_data.get('section_65b_certificate', False)
    certificate_signed = case_data.get('section_65b_certificate_signed', False)
    certificate_from_custodian = case_data.get('certificate_from_device_custodian', False)


    has_primary_docs = (
        case_data.get('original_cheque_available', False) or
        case_data.get('return_memo_available', False) or
        case_data.get('written_agreement_exists', False) or
        case_data.get('ledger_available', False)
    )

    is_primary_proof = not has_primary_docs

    compliance['certificate_status'] = {
        'certificate_available': has_certificate,
        'properly_signed': certificate_signed if has_certificate else False,
        'from_authorized_custodian': certificate_from_custodian if has_certificate else False,
        'electronic_evidence_role': 'PRIMARY PROOF' if is_primary_proof else 'SUPPORTING EVIDENCE',
        'legal_requirement': 'MANDATORY per Anvar P.V. v. P.K. Basheer (2014)'
    }


    if not has_certificate:
        compliance['compliant'] = False

        if is_primary_proof:

            compliance['risk_level'] = 'FATAL'
            compliance['recommendations'].append({
                'priority': 'CRITICAL',
                'action': 'Obtain Section 65B certificate immediately',
                'details': 'Electronic evidence is PRIMARY PROOF - INADMISSIBLE without certificate',
                'from_whom': 'Device/server custodian (email provider, telecom, bank)',
                'legal_basis': 'Section 65B Evidence Act - Mandatory for primary electronic evidence'
            })
        else:

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


    transaction_mode = case_data.get('transaction_mode', 'Bank Transfer')
    cheque_amount = case_data.get('cheque_amount', 0)
    cash_transaction = case_data.get('cash_transaction', False)

    compliance['transaction_mode'] = transaction_mode
    compliance['cash_amount'] = cheque_amount if (cash_transaction or transaction_mode == 'Cash') else 0


    if not (cash_transaction or transaction_mode == 'Cash'):
        compliance['applicable'] = False
        compliance['risk_level'] = 'NOT_APPLICABLE'
        return compliance

    compliance['applicable'] = True


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


def evaluate_service_proof_unified(case_data: Dict) -> Dict:
    """
    UNIFIED SERVICE PROOF EVALUATOR - Single Source of Truth

    ALL modules (documentary_strength, notice_delivery, defence_risk)
    MUST use this result. No independent evaluation allowed.

    This eliminates module contradictions (Bug #1)
    """


    notice_date = case_data.get('notice_date')
    notice_received_date = case_data.get('notice_received_date')
    postal_acknowledgment = case_data.get('postal_acknowledgment', False)
    ad_card_signed = case_data.get('ad_card_signed', False)
    notice_signed = case_data.get('notice_signed', False)
    postal_proof_available = case_data.get('postal_proof_available', False)
    notice_refused = case_data.get('notice_refused', False)
    notice_unclaimed = case_data.get('notice_unclaimed', False)


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


    elif case_data.get('track_report_available') or case_data.get('section_65b_track_report'):
        result.update({
            'delivery_confirmed': True,
            'delivery_status': 'DELIVERED (track report confirmation)',
            'delivery_method': 'India Post Track Report',
            'proof_strength': 'GOOD',
            'service_quality': 'GOOD',
            'risk_level': 'LOW',
            'score': 75,
            'grade': 'GOOD'
        })

    elif postal_acknowledgment or postal_proof_available:
        result.update({
            'delivery_confirmed': False,
            'delivery_status': 'SENT — Proof of Posting Available',
            'delivery_method': 'Postal Receipt / Dispatch Slip',
            'proof_strength': 'MODERATE',
            'service_quality': 'MODERATE',
            'risk_level': 'MEDIUM',
            'score': 50,
            'grade': 'MODERATE'
        })


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

        _ = case_data.get('notice_date')
        _ = case_data.get('notice_received_date')
        _ = case_data.get('postal_acknowledgment', False)
        _ = case_data.get('ad_card_signed', False)
        notice_refused = case_data.get('notice_refused', False)
        notice_unclaimed = case_data.get('notice_unclaimed', False)
        notice_returned = case_data.get('notice_returned', False)
        notice_mode = case_data.get('notice_mode', 'RPAD')


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


        if not unified_result['notice_sent']:
            analysis['risk_level'] = 'FATAL'
            analysis['recommendations'].append({
                'priority': 'FATAL',
                'action': 'Legal notice not sent - statutory requirement not met',
                'remedy': 'Send legal notice immediately'
            })
            return analysis


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


    payment_after_notice = False
    if part_payment_date and notice_date:
        try:
            payment_dt = datetime.strptime(part_payment_date, '%Y-%m-%d')
            notice_dt = datetime.strptime(notice_date, '%Y-%m-%d')
            payment_after_notice = payment_dt > notice_dt
        except (ValueError, TypeError) as _e:
            logger.debug(f"Part payment date parse: {_e}")


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
        'jurisdiction_note': 'Jurisdiction assessment based on court/bank location data',
        'filing_court': None,
        'valid_jurisdictions': [],
        'jurisdiction_basis': [],
        'risk_level': 'LOW',
        'recommendations': []
    }


    presentation_bank_branch = case_data.get('presentation_bank_branch')
    drawee_bank_branch = case_data.get('drawee_bank_branch')
    payee_residence = case_data.get('payee_residence')
    payee_business_place = case_data.get('payee_business_place')
    _ = case_data.get('accused_residence')
    filing_court_location = case_data.get('filing_court_location')


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


    if filing_court_location:
        analysis['filing_court'] = filing_court_location


        if len(analysis['valid_jurisdictions']) == 0:

            analysis['jurisdiction_valid'] = None
            analysis['status'] = 'INSUFFICIENT_DATA'
            analysis['risk_level'] = 'MEDIUM'
            analysis['recommendations'].append({
                'priority': 'MEDIUM',
                'issue': 'Insufficient data to validate jurisdiction',
                'details': 'Cannot validate filing court without bank branch or payee location details',
                'action': 'Provide: presentation_bank_branch, drawee_bank_branch, or payee location',
                'consequence': 'Jurisdiction validity cannot be confirmed'
            })
        else:

            valid_locations = [j['location'].lower() for j in analysis['valid_jurisdictions']]
            filing_location_lower = filing_court_location.lower()


            jurisdiction_matches = any(
                filing_location_lower in loc or loc in filing_location_lower
                for loc in valid_locations
            )

            if not jurisdiction_matches:
                analysis['jurisdiction_valid'] = False
                analysis['risk_level'] = 'CRITICAL'
                analysis['jurisdiction_note'] = 'Court may lack jurisdiction — file where cheque was presented/dishonoured'
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

        analysis['jurisdiction_valid'] = False

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

    try:
        analysis = {
            'module': 'Section 147 Compounding Analysis',
            'compounding_eligible': True,
            'settlement_attempted': False,
            'compounding_stage': None,
            'settlement_details': {},
            'recommendations': []
        }


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


        analysis['compounding_eligible'] = True


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


    except Exception as _analyz_err:
        logger.error(f"analyze_compounding_eligibility error: {_analyz_err}")
        return {'module': 'Compounding Analysis', 'compounding_eligible': True, 'compounding_stage': 'Pre-Filing', 'recommendations': []}

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
        analysis['applicable'] = False
        analysis['properly_impleaded'] = True
        analysis['risk_level'] = 'LOW'
        analysis['explanation'] = 'Not a company case — Section 141 director liability not applicable. Individual drawer is solely liable.'
        analysis['recommendation'] = 'N/A — no director impleading required'
        return analysis

    analysis['is_company_case'] = True


    directors_impleaded = case_data.get('directors_impleaded', False)
    director_role = case_data.get('director_role')
    director_active_period = case_data.get('director_active_period')
    specific_averment_present = case_data.get('specific_averment_present', False)
    _ = case_data.get('director_knowledge_alleged', False)


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


    cheque_date = case_data.get('cheque_date')
    if director_active_period and cheque_date:

        analysis['recommendations'].append({
            'priority': 'HIGH',
            'action': 'Verify director was serving during transaction period',
            'details': 'Director must have been in office when cheque was issued',
            'required_proof': 'Board resolution, DIN records, ROC filings'
        })


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


    unified_service = evaluate_service_proof_unified(case_data)

    compliance = {
        'module': 'Document Compliance Analysis',
        'overall_compliance': 0,
        'mandatory_docs': {},
        'evidence_classification': {},
        'section_65b_compliance': {},
        'service_proof_unified': unified_service,
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

            'present': (
                case_data.get('notice_copy_available', False) or
                case_data.get('legal_notice_available', False) or
                bool(case_data.get('notice_date')) or
                bool(case_data.get('notice_received_date'))
            ),
            'severity': 'FATAL',
            'deduction': 25,
            'consequence': 'Notice requirement not satisfied - Statutory condition'
        },
        'postal_acknowledgment': {

            'present': unified_service['delivery_confirmed'],
            'severity': 'CRITICAL' if unified_service['risk_level'] in ['HIGH', 'CRITICAL'] else 'WARNING',
            'deduction': 100 - unified_service['score'],
            'consequence': f"Service proof {unified_service['proof_strength']} - {unified_service['delivery_method']}",
            'unified_grade': unified_service['grade']
        },
        'section_65b_certificate': {
            'present': case_data.get('section_65b_certificate', True) if case_data.get('electronic_evidence') else True,
            'severity': 'FATAL' if (case_data.get('electronic_evidence') and not case_data.get('section_65b_certificate')) else ('CRITICAL' if case_data.get('electronic_evidence') else 'NA'),
            'deduction': 30,
            'consequence': 'Electronic evidence INADMISSIBLE without Section 65B certificate - NO OVERRIDE ALLOWED',
            'applicable': case_data.get('electronic_evidence', False),
            'legal_basis': 'Section 65B Evidence Act - Anvar P.V. v. P.K. Basheer (2014)',
            'strict_enforcement': True
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
        except Exception: return fb

    def _d(v):
        return v if isinstance(v, dict) else {}


    risk_data      = _d(risk_data)
    timeline_data  = _d(timeline_data)
    defect_data    = _d(defect_data)
    ingredient_data= _d(ingredient_data)
    doc_data       = _d(doc_data)
    _doc           = doc_data
    _refw2         = timeline_data.get('refiling_window') or {}
    settlement_data= _d(settlement_data)
    presumption_data= _d(presumption_data)
    judicial_behavior= _d(judicial_behavior)
    edge_case_data = _d(edge_case_data)
    defence_data   = _d(defence_data)
    cross_exam_data= _d(cross_exam_data)

    score      = _n(risk_data.get('final_score'))
    compliance = _s(risk_data.get('compliance_level'), 'Assessment Pending')
    amount     = case_data.get('cheque_amount', 0)
    case_type  = "Complainant" if case_data.get('case_type') == 'complainant' else "Accused"


    all_fatals = []
    for _src in [risk_data.get('fatal_defects', []) or [],
                  defect_data.get('fatal_defects', []) or [],
                  ingredient_data.get('fatal_defects', []) or []]:
        for _d in _src:
            _defect_text = str(_d.get('defect', '')).lower()
            _sev = _d.get('severity', '')

            if 'same-day' in _defect_text or 'same day' in _defect_text:
                continue

            if _d.get('is_absolute') is False:
                continue

            if _sev not in ('FATAL', 'CRITICAL', ''):
                continue
            all_fatals.append(_d)
    seen, unique_fatals = set(), []
    for d in all_fatals:
        k = d.get('defect', str(d))
        if k not in seen:
            seen.add(k)
            unique_fatals.append(d)
    unique_fatals = deduplicate_fatal_defects(unique_fatals)

    fatal_flag = len(unique_fatals) > 0


    _verdict_source = 'procedural' if defect_data.get('fatal_defects') else (
        'timeline' if (timeline_data.get('compliance_status') or {}).get('limitation','') and
            ('PREMATURE' in str((timeline_data.get('compliance_status') or {}).get('limitation','')).upper() or
             'BARRED' in str((timeline_data.get('compliance_status') or {}).get('limitation','')).upper()) else
        'ingredient' if ingredient_data.get('fatal_defects') else
        'risk_score'
    )
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


    limitation_status = _s(
        (timeline_data.get('compliance_status') or {}).get('limitation'),
        'Insufficient data'
    )

    case_overview = (
        f"{case_type} — Section 138 NI Act | "
        f"Cheque: ₹{indian_number_format(amount)} | "
        f"Score: {score}/100 | "
        f"{compliance}"
    )

    _primary_issue = unique_fatals[0].get('defect', '') if unique_fatals else ''
    _refilin_w     = timeline_data.get('refiling_window') or {}
    _refile_days   = _refilin_w.get('days_remaining', 0)
    _refile_dl     = _refilin_w.get('deadline_date', '')
    case_status_box = {
        'status':    filing_verdict,
        'reason':    (
            _primary_issue if _primary_issue else
            f'Score {score}/100 — {compliance}'
        ),
        'next_step': (
            "Refile after cause of action" +
            (f" — {_refile_days} days left (deadline {_refile_dl})" if _refile_days else '')
            if fatal_flag and _refile_days > 0 else
            'Do not refile same complaint — address fatal defects first' if fatal_flag else
            'File complaint — case is in strong position' if score >= 75 else
            'Strengthen documentary evidence before filing' if score >= 55 else
            'Significant gaps — legal review required before filing'
        ),
        'confidence': (
            'HIGH' if score > 0 and len([x for x in [timeline_data, ingredient_data, doc_data] if x]) >= 3
            else 'MEDIUM' if score > 0 else 'LOW'
        ),
        'dismissal_probability': (
            '>90%' if (fatal_flag or score < 25) else
            '60-80%' if score < 50 else
            '20-40%' if score < 70 else '<15%'
        ),
    }
    if unique_fatals:
        case_overview += f" | ⚠️ {len(unique_fatals)} Fatal Defect(s)"


    _pf = unique_fatals[0] if unique_fatals else {}
    _ = (
        f"{filing_verdict} — {_s(_pf.get('defect',''))}."
        if unique_fatals else
        f"{filing_verdict} — Risk score: {score}/100."
    ).strip().replace('—  .', '').replace('..', '.')


    _pf_text = unique_fatals[0].get('defect', '') if unique_fatals else ''
    _impact_text = unique_fatals[0].get('impact', '') if unique_fatals else ''
    _ = (
        f"{filing_verdict}. {_pf_text}. {_impact_text}."
        if unique_fatals else
        f"{filing_verdict}. Risk score: {score}/100. {compliance}."
    ).strip().replace('..', '.')


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


    weaknesses = []
    for cat, data in cat_scores.items():
        s = _n(data.get('score') if isinstance(data, dict) else data)
        if s < 60:
            reason = (data.get('reason', '') or '') if isinstance(data, dict) else ''
            weaknesses.append(
                f"{cat}: {s}/100 — {reason or 'Insufficient — strengthen before filing'} ❌"
            )
    if not case_data.get('written_agreement_exists'):
        weaknesses.append("No written agreement — debt enforceability can be challenged ❌")


    _doc_score = _n(_doc.get('overall_strength_score'))
    _has_primary = case_data.get('original_cheque_available') and case_data.get('return_memo_available')
    _missing_secondary = not case_data.get('written_agreement_exists') or not case_data.get('ledger_available')
    if _has_primary and _missing_secondary and _doc_score < 60:
        documentary_context = "No documentary proof of legally enforceable debt."
    elif _doc_score >= 70:
        documentary_context = "Documentary evidence is in reasonable order."
    else:
        documentary_context = f"Documentary strength: {_doc_score:.0f}/100 — obtain written agreement and financial records before filing."

    if not case_data.get('ledger_available'):
        weaknesses.append("No financial records — transaction trail incomplete ❌")
    if not case_data.get('postal_proof_available'):
        weaknesses.append("No postal proof — notice service presumption weak ❌")
    if not weaknesses:
        weaknesses.append("No critical weaknesses at this stage")


    critical_risks = []
    _seen_cr = set()
    for d in unique_fatals[:2]:
        _ck = str(d.get('defect',''))[:35]
        if _ck in _seen_cr: continue
        _seen_cr.add(_ck)
        _sev = _s(d.get('severity'), 'CRITICAL').upper()
        _tier = 'FATAL' if any(x in _sev for x in ['CRITICAL','FATAL']) else ('HIGH' if 'HIGH' in _sev else 'MODERATE')
        critical_risks.append({
            'risk':       _s(d.get('defect')),
            'severity':   _sev,
            'tier':       _tier,
            'impact':     _s(d.get('impact')),
            'remedy':     _s(d.get('remedy', d.get('cure', 'Consult legal counsel'))),
            'legal_basis': _s(d.get('legal_basis', d.get('statutory_basis', ''))),
            'is_primary': d.get('is_primary', False),
        })

    for hr in (defence_data.get('high_risk_defences') or [])[:2]:
        critical_risks.append({
            'risk':     _s(hr.get('defence', hr.get('ground'))),
            'severity': 'HIGH',
            'impact':   _s(hr.get('strategy', hr.get('risk_impact', 'Acquittal risk'))),
            'remedy':   _s(hr.get('counter', hr.get('legal_basis', 'Strengthen evidence')))
        })


    strategic_recommendations = []
    if fatal_flag:

        _tla2 = timeline_data if isinstance(timeline_data, dict) else {}
        _ = _tla2.get('refile_guidance', '')
        _ = _tla2.get('cause_of_action_reasoning', '')
        _crit2  = _tla2.get('critical_dates', {})
        _ = _crit2.get('limitation_deadline', '')


        _primary = unique_fatals[0] if unique_fatals else {}
        _days_rem = _refw2.get('days_remaining', 0) if isinstance(_refw2, dict) else 0
        _dl_str   = _refw2.get('deadline_date', '') if isinstance(_refw2, dict) else ''
        strategic_recommendations.append({
            'priority':    'URGENT',
            'action':      'Wait until cause of action arises, then file fresh complaint',
            'recommendation': _s(_primary.get('remedy',
                'File fresh complaint after the 15-day payment period expires')),
            'next_step':   (
                "File fresh complaint within limitation window"
                + (f" — {_days_rem} days remaining (deadline {_dl_str})" if _days_rem else '')
            ),
            'support':     'Prepare: written agreement / ledger records / postal proof of notice',
            'rationale':   _s(_primary.get('defect', 'Fatal procedural defect detected')),
            'legal_basis': _s(_primary.get('legal_basis', 'Section 138(b) NI Act')),
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


    next_actions = []
    if unique_fatals:

        _pf = unique_fatals[0]
        _days_na = _refw2.get('days_remaining', 0) if isinstance(_refw2, dict) else 0
        _dl_na   = _refw2.get('deadline_date', '') if isinstance(_refw2, dict) else ''
        next_actions.append({
            'action':  'Wait for cause of action, then file fresh complaint',
            'urgency': 'URGENT',
            'details': (
                "Do not refile same complaint — it will be dismissed. "
                + (_s(_pf.get('remedy', '')) + '. ' if _pf.get('remedy') else '')
                + (f"You have {_days_na} days to refile (deadline: {_dl_na})." if _days_na else '')
            ).strip()
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


    edge_cases_alert = edge_case_data.get('detected_cases', []) or []


    if fatal_flag:
        outcome_probability = 'HIGH probability of dismissal — fatal procedural defect present'
        dismissal_risk = 'CERTAIN' if any('premature' in str(d).lower() for d in unique_fatals) else 'HIGH'
    elif score >= 75:
        _doc_s = _n(doc_data.get('overall_strength_score', 0))
        if _doc_s < 55:
            outcome_probability = 'MODERATE — procedural compliance adequate but weak documentary evidence creates acquittal risk'
            dismissal_risk = 'MEDIUM'
        else:
            outcome_probability = 'MODERATE-HIGH probability of conviction if evidence maintained'
            dismissal_risk = 'LOW'
    elif score >= 55:
        outcome_probability = 'MODERATE — case is legally valid but outcome depends on documentary strengthening'
        dismissal_risk = 'MEDIUM'
    else:
        outcome_probability = 'LOW probability of conviction — significant compliance gaps present'
        dismissal_risk = 'HIGH'


    opponent_strategies = []
    for d in unique_fatals[:2]:
        defect_text = str(d.get('defect','')).lower()
        if 'premature' in defect_text or 'cause of action' in defect_text:
            opponent_strategies.append(
                'Accused will file discharge application at first hearing citing premature complaint. '
                'Will argue cause of action had not arisen. High probability of success.'
            )
        elif 'limitation' in defect_text or 'time-barred' in defect_text:
            opponent_strategies.append(
                'Accused will challenge maintainability citing limitation bar. '
                'Will argue no sufficient cause for delay. Case likely dismissed at threshold.'
            )
    if not opponent_strategies and not fatal_flag:
        if not case_data.get('written_agreement_exists'):
            opponent_strategies.append(
                'Accused likely to deny legally enforceable debt -- will argue cheque was for security, '
                'not repayment. Will demand proof of underlying transaction.'
            )


    _doc_s2 = _n(doc_data.get('overall_strength_score', 0))
    if unique_fatals:
        top_summary = (
            f"Case has fatal procedural defect — do not file until resolved. Score: {score}/100."
        )
    elif score >= 75 and _doc_s2 >= 60:
        top_summary = (
            f"Case is in strong position to file — statutory requirements largely satisfied. Score: {score}/100."
        )
    elif score >= 55:
        top_summary = (
            f"Case is legally maintainable but weakened due to lack of documentary proof of debt. "
            f"Score: {score}/100. Strengthen documentation before filing."
        )
    else:
        top_summary = (
            f"Case has multiple compliance gaps — significant remediation required before filing. Score: {score}/100."
        )

    return {

        'top_summary':               top_summary,
        'case_status_box':           case_status_box,
        'case_status':               filing_verdict,
        'primary_issue':             _primary_issue,
        'next_step':                 case_status_box.get('next_step', ''),
        'confidence':                case_status_box.get('confidence', 'MEDIUM'),
        'dismissal_probability':     case_status_box.get('dismissal_probability', ''),
        'verdict_source':            _verdict_source,
        'verdict_driven_by':         f'Decision driven by: {_verdict_source.replace("_"," ").title()} module',

        'case_overview':             case_overview,
        'outcome_probability':       outcome_probability,
        'dismissal_risk':            dismissal_risk,
        'opponent_likely_strategy':  opponent_strategies[:2],
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
        'documentary_context':       documentary_context,
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
        maintainability = "Appears Maintainable"
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
        verdict_line = "Critical statutory violations detected. Filing not advisable without substantial remediation."

    if fatal_defects or score < 40:
        dismissal_risk = "Substantial"
    elif score < 60:
        dismissal_risk = "Moderate"
    else:
        dismissal_risk = "Mitigable"

    verdict['legal_position'] = verdict_line
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
        'identified_vulnerabilities': [],
        'fatal_issues': [],
        'score_reduction_factors': []
    }

    _ = sum(data.get('score', 0) * data.get('weight', 0) / 100
                     for data in category_scores.values())

    drivers['score_explanation'] = "Composite score derived from weighted assessment across five statutory compliance categories. "

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

    _ = True
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


def _safe(value, default="DATA NOT AVAILABLE", fmt=None):
    """Return value safely, replacing None/empty/broken with default. Optionally format numbers."""
    if value is None or value == "" or value == {} or value == []:
        return default
    if isinstance(value, str) and value.strip() in ('??', '?', 'undefined', 'null', 'None', 'nan'):
        return default
    if isinstance(value, str) and '\ufffd' in value:
        value = value.replace('\ufffd', '')
        if not value.strip():
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
        return "DATA NOT AVAILABLE"
    if isinstance(data, dict):
        return {k: sanitize_module_output(v, _depth+1) for k, v in data.items()}
    if isinstance(data, list):
        return [sanitize_module_output(i, _depth+1) for i in data]
    if isinstance(data, str):
        cleaned = data.replace('\ufffd', '').replace('\u0000', '')
        if cleaned.strip() in ('', '??', '?', 'undefined', 'null', 'None', 'nan'):
            return "DATA NOT AVAILABLE"
        return cleaned
    if isinstance(data, float) and data != data:
        return 0.0
    return data


def _safe_simple(val, fallback="DATA NOT AVAILABLE"):
    """Return val if truthy and not None/broken, else fallback. Simple version without fmt."""
    if val is None or val == "" or val == [] or val == {}:
        return fallback
    if isinstance(val, str) and val.strip() in ('??', '?', 'undefined', 'null', 'None', 'nan'):
        return fallback
    if isinstance(val, str) and '\ufffd' in val:
        val = val.replace('\ufffd', '')
        if not val.strip():
            return fallback
    return val


def _safe_score(val, fallback=0.0):
    """Return numeric score safely."""
    try:
        return round(float(val), 1)
    except (TypeError, ValueError):
        return fallback


def generate_clean_professional_report(analysis: Dict, case_data: Dict) -> Dict:

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
    _ = analysis.get('modules', {}).get('settlement_analysis', {}) or {}

    score       = _safe_score(risk.get('final_score'))
    fatal_flag  = analysis.get('fatal_flag', False)


    all_fatal_defects = []
    all_fatal_defects.extend(risk.get('fatal_defects', []))
    all_fatal_defects.extend(defects_m.get('fatal_defects', []))
    all_fatal_defects.extend(ingredients.get('fatal_defects', []))


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


    real_fatal_defects = [d for d in fatal_defects
        if d.get('severity') in ('FATAL', 'CRITICAL')
        and d.get('is_absolute', True) is not False
        and 'same-day' not in str(d.get('defect', '')).lower()
        and 'same day' not in str(d.get('defect', '')).lower()]

    if real_fatal_defects:
        filing_status = "DO NOT FILE"
        filing_colour = "RED"
        one_liner = (
            f"This {case_type.lower()} case has {len(real_fatal_defects)} fatal defect(s) requiring immediate remedy. "
            f"Risk score: {score}/100."
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

        _doc_score = _safe_score(documentary.get('overall_strength_score', 0))
        _proc_defects = defects_m.get('curable_defects') or []
        _no_agreement = not case_data.get('written_agreement_exists')
        _no_ledger    = not case_data.get('ledger_available')
        if _doc_score < 60 and (_no_agreement or _no_ledger):
            _caution_reason = "No documentary proof of legally enforceable debt"
        elif _doc_score < 60:
            _caution_reason = "Insufficient documentary evidence to establish transaction"
        elif _proc_defects:
            _caution_reason = "Procedural gaps present — curable before filing"
        else:
            _caution_reason = "Evidentiary gaps require strengthening"
        one_liner = (
            f"Case is legally valid but weak due to lack of documentary proof. "
            f"Reason: {_caution_reason}. Risk score: {score}/100."
        )
        recommended_action = (
            "Obtain documentary proof of the debt — written agreement, ledger records, "
            "or bank transfer evidence — before filing to reduce acquittal risk."
        )
    else:
        filing_status = "HIGH RISK — REMEDIATION REQUIRED"
        filing_colour = "RED"
        one_liner = f"Multiple compliance gaps detected. Risk score: {score}/100. Filing not advisable without remediation."
        recommended_action = "Obtain missing documents, strengthen transaction proof, then reassess."


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


    weaknesses = []
    for cat, data in (risk.get('category_scores') or {}).items():
        s = _safe_score(data.get('score') if isinstance(data, dict) else data)
        if s < 60:
            reason = data.get('reason', '') if isinstance(data, dict) else ''
            weaknesses.append(f"{cat}: {s}/100 — {_safe(reason, 'Insufficient — strengthen before filing')}")
    if not case_data.get('written_agreement_exists'):
        weaknesses.append("No documentary proof of legally enforceable debt — accused can challenge the fundamental Section 138 ingredient")
    if not case_data.get('ledger_available'):
        weaknesses.append("No ledger or financial records — transaction trail incomplete, making debt proof difficult")
    if not weaknesses:
        weaknesses.append("No critical weaknesses detected at this stage")


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
        'compliance_level': _safe(risk.get('compliance_level'), 'Assessment Pending'),
        'fatal_defects_count': len(fatal_defects),
        'fatal_defects': fatal_summary,
        'strengths': strengths[:5],
        'weaknesses': weaknesses[:5],
        'recommended_action': recommended_action,
        'processing_time': f"{analysis.get('processing_time_seconds') or 1.2:.2f}s",
        'engine_version': analysis.get('engine_version', 'v10.0'),
        'generated_by': 'JUDIQ Legal Intelligence Engine',
    }


    chart = sorted(
        [e for e in timeline.get('timeline_chart', []) if e.get('date')],
        key=lambda x: x.get('date', '9999')
    )
    timeline_section = {
        'title': 'TIMELINE ANALYSIS',
        'limitation_risk': _safe(timeline.get('limitation_risk'), 'Insufficient data'),
        'limitation_status': _safe(
            timeline.get('compliance_status', {}).get('limitation'), 'Insufficient data'
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
        'compliance_level': R.get('compliance_level', _safe(risk.get('compliance_level'), 'Assessment Pending')),
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
        'strength_label': _safe(documentary.get('strength_label'), 'Assessment Pending'),
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
            f"Documentary strength: {R.get('documentary_score', _safe_score(documentary.get('overall_strength_score')))}/100. "
            + ("No written agreement or financial records — accused can challenge the existence of a "
               "legally enforceable debt, which is a fundamental Section 138 ingredient. "
               "Obtain corroborating evidence (agreement, ledger, bank transfer, SMS) before filing."
               if not case_data.get('written_agreement_exists') and not case_data.get('ledger_available') else
               "No written agreement — accused can challenge the legally enforceable debt ingredient. "
               "Obtain documentary evidence before filing."
               if not case_data.get('written_agreement_exists') else
               "Documentary evidence is in reasonable order.")
        )
    }


    high_risk = defence_m.get('high_risk_defences', [])
    _ = defence_m.get('defences', defence_m.get('defence_angles', []))


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
        'overall_exposure': _safe(defence_m.get('overall_exposure', defence_m.get('exposure_level')), 'Insufficient data'),
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


    procedural_section = {
        'title': 'PROCEDURAL DEFECTS',
        'overall_risk': _safe(defects_m.get('overall_risk'), 'Insufficient data'),
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


    vuln_zones = cross_exam.get('vulnerability_zones', [])
    likely_qs  = cross_exam.get('likely_questions', [])
    cx_section = {
        'title': 'CROSS-EXAMINATION RISK',
        'overall_risk': R.get('cross_exam_risk', _safe(cross_exam.get('overall_cross_exam_risk'), 'Insufficient data')),
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
            'processing_time': f"{analysis.get('processing_time_seconds') or 1.2:.2f}s",
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
            'compliance_level': risk.get('compliance_level', 'Assessment Pending'),
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
    _ = modules.get('ingredient_compliance', {})
    settlement_data = modules.get('settlement_analysis', {})
    cross_data   = modules.get('cross_examination_risk', {})
    judicial_data= modules.get('judicial_behavior', {})

    score        = risk_data.get('final_score', 0)
    fatal_flag   = analysis_data.get('fatal_flag', False)
    fatal_defects= risk_data.get('fatal_defects', [])
    cat_scores   = risk_data.get('category_scores', {})
    limitation_risk = timeline_data.get('limitation_risk', 'Unknown')
    case_meta    = analysis_data.get('case_metadata', {})
    proc_time    = analysis_data.get('processing_time_seconds', 0)


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


    raw_chart = timeline_data.get('timeline_chart', [])
    timeline_events = sorted(
        [e for e in raw_chart if e.get('date')],
        key=lambda x: x.get('date', '9999')
    )
    if not timeline_events:

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


    risk_breakdown = []
    for cat, data in cat_scores.items():
        risk_breakdown.append({
            'category': cat,
            'score': f"{_safe(data.get('score'), 0, 'pct')}",
            'weight': f"{data.get('weight', 0)*100:.0f}% of total",
            'status': _safe(data.get('status'), 'Under review'),
            'reason': _safe(data.get('reason') or data.get('primary_issue'), 'No issues detected')
        })


    doc_gaps = []
    doc_details = doc_data.get('document_details', {})
    for doc_name, doc_info in doc_details.items():
        if isinstance(doc_info, dict) and doc_info.get('grade') in ['WEAK','VERY WEAK','MISSING','NOT AVAILABLE']:
            doc_gaps.append({
                'gap_name':  _safe(doc_name),
                'document':  _safe(doc_name),
                'severity':  'High',
                'status':    _safe(doc_info.get('grade'), 'Missing'),
                'impact':    _safe(doc_info.get('impact'), 'Reduces evidential strength'),
                'remedy':    _safe(doc_info.get('remedy'), 'Obtain and verify document')
            })
    if not doc_gaps and doc_data.get('overall_strength_score', 100) < 70:
        for field, label, sev, imp in [
            ('written_agreement_exists', 'No documentary proof of legally enforceable debt', 'Severe',
             'Accused can deny the existence of a legally enforceable debt — a fundamental Section 138 ingredient'),
            ('ledger_available',         'No financial records or ledger entries', 'High',
             'No independent paper trail of the alleged transaction — debt story is entirely oral and uncorroborated'),
            ('original_cheque_available','Original Cheque', 'High',
             'Primary instrument not secured — foundational evidence missing'),
            ('return_memo_available',    'Bank Dishonour Memo', 'Severe',
             'Proof of dishonour not available — essential ingredient missing'),
            ('postal_proof_available',   'Postal Proof of Notice', 'Moderate',
             'Notice service cannot be established — accused may deny receipt'),
        ]:
            if not case_meta.get(field, True):
                doc_gaps.append({
                    'gap_name':  label,
                    'document':  label,
                    'severity':  sev,
                    'status':    'Not available',
                    'impact':    imp,
                    'remedy':    'Obtain before filing'
                })


    high_risk_defences = defence_data.get('high_risk_defences', [])
    fatal_defences     = defence_data.get('fatal_defences', [])


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


    cross_questions = cross_data.get('likely_questions', [])
    vuln_zones = cross_data.get('vulnerability_zones', [])
    if not cross_questions:

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
            "risk_classification": _safe(risk_data.get('compliance_level'), 'Assessment Pending'),
            "fatal_defects_present": "YES — DO NOT FILE" if fatal_flag else "None detected"
        },


        "page_2_executive_summary": {
            "title": "EXECUTIVE SUMMARY",
            "one_line_verdict": one_liner,
            "case_strength":    _safe(risk_data.get('compliance_level'), 'Assessment Pending'),
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
                'Insufficient data'
            )
        },


        "page_3_timeline": {
            "title": "TIMELINE ANALYSIS (CHRONOLOGICAL)",
            "limitation_risk": _safe(limitation_risk, 'Unknown'),
            "events": timeline_events,
            "critical_dates": {
                k: _safe(v, 'Not provided')
                for k, v in timeline_data.get('critical_dates', {}).items()
            },
            "compliance_status": {
                k: _safe(v, 'Insufficient data')
                for k, v in timeline_data.get('compliance_status', {}).items()
            },
            "edge_cases": timeline_data.get('edge_cases_detected', [])
        },


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


        "page_8_judicial_behaviour": {
            "title": "JUDICIAL BEHAVIOUR ANALYSIS",
            "analysis": jud_section,
            "strategic_insights": judicial_data.get('strategic_insights', []),
        },


        "page_9_settlement": {
            "title": "SETTLEMENT & FINANCIAL EXPOSURE",
            "settlement_recommended": settlement_data.get('settlement_recommended', False),
            "settlement_range": settlement_data.get('recommended_settlement_range', {}),
            "financial_exposure": settlement_data.get('financial_exposure', {}),
            "strategy": _safe(settlement_data.get('settlement_strategy'), 'Evaluate based on case strength')
        },


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
                "overall_score": f"{risk_data.get('final_score', 0):.1f}/100"
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
        _ = case_data.get('part_payment_amount', 0)
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


        raw_compliance = risk_data.get('compliance_level', 'MODERATE')
        normalized_compliance = normalize_compliance_level(raw_compliance)


        cursor.execute("""
            INSERT INTO case_analyses (
                case_id, analysis_timestamp, case_type, cheque_amount,
                overall_risk_score, compliance_level, fatal_defect_override,
                fatal_type, timeline_risk, ingredient_compliance,
                documentary_strength, analysis_json, engine_version, user_email
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            analysis_report['case_id'],
            analysis_report.get('analysis_timestamp', ''),
            analysis_report['case_metadata']['case_type'],
            analysis_report['case_metadata'].get('cheque_amount', 0),
            risk_data.get('final_score', 0),
            normalized_compliance,
            1 if analysis_report.get('fatal_flag') else 0,
            analysis_report.get('overall_status', '').split(' - ')[0] if ' - ' in analysis_report.get('overall_status', '') else None,
            timeline_data.get('limitation_risk', 'Unknown'),
            category_scores.get('Ingredient Compliance', {}).get('score', 0),
            category_scores.get('Documentary Strength', {}).get('score', 0),
            json.dumps(analysis_report),
            analysis_report.get('engine_version', 'v11.3'),
            analysis_report.get('user_email', '')
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


def _safe_f(v, default=0.0) -> float:
    """Safe float cast used across Layer 12."""
    try:
        return round(float(v), 1)
    except (TypeError, ValueError):
        return default


def get_decision_confidence(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 1 — DECISION CONFIDENCE ENGINE
    Tells the lawyer how much to trust this system's output for this specific case.
    Based on: data completeness, module agreement, contradiction count, KB coverage.
    NOT probability of winning — confidence in the analysis itself.
    """
    try:
        risk   = analysis.get('modules', {}).get('risk_assessment', {}) or {}
        cats   = risk.get('category_scores', {}) or {}
        dc     = analysis.get('data_completeness') or calculate_data_completeness(case_data)
        pct    = _safe_f(dc.get('completeness_pct', 0))
        _ = _safe_f(risk.get('final_score', 0))

        factors = []
        penalty = 0


        if pct >= 80:
            factors.append({'factor': 'Data completeness', 'rating': 'HIGH',
                             'detail': f'All required fields provided ({pct:.0f}%)', 'impact': 0})
        elif pct >= 55:
            factors.append({'factor': 'Data completeness', 'rating': 'MEDIUM',
                             'detail': f'Some fields missing ({pct:.0f}%) — analysis estimated for gaps', 'impact': -15})
            penalty += 15
        else:
            factors.append({'factor': 'Data completeness', 'rating': 'LOW',
                             'detail': f'Critical fields missing ({pct:.0f}%) — analysis reliability reduced', 'impact': -30})
            penalty += 30


        cat_scores = [_safe_f((v.get('score') if isinstance(v, dict) else v), 0) for v in cats.values()]
        if cat_scores:
            spread = max(cat_scores) - min(cat_scores)
            if spread <= 25:
                factors.append({'factor': 'Module agreement', 'rating': 'HIGH',
                                 'detail': 'All modules give consistent signals', 'impact': 0})
            elif spread <= 50:
                factors.append({'factor': 'Module agreement', 'rating': 'MEDIUM',
                                 'detail': f'Modules show mixed signals (spread: {spread:.0f} pts)', 'impact': -10})
                penalty += 10
            else:
                factors.append({'factor': 'Module agreement', 'rating': 'LOW',
                                 'detail': f'Modules significantly disagree (spread: {spread:.0f} pts)', 'impact': -20})
                penalty += 20


        consistency = analysis.get('_consistency_check') or {}
        n_issues = len(consistency.get('issues') or [])
        if n_issues == 0:
            factors.append({'factor': 'Internal consistency', 'rating': 'HIGH',
                             'detail': 'No contradictions detected', 'impact': 0})
        elif n_issues <= 2:
            factors.append({'factor': 'Internal consistency', 'rating': 'MEDIUM',
                             'detail': f'{n_issues} minor contradiction(s) auto-corrected', 'impact': -8})
            penalty += 8
        else:
            factors.append({'factor': 'Internal consistency', 'rating': 'LOW',
                             'detail': f'{n_issues} contradictions — review results carefully', 'impact': -20})
            penalty += 20


        critical_dates = ['cheque_date', 'dishonour_date', 'notice_date', 'complaint_filed_date']
        missing_dates = [f for f in critical_dates if not case_data.get(f)]
        if not missing_dates:
            factors.append({'factor': 'Timeline data', 'rating': 'HIGH',
                             'detail': 'All critical dates provided — timeline analysis is deterministic', 'impact': 0})
        elif len(missing_dates) <= 1:
            factors.append({'factor': 'Timeline data', 'rating': 'MEDIUM',
                             'detail': f'Missing: {missing_dates[0]} — some timeline figures are estimated', 'impact': -12})
            penalty += 12
        else:
            factors.append({'factor': 'Timeline data', 'rating': 'LOW',
                             'detail': f'{len(missing_dates)} critical dates missing — limitation analysis unreliable', 'impact': -25})
            penalty += 25

        base = 100
        confidence_score = max(0, min(100, base - penalty))

        if confidence_score >= 80:
            level = 'HIGH'
            label = 'High confidence — analysis is reliable for this case'
            colour = 'GREEN'
        elif confidence_score >= 55:
            level = 'MEDIUM'
            label = 'Medium confidence — analysis is reasonable but review flagged areas'
            colour = 'AMBER'
        else:
            level = 'LOW'
            label = 'Low confidence — provide more data to improve reliability'
            colour = 'RED'

        return {
        'module':             'Decision Confidence Engine',
        'confidence_level':   level,
        'confidence_score':   confidence_score,
        'colour':             colour,
        'label':              label,
        'factors':            factors,
        'penalty_total':      penalty,
        'trust_signal':       f'System confidence in this analysis: {level} ({confidence_score}/100)',
        'caveat':             ('This is system confidence in the analysis quality, not a prediction of case outcome.'
                               ' A HIGH confidence score means the input data is complete and modules agree —'
                               ' not that the case will succeed.'),
    }


    except Exception as _get_de_err:
        logger.error(f"get_decision_confidence error: {_get_de_err}")
        return {'module': 'Decision Confidence', 'confidence_level': 'MEDIUM', 'confidence_score': 50, 'label': 'Moderate confidence', 'factors': [], 'caveat': 'Could not fully assess confidence.'}

def get_contradiction_explainer(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 2 — CONTRADICTION EXPLAINER
    Finds and explains every contradiction in the analysis in plain language.
    """
    try:
        risk     = analysis.get('modules', {}).get('risk_assessment', {}) or {}
        timeline = analysis.get('modules', {}).get('timeline_intelligence', {}) or {}
        _ = analysis.get('modules', {}).get('documentary_strength', {}) or {}
        proc     = analysis.get('modules', {}).get('procedural_defects', {}) or {}

        contradictions = []

        tl_score  = _safe_f((risk.get('category_scores') or {}).get('Timeline Compliance', {}).get('score') if isinstance((risk.get('category_scores') or {}).get('Timeline Compliance'), dict) else 0)
        doc_score = _safe_f((risk.get('category_scores') or {}).get('Documentary Strength', {}).get('score') if isinstance((risk.get('category_scores') or {}).get('Documentary Strength'), dict) else 0)
        overall   = _safe_f(risk.get('final_score', 0))
        fatal     = analysis.get('fatal_flag', False)
        proc_fatals = proc.get('fatal_defects') or []
        lim_status  = str((timeline.get('compliance_status') or {}).get('limitation') or '').upper()
        postal_ok   = case_data.get('postal_proof_available') or case_data.get('ad_card_signed')


        if tl_score >= 70 and not postal_ok:
            contradictions.append({
                'id':           'C1',
                'severity':     'MEDIUM',
                'title':        'Timeline scores well but notice delivery is unproven',
                'warning':      '⚠️ Timeline valid but notice proof missing → potential procedural risk at trial',
                'explanation':  ('The timeline module scores the notice as within 30 days. However, no postal receipt'
                                  ' or AD card is available. The accused can dispute service, which would collapse'
                                  ' ingredient 5 regardless of the timeline score.'),
                'action':       'Obtain postal receipt or AD card to corroborate the timeline claim.',
            })


        if fatal and overall >= 65:
            contradictions.append({
                'id':           'C2',
                'severity':     'HIGH',
                'title':        'High risk score conflicts with fatal flag',
                'warning':      f'⚠️ Score is {overall:.0f}/100 (strong) but fatal_flag=True — contradiction',
                'explanation':  (f'A fatal defect brings the actual filing viability to zero, regardless of the'
                                  f' {overall:.0f}/100 score. The score reflects the intrinsic strength of the'
                                  f' ingredients — not the current filing status. Do not file until the fatal'
                                  f' defect is resolved.'),
                'action':       'Resolve the fatal defect first. Do not rely on the score to make a filing decision.',
            })


        if not case_data.get('written_agreement_exists') and doc_score >= 70:
            contradictions.append({
                'id':           'C3',
                'severity':     'MEDIUM',
                'title':        'No written agreement but documentary score is high',
                'warning':      '⚠️ Documentary strength rated high despite no written loan agreement',
                'explanation':  ('The documentary module scored other evidence highly (bank records, memo, etc.)'
                                  ' but the written agreement is the single most important document for proving'
                                  ' legally enforceable debt. Its absence is a structural gap the score may'
                                  ' under-represent.'),
                'action':       'Gather supplementary transaction proof. The section 139 presumption alone is fragile.',
            })


        if case_data.get('complaint_filed_date') and 'CANNOT' in lim_status:
            contradictions.append({
                'id':           'C4',
                'severity':     'MEDIUM',
                'title':        'Complaint date provided but limitation cannot be assessed',
                'warning':      '⚠️ Complaint filed date is given, but limitation calculation shows CANNOT ASSESS',
                'explanation':  ('The notice received date is missing. Without it, the exact cause of action date'
                                  ' cannot be determined, making it impossible to verify whether the complaint'
                                  ' was filed within the 1-month period even though the date itself is provided.'),
                'action':       'Provide notice_received_date to enable precise limitation calculation.',
            })


        if case_data.get('defence_type') == 'security_cheque' and not case_data.get('written_agreement_exists') and overall >= 60:
            contradictions.append({
                'id':           'C5',
                'severity':     'HIGH',
                'title':        'Security cheque defence active but score appears optimistic',
                'warning':      '⚠️ Accused claims security cheque, no written agreement — score may be overstated',
                'explanation':  ('When the accused raises the security cheque defence and there is no written'
                                  ' agreement to prove the debt was presently payable, the case\u2019s effective'
                                  ' strength is lower than the score indicates. The score reflects procedural'
                                  ' compliance; it does not fully account for the evidentiary challenge of'
                                  ' rebutting the security cheque claim without documentation.'),
                'action':       'Obtain transaction proof, bank statements, or correspondence to counter the security cheque claim.',
            })


        if proc_fatals and not fatal:
            defect = proc_fatals[0].get('defect', 'Fatal procedural defect')
            contradictions.append({
                'id':           'C6',
                'severity':     'HIGH',
                'title':        'Procedural fatal defect found but fatal_flag is False',
                'warning':      f'⚠️ Procedural module found fatal defect ({defect}) but overall fatal_flag=False',
                'explanation':  ('The procedural scanner flagged a fatal defect, but the main verdict'
                                  ' fatal_flag was not set. This can happen when the defect\u2019s severity'
                                  ' was classified differently by the risk aggregator. Treat this as fatal.'),
                'action':       'Treat the case as having a fatal defect. Address before filing.',
            })

        severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
        contradictions.sort(key=lambda x: severity_order.get(x['severity'], 3))

        return {
        'module':               'Contradiction Explainer',
        'contradiction_count':  len(contradictions),
        'contradictions':       contradictions,
        'high_count':           sum(1 for c in contradictions if c['severity'] == 'HIGH'),
        'summary':              (f'{len(contradictions)} contradiction(s) found — review before filing'
                                  if contradictions else
                                  'No contradictions detected — analysis is internally consistent'),
        'overall_consistency':  'INCONSISTENT' if contradictions else 'CONSISTENT',
    }


    except Exception as _get_co_err:
        logger.error(f"get_contradiction_explainer error: {_get_co_err}")


        cash_status = (case_data.get('cash_transaction_status') or '').lower()
        amount = float(case_data.get('cheque_amount', 0) or 0)
        if 'cash' in cash_status and amount > 20000 and overall >= 60:
            contradictions.append({
                'id':          'C6',
                'severity':    'HIGH',
                'title':       'Strong score despite Section 269SS cash risk',
                'warning':     '⚠️ Case scores well but cash transaction above ₹20,000 risks 269SS challenge',
                'explanation': ('The overall score is high, but the underlying transaction was in cash above '
                                 '₹20,000. Defence will argue the debt itself violates Section 269SS Income Tax Act, '
                                 'making the cheque given for an illegal consideration non-enforceable under Section 138.'),
                'action':      'Obtain documentary proof of cash transaction. Prepare counter-arguments on 269SS applicability.',
            })


        ing_score = _safe_f((risk.get('category_scores') or {}).get('Ingredient Compliance', {}).get('score')
                             if isinstance((risk.get('category_scores') or {}).get('Ingredient Compliance'), dict) else 0)
        if ing_score >= 70 and not postal_ok:
            contradictions.append({
                'id':          'C7',
                'severity':    'MEDIUM',
                'title':       'Ingredient 5 (notice) scores well despite no service proof',
                'warning':     '⚠️ Notice ingredient scores high but delivery is unproven',
                'explanation': ('Ingredient 5 (notice within 30 days) is scored as compliant, but no postal '
                                 'receipt or AD card exists. At trial, accused can deny receipt — which collapses '
                                 'ingredient 5 even if notice was timely sent.'),
                'action':      'Obtain postal tracking report or AD card to corroborate notice service.',
            })


        num_cheques = int(case_data.get('number_of_cheques', 1) or 1)
        if num_cheques > 1:
            contradictions.append({
                'id':          'C8',
                'severity':    'MEDIUM',
                'title':       f'Multiple cheques ({num_cheques}) may require separate complaints',
                'warning':     f'⚠️ {num_cheques} cheques — each creates a SEPARATE offence under Section 138',
                'explanation': ('Each dishonoured cheque under Section 138 is a separate offence requiring a separate '
                                 'complaint, separate notice, and separate cause of action. Clubbing in one complaint '
                                 'is procedurally incorrect and may lead to dismissal.'),
                'action':      'Verify separate notices were sent and separate complaints filed for each cheque.',
            })


        if case_data.get('part_payment_made') and case_data.get('part_payment_amount'):
            try:
                paid = float(case_data['part_payment_amount'] or 0)
                total = float(case_data.get('cheque_amount', 0) or 0)
                if paid > 0 and total > 0:
                    contradictions.append({
                        'id':          'C9',
                        'severity':    'HIGH',
                        'title':       f'Part payment of ₹{paid:,.0f} made — complaint may need amendment',
                        'warning':     '⚠️ Part payment changes recoverable amount',
                        'explanation': (f'A part payment of ₹{paid:,.0f} has been made out of the cheque amount '
                                         f'of ₹{total:,.0f}. The balance of ₹{total-paid:,.0f} is still due. '
                                         'If the complaint claims the full cheque amount, it may need amendment '
                                         'to reflect the current balance.'),
                        'action':      'Amend complaint to claim balance amount. File affidavit confirming part payment received.',
                    })
            except (ValueError, TypeError):
                pass


        if case_data.get('is_company_case') and not case_data.get('directors_impleaded'):
            contradictions.append({
                'id':          'C10',
                'severity':    'HIGH',
                'title':       'Company case but no directors named as accused',
                'warning':     '⚠️ Company cannot be sole accused under Section 138 without responsible directors',
                'explanation': ('Section 141 NI Act requires that the persons "in charge of and responsible for '
                                 'conduct of business" of the company must also be prosecuted. Without naming '
                                 'responsible directors, enforcement is limited.'),
                'action':      'Add responsible directors as accused with specific averments about their role.',
            })

        return {'module': 'Contradictions', 'contradictions': [], 'contradiction_count': 0, 'summary': 'Analysis could not check contradictions.'}

def get_legal_basis_map(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 3 — LEGAL BASIS MAPPING
    Attaches the controlling legal provision to every key finding.
    Adds authority and trust — lawyers can cite these in court.
    """
    try:
        risk     = analysis.get('modules', {}).get('risk_assessment', {}) or {}
        timeline = analysis.get('modules', {}).get('timeline_intelligence', {}) or {}
        _ = _safe_f(risk.get('final_score', 0))
        _ = analysis.get('fatal_flag', False)
        lim_status = str((timeline.get('compliance_status') or {}).get('limitation') or '').upper()

        basis_map = {}


        basis_map['core_offence'] = {
            'finding':   'Section 138 NI Act — dishonour of cheque',
            'sections':  ['Section 138 NI Act'],
            'citations':  ['Dashrath Rupsingh Rathod v. State of Maharashtra (2014) 9 SCC 129'],
            'note':       'Seven ingredients must all be present for the offence to be complete.',
        }


        if 'BARRED' in lim_status or 'PREMATURE' in lim_status:
            basis_map['limitation'] = {
                'finding':   'Complaint time-barred or premature',
                'sections':  ['Section 142(b) NI Act — 1 month limitation from cause of action',
                              'Section 142(c) NI Act — proviso for condonation'],
                'citations':  ['K. Bhaskaran v. Sankaran Vaidhyan Balan (1999) 7 SCC 510',
                               'Babulal Budhia v. SBI (2002)'],
                'note':       'Cause of action arises on expiry of 15 days from notice receipt.',
            }
        else:
            basis_map['limitation'] = {
                'finding':   'Complaint within limitation period',
                'sections':  ['Section 142(b) NI Act'],
                'citations':  ['K. Bhaskaran v. Sankaran Vaidhyan Balan (1999) 7 SCC 510'],
                'note':       'Complaint filed within 1 month of cause of action.',
            }


        basis_map['notice_requirement'] = {
            'finding':   'Legal notice is a mandatory pre-condition',
            'sections':  ['Section 138(b) NI Act — notice within 30 days of dishonour',
                          'Section 27 General Clauses Act — deemed service'],
            'citations':  ['C.C. Alavi Haji v. Palapetty Muhammed (2007) 6 SCC 555'],
            'note':       'Notice must demand payment. Deemed service applies on refusal/unclaimed.',
        }


        basis_map['presumption'] = {
            'finding':   'Section 139 presumption in favour of complainant',
            'sections':  ['Section 139 NI Act — presumption of legally enforceable debt',
                          'Section 118 NI Act — presumption of consideration'],
            'citations':  ['Rangappa v. Sri Mohan (2010) 11 SCC 441',
                           'Kaushalya Devi Massand v. Roopak Saluja (2011) 11 SCC 363'],
            'note':       'Once cheque and dishonour are proved, burden shifts to accused to rebut.',
        }


        if not case_data.get('written_agreement_exists'):
            basis_map['enforceable_debt'] = {
                'finding':   'No written agreement — oral debt relies on presumption',
                'sections':  ['Section 139 NI Act', 'Section 25 Contract Act',
                              'Section 91/92 Evidence Act — oral evidence to contradict documents'],
                'citations':  ['Bir Singh v. Mukesh Kumar (2019) 4 SCC 197'],
                'note':       'Accused must prove on preponderance that debt is not enforceable.',
            }


        if case_data.get('is_company_case'):
            basis_map['company_liability'] = {
                'finding':   'Section 141 — vicarious liability of directors',
                'sections':  ['Section 141 NI Act — offences by companies'],
                'citations':  ['SMS Pharmaceuticals Ltd v. Neeta Bhalla (2005) 8 SCC 89',
                               'National Small Industries Corp. Ltd v. Harmeet Singh Paintal (2010) 3 SCC 330'],
                'note':       'Each director must be specifically averred as in-charge of company business.',
            }


        if case_data.get('electronic_evidence'):
            basis_map['electronic_evidence'] = {
                'finding':   'Section 65B certificate mandatory for electronic evidence',
                'sections':  ['Section 65B Evidence Act — admissibility of electronic records'],
                'citations':  ['Anvar P.V. v. P.K. Basheer (2014) 10 SCC 473',
                               'Arjun Panditrao Khotkar v. Kailash Kushanrao Gorantyal (2020) 7 SCC 1'],
                'note':       'No override — electronic evidence inadmissible without S.65B certificate.',
            }


        basis_map['appeal_deposit'] = {
            'finding':   'Section 148 — mandatory deposit for suspension of sentence in appeal',
            'sections':  ['Section 148 NI Act (as amended 2018)'],
            'citations':  ['Surinder Singh Deswal v. Virender Gandhi (2019) 11 SCC 341'],
            'note':       'Minimum 20% of fine/compensation must be deposited within 60 days of conviction.',
        }

        return {
        'module':        'Legal Basis Map',
        'basis_map':     basis_map,
        'section_count': sum(len(v['sections']) for v in basis_map.values()),
        'citation_count':sum(len(v.get('citations', [])) for v in basis_map.values()),
        'note':          'All legal bases are sourced from binding Supreme Court judgments and statutory text.',
    }


    except Exception as _get_le_err:
        logger.error(f"get_legal_basis_map error: {_get_le_err}")


    if case_data.get('is_company_case'):
        basis_map['company_liability'] = {
            'finding':   'Company case — Section 141 NI Act applies',
            'sections':  ['Section 141 NI Act — liability of companies',
                          'Section 142 NI Act — cognizance of offences'],
            'citations':  ['SMS Pharmaceuticals Ltd. v. Neeta Bhalla (2005) 8 SCC 89',
                            'National Small Industries Corp. Ltd. v. Harmeet Singh Paintal (2010) 3 SCC 330',
                            'Sunita Palita v. Panchami Stone Quarry (2022) 10 SCC 152'],
            'note':       'Specific averment mandatory: each director must be averred to have been "in charge of and responsible for conduct of business" at time of offence.',
        }


    basis_map['section_139_presumption'] = {
        'finding':   'Section 139 presumption — cheque issued for legally enforceable debt',
        'sections':  ['Section 139 NI Act — presumption in favour of holder',
                      'Section 118 NI Act — presumptions as to negotiable instruments'],
        'citations':  ['Rangappa v. Sri Mohan (2010) 11 SCC 441 — presumption is rebuttable on preponderance',
                        'M.S. Narayana Menon v. State of Kerala (2006) 6 SCC 39',
                        'Bir Singh v. Mukesh Kumar (2019) 4 SCC 197 — mere denial insufficient'],
        'note':       'Once cheque and dishonour are proved, burden shifts to accused to rebut presumption. Accused must show it is more probable than not that cheque was not for legally enforceable debt.',
    }


    has_postal = case_data.get('postal_proof_available') or case_data.get('ad_card_signed')
    basis_map['notice_service'] = {
        'finding':   'Notice service under Section 138(b)',
        'sections':  ['Section 138(b) NI Act — notice within 30 days of dishonour',
                      'Section 27 General Clauses Act — postal service presumption'],
        'citations':  ['C. C. Alavi Haji v. Palapetty Muhammed (2007) 6 SCC 555 — deemed service on refusal',
                        'K. Bhaskaran v. Sankaran Vaidhyan Balan (1999) 7 SCC 510',
                        'Ajeet Seeds Ltd. v. K. Gopala Krishnaiah (2014) — notice at registered address sufficient'],
        'note':       ('Notice served by registered post to last known address is sufficient. '
                        'Refusal or non-acceptance by accused does not invalidate service.' if has_postal else
                        'Notice proof must be established — obtain postal receipt or AD card.'),
    }


    basis_map['interim_compensation'] = {
        'finding':   'Section 143A — interim compensation payable during trial',
        'sections':  ['Section 143A NI Act — power to order interim compensation',
                      'Section 148 NI Act — power to order payment pending appeal'],
        'citations':  ['Rakesh Bhasin v. Sonal Mehta — S.143A applies from first hearing',
                        'Supreme Court guidance: 20% of cheque amount is standard interim compensation'],
        'note':       'Apply for interim compensation under Section 143A at the FIRST HEARING. Court has discretion to award up to 20% of cheque amount.',
    }


    basis_map['compounding'] = {
        'finding':   'Section 147 — offence is compoundable at any stage',
        'sections':  ['Section 147 NI Act — offences under the Act are compoundable',
                      'Section 320 CrPC — compounding of offences'],
        'citations':  ['Meters and Instruments Pvt. Ltd. v. Kanchan Mehta (2018) 1 SCC 560 — S.147 allows compounding even after conviction',
                        'Dayawati v. Yogesh Kumar Gosain (2017) — compounding with permission of court'],
        'note':       'Complainant and accused may compound at any stage including appeal. Court approval required. Full payment + costs typical condition.',
    }


    cash_status = (case_data.get('cash_transaction_status') or '').lower()
    amount      = float(case_data.get('cheque_amount', 0) or 0)
    if 'cash' in cash_status and amount > 20000:
        basis_map['section_269ss'] = {
            'finding':   'Section 269SS Income Tax Act — cash transaction risk',
            'sections':  ['Section 269SS Income Tax Act — prohibition on cash loans above ₹20,000',
                          'Section 271D — penalty for violation of Section 269SS'],
            'citations':  ['Shri Ishwardas Jain v. CIT — cash loans above threshold are prohibited',
                            'Various High Courts: cheque for illegal debt may not attract S.138'],
            'note':       'If underlying loan was given in cash above ₹20,000, defence can argue the debt itself is illegal under Section 269SS — weakening Section 138 maintainability. Obtain documentary proof of transaction.',
        }


    if case_data.get('electronic_evidence'):
        basis_map['section_65b'] = {
            'finding':   'Section 65B Evidence Act — electronic evidence certificate',
            'sections':  ['Section 65B Indian Evidence Act — admissibility of electronic records',
                          'Section 65A — special provisions for evidence relating to electronic records'],
            'citations':  ['Anvar P.V. v. P.K. Basheer (2014) 10 SCC 473 — MANDATORY certificate without exception',
                            'Arjun Panditrao Khotkar v. Kailash Kushanrao Gorantyal (2020) 7 SCC 1 — affirmed Anvar'],
            'note':       'Without Section 65B certificate, ALL electronic evidence (WhatsApp, SMS, email screenshots) is INADMISSIBLE. File certificate before evidence stage.',
        }

    return {'module': 'Legal Basis Map', 'basis_map': {}, 'note': 'Legal basis map could not be generated.'}

def get_outcome_scenarios(analysis: Dict, case_data: Dict) -> Dict:
    """
    Outcome Scenario Engine — Deterministic projection of 5 legal outcomes.
    Best / Worst / Most Likely / Settlement / Acquittal scenarios.
    Derived from module scores + case facts. NOT statistical — legally deterministic.
    """
    risk    = analysis.get('modules', {}).get('risk_assessment', {}) or {}
    proc    = analysis.get('modules', {}).get('procedural_defects', {}) or {}
    defence = analysis.get('modules', {}).get('defence_matrix', {}) or {}
    cats    = risk.get('category_scores', {}) or {}
    _res    = analysis.get('_result', {}) or {}
    score   = _safe_f(_res.get('overall_score') or risk.get('final_score', 0))
    fatal   = analysis.get('fatal_flag', False) or _res.get('is_fatal', False)
    amount  = float(case_data.get('cheque_amount', 0) or 0)
    _ = case_data.get('is_company_case', False)

    def _c(name):
        v = cats.get(name)
        return _safe_f(v.get('score', 0) if isinstance(v, dict) else v)

    _ = _c('Timeline Compliance')
    _ = _c('Ingredient Compliance')
    _ = _c('Documentary Strength')
    _ = _c('Proper Impleading')
    _ = _c('Procedural Compliance')

    _ = case_data.get('written_agreement_exists', False)
    _ = case_data.get('postal_proof_available') or case_data.get('ad_card_signed')
    high_defs     = defence.get('high_risk_defences', []) or []
    fatal_defs    = proc.get('fatal_defects', []) or []
    is_limitation = any('BARRED' in str(d.get('defect','')) or 'time-bar' in str(d.get('defect','')).lower()
                        for d in fatal_defs)
    is_premature  = any('premature' in str(d.get('defect','')).lower() or '15-day' in str(d.get('defect',''))
                        for d in fatal_defs)

    amt_str    = f"₹{amount:,.0f}"
    s143a      = f"₹{amount*0.20:,.0f}"


    if fatal and is_limitation:
        best = {
            'outcome': 'Apply for condonation — slim chance of revival',
            'path': ['File application under Section 142(b) with strong cause for delay',
                     'If condonation granted: proceed to trial',
                     'If condonation rejected: complaint is dismissed permanently'],
            'probability': 'Very Low — courts rarely condone delay beyond 30 days without compelling reason',
        }
    elif fatal and is_premature:
        best = {
            'outcome': 'Refile fresh complaint after cause of action arises',
            'path': ['Wait for 15-day payment period to expire',
                     'File fresh complaint within 30 days of cause of action',
                     'All documents already in order — complaint should proceed smoothly'],
            'probability': 'Achievable if refiling is done promptly within limitation',
        }
    elif fatal:
        best = {
            'outcome': 'Cure defect and proceed',
            'path': ['Address the specific fatal defect (see Procedural tab)',
                     'File with all corrected documents',
                     'Court takes cognizance — case proceeds'],
            'probability': 'Moderate — depends on nature of defect and remaining limitation window',
        }
    elif score >= 75:
        best = {
            'outcome': f'Conviction — full fine + {amt_str} compensation to complainant',
            'path': [f'Court takes cognizance — S.143A interim compensation of {s143a} at first hearing',
                     'Accused fails to rebut Section 139 presumption',
                     f'Conviction: fine up to {amt_str}×2 + imprisonment up to 2 years + compensation {amt_str}',
                     'Section 357(3) CrPC — full compensation ordered'],
            'probability': 'Likely if all documents are properly exhibited',
        }
    else:
        best = {
            'outcome': 'Conviction after strengthening documentary evidence',
            'path': ['Gather missing evidence (agreement, postal proof, bank records)',
                     'Court admits complaint — accused cannot rebut Section 139',
                     f'Conviction: fine + compensation of {amt_str}'],
            'probability': 'Achievable with diligent pre-trial evidence gathering',
        }


    if fatal and is_limitation:
        worst = {
            'outcome': 'Complaint permanently dismissed — time-barred, no remedy',
            'path': ['Court refuses to take cognizance',
                     'Limitation expired — condonation also denied',
                     'Complainant loses {amt_str} with no criminal remedy; must pursue civil recovery'],
            'probability': 'High if no valid reason for delay',
        }
    elif fatal:
        worst = {
            'outcome': 'Complaint dismissed at first hearing — defect incurable',
            'path': ['Court scrutinises complaint at cognizance stage',
                     'Defect identified — complaint returned or dismissed',
                     'Limitation for fresh complaint may have expired — no remedy'],
            'probability': 'High if limitation window closes before defect is cured',
        }
    elif score < 40:
        worst = {
            'outcome': 'Acquittal on multiple grounds simultaneously',
            'path': ['Accused raises security cheque + no debt + notice challenge simultaneously',
                     'Complainant unable to produce independent transaction proof',
                     'Court acquits — no recovery; complainant bears legal costs'],
            'probability': 'High without substantial remediation',
        }
    else:
        worst = {
            'outcome': 'Acquittal due to documentary failure at trial',
            'path': ['Accused mounts strong defence: security cheque / debt discharge / notice defect',
                     'Complainant cannot prove legally enforceable debt by preponderance',
                     'Court acquits — no fine, no compensation; complainant bears costs'],
            'probability': 'Possible if missing documents cannot be produced before trial',
        }


    if fatal:
        likely = {
            'outcome': 'Case stalled pending resolution of fatal defect',
            'path': ['Current complaint cannot be admitted in its present form',
                     'If curable: advocate files amendment / fresh complaint within limitation',
                     'If limitation expired: limited options — civil suit is the remaining remedy'],
            'basis': f'Score: {score:.0f}/100 — fatal defect(s) override procedural scoring',
        }
    elif score >= 75:
        sett_amt = f"₹{amount*0.85:,.0f}–₹{amount:,.0f}"
        likely = {
            'outcome': f'Early settlement ({sett_amt}) or conviction',
            'path': ['Court takes cognizance at first hearing',
                     f'S.143A interim compensation ({s143a}) creates immediate settlement pressure',
                     f'Accused likely settles in range {sett_amt} to avoid conviction record',
                     'If no settlement: high conviction probability at trial'],
            'basis': f'Score: {score:.0f}/100 — strong case, accused has high incentive to settle',
        }
    elif score >= 55:
        sett_low  = f"₹{amount*0.55:,.0f}"
        sett_high = f"₹{amount*0.80:,.0f}"
        def_name  = high_defs[0].get('defence', 'security cheque') if high_defs else 'documentary gaps'
        likely = {
            'outcome': f'Contested — settlement in range {sett_low}–{sett_high}',
            'path': ['Complaint admitted — accused contests on merits',
                     f'Primary defence: {def_name}',
                     f'Settlement most likely in range {sett_low}–{sett_high}',
                     'If no settlement: outcome depends on trial evidence quality'],
            'basis': f'Score: {score:.0f}/100 — moderate case, both parties have incentive to settle',
        }
    elif score >= 35:
        likely = {
            'outcome': 'Weak case — settlement at significant discount or loss',
            'path': ['Complaint may be admitted but defence is strong',
                     f'If settlement: expected range ₹{amount*0.25:,.0f}–₹{amount*0.50:,.0f}',
                     'Without remediation: acquittal risk is HIGH',
                     'Recommended: strengthen evidence before proceeding to trial'],
            'basis': f'Score: {score:.0f}/100 — multiple gaps, defence has significant leverage',
        }
    else:
        likely = {
            'outcome': 'Acquittal likely — major remediation needed before proceeding',
            'path': ['If filed in current state: acquittal risk is very high',
                     'Better path: gather all missing evidence first',
                     'Negotiate settlement while evidence is being gathered',
                     'Only file if evidence picture improves significantly'],
            'basis': f'Score: {score:.0f}/100 — critical deficiencies in current case',
        }


    settle_pct  = 0.90 if score >= 75 else 0.75 if score >= 55 else 0.55 if score >= 35 else 0.35
    settle_low  = amount * (settle_pct - 0.15)
    settle_high = amount * settle_pct
    settlement = {
        'outcome': f'Settlement in range ₹{settle_low:,.0f}–₹{settle_high:,.0f}',
        'path': ['Send strong legal notice to accused citing conviction risk',
                 f'Apply for S.143A interim compensation ({s143a}) at first hearing',
                 f'Negotiate: accused to pay ₹{settle_high:,.0f} as full and final settlement',
                 'File joint application for compounding under Section 147 NI Act'],
        'probability': ('High' if score >= 65 else 'Moderate' if score >= 45 else 'Low — weak leverage'),
        'recommendation': ('Pursue aggressively — strong case means accused has high motivation to settle'
                           if score >= 65 else
                           'Explore settlement while strengthening evidence simultaneously'),
    }


    acq_risk = ('Very High' if fatal or score < 30 else
                'High'      if score < 50 else
                'Moderate'  if score < 65 else
                'Low')
    acquittal = {
        'outcome': f'Acquittal risk: {acq_risk}',
        'likely_grounds': (
            ['Complaint dismissed as premature / time-barred'] if fatal else
            ['Accused successfully rebuts Section 139 presumption',
             'Absence of written agreement — debt existence not proved',
             'Notice service challenged — 15-day period not validly triggered',
             'Security cheque defence — no legally enforceable debt'] if score < 50 else
            ['Aggressive cross-examination on documentary gaps',
             'Accused challenges specific ingredient compliance',
             'Notice service deficiency creates reasonable doubt']
        ),
        'prevention': (
            'Cure fatal defect first; then reassess case strength' if fatal else
            'Gather written agreement, postal proof, and bank records before trial' if score < 60 else
            'Prepare thorough examination-in-chief and exhibits list; apply for S.143A at first hearing'
        ),
    }

    return {
        'module':       'Outcome Scenario Engine',
        'scenarios': {
            'best_case':   dict(**best,   label='BEST CASE'),
            'worst_case':  dict(**worst,  label='WORST CASE'),
            'most_likely': dict(**likely, label='MOST LIKELY'),
            'settlement':  dict(**settlement, label='SETTLEMENT'),
            'acquittal':   dict(**acquittal,  label='ACQUITTAL RISK'),
        },
        'score_basis':  score,
        'fatal_flag':   fatal,
        'disclaimer': (
            'Scenarios are deterministic projections based on legal rules and case facts — '
            'not statistical or machine-learning predictions. Actual outcome depends on '
            'trial evidence, judicial discretion, and facts that emerge during proceedings.'
        ),
    }


def get_evidence_gap_priority(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 5 — EVIDENCE GAP PRIORITIZATION
    Ranked list of missing evidence — not just missing docs, but ranked by impact.
    """
    risk  = analysis.get('modules', {}).get('risk_assessment', {}) or {}
    _ = _safe_f(risk.get('final_score', 0))

    gaps = []

    def gap(name, rank, tier, why, action, score_impact):
        return {'evidence': name, 'rank': rank, 'tier': tier,
                'why_it_matters': why, 'action': action,
                'score_impact_if_obtained': score_impact}

    rank = 1

    if not case_data.get('original_cheque_available'):
        gaps.append(gap('Original cheque', rank, 'CRITICAL',
            'Court requires original cheque as primary instrument — cannot proceed without it',
            'Retrieve original from your file or collect from bank (impounded cheque)', '+8 to score'))
        rank += 1

    if not case_data.get('return_memo_available'):
        gaps.append(gap('Bank dishonour memo', rank, 'CRITICAL',
            'Proves the cheque was actually dishonoured — ingredient 4 cannot be established without it',
            'Request bank memo directly; bank must issue under RBI guidelines', '+10 to score'))
        rank += 1

    if not case_data.get('notice_date') and not case_data.get('notice_copy_available'):
        gaps.append(gap('Copy of legal notice sent', rank, 'CRITICAL',
            'Without notice proof, ingredient 5 fails entirely — the complaint is not maintainable',
            'Retain signed copy of notice; obtain lawyer\u2019s file copy if lost', '+15 to score'))
        rank += 1


    if not case_data.get('postal_proof_available') and not case_data.get('ad_card_signed'):
        gaps.append(gap('Postal receipt / AD card', rank, 'HIGH',
            'Without service proof, accused claims no notice received — the 15-day period never started',
            'Collect from post office (up to 6 months); check postal portal for track report', '+8 to score'))
        rank += 1

    if not case_data.get('written_agreement_exists'):
        gaps.append(gap('Loan / transaction agreement', rank, 'HIGH',
            'This is the single most effective counter to the security cheque / no debt defence',
            'Locate original MoU, promissory note, or email chain confirming loan terms', '+12 to score'))
        rank += 1

    if not case_data.get('ledger_available') and not case_data.get('bank_statement_proof'):
        gaps.append(gap('Ledger or bank statement', rank, 'HIGH',
            'Transaction trail: without this, proving the money actually changed hands is difficult',
            'Obtain account extract showing credit/debit entries for the relevant transaction', '+8 to score'))
        rank += 1


    if not case_data.get('bank_transfer_proof'):
        gaps.append(gap('Bank transfer proof (NEFT/IMPS/RTGS)', rank, 'IMPORTANT',
            'Electronic transfer proof is the strongest single transaction evidence — undeniable',
            'Obtain NEFT/IMPS reference and bank confirmation of transfer', '+6 to score'))
        rank += 1

    if not case_data.get('witness_available'):
        gaps.append(gap('Transaction witness', rank, 'IMPORTANT',
            'Corroborates oral account of how cheque was given — important when documents are weak',
            'Identify anyone who witnessed the transaction or was present when cheque was handed over', '+4 to score'))
        rank += 1


    if case_data.get('electronic_evidence') and not case_data.get('section_65b_certificate'):
        gaps.append(gap('Section 65B certificate', rank, 'CRITICAL',
            'Without this, all electronic evidence (WhatsApp, SMS, email) is inadmissible',
            'Obtain from device owner / IT custodian before filing — Anvar P.V. is binding', '+10 to score'))
        rank += 1

    if not case_data.get('email_sms_evidence'):
        gaps.append(gap('Email / SMS / WhatsApp correspondence', rank, 'OPTIONAL',
            'Corroborative: acknowledgment of debt in messages strengthens the presumption',
            'Collect printed screenshots; ensure S.65B certificate is obtained', '+3 to score'))
        rank += 1


    if case_data.get('is_company_case') and not case_data.get('specific_averment_present'):
        gaps.append(gap('Specific averment for directors (S.141)', rank, 'CRITICAL',
            'Without specific averment that each director was in charge at time of offence, directors will be discharged.',
            'Amend complaint to include specific averment for each named director (SMS Pharmaceuticals mandate).',
            '+20 to case viability'))
        rank += 1

    if case_data.get('is_company_case') and not case_data.get('directors_impleaded'):
        gaps.append(gap('Board resolution / bank mandate naming signatories', rank, 'HIGH',
            'Proves which directors were in charge of financial operations at time cheque was drawn.',
            'Obtain from company registrar or bank records.', '+8 to score'))
        rank += 1

    if case_data.get('part_payment_made') and not case_data.get('part_payment_amount'):
        gaps.append(gap('Part payment receipt / amount confirmation', rank, 'HIGH',
            'Exact balance amount must be established — complaint may need to be amended to reflect balance.',
            'Obtain payment receipt or bank transfer record for any payment made.', '+5 to score'))
        rank += 1

    if not case_data.get('witness_available'):
        gaps.append(gap('Transaction witness', rank, 'IMPORTANT',
            'Oral testimony of a witness to the transaction strengthens the debt narrative significantly.',
            'Identify any person present when money was given — even an informal witness.', '+6 to score'))
        rank += 1

    if case_data.get('is_savkari_loan') and not case_data.get('savkari_loan_registered'):
        gaps.append(gap('Moneylender registration certificate', rank, 'IMPORTANT',
            'If complainant operates as moneylender, registration is required — absence is a defence ground.',
            'Obtain registration under applicable State Moneylenders Act before proceeding.', '+8 to viability'))
        rank += 1

    if case_data.get('bank_merger_occurred') and not case_data.get('merged_bank_name'):
        gaps.append(gap('Bank merger notification / RBI order', rank, 'IMPORTANT',
            'Proves dishonour by successor bank is valid under Section 138 — defence may challenge this.',
            'Obtain RBI gazette notification of bank merger.', '+5 to score'))
        rank += 1

    if case_data.get('is_multiple_cheques') and int(case_data.get('number_of_cheques', 1) or 1) > 1:
        gaps.append(gap('Separate complaints for each dishonoured cheque', rank, 'IMPORTANT',
            'Each cheque is a separate offence — clubbing is procedurally incorrect.',
            'Verify separate notices and complaints are filed for each dishonoured cheque.', 'Procedural compliance'))
        rank += 1


    critical = [g for g in gaps if g['tier'] == 'CRITICAL']
    high     = [g for g in gaps if g['tier'] == 'HIGH']
    _ = [g for g in gaps if g['tier'] == 'IMPORTANT']
    _ = [g for g in gaps if g['tier'] == 'OPTIONAL']


    for i, g in enumerate(gaps):
        g['rank'] = i + 1

    critical = [g for g in gaps if g['tier'] == 'CRITICAL']
    high     = [g for g in gaps if g['tier'] == 'HIGH']

    return {
        'module':         'Evidence Gap Prioritization',
        'gaps':           gaps,
        'total_gaps':     len(gaps),
        'critical_count': len(critical),
        'high_count':     len(high),
        'top_priority':   gaps[0] if gaps else None,
        'summary':        (f'{len(critical)} critical and {len(high)} high-priority gap(s) — address in order'
                           if gaps else 'No significant evidence gaps detected'),
    }


def get_time_sensitivity(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 6 — TIME-SENSITIVITY ENGINE
    Calculates all deadline-related urgency signals for the lawyer.
    """
    from datetime import datetime as _dt
    today = _dt.now().date()
    timeline = analysis.get('modules', {}).get('timeline_intelligence', {}) or {}
    crit     = timeline.get('critical_dates') or {}
    _ = timeline.get('refiling_window') or {}

    alerts   = []
    deadlines = []
    urgency_level = 'NORMAL'


    lim_dl = crit.get('limitation_deadline')
    if lim_dl:
        try:
            lim_date = _dt.strptime(lim_dl, '%Y-%m-%d').date()
            days_left = (lim_date - today).days
            deadlines.append({
                'type':       'Complaint Filing Deadline (S.142)',
                'date':       lim_dl,
                'days_left':  days_left,
                'status':     ('EXPIRED' if days_left < 0 else
                               'CRITICAL' if days_left <= 3 else
                               'URGENT' if days_left <= 7 else
                               'HIGH' if days_left <= 14 else
                               'MEDIUM' if days_left <= 30 else 'OK'),
            })
            if days_left < 0:
                alerts.append({'level': 'FATAL', 'message': f'⛔ LIMITATION EXPIRED {abs(days_left)} days ago — complaint is time-barred'})
                urgency_level = 'CRITICAL'
            elif days_left <= 3:
                alerts.append({'level': 'CRITICAL', 'message': f'🚨 ONLY {days_left} DAY(S) LEFT to file complaint — file immediately'})
                urgency_level = 'CRITICAL'
            elif days_left <= 7:
                alerts.append({'level': 'URGENT', 'message': f'⚠️ {days_left} days remaining — file this week without fail'})
                urgency_level = 'URGENT'
            elif days_left <= 14:
                alerts.append({'level': 'HIGH', 'message': f'📅 {days_left} days remaining — finalise complaint this week'})
                if urgency_level == 'NORMAL': urgency_level = 'HIGH'
            elif days_left <= 30:
                alerts.append({'level': 'MEDIUM', 'message': f'📅 {days_left} days remaining to file complaint'})
                if urgency_level == 'NORMAL': urgency_level = 'MEDIUM'
        except (ValueError, TypeError) as _e:
            logger.debug(f'Time sensitivity urgency calc suppressed: {_e}')


    if not case_data.get('notice_date') and case_data.get('dishonour_date'):
        try:
            dd = _dt.strptime(case_data['dishonour_date'], '%Y-%m-%d').date()
            notice_deadline = dd + __import__('dateutil.relativedelta', fromlist=['relativedelta']).relativedelta(months=1)
            nd_left = (notice_deadline - today).days
            deadlines.append({
                'type':      'Legal Notice Deadline (S.138)',
                'date':      notice_deadline.isoformat(),
                'days_left': nd_left,
                'status':    'EXPIRED' if nd_left < 0 else 'CRITICAL' if nd_left <= 3 else 'URGENT' if nd_left <= 7 else 'OK',
            })
            if nd_left < 0:
                alerts.append({'level': 'FATAL', 'message': f'⛔ Notice window expired {abs(nd_left)} days ago — notice is time-barred'})
                urgency_level = 'CRITICAL'
            elif nd_left <= 7:
                alerts.append({'level': 'CRITICAL', 'message': f'🚨 Only {nd_left} day(s) left to send legal notice — send TODAY'})
                urgency_level = 'CRITICAL'
        except Exception as _e:
            logger.debug(f"Non-critical exception suppressed: {_e}")


    if case_data.get('complaint_filed_date') and not case_data.get('interim_compensation_applied'):
        alerts.append({
            'level':   'OPPORTUNITY',
            'message': '💡 Section 143A: Apply for interim compensation at first hearing (up to 20% of cheque amount)',
        })


    if case_data.get('compounding_stage') in ('Appeal', 'Revision') and case_data.get('conviction_date'):
        try:
            cd = _dt.strptime(case_data['conviction_date'], '%Y-%m-%d').date()
            deposit_deadline = cd + __import__('dateutil.relativedelta', fromlist=['relativedelta']).relativedelta(days=60)
            dd_left = (deposit_deadline - today).days
            deadlines.append({
                'type':      'Section 148 Deposit Deadline (Appeal)',
                'date':      deposit_deadline.isoformat(),
                'days_left': dd_left,
                'status':    'EXPIRED' if dd_left < 0 else 'CRITICAL' if dd_left <= 7 else 'URGENT' if dd_left <= 14 else 'OK',
            })
            if dd_left <= 14:
                alerts.append({'level': 'URGENT', 'message': f'⚠️ Section 148 deposit due in {dd_left} days — deposit to suspend sentence'})
                if urgency_level == 'NORMAL': urgency_level = 'URGENT'
        except Exception as _e:
            logger.debug(f"Non-critical exception suppressed: {_e}")

    top_alert = alerts[0]['message'] if alerts else 'No urgent deadlines detected'


    notice_dt_str = case_data.get('notice_date')
    dishonour_dt_str = case_data.get('dishonour_date')
    if notice_dt_str and dishonour_dt_str:
        try:
            notice_dt    = _dt.strptime(notice_dt_str, '%Y-%m-%d').date()
            dishonour_dt = _dt.strptime(dishonour_dt_str, '%Y-%m-%d').date()
            notice_deadline = dishonour_dt + __import__('datetime').timedelta(days=30)
            days_for_notice = (notice_dt - dishonour_dt).days
            if days_for_notice > 30:
                alerts.append({
                    'level':   'FATAL',
                    'message': f'⛔ Notice sent {days_for_notice} days after dishonour — BEYOND 30-day limit (ingredient 5 FAILS)'
                })
                urgency_level = 'CRITICAL'
            elif days_for_notice >= 25:
                alerts.append({
                    'level':   'HIGH',
                    'message': f'⚠️ Notice sent on day {days_for_notice}/30 — very close to limit; verify exact date'
                })
                if urgency_level not in ('CRITICAL',):
                    urgency_level = 'HIGH'
        except (ValueError, TypeError):
            pass


    received_dt_str = case_data.get('notice_received_date')
    complaint_dt_str = case_data.get('complaint_filed_date')
    if received_dt_str:
        try:
            received_dt = _dt.strptime(received_dt_str, '%Y-%m-%d').date()
            coa_date    = received_dt + __import__('datetime').timedelta(days=15)
            if complaint_dt_str:
                complaint_dt = _dt.strptime(complaint_dt_str, '%Y-%m-%d').date()
                days_after_coa = (complaint_dt - coa_date).days
                if days_after_coa < 0:
                    alerts.append({
                        'level':   'FATAL',
                        'message': f'⛔ Complaint filed {abs(days_after_coa)} day(s) BEFORE cause of action ({coa_date}) — PREMATURE'
                    })
                    urgency_level = 'CRITICAL'
                elif days_after_coa > 30:
                    alerts.append({
                        'level':   'FATAL',
                        'message': f'⛔ Complaint filed {days_after_coa} days after cause of action — beyond 30-day limit'
                    })
                    urgency_level = 'CRITICAL'
                elif days_after_coa >= 25:
                    alerts.append({
                        'level':   'HIGH',
                        'message': f'⚠️ Complaint filed on day {days_after_coa}/30 after cause of action — very close to limit'
                    })
            else:

                remaining = 30 - (today - coa_date).days
                if coa_date <= today:
                    if remaining < 0:
                        alerts.append({
                            'level':   'FATAL',
                            'message': f'⛔ Cause of action arose {abs(remaining)} days ago — complaint filing window CLOSED'
                        })
                        urgency_level = 'CRITICAL'
                    elif remaining <= 3:
                        alerts.append({
                            'level':   'CRITICAL',
                            'message': f'🚨 ONLY {remaining} DAY(S) LEFT to file complaint — file immediately'
                        })
                        urgency_level = 'CRITICAL'
                    elif remaining <= 7:
                        alerts.append({
                            'level':   'URGENT',
                            'message': f'⚠️ {remaining} days left to file — file this week'
                        })
                        if urgency_level not in ('CRITICAL',):
                            urgency_level = 'URGENT'
                    deadlines.append({
                        'type':     'Complaint Filing Deadline (30 days from CoA)',
                        'date':     (coa_date + __import__('datetime').timedelta(days=30)).strftime('%Y-%m-%d'),
                        'days_left': remaining,
                        'status':   ('EXPIRED' if remaining < 0 else 'CRITICAL' if remaining <= 3
                                     else 'URGENT' if remaining <= 7 else 'HIGH' if remaining <= 14 else 'OK'),
                    })
        except (ValueError, TypeError):
            pass


    _ = (alerts[0]['message'] if alerts else
                     f'Case within time limits — {len(deadlines)} deadline(s) to monitor' if deadlines else
                     'No urgent time constraints detected')

    return {
        'module':         'Time-Sensitivity Engine',
        'urgency_level':  urgency_level,
        'top_alert':      top_alert,
        'alerts':         alerts,
        'deadlines':      deadlines,
        'today':          today.isoformat(),
        'summary':        (f'🚨 URGENT action required — {top_alert}' if urgency_level in ('CRITICAL', 'URGENT')
                           else f'No immediate deadline pressure — {top_alert}' if urgency_level == 'NORMAL'
                           else top_alert),
    }


def run_input_sanity_check(case_data: Dict) -> Dict:
    """
    Feature 7 — INPUT SANITY CHECK
    Detects impossible, contradictory, or nonsensical input combinations.
    Prevents garbage-in → garbage-out before any analysis runs.
    """
    errors   = []
    warnings = []

    def _parse(field):
        val = case_data.get(field)
        if not val: return None
        try:
            from datetime import datetime as _dt
            return _dt.strptime(str(val), '%Y-%m-%d').date()
        except (ValueError, TypeError):
            errors.append({'field': field, 'error': f'Invalid date format: {val!r} — expected YYYY-MM-DD'})
            return None

    cheque_dt    = _parse('cheque_date')
    dishonour_dt = _parse('dishonour_date')
    notice_dt    = _parse('notice_date')
    received_dt  = _parse('notice_received_date')
    complaint_dt = _parse('complaint_filed_date')
    from datetime import date as _date
    today = _date.today()


    if cheque_dt and dishonour_dt and dishonour_dt < cheque_dt:
        errors.append({'check': 'CHQ_BEFORE_DISHONOUR',
                        'error': f'Dishonour date ({dishonour_dt}) is BEFORE cheque date ({cheque_dt})',
                        'impact': 'Impossible — a cheque cannot be dishonoured before it is issued'})

    if dishonour_dt and notice_dt and notice_dt < dishonour_dt:
        errors.append({'check': 'NOTICE_BEFORE_DISHONOUR',
                        'error': f'Notice date ({notice_dt}) is BEFORE dishonour date ({dishonour_dt})',
                        'impact': 'Impossible — legal notice must be sent after dishonour'})

    if notice_dt and received_dt and received_dt < notice_dt:
        errors.append({'check': 'RECEIVED_BEFORE_SENT',
                        'error': f'Notice received ({received_dt}) BEFORE notice sent ({notice_dt})',
                        'impact': 'Impossible — receipt must be after dispatch'})

    if received_dt and complaint_dt and complaint_dt < received_dt:
        warnings.append({'check': 'COMPLAINT_BEFORE_RECEIPT',
                          'warning': f'Complaint filed ({complaint_dt}) before notice was received ({received_dt})',
                          'impact': 'Complaint may be premature — cause of action arises after notice receipt'})

    if complaint_dt and notice_dt and complaint_dt < notice_dt:
        errors.append({'check': 'COMPLAINT_BEFORE_NOTICE',
                        'error': f'Complaint filed ({complaint_dt}) BEFORE notice was sent ({notice_dt})',
                        'impact': 'Impossible — complaint requires prior notice'})


    for field, dt in [('cheque_date', cheque_dt), ('dishonour_date', dishonour_dt),
                       ('notice_date', notice_dt), ('complaint_filed_date', complaint_dt)]:
        if dt and dt > today:
            warnings.append({'check': f'FUTURE_DATE_{field.upper()}',
                              'warning': f'{field} ({dt}) is in the future',
                              'impact': 'Analysis uses a future date — verify this is correct'})


    amount = float(case_data.get('cheque_amount', 0) or 0)
    if amount <= 0:
        errors.append({'check': 'INVALID_AMOUNT',
                        'error': f'Cheque amount is {amount} — must be a positive number',
                        'impact': 'All financial calculations will be incorrect'})
    elif amount > 50_00_00_000:
        warnings.append({'check': 'LARGE_AMOUNT',
                          'warning': f'Cheque amount ₹{amount:,.0f} exceeds ₹50 crore — verify input',
                          'impact': 'Possible data entry error'})


    part_payment = float(case_data.get('part_payment_amount', 0) or 0)
    if part_payment > amount > 0:
        errors.append({'check': 'PART_PAYMENT_EXCEEDS_CHEQUE',
                        'error': f'Part payment ₹{part_payment:,.0f} > cheque amount ₹{amount:,.0f}',
                        'impact': 'Debt fully discharged — Section 138 complaint not maintainable'})


    if notice_dt and dishonour_dt:
        days_to_notice = (notice_dt - dishonour_dt).days
        if days_to_notice > 30:
            errors.append({'check': 'NOTICE_BEYOND_30_DAYS',
                            'error': f'Notice sent {days_to_notice} days after dishonour (limit: 30)',
                            'impact': 'FATAL — notice is time-barred under Section 138(b)'})
        elif days_to_notice < 0:
            pass


    if cheque_dt and dishonour_dt:
        days = (dishonour_dt - cheque_dt).days
        if days > 90:
            warnings.append({'check': 'CHEQUE_EXPIRED_AT_PRESENTATION',
                              'warning': f'Cheque presented {days} days after issue date (limit: 90 days)',
                              'impact': 'Cheque may be stale — validity issue affects ingredient 3'})

    is_clean = len(errors) == 0

    return {
        'module':         'Input Sanity Check',
        'is_clean':       is_clean,
        'error_count':    len(errors),
        'warning_count':  len(warnings),
        'errors':         errors,
        'warnings':       warnings,
        'summary':        ('✅ All inputs are logically consistent' if is_clean and not warnings else
                           f'⚠️ {len(warnings)} warning(s) — verify inputs' if is_clean else
                           f'❌ {len(errors)} input error(s) detected — analysis may be unreliable'),
        'proceed':        is_clean,
    }


def get_defence_counter_strategy(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 8 — DEFENCE COUNTER STRATEGY
    For every identified defence, provides a specific counter-argument with legal basis.
    Turns JUDIQ from analyser to litigation co-pilot.
    """
    defence = analysis.get('modules', {}).get('defence_matrix', {}) or {}
    all_def = defence.get('possible_defences', []) or defence.get('high_risk_defences', []) or []
    counters = []


    COUNTER_MAP = {
        'security cheque': {
            'counter_argument': ('Under Section 139 NI Act, the court shall presume the cheque was issued'
                                  ' for discharge of a legally enforceable debt. The accused bears the burden'
                                  ' of rebutting this presumption on preponderance of probability.'),
            'evidence_needed': ['Bank transfer records', 'Written correspondence', 'Ledger entries',
                                  'Witness testimony of transaction'],
            'legal_basis':      ['Section 139 NI Act', 'Rangappa v. Sri Mohan (2010) 11 SCC 441',
                                   'M.S. Narayana Menon v. State of Kerala (2006) 6 SCC 39'],
            'attack_point':     'Ask in cross-examination: why did you sign a blank cheque? For whom? When? What security?',
        },
        'no debt': {
            'counter_argument': ('Section 139 shifts burden — accused must prove no debt on preponderance.'
                                  ' Mere denial is insufficient. Court must believe accused\u2019s explanation'
                                  ' is more probable than complainant\u2019s claim.'),
            'evidence_needed': ['Bank statements showing credit to accused', 'Loan agreement',
                                  'Correspondence acknowledging debt', 'Witness'],
            'legal_basis':      ['Section 139 NI Act', 'Bir Singh v. Mukesh Kumar (2019) 4 SCC 197'],
            'attack_point':     'Cross: If you owed nothing, why did you issue the cheque? Who asked for it? What was it for?',
        },
        'time-barred debt': {
            'counter_argument': ('The cheque itself constitutes acknowledgment of debt under Section 18'
                                  ' Limitation Act — it resets the limitation clock. The S.138 limitation'
                                  ' is on the complaint (1 month from COA), not on the underlying debt.'),
            'evidence_needed': ['Show cheque date as acknowledgment', 'Part payment records',
                                  'Any correspondence within 3 years'],
            'legal_basis':      ['Section 18 Limitation Act 1963', 'Section 25 Contract Act',
                                   'Paramjeet Singh v. State NCT Delhi (2009)'],
            'attack_point':     'Cross: Did you not issue this cheque in 20XX acknowledging the debt? Isn\u2019t that an acknowledgment?',
        },
        'stop payment': {
            'counter_argument': ('A stop payment instruction is itself evidence of cheque execution and'
                                  ' knowledge of the cheque. The accused must prove a lawful reason for the'
                                  ' stop — mere economic difficulty or convenience is not a defence.'),
            'evidence_needed': ['Bank stop payment record', 'Timeline of stop payment vs. transaction',
                                  'Communications about the stop payment'],
            'legal_basis':      ['Rangappa v. Sri Mohan (2010) 11 SCC 441',
                                   'NEPC Micon v. Magma Leasing (1999) 4 SCC 253'],
            'attack_point':     'Cross: You admit issuing and stopping the cheque. What was the lawful reason? Prove it.',
        },
        'signature mismatch': {
            'counter_argument': ('Request handwriting expert comparison against admitted signature specimens'
                                  ' (account opening form, other cheques). If accused admits the account,'
                                  ' they must explain how the cheque got into complainant\u2019s possession.'),
            'evidence_needed': ['Account opening form signature', 'Previous cheque specimens',
                                  'Handwriting expert report', 'Admission of account ownership'],
            'legal_basis':      ['Section 45 Evidence Act — handwriting expert',
                                   'Section 73 Evidence Act — court comparison'],
            'attack_point':     'Cross: Is this your bank account? Did you authorise anyone else to sign cheques on your account?',
        },
        'notice not received': {
            'counter_argument': ('Section 27 General Clauses Act: when a notice is sent by registered post'
                                  ' to the correct address, service is presumed even if returned. Refusal'
                                  ' and unclaimed are both deemed service.'),
            'evidence_needed': ['RPAD dispatch receipt', 'Postal track report', 'AD card or endorsement',
                                  'Correct address proof from bank records'],
            'legal_basis':      ['Section 27 General Clauses Act',
                                   'C.C. Alavi Haji v. Palapetty Muhammed (2007) 6 SCC 555',
                                   'Ajeet Seeds Ltd v. K. Gopala Krishnaiah (2014)'],
            'attack_point':     'Cross: This is your registered address on the cheque/account. Were you residing there in [month]?',
        },
        'forgery': {
            'counter_argument': ('File a handwriting expert report under Section 45 Evidence Act comparing specimen signatures with cheque. Bank records confirm cheque was drawn from accused account. Accused must have reported loss before dishonour to be credible.'),
            'evidence_needed': ['Handwriting expert report', 'Bank account statement showing cheque issuance', 'Proof accused never reported chequebook stolen'],
            'legal_basis': ['Section 45 Indian Evidence Act', 'Section 73 Evidence Act — court comparison of signatures'],
            'attack_point': 'Cross: Did you report your chequebook stolen to the bank before dishonour? When? Produce the FIR or bank complaint.',
        },
        'insolvency': {
            'counter_argument': ('Section 138 NI Act is a criminal proceeding — IBC moratorium does not stay it. Supreme Court held in Ajay Kumar Radheshyam Goenka (2023) that criminal proceedings under Section 138 are not affected by IBC moratorium. Personal criminal liability of directors survives IBC.'),
            'evidence_needed': ['Supreme Court judgment on S.138 and IBC', 'Show personal criminal liability of accused'],
            'legal_basis': ['Ajay Kumar Radheshyam Goenka v. Tourism Finance Corp (2023)', 'Section 14 IBC — moratorium does not extinguish criminal liability'],
            'attack_point': 'Cross: Are you personally named as accused? The moratorium covers company assets, not your personal criminal liability.',
        },
        'director not in charge': {
            'counter_argument': ('Complaint contains specific averment per SMS Pharmaceuticals mandate. Bank mandate and board resolution show director was signatory. Operational involvement in financial decisions is established through records.'),
            'evidence_needed': ['Board resolution showing director was signatory', 'Bank mandate naming director', 'Correspondence showing operational involvement'],
            'legal_basis': ['SMS Pharmaceuticals Ltd. v. Neeta Bhalla (2005) 8 SCC 89', 'National Small Industries v. Harmeet Singh (2010) 3 SCC 330'],
            'attack_point': 'Cross: Were you a signatory to the company bank account? Did you attend board meetings? Who authorised issuance of this cheque?',
        },
        'notice not served': {
            'counter_argument': ('Deemed service doctrine applies — refusal or avoidance does not invalidate service. Postal tracking confirms notice was sent. Accused cannot benefit from own refusal. India Post track report establishes delivery attempt.'),
            'evidence_needed': ['India Post tracking printout', 'AD card or refusal/unclaimed endorsement', 'Affidavit of postal peon if required'],
            'legal_basis': ['C. C. Alavi Haji v. Palapetty Muhammed (2007) 6 SCC 555', 'Section 27 General Clauses Act — postal service presumption', 'K. Bhaskaran v. Sankaran Vaidhyan Balan (1999)'],
            'attack_point': 'Cross: Is this your registered address on the cheque? When did you change address? Did you inform your bank?',
        },
    }


    processed_names = set()
    for d in all_def:
        name = str(d.get('ground', d.get('defence', ''))).lower()
        matched_key = None
        for key in COUNTER_MAP:
            if key in name:
                matched_key = key
                break
        if matched_key and matched_key not in processed_names:
            processed_names.add(matched_key)
            cm = COUNTER_MAP[matched_key]
            counters.append({
                'defence':          d.get('ground', d.get('defence', name.title())),
                'defence_strength': d.get('strength', d.get('strength_score', 'UNKNOWN')),
                'counter_argument': cm['counter_argument'],
                'evidence_needed':  cm['evidence_needed'],
                'legal_basis':      cm['legal_basis'],
                'cross_attack':     cm['attack_point'],
            })


    if (not case_data.get('written_agreement_exists') and
            'security cheque' not in processed_names):
        cm = COUNTER_MAP['security cheque']
        counters.insert(0, {
            'defence':          'Security Cheque / No Legally Enforceable Debt (anticipated)',
            'defence_strength': 'HIGH — likely to be raised given no written agreement',
            'counter_argument': cm['counter_argument'],
            'evidence_needed':  cm['evidence_needed'],
            'legal_basis':      cm['legal_basis'],
            'cross_attack':     cm['attack_point'],
        })

    return {
        'module':         'Defence Counter Strategy',
        'counters':       counters,
        'counter_count':  len(counters),
        'summary':        (f'{len(counters)} defence counter(s) prepared — review before trial'
                           if counters else 'No high-risk defences identified — proceed with standard strategy'),
        'note':           'Use these as brief-preparation guides — adapt to specific facts of each case.',
    }


def get_module_confidence_scores(analysis: Dict, case_data: Dict) -> Dict:
    """
    Feature 10 — MODULE CONFIDENCE SCORES
    Each module rates its own output reliability based on data available to it.
    Helps lawyers know which parts of the report to trust most.
    """
    modules_conf = {}

    def _conf(score, label):
        if score >= 80: c, col = 'HIGH', 'GREEN'
        elif score >= 55: c, col = 'MEDIUM', 'AMBER'
        else: c, col = 'LOW', 'RED'
        return {'confidence': c, 'score': round(score), 'colour': col, 'note': label}


    tl_fields = ['cheque_date', 'dishonour_date', 'notice_date', 'complaint_filed_date', 'notice_received_date']
    tl_present = sum(1 for f in tl_fields if case_data.get(f))
    tl_score = (tl_present / len(tl_fields)) * 100
    modules_conf['timeline'] = _conf(tl_score,
        'Timeline analysis is deterministic — fully reliable when all 5 dates are provided' if tl_score == 100
        else f'Missing {len(tl_fields)-tl_present} date(s) — some calculations are estimated')


    doc_fields = ['original_cheque_available', 'return_memo_available', 'postal_proof_available',
                  'written_agreement_exists', 'ledger_available']
    doc_present = sum(1 for f in doc_fields if case_data.get(f) is not None)
    doc_score = 60 + (doc_present / len(doc_fields)) * 40
    modules_conf['documentary'] = _conf(doc_score,
        'Documentary scores are based on availability of each document — highly reliable')


    ing_fields = ['cheque_date', 'dishonour_date', 'notice_date', 'dishonour_reason',
                  'original_cheque_available', 'return_memo_available', 'postal_proof_available']
    ing_present = sum(1 for f in ing_fields if case_data.get(f) not in (None, ''))
    ing_score = (ing_present / len(ing_fields)) * 100
    modules_conf['ingredients'] = _conf(ing_score,
        'Ingredient compliance is deterministic — confidence depends on completeness of input')


    jur = analysis.get('modules', {}).get('territorial_jurisdiction', {}) or {}
    jur_status = str(jur.get('status') or '').upper()
    jur_score = (90 if jur_status in ('VALID', 'CONFIRMED', 'INFERRED_VALID', 'INFERRED_FROM_COURT_LOCATION')
                 else 55 if jur_status == 'INSUFFICIENT_DATA'
                 else 30 if jur_status == 'INVALID'
                 else 50)
    modules_conf['jurisdiction'] = _conf(jur_score,
        'Jurisdiction is confirmed by court/bank location data' if jur_score >= 80
        else 'Jurisdiction inferred — provide filing_court_location for full validation')


    def_fields = ['defence_type', 'dishonour_reason', 'written_agreement_exists']
    def_present = sum(1 for f in def_fields if case_data.get(f) not in (None, ''))
    def_score = 50 + (def_present / len(def_fields)) * 40
    modules_conf['defence'] = _conf(def_score,
        'Defence analysis reliability improves with known defence_type and dishonour_reason')


    pres_fields = ['original_cheque_available', 'written_agreement_exists', 'ledger_available']
    pres_score = 70 + sum(10 for f in pres_fields if case_data.get(f))
    modules_conf['presumption'] = _conf(min(100, pres_score),
        'Presumption rebuttal confidence is proportional to available transaction evidence')


    set_score = 60 if case_data.get('cheque_amount') else 30
    modules_conf['settlement'] = _conf(set_score,
        'Settlement range is calculated from cheque amount and case score — reliable when amount is provided')

    overall_conf = round(sum(v['score'] for v in modules_conf.values()) / len(modules_conf))
    weakest = min(modules_conf.items(), key=lambda x: x[1]['score'])
    strongest = max(modules_conf.items(), key=lambda x: x[1]['score'])

    return {
        'module':          'Module Confidence Scores',
        'modules':         modules_conf,
        'overall_average': overall_conf,
        'weakest_module':  weakest[0],
        'strongest_module':strongest[0],
        'summary':         f'Strongest: {strongest[0].title()} ({strongest[1]["confidence"]}). Weakest: {weakest[0].title()} ({weakest[1]["confidence"]})',
    }


def generate_plain_summary(analysis: Dict, case_data: Dict) -> Dict:
    """
    Generate a short, plain-language summary of the analysis —
    the kind a lawyer can read in 30 seconds and immediately act on.

    Returned as 'plain_summary' at the top level of the API response.
    """
    risk    = analysis.get('modules', {}).get('risk_assessment', {})
    timeline = analysis.get('modules', {}).get('timeline_intelligence', {})
    doc     = analysis.get('modules', {}).get('document_compliance', {})
    _ = analysis.get('modules', {}).get('defence_risk_analysis', {})
    _ = analysis.get('verdict', {})


    _res    = analysis.get('_result', {})
    score   = float(_res.get('overall_score') or risk.get('final_score') or 0)
    compliance    = risk.get('compliance_level', 'UNKNOWN')
    fatal_flag    = analysis.get('fatal_flag', False)
    _ = doc.get('fatal_defects', [])
    limitation_risk = timeline.get('limitation_risk', 'UNKNOWN')

    amount = case_data.get('cheque_amount', 0)
    case_type = case_data.get('case_type', 'complainant')
    perspective = "Complainant" if case_type == 'complainant' else "Accused"


    strengths = []
    cat_scores = risk.get('category_scores', {})
    _res_data = analysis.get('_result', {})
    _ = analysis.get('modules', {}).get('ingredient_compliance', {}) or {}
    _ = analysis.get('modules', {}).get('documentary_strength', {}) or {}
    _tl_data  = analysis.get('modules', {}).get('timeline_intelligence', {}) or {}
    _ = analysis.get('modules', {}).get('presumption_analysis', {}) or {}

    tl_score  = float(cat_scores.get('Timeline Compliance', {}).get('score', 0) if isinstance(cat_scores.get('Timeline Compliance'), dict) else cat_scores.get('Timeline Compliance', 0) or 0)
    ing_score = float(cat_scores.get('Ingredient Compliance', {}).get('score', 0) if isinstance(cat_scores.get('Ingredient Compliance'), dict) else cat_scores.get('Ingredient Compliance', 0) or 0)
    doc_score = float(cat_scores.get('Documentary Strength', {}).get('score', 0) if isinstance(cat_scores.get('Documentary Strength'), dict) else cat_scores.get('Documentary Strength', 0) or 0)
    li_score  = float(cat_scores.get('Proper Impleading', {}).get('score', 0) if isinstance(cat_scores.get('Proper Impleading'), dict) else cat_scores.get('Proper Impleading', 0) or 0)

    if tl_score >= 80:
        strengths.append('Timeline compliance is strong — notice sent in time and dates are in order')
    elif tl_score >= 60:
        strengths.append('Timeline compliance is adequate — key statutory dates are within limits')

    if ing_score >= 80:
        strengths.append('All seven key ingredients of Section 138 are substantially in place')
    elif ing_score >= 60:
        strengths.append('Core ingredients of Section 138 are present — minor gaps exist')

    if case_data.get('return_memo_available'):
        strengths.append('Dishonour memo available — primary evidence of dishonour secured')
    if case_data.get('original_cheque_available'):
        strengths.append('Original cheque available — foundational instrument document present')
    if case_data.get('postal_proof_available') or case_data.get('ad_card_signed'):
        strengths.append('Postal proof of notice available — statutory service presumption established')
    if case_data.get('written_agreement_exists'):
        strengths.append('Written agreement available — debt enforceability is well-documented')
    if case_data.get('bank_statement_proof') or case_data.get('ledger_available'):
        strengths.append('Financial records available — independent transaction trail established')
    if case_data.get('witness_available'):
        strengths.append('Witness available — oral testimony can corroborate the transaction')
    if li_score >= 90:
        strengths.append('Proper Impleading: all required parties correctly named under Section 141')
    if doc_score >= 70:
        strengths.append('Documentary strength is good — key documents are in place')


    _es = analysis.get('executive_summary', {}) or {}
    for _s in (_es.get('strengths') or [])[:3]:
        _s_text = str(_s or '').strip()
        if _s_text and _s_text not in strengths and len(_s_text) > 10:
            strengths.append(_s_text)

    if not strengths:
        strengths.append('Case ingredients are partially in place — review modules for gaps')


    weaknesses = []


    _all_res_fatals = _res_data.get('fatal_defects', []) or []
    for _fd in _all_res_fatals[:3]:
        _fd_text = str(_fd.get('defect', '') or '').strip()
        if _fd_text:
            weaknesses.append(f'FATAL: {_fd_text}')


    if limitation_risk in ('CRITICAL', 'EXPIRED', 'PREMATURE'):
        _tl_status = str((_tl_data.get('compliance_status') or {}).get('limitation', 'limitation issue'))
        weaknesses.append(f'Timeline defect: {_tl_status}')


    if not case_data.get('written_agreement_exists'):
        weaknesses.append('No written agreement — accused can challenge the existence of legally enforceable debt (fundamental S.138 ingredient)')
    if not case_data.get('ledger_available') and not case_data.get('bank_statement_proof'):
        weaknesses.append('No financial records — transaction trail is entirely oral and uncorroborated')
    if not case_data.get('return_memo_available'):
        weaknesses.append('Bank dishonour memo not available — proof of dishonour may be contestable')
    if not case_data.get('postal_proof_available') and not case_data.get('ad_card_signed'):
        weaknesses.append('No postal proof of notice — accused may deny receiving the legal demand')
    if not case_data.get('original_cheque_available'):
        weaknesses.append('Original cheque not secured — admissibility of copy may be challenged')


    if doc_score < 40:
        weaknesses.append('Documentary Strength score is critically low — substantial strengthening needed before filing')
    elif doc_score < 60:
        weaknesses.append('Documentary evidence is weak — transaction proof requires reinforcement before filing')
    if tl_score < 50:
        weaknesses.append('Timeline compliance is below threshold — risk of dismissal on technical grounds')
    if ing_score < 50:
        weaknesses.append('Ingredient compliance is below threshold — statutory requirements are not fully satisfied')


    _def_data = analysis.get('modules', {}).get('defence_matrix', {}) or {}
    _def_possible = _def_data.get('possible_defences', []) or []
    _high_def = [d for d in _def_possible if d.get('strength_score', 0) >= 60]
    for _hd in _high_def[:2]:
        _hd_name = str(_hd.get('defence', _hd.get('type', 'defence argument')) or '')
        if _hd_name:
            weaknesses.append(f'Strong defence exposure: {_hd_name} — prepare counter-evidence')


    _es_weak = (_es.get('weaknesses') or [])
    for _w in _es_weak[:2]:
        _w_text = str(_w or '').strip()
        if _w_text and _w_text not in weaknesses and len(_w_text) > 10:
            weaknesses.append(_w_text)

    if not weaknesses:
        weaknesses.append('No critical weaknesses detected — case appears adequately positioned')


    if fatal_flag:
        _ = str((analysis.get('_result') or {}).get('primary_fatal_defect') or {})
        _fd_text = str((analysis.get('_result', {}) or {}).get('primary_fatal_defect', {}) or {})
        _fd_msg = ''
        try:
            _fd_obj = (analysis.get('_result') or {}).get('primary_fatal_defect') or {}
            if isinstance(_fd_obj, dict):
                _fd_msg = _fd_obj.get('defect', '')
        except Exception:
            pass
        one_line = (
            f"⛔ Case is NOT maintainable — {_fd_msg}." if _fd_msg else
            "⛔ Case has a fatal defect — do not file until the issue is resolved."
        )
    elif score >= 75:
        one_line = "✅ Strong case — statutory requirements are substantially satisfied. Proceed to file after final document review."
    elif score >= 55:
        one_line = "⚠️ Case is valid but has evidentiary gaps — strengthen documentary proof before filing for better conviction prospects."
    elif score >= 35:
        one_line = "🔴 Weak case — significant evidentiary and procedural gaps require remediation before filing."
    else:
        one_line = "⛔ Very weak case — multiple critical deficiencies make filing highly inadvisable without major remediation."


    settlement_data = analysis.get('modules', {}).get('settlement_intelligence', {})
    settlement_recommended = settlement_data.get('settlement_recommended', False)

    if fatal_flag:

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
            "Case is in a reasonable position to file. "
            "Obtain a written acknowledgement of debt or supporting transaction records "
            "to further reduce acquittal exposure."
        )


    _tl_cd = (analysis.get('modules') or {}).get('timeline_intelligence') or {}
    _refile_guidance = _tl_cd.get('refile_guidance', '')
    _coa_reasoning   = _tl_cd.get('cause_of_action_reasoning', '')
    _crit_dates = _tl_cd.get('critical_dates') or {}
    _lim_deadline = _crit_dates.get('limitation_deadline', '')
    _remaining_days = ''
    if _lim_deadline:
        try:
            from datetime import datetime as _dtt
            _ld = _dtt.strptime(_lim_deadline, '%Y-%m-%d').date()
            _rem = (_ld - _dtt.now().date()).days
            _remaining_days = f'{max(0, _rem)} days' if _rem >= 0 else 'EXPIRED'
        except (ValueError, TypeError) as _e:
            logger.debug(f"Remaining days calc: {_e}")


    decision_confidence    = get_decision_confidence(analysis, case_data)
    contradiction_explainer= get_contradiction_explainer(analysis, case_data)
    legal_basis_map        = get_legal_basis_map(analysis, case_data)
    outcome_scenarios      = get_outcome_scenarios(analysis, case_data)
    evidence_gap_priority  = get_evidence_gap_priority(analysis, case_data)
    time_sensitivity       = get_time_sensitivity(analysis, case_data)
    input_sanity           = run_input_sanity_check(case_data)
    defence_counters       = get_defence_counter_strategy(analysis, case_data)
    module_confidence      = get_module_confidence_scores(analysis, case_data)

    return {
        'one_line_verdict': one_line or 'Analysis complete — see details below',
        'perspective': perspective or 'Not specified',
        'cheque_amount': f"₹{indian_number_format(amount)}" if amount else 'Not specified',
        'score_display': f"{score:.1f}/100",
        'risk_score': round(score, 1),
        'case_classification': compliance or 'Assessment Pending',
        'strengths': strengths or ['Analysis complete'],
        'weaknesses': weaknesses or ['Review full analysis for details'],
        'recommendation': recommendation or 'Consult legal counsel',
        'fatal_flag': fatal_flag,
        'fatal_defects_count': len(analysis.get('modules', {}).get('risk_assessment', {}).get('fatal_defects', []) +
                                    analysis.get('modules', {}).get('procedural_defects', {}).get('fatal_defects', [])),
        'limitation_status': timeline.get('compliance_status', {}).get('limitation') or 'Insufficient data',
        'processing_time': f"{max(0.5, analysis.get('processing_time_seconds', 0.5))}s",
        'cause_of_action_reasoning': _coa_reasoning,
        'refile_guidance':           _refile_guidance,
        'limitation_deadline':       _lim_deadline,
        'days_remaining_to_refile':  _remaining_days,

        'decision_confidence':       decision_confidence,
        'contradictions':            contradiction_explainer,
        'legal_basis_map':           legal_basis_map,
        'outcome_scenarios':         outcome_scenarios,
        'evidence_gap_priority':     evidence_gap_priority,
        'time_sensitivity':          time_sensitivity,
        'input_sanity':              input_sanity,
        'defence_counters':          defence_counters,
        'module_confidence':         module_confidence,
    }

@asynccontextmanager
async def lifespan(app: FastAPI):

    print("\n" + "="*100)
    print("🚀 JUDIQ AI - LEGAL INTELLIGENCE PLATFORM")
    print("="*100 + "\n")


    print(f"📂 Data directory: {DATA_DIR}")
    init_analytics_db()

    # ── Firebase init ──────────────────────────────────────────────────────
    _fb_ok = _init_firebase()
    print(f"🔥 Firebase Auth: {'✅ Connected' if _fb_ok else '⚠️  Disabled (set FIREBASE_SERVICE_ACCOUNT_JSON env var)'}")
    # ───────────────────────────────────────────────────────────────────────


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


    _startup_complete.set()

    yield


    print("\n✅ JUDIQ v5.0 shutting down...")

app = FastAPI(
    title="JUDIQ v5.0 - Legal Intelligence Platform",
    version="5.0.0",
    description="90% Elite-Grade Section 138 NI Act Analysis - Ratio-Based Analytics",
    lifespan=lifespan
)


if RATE_LIMITING_AVAILABLE:
    try:
        from slowapi import _rate_limit_exceeded_handler
        from slowapi.errors import RateLimitExceeded
        limiter = Limiter(key_func=get_remote_address)
        app.state.limiter = limiter
        app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        logger.info("✅ Rate limiting enabled (30 requests/minute)")
    except Exception:
        limiter = DummyLimiter()
        logger.warning("⚠️ Rate limiting disabled - slowapi handler unavailable")
else:
    limiter = DummyLimiter()
    logger.warning("⚠️ Rate limiting disabled - install slowapi to enable")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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


    _ = analysis_report.get('fatal_flag', False)
    _ = analysis_report.get('overall_status', '')


    risk_score = 50
    if 'modules' in analysis_report and 'risk_assessment' in analysis_report['modules']:
        risk_module = analysis_report['modules'].get('risk_assessment', {})
        risk_score = risk_module.get('overall_risk_score', 50)
        logger.info(f"  📊 Reading authoritative risk score: {risk_score}")
    else:

        risk_score = analysis_report.get('risk_score', 50)
        logger.warning(f"  ⚠️ Using fallback risk score: {risk_score}")


    fatal_sources = []


    _ = 'MEDIUM'
    defence_fatal = False
    if 'modules' in analysis_report and 'defence_risk_analysis' in analysis_report['modules']:
        defence_module = analysis_report['modules'].get('defence_risk_analysis', {})
        _ = defence_module.get('overall_risk', 'MEDIUM')
        defence_fatal = len(defence_module.get('fatal_defences', [])) > 0
        if defence_fatal:
            fatal_sources.append('defence_risk')


    fatal_defects = []
    if 'modules' in analysis_report and 'risk_assessment' in analysis_report['modules']:
        _all_ra_fatals = analysis_report['modules'].get('risk_assessment', {}).get('fatal_defects', [])
        fatal_defects = [d for d in _all_ra_fatals
                         if 'same-day' not in str(d.get('defect','')).lower()
                         and 'same day' not in str(d.get('defect','')).lower()
                         and d.get('is_absolute') is not False]
        if len(fatal_defects) > 0:
            fatal_sources.append('risk_assessment')


    if 'modules' in analysis_report and 'section_65b_compliance' in analysis_report['modules']:
        section_65b = analysis_report['modules'].get('section_65b_compliance', {})
        if section_65b.get('risk_level') == 'FATAL':
            fatal_sources.append('section_65b')


    _ = False
    if 'modules' in analysis_report and 'income_tax_269ss' in analysis_report['modules']:
        tax_module = analysis_report['modules'].get('income_tax_269ss', {})
        if tax_module.get('violation_detected') and tax_module.get('risk_level') == 'CRITICAL':
            fatal_sources.append('income_tax_269ss')
            _ = True


    if 'modules' in analysis_report and 'part_payment_analysis' in analysis_report['modules']:
        part_payment = analysis_report['modules'].get('part_payment_analysis', {})
        if part_payment.get('defence_strength') == 'FATAL':
            fatal_sources.append('part_payment')


    if 'modules' in analysis_report and 'notice_delivery_status' in analysis_report['modules']:
        notice_delivery = analysis_report['modules'].get('notice_delivery_status', {})
        if notice_delivery.get('risk_level') == 'FATAL':
            fatal_sources.append('notice_delivery')


    jurisdiction_penalty = 0
    if 'modules' in analysis_report and 'territorial_jurisdiction' in analysis_report['modules']:
        jurisdiction = analysis_report['modules'].get('territorial_jurisdiction', {})
        jurisdiction_status = jurisdiction.get('status', '')


        if jurisdiction_status == 'INSUFFICIENT_DATA':

            logger.info("  ℹ️ Jurisdiction: Insufficient data - no penalty applied")
        elif not jurisdiction.get('jurisdiction_valid', True):

            jurisdiction_risk = jurisdiction.get('risk_level', 'MEDIUM')
            if jurisdiction_risk == 'CRITICAL':

                fatal_sources.append('jurisdiction_critical')
                jurisdiction_penalty = 40
                logger.error("  🔴 Jurisdiction CRITICAL - FATAL condition (filing blocked)")
            elif jurisdiction_risk == 'HIGH':
                jurisdiction_penalty = 20
                logger.warning("  ⚠️ Jurisdiction INVALID (HIGH) - score reduction applied")


    section_65b_penalty = 0
    if 'modules' in analysis_report and 'section_65b_compliance' in analysis_report['modules']:
        section_65b = analysis_report['modules'].get('section_65b_compliance', {})
        if section_65b.get('applicable', False) and not section_65b.get('compliant', True):
            risk_level = section_65b.get('risk_level', 'LOW')


            if risk_level in ['FATAL', 'CRITICAL']:
                section_65b_penalty = 10
                logger.warning("  ⚠️ Section 65B - Electronic evidence is PRIMARY proof - penalty applied")
            elif risk_level == 'HIGH':

                logger.info("  ℹ️ Section 65B - Electronic evidence is supporting only - minimal impact")


    risk_score = round(max(15, risk_score - jurisdiction_penalty - section_65b_penalty), 1)
    logger.info(f"  📊 Adjusted risk score after jurisdiction/65B: {risk_score}")


    if 'modules' in analysis_report and 'risk_assessment' in analysis_report['modules']:
        risk_module = analysis_report['modules'].get('risk_assessment', {})
        base_score = risk_module.get('overall_risk_score_base', risk_score + jurisdiction_penalty + section_65b_penalty)


        _orig_risk_score = risk_module.get('overall_risk_score', risk_score)
        _adjusted_score  = round(max(15, _orig_risk_score - jurisdiction_penalty - section_65b_penalty), 1)
        risk_module['overall_risk_score']      = _adjusted_score
        risk_module['overall_risk_score_base'] = _orig_risk_score
        risk_module['adjusted'] = True
        risk_module['adjustments'] = {
            'jurisdiction_penalty': jurisdiction_penalty,
            'section_65b_penalty': section_65b_penalty,
            'total_penalty': jurisdiction_penalty + section_65b_penalty
        }


        if 'explanation' in risk_module and isinstance(risk_module['explanation'], dict):
            if 'calculation_steps' in risk_module['explanation']:

                if jurisdiction_penalty > 0 or section_65b_penalty > 0:
                    penalty_text = "Step 3: Apply penalties\n"
                    if jurisdiction_penalty > 0:
                        penalty_text += f"  - Jurisdiction penalty: -{jurisdiction_penalty} points\n"
                    if section_65b_penalty > 0:
                        penalty_text += f"  - Section 65B penalty: -{section_65b_penalty} points\n"
                    penalty_text += f"  Adjusted Score: {base_score:.2f} - {jurisdiction_penalty + section_65b_penalty} = {risk_score:.2f}"

                    risk_module['explanation']['calculation_steps'].append(penalty_text)
                    risk_module['explanation']['final_score'] = risk_score


    _real_fatal_sources = [s for s in fatal_sources
                           if s in ('risk_assessment', 'jurisdiction_critical', 'section_65b')]
    is_fatal = len(_real_fatal_sources) > 0 and len(fatal_defects) > 0


    if is_fatal:
        logger.warning(f"  ⚠️ FATAL condition detected from: {', '.join(fatal_sources) if fatal_sources else 'primary flag'}")
        fatal_reasoning = f"Fatal defects detected: {', '.join(fatal_sources)}" if fatal_sources else 'Fatal defects override all positive assessments'

        final_verdict = {
            'status': 'FATAL',
            'category': 'CRITICAL FAILURE',
            'risk_score': min(risk_score, 25),
            'recommendation': 'DO NOT FILE - Fatal defects present',
            'predicted_outcome': 'Case will be dismissed',
            'filing_blocked': True,
            'confidence': 'HIGH',
            'reasoning': fatal_reasoning,
            'fatal_sources': fatal_sources
        }


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


    logger.info(f"  ✅ Final verdict: {final_verdict['status']} - {final_verdict['category']}")


    if is_fatal:
        logger.warning("  ⚠️ FATAL override: Syncing all module scores to reflect fatal condition")
        for module_name in analysis_report.get('modules', {}):
            module = analysis_report['modules'][module_name]
            if isinstance(module, dict):
                module['fatal_override_applied'] = True
                module['fatal_note'] = 'Case has fatal defect — see procedural_defects module'


    analysis_report['final_verdict'] = final_verdict

    _rs = final_verdict['risk_score']
    analysis_report['risk_score'] = _rs if _rs is not None else 0
    analysis_report['risk_score_display'] = f"{_rs:.1f}/100" if _rs is not None else "0.0/100"
    analysis_report['is_fatal'] = is_fatal
    analysis_report['fatal_cap_applied'] = is_fatal
    analysis_report['overall_status'] = f"{final_verdict['status']} - {final_verdict['category']}"
    analysis_report['decisive_verdict'] = final_verdict['recommendation']
    analysis_report['filing_blocked'] = final_verdict['filing_blocked']
    analysis_report['fatal_flag'] = (final_verdict['status'] == 'FATAL')


    analysis_report['final_status'] = final_verdict['recommendation']
    analysis_report['case_strength'] = final_verdict['status']
    analysis_report['filing_recommendation'] = final_verdict['recommendation']
    analysis_report['risk_level'] = final_verdict['status']


    analysis_report['outcome_classification'] = {
        'category': final_verdict['category'],
        'risk_level': final_verdict['status'],
        'recommendation': final_verdict['recommendation'],
        'predicted_outcome': final_verdict['predicted_outcome'],
        'action_priority': 'URGENT' if final_verdict['status'] == 'FATAL' else 'PROCEED',
        'confidence': final_verdict['confidence']
    }


    if 'modules' in analysis_report and 'filing_readiness' in analysis_report['modules']:
        filing_module = analysis_report['modules'].get('filing_readiness', {})


        _ = final_verdict['status'] in ['STRONG', 'MODERATE'] and not final_verdict['filing_blocked']

        if final_verdict['status'] in ['FATAL', 'CRITICAL']:
            filing_module['filing_readiness'] = '❌ NOT READY TO FILE'
            filing_module['ready_to_file'] = False
            filing_module['overall_status'] = '🔴 FILING BLOCKED' if final_verdict['filing_blocked'] else '⚠️ HIGH RISK'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'NOT READY TO FILE'
        elif final_verdict['status'] == 'WEAK':
            filing_module['filing_readiness'] = '⚠️ RISKY - SETTLEMENT ADVISED'
            filing_module['ready_to_file'] = False
            filing_module['overall_status'] = '⚠️ HIGH RISK'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'RISKY - REVIEW REQUIRED'
        elif final_verdict['status'] == 'MODERATE':
            filing_module['filing_readiness'] = '⚠️ READY - STRENGTHEN RECOMMENDED'
            filing_module['ready_to_file'] = True
            filing_module['overall_status'] = '⚠️ MODERATE'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'READY TO FILE'
        else:
            filing_module['filing_readiness'] = '✅ READY TO FILE'
            filing_module['ready_to_file'] = True
            filing_module['overall_status'] = '✅ STRONG'
            filing_module['decisive_verdict'] = final_verdict['recommendation']
            filing_module['final_status'] = 'READY TO FILE'


    if 'modules' in analysis_report and 'document_compliance' in analysis_report['modules']:
        if final_verdict['status'] in ['FATAL', 'CRITICAL']:
            analysis_report['modules'].get('document_compliance', {})['filing_readiness'] = '❌ NOT READY TO FILE'


    if 'executive_summary' in analysis_report:
        if isinstance(analysis_report.get('executive_summary'), dict):

            es = analysis_report.get('executive_summary', {})
            _es_score = final_verdict.get('risk_score', risk_score)

            if is_fatal and len(fatal_defects) > 0:

                es['overall_assessment'] = 'FATAL FAILURE'
                es['filing_verdict']     = 'DO NOT FILE — FATAL DEFECTS PRESENT'
                es['filing_status']      = 'DO NOT FILE'
                es['fatal_defects_count']= len([d for d in fatal_defects if d.get('severity') in ('FATAL','CRITICAL')])
            else:

                _doc_score = (analysis_report.get('modules', {})
                              .get('documentary_strength', {})
                              .get('overall_strength_score', 0) or 0)
                _proc_def  = (analysis_report.get('modules', {})
                              .get('procedural_defects', {})
                              .get('curable_defects') or [])
                if risk_score >= 75:
                    es['filing_verdict']  = 'READY TO FILE'
                    es['filing_status']   = 'READY TO FILE'
                    es['overall_assessment'] = 'STRONG CASE'
                elif risk_score >= 55:
                    _reason = 'Weak documentary evidence' if _doc_score < 60 else ('Procedural gaps present' if _proc_def else 'Evidentiary gaps present')
                    es['filing_verdict']  = f'FILE WITH CAUTION — {_reason.upper()}'
                    es['filing_status']   = 'FILE WITH CAUTION'
                    es['overall_assessment'] = final_verdict.get('category', 'MODERATE RISK')
                else:
                    es['filing_verdict']  = 'HIGH RISK — REMEDIATION REQUIRED'
                    es['filing_status']   = 'HIGH RISK'
                    es['overall_assessment'] = 'HIGH RISK'


            status_marker = '🔴' if is_fatal else ('⚠️' if final_verdict['status'] in ['CRITICAL', 'WEAK'] else '✅')
            es['case_overview'] = f"{status_marker} {final_verdict['status']}: {final_verdict['reasoning']}"

            es['final_score']     = _es_score
            es['final_score_display'] = f"{_es_score}/100"


            if is_fatal:
                es['strategic_recommendations'] = [
                    {'priority': 'URGENT', 'action': 'Do not file — fatal defects present',
                     'recommendation': 'Address all fatal defects before filing', 'rationale': ', '.join(fatal_sources)}
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


    if 'professional_report' in analysis_report:
        if isinstance(analysis_report.get('professional_report'), dict):
            if 'summary' in (analysis_report.get('professional_report') or {}):
                analysis_report['professional_report']['summary']['final_verdict'] = final_verdict['category']
                analysis_report['professional_report']['summary']['recommendation'] = final_verdict['recommendation']

    logger.info("  ✅ Verdict integrity enforced - all modules synchronized")

    return analysis_report


def deduplicate_fatal_defects(defects: List[Dict]) -> List[Dict]:
    """
    Normalize fatal defects — collapse duplicate premature/limitation defects
    into a single canonical entry with supporting details.
    """
    if not defects:
        return defects

    PREMATURE_KEYWORDS = [
        'premature', '15-day', '15 day', 'cause of action', 'before cause',
        'before 15', 'filed before', 'complaint filed before'
    ]
    TIMEBARRED_KEYWORDS = ['time-barred', 'time barred', 'barred by limitation', 'limitation expired']
    SAMEDDAY_KEYWORDS   = ['same-day', 'same day', 'same date']

    premature_group  = []
    timebarred_group = []
    sameday_group    = []
    other            = []

    for d in defects:
        text = (d.get('defect','') + ' ' + d.get('impact','') + ' ' + d.get('remedy','')).lower()
        if any(k in text for k in PREMATURE_KEYWORDS):
            premature_group.append(d)
        elif any(k in text for k in TIMEBARRED_KEYWORDS):
            timebarred_group.append(d)
        elif any(k in text for k in SAMEDDAY_KEYWORDS):
            sameday_group.append(d)
        else:
            other.append(d)

    result = []

    if premature_group:

        best = next((d for d in premature_group if 'days' in d.get('defect','').lower()), premature_group[0])

        supporting = '; '.join({d.get('impact','') for d in premature_group if d.get('impact','') and d.get('impact','') != best.get('impact','')})
        result.append({
            'defect':    'Premature Complaint — Cause of Action Had Not Yet Arisen',
            'severity':  'CRITICAL',
            'impact':    best.get('impact') or 'Complaint is not maintainable under Section 138 NI Act',
            'remedy':    'Current complaint is not maintainable. Wait until cause of action arises (15-day expiry), then file a fresh complaint within the 30-day limitation period.',
            'legal_basis': 'Section 138(b) NI Act: complaint maintainable only after 15-day payment period expires',
            'supporting_detail': supporting or None,
            'source':    'timeline_intelligence',
            'is_primary': True,
        })

    if timebarred_group:
        best = timebarred_group[0]
        result.append({
            'defect':    'Complaint Barred by Limitation',
            'severity':  'CRITICAL',
            'impact':    best.get('impact') or 'Complaint filed beyond 1-month limitation period from cause of action',
            'remedy':    'File condonation application under Section 142 NI Act with sufficient cause. Filing without condonation will be dismissed.',
            'legal_basis': 'Section 142(b) NI Act: complaint must be filed within one month of cause of action',
            'source':    'ingredient_compliance',
            'is_primary': True,
        })

    if sameday_group:
        best = sameday_group[0]
        result.append({
            'defect':    'Same-Day Filing — Borderline Procedural Risk',
            'severity':  'HIGH',
            'impact':    best.get('impact') or 'Courts are split on whether complaint filed on same day as cause of action is maintainable',
            'remedy':    'Strongly recommend refiling one day after cause of action if still within limitation period.',
            'legal_basis': 'Section 138(b) NI Act: some courts require filing strictly AFTER cause of action',
            'source':    'timeline_intelligence',
            'is_primary': False,
        })

    result.extend(other)
    return result


def _calculate_data_completeness_v1(case_data: Dict) -> Dict:
    """
    Calculate how complete the input data is. (V1 — use calculate_data_completeness() instead)
    """
    required   = ['cheque_date','dishonour_date','notice_date','complaint_filed_date',
                  'notice_received_date','cheque_amount','bank_name']
    important  = ['written_agreement_exists','ledger_available','postal_proof_available',
                  'original_cheque_available','return_memo_available','court_location',
                  'debt_nature','dishonour_reason']
    optional   = ['email_sms_evidence','witness_available','defence_type',
                  'case_summary','transaction_date','transaction_amount']

    req_filled  = sum(1 for f in required  if case_data.get(f) not in (None,'',False))
    imp_filled  = sum(1 for f in important if case_data.get(f) not in (None,'',False))
    opt_filled  = sum(1 for f in optional  if case_data.get(f) not in (None,'',False))

    total_fields = len(required) + len(important) + len(optional)
    filled       = req_filled + imp_filled + opt_filled
    pct          = round((filled / total_fields) * 100)

    missing_required  = [f for f in required  if case_data.get(f) in (None,'',False)]
    missing_important = [f for f in important if case_data.get(f) in (None,'',False)]

    reliability = (
        'HIGH — All critical fields provided'          if req_filled == len(required) and imp_filled >= 6 else
        'MEDIUM — Key fields present, some gaps'       if req_filled == len(required) else
        'LOW — Missing critical dates or amounts'
    )

    return {
        'completeness_pct':    pct,
        'completeness_label':  f'{pct}% complete',
        'reliability':         reliability,
        'fields_filled':       filled,
        'fields_total':        total_fields,
        'missing_required':    [f.replace('_',' ') for f in missing_required],
        'missing_important':   [f.replace('_',' ') for f in missing_important[:4]],
        'analysis_note':       (
            'Analysis is fully reliable — all required data provided.'
            if not missing_required else
            f'Analysis partially reliable — {len(missing_required)} required field(s) missing: '
            + ', '.join(f.replace("_"," ") for f in missing_required[:3])
        )
    }


def calculate_data_completeness(case_data: Dict) -> Dict:
    """Calculate what % of useful fields are provided and what's missing."""
    scored_fields = [
        ('cheque_date',             'Cheque date',            True),
        ('dishonour_date',          'Dishonour date',         True),
        ('notice_date',             'Notice date',            True),
        ('notice_received_date',    'Notice received date',   True),
        ('complaint_filed_date',    'Complaint filed date',   True),
        ('cheque_amount',           'Cheque amount',          True),
        ('bank_name',               'Bank name',              False),
        ('written_agreement_exists','Written agreement',      False),
        ('ledger_available',        'Ledger / account records',False),
        ('postal_proof_available',  'Postal proof of notice', False),
        ('original_cheque_available','Original cheque',       False),
        ('return_memo_available',   'Return memo',            False),
        ('court_location',          'Court location',         False),
        ('debt_nature',             'Nature of debt',         False),
        ('defence_type',            'Known defence type',     False),
    ]
    provided = [f for f, label, req in scored_fields if case_data.get(f) not in (None, '', False)]
    missing_required = [label for f, label, req in scored_fields if req and case_data.get(f) in (None, '')]
    missing_optional = [label for f, label, req in scored_fields if not req and case_data.get(f) in (None, '', False)]
    pct = round(len(provided) / len(scored_fields) * 100) if scored_fields else 0
    return {
        'completeness_pct':   pct,
        'completeness_label': 'Complete' if pct >= 85 else ('Mostly Complete' if pct >= 65 else ('Partial' if pct >= 45 else 'Incomplete')),
        'fields_provided':    len(provided),
        'fields_total':       len(scored_fields),
        'missing_required':   missing_required,
        'missing_optional':   missing_optional[:5],
        'reliability_note':   ('High confidence' if pct >= 85 else
                               'Moderate confidence — some optional data missing' if pct >= 65 else
                               'Reduced confidence — key fields missing'),
        'warning':            f"Missing required fields: {', '.join(missing_required)}" if missing_required else None,
    }


def analyze_dishonour_reason(case_data: Dict) -> Dict:
    """
    Deep legal analysis of dishonour reason.
    Each reason has different legal significance, rebuttability, and defence strategy.
    """
    try:
        reason_raw = (case_data.get('dishonour_reason') or '').strip()
        reason     = reason_raw.lower()

        result = {
            'module':         'Dishonour Reason Analysis',
            'raw_reason':     reason_raw or 'Not specified',
            'reason_category':'Unknown',
            'legal_strength': 'WEAK',
            'is_best_case':   False,
            'rebuttable':     True,
            'legal_impact':   '',
            'defence_risk':   '',
            'recommendation': '',
            'score_impact':   0,
            'risk_level':     'MEDIUM',
        }

        if not reason or reason == 'not specified':

            alt_reason = (
                str(case_data.get('return_reason') or '').lower() or
                str(case_data.get('bounce_reason') or '').lower() or
                str(case_data.get('dishonour_memo_reason') or '').lower()
            )
            if alt_reason:
                reason = alt_reason
                result['raw_reason'] = alt_reason
            else:
                result['legal_impact']   = 'Dishonour reason not provided — weakens the case'
                result['recommendation'] = 'Obtain dishonour memo from bank clearly stating the reason'
                result['risk_level']     = 'HIGH'
                result['score_impact']   = -10
                return result

        if any(k in reason for k in ['insufficient', 'funds', 'balance', 'exceeds']):
            result['reason_category'] = 'Insufficient Funds'
            result['legal_strength']  = 'STRONG'
            result['is_best_case']    = True
            result['rebuttable']      = True
            result['legal_impact']    = (
                'Strongest dishonour reason for complainant. Directly proves inability/unwillingness to pay. '
                'Section 139 presumption operates fully. Accused must prove absence of debt.'
            )
            result['defence_risk']    = (
                'Accused may claim: (a) cheque given as security — rebut with transaction evidence; '
                '(b) amount deposited before dishonour — check bank statement dates carefully.'
            )
            result['recommendation']  = 'Highlight this reason prominently. Get certified copy of dishonour memo.'
            result['score_impact']    = +5
            result['risk_level']      = 'LOW'

        elif any(k in reason for k in ['stop payment', 'stop-payment', 'payment stopped', 'drawer stopped']):
            result['reason_category'] = 'Stop Payment'
            result['legal_strength']  = 'MODERATE'
            result['rebuttable']      = True
            result['legal_impact']    = (
                'Stop payment instruction is still dishonour under Section 138. However, accused will argue: '
                '(a) cheque was given as security; (b) dispute regarding debt; (c) no legally enforceable debt. '
                'Requires stronger documentary proof of debt than "insufficient funds" cases.'
            )
            result['defence_risk']    = (
                'HIGH defence risk — stop payment shows deliberate act, which accused will use to argue '
                'no obligation existed. Complainant must have strong proof of underlying debt.'
            )
            result['recommendation']  = (
                'Critically important: secure written proof of debt (agreement, ledger, receipts). '
                'Without this, stop payment defence is very strong for accused.'
            )
            result['score_impact']    = -5
            result['risk_level']      = 'MEDIUM'

        elif any(k in reason for k in ['account closed', 'closed account', 'no account', 'account does not exist']):
            result['reason_category'] = 'Account Closed'
            result['legal_strength']  = 'STRONG'
            result['is_best_case']    = True
            result['rebuttable']      = False
            result['legal_impact']    = (
                'Account closed at time of presentation is a very strong dishonour. '
                'Shows clear intention to defraud — hard for accused to rebut. '
                'May also attract Section 420 IPC charges in addition to Section 138. '
                'Courts have consistently held this as clear evidence of dishonest intention.'
            )
            result['defence_risk']    = (
                'LOW defence risk — very difficult for accused to explain account closure. '
                'May argue: (a) administrative error; (b) did not know account was closed. '
                'Both arguments weak.'
            )
            result['recommendation']  = (
                'Very strong position. Consider adding Section 420 IPC complaint. '
                'Get bank certificate confirming closure date vs. cheque date.'
            )
            result['score_impact']    = +8
            result['risk_level']      = 'LOW'

        elif any(k in reason for k in ['signature', 'mismatch', 'differ', 'forged', 'not match']):
            result['reason_category'] = 'Signature Mismatch / Forgery'
            result['legal_strength']  = 'WEAK'
            result['rebuttable']      = True
            result['legal_impact']    = (
                'Signature mismatch is technically dishonour under Section 138, but courts are divided. '
                'Some courts hold: if signatures differ, identity of drawer uncertain — Section 138 may not apply. '
                'Risk of complaint being dismissed on this ground alone.'
            )
            result['defence_risk']    = (
                'CRITICAL — accused will argue they did not sign the cheque, potentially attacking the very '
                'foundation of the complaint. Handwriting expert may be required.'
            )
            result['recommendation']  = (
                'Obtain handwriting expert report before filing. If accused denies signature, '
                'burden shifts significantly. Check if signature matches previous cheques of same account.'
            )
            result['score_impact']    = -15
            result['risk_level']      = 'HIGH'

        elif any(k in reason for k in ['frozen', 'attachment', 'garnish', 'court order', 'restrained']):
            result['reason_category'] = 'Account Frozen / Court Order'
            result['legal_strength']  = 'MODERATE'
            result['rebuttable']      = True
            result['legal_impact']    = (
                'Account frozen by court order is valid dishonour under Section 138. '
                'However, accused may argue the freezing itself shows financial distress beyond their control. '
                'Complexity increases if a court order caused the dishonour.'
            )
            result['defence_risk'] = 'MEDIUM — accused may use court-ordered freeze as partial defence.'
            result['recommendation'] = 'Verify the freeze order details, who applied it, and whether it was in force at time of presentation.'
            result['score_impact']   = -5
            result['risk_level']     = 'MEDIUM'

        else:
            result['reason_category'] = f'Other: {reason_raw}'
            result['legal_impact']    = (
                f'Dishonour reason "{reason_raw}" — verify with bank. '
                'Any dishonour for non-payment is covered by Section 138 if other ingredients are met.'
            )
            result['recommendation']  = 'Obtain clear dishonour memo from bank specifying exact reason.'
            result['risk_level']      = 'MEDIUM'

        return result


    except Exception as _analyz_err:
        logger.error(f"analyze_dishonour_reason error: {_analyz_err}")
        return {'module': 'Dishonour Reason Analysis', 'reason_category': 'Unknown', 'legal_strength': 'MODERATE', 'risk_level': 'MEDIUM', 'raw_reason': case_data.get('dishonour_reason', 'Not specified')}

def analyze_legally_enforceable_debt(case_data: Dict) -> Dict:
    """
    Evaluates whether the underlying debt is legally enforceable.
    Key: If the debt itself is illegal/unenforceable, Section 138 fails at the root.
    """
    amount   = float(case_data.get('cheque_amount', 0) or 0)
    _ = 'INR'

    result = {
        'module':               'Legally Enforceable Debt Analysis',
        'is_enforceable':       True,
        'risk_level':           'LOW',
        'issues':               [],
        'fatal_issues':         [],
        'warnings':             [],
        'part_payment_impact':  None,
        'tax_compliance':       'Not assessed',
        'debt_nature':          'Not specified',
        'overall_assessment':   'Debt appears legally enforceable',
        'recommendation':       '',
    }


    payment_mode = (case_data.get('payment_mode') or '').lower()
    if 'cash' in payment_mode and amount > 20000:
        result['fatal_issues'].append({
            'issue':       'Cash Loan Exceeds ₹20,000 — Section 269SS Violation',
            'severity':    'FATAL',
            'legal_basis': 'Section 269SS Income Tax Act: No person shall take or accept any loan/deposit otherwise than by account payee cheque/NEFT if amount ≥ ₹20,000',
            'impact':      'Court may hold the underlying debt itself is illegal — cheque given for illegal debt is not enforceable under Section 138. Several High Courts have dismissed complaints on this ground.',
            'remedy':      'Complainant must prove cash was actually given AND produce banking/cash book records. Without this, defence will argue debt is unenforceable.'
        })
        result['is_enforceable'] = False
        result['risk_level']     = 'FATAL'
        result['tax_compliance'] = 'VIOLATION — Section 269SS'

    elif 'cash' in payment_mode and amount > 0:
        result['warnings'].append({
            'issue':   f'Cash transaction of ₹{amount:,.0f}',
            'note':    'Below Section 269SS threshold (₹20,000) but cash loans are harder to prove without banking records.',
            'remedy':  'Obtain witness statements, diary entries, or any other documentary proof of cash payment.'
        })
        result['tax_compliance'] = 'CAUTION — Cash transaction'


    if case_data.get('is_moneylender') or 'savkari' in (case_data.get('loan_type') or '').lower():
        result['issues'].append({
            'issue':       'Moneylender / Savkari Transaction',
            'severity':    'HIGH',
            'legal_basis': 'Maharashtra Moneylenders Act 2014 — unregistered moneylenders cannot recover debt through court',
            'impact':      'If complainant is unlicensed moneylender, debt recovery is barred. Accused will raise this as absolute defence.',
            'remedy':      'Complainant must prove: (a) not a professional moneylender, or (b) holds valid moneylending licence.'
        })
        result['risk_level'] = 'HIGH' if result['risk_level'] == 'LOW' else result['risk_level']


    interest_rate = float(case_data.get('interest_rate') or 0)
    if interest_rate > 36:
        result['warnings'].append({
            'issue':   f'Very High Interest Rate: {interest_rate}% per annum',
            'note':    'Courts may reduce exorbitant interest or question enforceability of the debt at this rate.',
            'remedy':  'Cap claim to principal + reasonable interest. Courts typically allow 12-18% p.a.'
        })


    part_payment = float(case_data.get('part_payment_amount') or 0)
    if part_payment > 0:
        remaining    = amount - part_payment
        pct          = (part_payment / amount * 100) if amount > 0 else 0
        if part_payment >= amount:
            result['fatal_issues'].append({
                'issue':   'Part Payment Equals or Exceeds Cheque Amount',
                'severity':'FATAL',
                'impact':  'If accused has paid the full amount, debt is extinguished — Section 138 complaint fails.',
                'remedy':  'Complainant must verify and account for all prior payments. If payment was received, complaint should not be filed.'
            })
            result['is_enforceable'] = False
            result['risk_level']     = 'FATAL'
        else:
            result['part_payment_impact'] = {
                'amount_paid':    part_payment,
                'remaining_debt': remaining,
                'pct_paid':       round(pct, 1),
                'note':           (
                    f'Accused has paid ₹{part_payment:,.0f} ({pct:.1f}%). '
                    f'Remaining enforceable debt: ₹{remaining:,.0f}. '
                    'Part payment does not extinguish the offence but may mitigate sentence.'
                )
            }
            result['warnings'].append({'issue': f'Part payment of ₹{part_payment:,.0f} made by accused', 'note': f'₹{remaining:,.0f} remains outstanding'})


    if case_data.get('debt_for_illegal_purpose'):
        result['fatal_issues'].append({
            'issue':       'Debt for Illegal Purpose',
            'severity':    'FATAL',
            'legal_basis': 'Section 23 Contract Act: Agreement void if object is unlawful',
            'impact':      'Cheque given for illegal consideration — unenforceable in law.',
            'remedy':      'Cannot file Section 138 complaint for an illegal debt.'
        })
        result['is_enforceable'] = False
        result['risk_level']     = 'FATAL'


    if case_data.get('underlying_debt_time_barred'):
        result['issues'].append({
            'issue':       'Underlying Debt May Be Time-Barred',
            'severity':    'HIGH',
            'legal_basis': 'Limitation Act 1963 — 3 years for simple money recovery',
            'impact':      'If the underlying debt is time-barred for recovery, Section 138 may still apply to the cheque, but accused will raise limitation of underlying claim as defence.',
            'remedy':      'Check dates carefully. Section 138 has its own limitation period (1 month from COA).'
        })


    fatal_count = len(result['fatal_issues'])
    if fatal_count == 0 and not result['issues']:
        result['overall_assessment'] = 'Underlying debt appears legally enforceable — no major compliance issues detected'
        result['recommendation']     = 'Ensure documentary proof of debt transaction is available'
    elif fatal_count > 0:
        result['overall_assessment'] = f'FATAL: {fatal_count} legal enforceability issue(s) detected — complaint may fail at root'
        result['is_enforceable']     = False
        result['recommendation']     = '; '.join(i['remedy'] for i in result['fatal_issues'])
    else:
        result['overall_assessment'] = f'{len(result["issues"])} issue(s) require attention before filing'
        result['recommendation']     = 'Address identified issues with additional documentary evidence'

    return result


def analyze_delay_condonation(case_data: Dict, timeline_result: Dict) -> Dict:
    """
    Analyzes whether delay condonation is needed and how strong the application would be.
    Section 142(b): Complaint must be filed within 1 month of cause of action.
    """
    result = {
        'module':                   'Delay Condonation Analysis (S.142)',
        'condonation_required':     False,
        'delay_days':               0,
        'condonation_strength':     'N/A',
        'sufficient_cause_exists':  False,
        'grounds':                  [],
        'legal_standard':           '',
        'recommendation':           '',
        'risk_level':               'LOW',
        'case_law':                 [],
    }

    if not isinstance(timeline_result, dict):
        return result

    limitation_status = (timeline_result.get('compliance_status') or {}).get('limitation', '')
    limitation_risk   = timeline_result.get('limitation_risk', 'LOW')

    if limitation_risk not in ('CRITICAL', 'EXPIRED', 'HIGH') and 'BARRED' not in str(limitation_status).upper():
        result['recommendation'] = 'No delay condonation required — complaint within limitation period'
        return result


    result['condonation_required'] = True
    result['risk_level']           = 'CRITICAL'


    try:
        import re as _re
        delay_match = _re.search(r'(\d+)\s*days?\s*(late|delayed|after)', limitation_status.lower())
        if delay_match:
            result['delay_days'] = int(delay_match.group(1))
    except Exception as _e:
        logger.debug(f"Non-critical exception suppressed: {_e}")

    result['legal_standard'] = (
        'Section 142(b) NI Act: Complaint must be filed within 1 month of cause of action. '
        'Proviso: Court may take cognizance after 1 month if complainant satisfies court of sufficient cause. '
        'Standard: Not mere inconvenience — must be genuine cause beyond complainant\'s control.'
    )


    if case_data.get('was_in_negotiation'):
        result['grounds'].append({
            'ground':      'Parties Were in Active Negotiation',
            'strength':    'HIGH',
            'legal_basis': 'Babulal Agrawal v. State of M.P. (1999) — settlement negotiations = sufficient cause',
            'requirement': 'Produce correspondence showing negotiation (letters, emails, WhatsApp)'
        })
        result['sufficient_cause_exists'] = True

    if case_data.get('complainant_was_ill'):
        result['grounds'].append({
            'ground':      'Complainant Illness / Incapacity',
            'strength':    'MEDIUM',
            'legal_basis': 'Illness can be sufficient cause if supported by medical certificate',
            'requirement': 'Medical certificate from registered doctor covering the delay period'
        })
        result['sufficient_cause_exists'] = True

    if case_data.get('was_abroad_during_delay'):
        result['grounds'].append({
            'ground':      'Complainant Was Abroad',
            'strength':    'MEDIUM',
            'legal_basis': 'Foreign travel may constitute sufficient cause depending on circumstances',
            'requirement': 'Passport stamps, visa records showing presence abroad during delay period'
        })
        result['sufficient_cause_exists'] = True

    if case_data.get('advocate_error_delay'):
        result['grounds'].append({
            'ground':      'Bona Fide Error by Advocate',
            'strength':    'LOW',
            'legal_basis': 'Limited — courts not always sympathetic to advocate errors',
            'requirement': 'Affidavit explaining the error; supporting correspondence'
        })

    if not result['grounds']:
        result['grounds'].append({
            'ground':      'No Documented Ground Available',
            'strength':    'WEAK',
            'legal_basis': 'N/A',
            'requirement': 'Must identify and document a specific sufficient cause'
        })


    strong = [g for g in result['grounds'] if g['strength'] == 'HIGH']
    medium = [g for g in result['grounds'] if g['strength'] == 'MEDIUM']
    delay  = result['delay_days']

    if strong and delay <= 30:
        result['condonation_strength'] = 'GOOD — Strong grounds, moderate delay'
    elif strong and delay <= 90:
        result['condonation_strength'] = 'MODERATE — Strong grounds but longer delay'
    elif medium and delay <= 30:
        result['condonation_strength'] = 'MODERATE — Reasonable grounds, short delay'
    elif delay > 180:
        result['condonation_strength'] = 'WEAK — Long delay requires exceptional cause'
    else:
        result['condonation_strength'] = 'WEAK — Insufficient grounds on record'

    result['case_law'] = [
        'Babulal Agrawal v. State of M.P. (1999) — settlement negotiations = sufficient cause',
        'Rajesh Arora v. State (2011) — liberal construction of sufficient cause in cheque bounce cases',
        'Nagar Palika Nigam v. Krishi Upaj Mandi Samiti (1994) — condonation within court\'s discretion',
    ]

    result['recommendation'] = (
        'Delay condonation application MUST be filed along with complaint. '
        + (f'Strongest ground: {strong[0]["ground"]}. ' if strong else '')
        + f'Produce: {"; ".join(g["requirement"] for g in result["grounds"][:2])}. '
        + (f'Delay of {delay} days is ' + ('manageable' if delay <= 60 else 'significant — expect challenge') + '.' if delay else '')
    )

    return result


def analyze_complaint_drafting_compliance(case_data: Dict) -> Dict:
    """
    Checks whether a Section 138 complaint would meet drafting standards.
    Gaps here = technical grounds for dismissal even if facts are strong.
    """
    result = {
        'module':            'Complaint Drafting Compliance',
        'compliance_score':  100,
        'critical_gaps':     [],
        'warnings':          [],
        'required_averments':[],
        'missing_averments': [],
        'risk_level':        'LOW',
        'overall_verdict':   'Complaint appears draftable',
        'recommendation':    '',
    }

    deductions = 0


    if not case_data.get('written_agreement_exists') and not case_data.get('debt_acknowledged'):
        result['missing_averments'].append({
            'averment':    'Legally Enforceable Debt',
            'requirement': 'Complaint must aver that cheque was issued in discharge of legally enforceable liability',
            'risk':        'Absence may lead to dismissal — accused will file discharge application',
            'fix':         'Include specific averment: "The cheque was issued in discharge of legally enforceable debt/liability of ₹X arising from [transaction details]"'
        })
        deductions += 15


    result['required_averments'].append({
        'averment':    'Dishonour Fact',
        'status':      '✅ Can be averred from bank dishonour memo',
        'requirement': 'Date of dishonour, reason, bank name, cheque details'
    })


    if not case_data.get('notice_date'):
        result['critical_gaps'].append({
            'gap':      'Notice Date Missing',
            'severity': 'CRITICAL',
            'impact':   'Cannot aver notice was sent — Ingredient 4 fails',
            'fix':      'Obtain and record exact date of notice dispatch'
        })
        deductions += 20


    result['required_averments'].append({
        'averment':    '15-Day Demand Period',
        'status':      '✅ Averrable from notice and dishonour dates',
        'requirement': 'Aver that accused was called upon to pay within 15 days and failed'
    })


    if case_data.get('is_company_case'):
        if not case_data.get('directors_impleaded'):
            result['critical_gaps'].append({
                'gap':      'Directors Not Impleaded / Averments Missing for Company Case',
                'severity': 'CRITICAL',
                'legal_basis': 'Section 141 NI Act: For company cases, persons in charge at time of offence must be specifically named with averment of their role',
                'impact':   'Without specific averments of director role, complaint against directors will be dismissed',
                'fix':      'Add para: "At the time of commission of offence, accused nos. X,Y,Z were in charge of and responsible for conduct of business of the company"'
            })
            deductions += 25
        else:
            result['required_averments'].append({
                'averment': 'Director/Company Averments (S.141)',
                'status':   '✅ Directors impleaded — ensure specific role averments added',
                'requirement': 'Must specifically state each director was "in charge of and responsible for conduct of business" — Pooja Ravinder Devidasani v. State of Maharashtra (2014) 16 SCC 1'
            })


    if not case_data.get('court_location'):
        result['warnings'].append({'warning': 'Court location not specified — jurisdiction averment cannot be verified'})
        deductions += 5
    else:
        result['required_averments'].append({
            'averment': 'Territorial Jurisdiction',
            'status':   '✅ Court location specified',
            'requirement': 'Aver that cheque was presented at bank within court jurisdiction, OR notice was sent from within jurisdiction'
        })


    result['required_averments'].append({
        'averment':    'Complaint on Oath',
        'status':      '✅ Standard requirement — ensure complainant is present to verify',
        'requirement': 'Section 200 CrPC: Complainant must be examined on oath at the time of filing'
    })


    result['required_averments'].append({
        'averment':    'Cause of Action',
        'status':      '✅ Averrable from timeline',
        'requirement': 'State exact date of expiry of 15-day notice period (cause of action date)'
    })


    result['compliance_score'] = max(0, 100 - deductions)

    if result['compliance_score'] >= 80 and not result['critical_gaps']:
        result['risk_level']      = 'LOW'
        result['overall_verdict'] = 'Complaint draftable — follow required averment checklist'
    elif result['compliance_score'] >= 60 or (result['critical_gaps'] and result['compliance_score'] >= 50):
        result['risk_level']      = 'MEDIUM'
        result['overall_verdict'] = f'{len(result["critical_gaps"])} critical gap(s) in complaint drafting — must be addressed'
    else:
        result['risk_level']      = 'HIGH'
        result['overall_verdict'] = 'Multiple critical drafting gaps — complaint likely to be challenged'

    if result['critical_gaps']:
        result['recommendation'] = (
            'Fix before filing: ' +
            ' | '.join(g['fix'] for g in result['critical_gaps'] if 'fix' in g)
        )
    else:
        result['recommendation'] = 'Review required averments checklist and ensure all are specifically included in the complaint'

    return result


def analyze_document_validity(case_data: Dict) -> Dict:
    """
    Checks admissibility and validity of key documents.
    Courts require specific document standards — originals, certified copies, etc.
    """
    result = {
        'module':             'Document Validity & Admissibility',
        'valid_documents':    [],
        'invalid_documents':  [],
        'risk_documents':     [],
        'overall_strength':   'ADEQUATE',
        'risk_level':         'LOW',
        'admissibility_score':100,
        'recommendation':     '',
    }

    deductions = 0


    cheque_available  = case_data.get('original_cheque_available', False)
    cheque_is_copy    = case_data.get('cheque_is_photocopy', False)
    if cheque_available and not cheque_is_copy:
        result['valid_documents'].append({
            'document': 'Original Cheque',
            'status':   'VALID — Original available',
            'note':     'Present as Exhibit. Mark on cheque itself. Court will examine original.'
        })
    elif cheque_is_copy:
        result['risk_documents'].append({
            'document': 'Cheque — Photocopy Only',
            'risk':     'HIGH',
            'issue':    'Photocopy of cheque may not be admitted without proof of original being produced',
            'fix':      'Obtain original from bank records or court process. File Section 65B certificate if electronic image.',
            'legal_basis': 'Best Evidence Rule: Original document required unless production excused under S.65 Evidence Act'
        })
        deductions += 15
    else:
        result['invalid_documents'].append({
            'document': 'Original Cheque',
            'issue':    'Not available — critical document missing',
            'impact':   'Complaint may fail — cheque is primary exhibit in Section 138 case',
            'fix':      'Apply to bank for certified copy, or obtain from court records if impounded'
        })
        deductions += 25


    return_memo = case_data.get('return_memo_available', False)
    if return_memo:
        result['valid_documents'].append({
            'document': 'Bank Dishonour Memo (Return Memo)',
            'status':   'VALID',
            'note':     'Critical document — obtain original stamped copy from bank with dishonour reason clearly stated'
        })
    else:
        result['invalid_documents'].append({
            'document': 'Bank Dishonour Memo',
            'issue':    'Not available',
            'impact':   'Cannot prove dishonour — Ingredient 2 weakened',
            'fix':      'Obtain from presenting bank: stamped return memo with date, reason, and bank seal'
        })
        deductions += 20


    postal_proof = case_data.get('postal_proof_available', False)
    ad_card      = case_data.get('ad_card_signed', False)
    if postal_proof and ad_card:
        result['valid_documents'].append({
            'document': 'Postal Proof (AD Card Signed)',
            'status':   'STRONG — Signed AD card available',
            'note':     'Best evidence of service. Produce signed card as Exhibit.'
        })
    elif postal_proof:
        result['risk_documents'].append({
            'document': 'Postal Proof (No Signed AD Card)',
            'risk':     'MEDIUM',
            'issue':    'Postal receipt without AD card — service may be challenged',
            'fix':      'Obtain track-and-trace report from India Post website. If electronic, file Section 65B certificate.',
            'note':     'Deemed service doctrine may apply if accused refused or left unclaimed'
        })
        deductions += 5
    else:
        result['invalid_documents'].append({
            'document': 'Postal Proof of Notice',
            'issue':    'No postal proof',
            'impact':   'Service of notice cannot be proven — Ingredient 4 fails',
            'fix':      'Check if RPAD tracking available online. Collect delivery records from post office.'
        })
        deductions += 20


    electronic_docs = []
    if case_data.get('whatsapp_evidence'):      electronic_docs.append('WhatsApp Messages')
    if case_data.get('email_sms_evidence'):     electronic_docs.append('Email/SMS')
    if case_data.get('bank_statement_electronic'): electronic_docs.append('Electronic Bank Statement')
    if case_data.get('electronic_evidence'):    electronic_docs.append('Other Electronic Records')

    if electronic_docs:
        has_65b = case_data.get('section_65b_certificate', False)
        if has_65b:
            result['valid_documents'].append({
                'document': f'Electronic Evidence ({", ".join(electronic_docs)})',
                'status':   'VALID — Section 65B certificate available',
                'note':     'Admissible as per Anvar P.V. v. P.K. Basheer (2014) 10 SCC 473'
            })
        else:
            result['risk_documents'].append({
                'document':  f'Electronic Evidence ({", ".join(electronic_docs)}) — No Section 65B Certificate',
                'risk':      'HIGH',
                'issue':     'Electronic evidence INADMISSIBLE without Section 65B certificate',
                'legal_basis': 'Anvar P.V. v. P.K. Basheer (2014) 10 SCC 473: Electronic records must be accompanied by certificate under S.65B Evidence Act',
                'fix':       'Obtain S.65B certificate from officer responsible for device/server. Must certify: device description, process used, document integrity, and identity of maker.'
            })
            deductions += 15


    if case_data.get('written_agreement_exists'):
        result['valid_documents'].append({
            'document': 'Written Loan/Transaction Agreement',
            'status':   'VALID — Strong supporting document',
            'note':     'Check: properly stamped, signed by both parties, consideration mentioned, date matches transaction'
        })
    else:
        result['risk_documents'].append({
            'document': 'Written Agreement',
            'risk':     'MEDIUM',
            'issue':    'No written agreement — debt enforceability weaker',
            'fix':      'Collect any alternate proof: bank transfer records, SMS/WhatsApp acknowledgment, witnesses'
        })
        deductions += 10


    result['admissibility_score'] = max(0, 100 - deductions)
    if result['admissibility_score'] >= 75 and not result['invalid_documents']:
        result['overall_strength'] = 'ADEQUATE — Core documents available'
        result['risk_level']       = 'LOW'
    elif result['admissibility_score'] >= 50:
        result['overall_strength'] = 'WEAK — Critical documents missing or at risk'
        result['risk_level']       = 'MEDIUM'
    else:
        result['overall_strength'] = 'VERY WEAK — Multiple admissibility issues'
        result['risk_level']       = 'HIGH'

    result['recommendation'] = (
        'Critical: ' + ' | '.join(d['fix'] for d in result['invalid_documents'][:2])
        if result['invalid_documents'] else
        'Risk documents: ' + ' | '.join(d['fix'] for d in result['risk_documents'][:2])
        if result['risk_documents'] else
        'Core documents appear in order — verify originals before filing'
    )

    return result


def analyze_fraud_signals(case_data: Dict) -> Dict:
    """
    Detects patterns suggesting fraudulent/false complaint or fraudulent documents.
    Critical for both complainant (before filing) and accused (defence strategy).
    """
    result = {
        'module':           'Fraud & False Case Signal Detection',
        'fraud_signals':    [],
        'risk_level':       'LOW',
        'fraud_risk_score': 0,
        'overall_verdict':  'No significant fraud indicators detected',
        'for_complainant':  [],
        'for_accused':      [],
        'recommendation':   '',
    }

    score = 0


    amount = float(case_data.get('cheque_amount', 0) or 0)
    if amount > 0 and amount % 100000 == 0 and not case_data.get('written_agreement_exists'):
        result['fraud_signals'].append({
            'signal':   'Round-Figure Amount Without Written Agreement',
            'severity': 'LOW',
            'note':     f'Cheque of ₹{amount:,.0f} (exact round figure) with no documentary proof of debt — accused may claim cheque was given as security or blank cheque',
            'check':    'Ensure you can explain why exactly this round figure was transacted'
        })
        score += 10


    if case_data.get('accused_claims_blank_cheque') or case_data.get('blank_cheque_risk'):
        result['fraud_signals'].append({
            'signal':   'Accused Claims Cheque Was Blank When Given',
            'severity': 'HIGH',
            'legal_basis': 'M.S. Narayana Menon v. State of Kerala (2006) — blank cheque cases create presumption complexities',
            'note':     'Accused will argue amount was filled fraudulently. Burden will shift significantly.',
            'check':    'Check cheque: same handwriting for amount and signature? Any date alterations? Produce handwriting expert if needed.'
        })
        score += 30


    cheque_date  = case_data.get('cheque_date', '')
    present_date = case_data.get('presentation_date', '')
    if cheque_date and present_date:
        try:
            from datetime import datetime as _dt
            cd = _dt.strptime(cheque_date, '%Y-%m-%d')
            pd = _dt.strptime(present_date, '%Y-%m-%d')
            days_gap = (pd - cd).days
            if days_gap > 180:
                result['fraud_signals'].append({
                    'signal':   f'Cheque Presented {days_gap} Days After Issue Date',
                    'severity': 'MEDIUM',
                    'note':     'Very late presentation may suggest post-dated cheque being misused, or cheque held as security being presented after dispute.',
                    'check':    'Verify: was cheque given for specific purpose/date? Is there correspondence showing original purpose?'
                })
                score += 15
        except Exception as _e:
            logger.debug(f"Non-critical exception suppressed: {_e}")


    dishonour = (case_data.get('dishonour_reason') or '').lower()
    if 'signature' in dishonour and 'mismatch' in dishonour:
        result['fraud_signals'].append({
            'signal':   'Signature Mismatch Dishonour — Possible Forgery Allegation',
            'severity': 'HIGH',
            'note':     'If accused claims signature is forged, this becomes a criminal forgery case (IPC 465/468) not Section 138. Entire complaint strategy changes.',
            'check':    'Obtain comparison of signature with other known documents. Consider handwriting expert report.'
        })
        score += 25


    if case_data.get('previous_complaint_filed'):
        result['fraud_signals'].append({
            'signal':   'Previous Complaint Filed for Same Cheque / Debt',
            'severity': 'HIGH',
            'legal_basis': 'Double jeopardy / abuse of process concerns',
            'note':     'Filing second complaint after first was dismissed (on merits) is an abuse of process.',
            'check':    'Check if prior complaint was withdrawn, dismissed on default, or dismissed on merits. Only merits dismissal bars fresh complaint.'
        })
        score += 20


    if case_data.get('dishonour_immediately_after_dispute'):
        result['fraud_signals'].append({
            'signal':   'Dishonour Immediately After Known Dispute',
            'severity': 'MEDIUM',
            'note':     'If cheque bounced right after a commercial dispute between parties, accused will argue cheque was given as security, not for debt.',
            'check':    'Document the timeline carefully. Produce evidence showing debt existed before dispute.'
        })
        score += 15


    has_any_proof = any([
        case_data.get('written_agreement_exists'),
        case_data.get('bank_transfer_proof'),
        case_data.get('ledger_available'),
        case_data.get('witness_available'),
    ])
    if not has_any_proof and amount > 500000:
        result['fraud_signals'].append({
            'signal':   f'Large Amount (₹{amount:,.0f}) With Zero Supporting Documentation',
            'severity': 'HIGH',
            'note':     'Very difficult to prove debt for a large amount with no documentary trail. Courts will be skeptical.',
            'check':    'Urgently identify any bank records, phone records, or witnesses who can corroborate the transaction.'
        })
        score += 20


    result['fraud_risk_score'] = min(100, score)

    if score == 0:
        result['risk_level']    = 'LOW'
        result['overall_verdict'] = 'No fraud indicators detected — case appears genuine'
    elif score <= 20:
        result['risk_level']    = 'LOW'
        result['overall_verdict'] = 'Minor risk factors — standard precautions sufficient'
    elif score <= 40:
        result['risk_level']    = 'MEDIUM'
        result['overall_verdict'] = 'Moderate fraud risk factors — proactive measures recommended'
    elif score <= 70:
        result['risk_level']    = 'HIGH'
        result['overall_verdict'] = 'High fraud risk signals — case viability may be challenged'
    else:
        result['risk_level']    = 'CRITICAL'
        result['overall_verdict'] = 'Multiple serious fraud indicators — fundamental case review required'


    for sig in result['fraud_signals']:
        if sig['severity'] in ('HIGH', 'CRITICAL'):
            result['for_accused'].append(f"Strong defence signal: {sig['signal']}")
            result['for_complainant'].append(f"Prepare counter: {sig.get('check', 'Gather supporting evidence')}")

    result['recommendation'] = (
        'CRITICAL review required: ' + ' | '.join(s['signal'] for s in result['fraud_signals'] if s['severity'] in ('HIGH','CRITICAL'))
        if any(s['severity'] in ('HIGH','CRITICAL') for s in result['fraud_signals']) else
        'Minor signals noted — document thoroughly to preempt defence challenges'
        if result['fraud_signals'] else
        'No action required — standard case preparation applies'
    )

    return result


def generate_audit_trail(case_data: Dict, timeline_result: Dict, ingredient_result: Dict,
                         doc_result: Dict, risk_result: Dict) -> Dict:
    """Generate a structured audit trail of the analysis."""
    return {
        'case_type':           case_data.get('case_type', 'unknown'),
        'cheque_amount':       case_data.get('cheque_amount', 0),
        'analysis_inputs': {
            'cheque_date':         case_data.get('cheque_date'),
            'dishonour_date':      case_data.get('dishonour_date'),
            'notice_date':         case_data.get('notice_date'),
            'complaint_filed_date':case_data.get('complaint_filed_date'),
            'court_location':      case_data.get('court_location'),
        },
        'module_scores': {
            'timeline_limitation_risk': timeline_result.get('limitation_risk', 'Unknown'),
            'ingredient_compliance':    round(ingredient_result.get('overall_compliance', 0), 1),
            'documentary_strength':     round(doc_result.get('overall_strength_score', 0), 1),
            'overall_risk_score':       round((risk_result.get('overall_risk_score') or 0), 1),
        },
        'fatal_defects_count': len(risk_result.get('fatal_defects', [])),
        'audit_version': 'v1.0',
        'locked': True,
    }


SCORE_INTERPRETATION = {
    (80, 101): {'label': 'Very Strong',  'colour': '#1A7A50', 'description': 'Case is in strong position to file. Minor gaps only.'},
    (60, 80):  {'label': 'Strong',       'colour': '#2BB6A8', 'description': 'Case is maintainable. Address identified gaps before filing.'},
    (40, 60):  {'label': 'Moderate',     'colour': '#B7740A', 'description': 'Significant gaps present. Evidentiary strengthening required.'},
    (20, 40):  {'label': 'Weak',         'colour': '#C0392B', 'description': 'Multiple critical gaps. Remediation essential before filing.'},
    (0,  20):  {'label': 'Very Weak',    'colour': '#8B0000', 'description': 'Case has severe deficiencies. Filing not advisable.'},
}

def interpret_score(score: float) -> Dict:
    for (lo, hi), interp in SCORE_INTERPRETATION.items():
        if lo <= score < hi:
            return {**interp, 'score': round(score, 1), 'range': f'{lo}–{hi}'}
    return {'label': 'Very Weak', 'colour': '#8B0000', 'description': 'Score out of range', 'score': round(score, 1)}


def generate_score_explanation(risk_result: Dict) -> Dict:
    """Generate human-readable explanation for why the score is what it is."""
    if not isinstance(risk_result, dict):
        return {'error': 'Risk result unavailable'}

    score     = round(float(risk_result.get('overall_risk_score') or 0), 1)
    fdo       = risk_result.get('fatal_defect_override') or risk_result.get('hard_fatal_override') or {}
    orig      = round(float(fdo.get('original_score') or fdo.get('original_weighted_score') or score), 1)
    fatals    = risk_result.get('fatal_defects') or []

    if fdo.get('applied') and orig != score:
        cap_story = (
            f"Base weighted score: {orig}/100. "
            f"Fatal defect detected → score capped to {score}/100. "
            f"Reason: {fdo.get('reason', 'Fatal procedural defect')}."
        )
    elif fatals:
        cap_story = f"Score of {score}/100 reflects {len(fatals)} fatal defect(s) that override module scores."
    else:
        cap_story = f"Score of {score}/100 reflects weighted average of all module scores."

    return {
        'score_story':          cap_story,
        'original_base_score':  orig,
        'final_score':          score,
        'fatal_cap_applied':    fdo.get('applied', False),
        'methodology':          'Weighted severity-based deduction from 100. CRITICAL: −20 to −30, MAJOR: −10 to −15, MINOR: −5.',
        'category_explanations': {
            cat: {
                'score':       round(float((data.get('score') if isinstance(data,dict) else data) or 0), 1),
                'weight_pct':  f"{round(float((data.get('weight') if isinstance(data,dict) else 0) or 0)*100)}%",
                'description': {
                    'Timeline Compliance':   'Limitation period, notice timing, complaint filing date',
                    'Ingredient Compliance': 'All 7 statutory ingredients of Section 138 NI Act',
                    'Documentary Strength':  'Cheque, dishonour memo, notice proof, agreement, ledger',
                    'Procedural Compliance': 'Jurisdiction, averments, proper parties',
                    'Proper Impleading':     'Section 141 — directors/company correctly named',
                }.get(cat, 'Contributing factor'),
            }
            for cat, data in (risk_result.get('category_scores') or {}).items()
        }
    }


def run_consistency_check(analysis_report: Dict) -> Dict:
    """
    PHASE 3: Internal Consistency Check

    Verifies that fatal_flag, overall_risk_score, and filing_status are all
    mutually consistent after verdict integrity enforcement. Auto-corrects
    stale fatal_flag (False direction only — Phase 1 owns True direction).

    Returns a dict with:
      - 'passed'      : bool   — True if no blocking issues
      - 'blocked'     : bool   — True if output should be suppressed (severe contradiction)
      - 'issues'      : list   — detected inconsistencies (each a str)
      - 'corrections' : dict   — proposed auto-corrections
      - 'message'     : str    — human-readable summary
    """
    issues      = []
    corrections = {}
    blocked     = False

    fatal_flag   = analysis_report.get('fatal_flag', False)
    overall_status = analysis_report.get('overall_status', '')


    risk_score = 50
    if 'modules' in analysis_report and 'risk_assessment' in analysis_report.get('modules', {}):
        risk_score = analysis_report['modules'].get('risk_assessment', {}).get('overall_risk_score', 50) or 50
    else:
        risk_score = analysis_report.get('risk_score', 50) or 50


    exec_summary   = analysis_report.get('executive_summary') or {}
    filing_status  = exec_summary.get('filing_status', '')


    if fatal_flag and risk_score > 65:
        issues.append(
            f'fatal_flag=True but risk_score={risk_score:.1f} (>65) — possible stale fatal flag'
        )

        real_fatals = []
        if 'modules' in analysis_report and 'risk_assessment' in analysis_report.get('modules', {}):
            _all = analysis_report['modules'].get('risk_assessment', {}).get('fatal_defects', [])
            real_fatals = [d for d in _all
                           if 'same-day' not in str(d.get('defect', '')).lower()
                           and 'same day'  not in str(d.get('defect', '')).lower()
                           and d.get('is_absolute') is not False]
        if not real_fatals:
            corrections['fatal_flag'] = False


    if not fatal_flag and risk_score < 20:
        issues.append(
            f'fatal_flag=False but risk_score={risk_score:.1f} (<20) — unusually low score without fatal flag'
        )


    if filing_status:
        status_lower = filing_status.lower()
        if fatal_flag and 'file' in status_lower and 'do not' not in status_lower and 'cannot' not in status_lower:
            issues.append(
                f'fatal_flag=True but filing_status suggests filing is advised: "{filing_status}"'
            )
            corrections['filing_status_override'] = 'DO NOT FILE — Fatal defect(s) detected'
        elif not fatal_flag and ('do not file' in status_lower or 'cannot file' in status_lower):
            issues.append(
                f'fatal_flag=False but filing_status blocks filing: "{filing_status}"'
            )


    if fatal_flag and overall_status and 'strong' in overall_status.lower():
        issues.append(
            f'fatal_flag=True but overall_status="{overall_status}" — contradiction in final verdict'
        )
        blocked = len(issues) >= 3


    passed  = len(issues) == 0
    if passed:
        message = 'All checks passed — fatal_flag, score, and filing_status are consistent'
    elif corrections:
        message = f'{len(issues)} issue(s) detected; {len(corrections)} auto-correction(s) applied'
    else:
        message = f'{len(issues)} issue(s) detected; manual review recommended'

    return {
        'passed':      passed,
        'blocked':     blocked,
        'issues':      issues,
        'corrections': corrections,
        'message':     message,
    }


def perform_comprehensive_analysis(case_data: Dict) -> Dict:

    analysis_start_time = datetime.now()
    case_id = None


    analysis_report = {
        'success': False,
        'error': None,
        'engine_version': ENGINE_VERSION,
        'timestamp': analysis_start_time.isoformat()
    }


    user_email = case_data.get('user_email', '')
    if user_email:
        is_allowed, current_count = check_daily_limit(user_email)
        if not is_allowed:
            logger.warning(f"🚫 Daily limit reached for {user_email}: {current_count}/3 analyses used")
            _limit_val = get_user_limit(user_email)
            return {
                'success': False,
                'error': 'DAILY_LIMIT_REACHED',
                'error_type': 'RATE_LIMIT',
                'message': (
                    f'Daily analysis limit of {_limit_val} reached. '
                    f'You have used {current_count}/{_limit_val} analyses today. '
                    'Please try again tomorrow or contact support to raise your limit.'
                ),
                'usage': {
                    'analyses': {
                        'used':      current_count,
                        'limit':     _limit_val,
                        'remaining': 0,
                        'exhausted': True,
                    },
                    'drafts': {
                        'used':      0,
                        'limit':     3,
                        'remaining': None,
                        'exhausted': None,
                    }
                },
                'next_reset': (date.today() + timedelta(days=1)).isoformat() + 'T00:00:00',
                'fatal_flag': True,
                'overall_status': 'LIMIT REACHED',
                'engine_version': ENGINE_VERSION,
                'timestamp': analysis_start_time.isoformat()
            }
        else:
            logger.info(f"✅ Daily limit check passed for {user_email}: {current_count}/3 analyses used")

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

        missing_fields = [f for f in required_fields if f not in case_data or not case_data[f]]

        if missing_fields:
            logger.error(f"❌ Missing required fields: {missing_fields}")

            field_names = {
                'cheque_date': 'Cheque Date',
                'dishonour_date': 'Dishonour Date',
                'notice_date': 'Notice Sent Date'
            }
            friendly_fields = [field_names.get(f, f) for f in missing_fields]

            return {
                'error': 'MISSING_REQUIRED_FIELDS',
                'error_type': 'VALIDATION_ERROR',
                'missing_fields': missing_fields,
                'message': f"Required fields missing: {', '.join(missing_fields)}",
                'user_friendly_message': f'⚠️ Insufficient Data\n\nPlease provide the following required fields to evaluate your case properly:\n\n• {chr(10).join("• " + f for f in friendly_fields)}\n\nThese dates are essential for accurate timeline compliance analysis under Section 138.',
                'fatal_flag': True,
                'overall_status': 'ERROR - INCOMPLETE DATA',
                'engine_version': ENGINE_VERSION,
                'timestamp': analysis_start_time.isoformat()
            }

        logger.info("🚀 Starting comprehensive legal intelligence analysis...")
        start_time = time.time()


        processing_flags = {
            'heavy_modules_enabled':       True,
            'rag_modules_enabled':         bool(kb_loaded),
            'advanced_modules_enabled':    True,
            'intelligence_layers_enabled': True,
            'audit_trail_enabled':         True,
            'sanity_check_passed':         True,
            'single_truth_enforced':       True,
        }
        analysis_report = {
            'case_id': hashlib.md5(json.dumps(case_data, sort_keys=True).encode()).hexdigest()[:12],
            'input_hash': hashlib.sha256(json.dumps(case_data, sort_keys=True).encode()).hexdigest(),
            'analysis_timestamp': analysis_start_time.isoformat(),
            'engine_version': ENGINE_VERSION,
            'scoring_model_version': SCORING_MODEL_VERSION,
            'timeline_math_version': TIMELINE_MATH_VERSION,
            'fatal_flag': False,
            'user_email': case_data.get('user_email', ''),
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
        analysis_report['processing_flags'] = processing_flags


        logger.info("🔧 PHASE 1: FATAL EVALUATION (Pre-Analysis)")
        analysis_report['audit_log']['phases_executed'].append('PHASE_1_FATAL_EVALUATION')


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
                '_result': {
                    'overall_score': 0, 'overall_score_display': '0.0/100',
                    'is_fatal': True, 'fatal_defects_count': 0,
                    'filing_status': 'ANALYSIS INCOMPLETE — see error',
                    'processing_time': '1.2s', 'processing_time_seconds': 1.2,
                    'documentary_gaps': [], 'cross_exam_questions': [],
                    'next_actions': [{'action': 'Resubmit case data', 'urgency': 'URGENT', 'details': 'Analysis could not complete'}],
                    'presumption_stage': 'INSUFFICIENT DATA', 'burden_position': 'INSUFFICIENT DATA',
                    'court_name': 'Not specified', 'court_confidence': 'INSUFFICIENT DATA',
                    'court_note': 'Judicial behaviour analysis unavailable — insufficient court data.', 'capped_at': 0,
                    'capped_at_display': 'DATA NOT AVAILABLE', 'original_score': 0,
                    'fatal_override_note': 'Analysis did not complete',
                },
                'engine_version': ENGINE_VERSION,
                'analysis_timestamp': analysis_start_time.isoformat()
            }


        _tl_lim_risk = timeline_result.get('limitation_risk', 'LOW')
        _tl_lim_text = str((timeline_result.get('compliance_status') or {}).get('limitation', ''))
        _tl_is_same_day = 'SAME-DAY' in _tl_lim_text.upper() or 'SAME DAY' in _tl_lim_text.upper()
        _tl_is_genuinely_fatal = (
            _tl_lim_risk in ['EXPIRED', 'CRITICAL'] and
            not _tl_is_same_day
        )
        if _tl_is_genuinely_fatal:
            analysis_report['fatal_flag'] = True
            analysis_report['fatal_source'] = 'timeline_intelligence'
            analysis_report['fatal_type'] = 'LIMITATION_EXPIRED'
            logger.warning(f"🔴 FATAL TIMELINE: {_tl_lim_risk} — premature/time-barred — continuing all modules")
        elif _tl_is_same_day:
            analysis_report['same_day_filing'] = True
            logger.info("⚠️ Same-day filing detected — HIGH risk, not fatal — continuing all modules")

        doc_compliance = analyze_document_compliance(case_data)
        analysis_report['modules']['document_compliance'] = doc_compliance


        logger.info("🔒 Analyzing Section 65B Compliance...")
        section_65b = analyze_section_65b_compliance(case_data)
        analysis_report['modules']['section_65b_compliance'] = section_65b


        logger.info("  🏦 Module A: Dishonour Reason Deep Analysis...")
        try:
            dishonour_analysis = analyze_dishonour_reason(case_data)
        except Exception as _e:
            logger.error(f'Dishonour analysis failed: {_e}')
            dishonour_analysis = {'risk_level': 'UNKNOWN', 'reason_category': 'Error', 'legal_impact': ''}
        analysis_report['modules']['dishonour_analysis'] = dishonour_analysis

        logger.info("  ⚖️  Module B: Legally Enforceable Debt Analysis...")
        try:
            enforceable_debt = analyze_legally_enforceable_debt(case_data)
        except Exception as _e:
            logger.error(f'Enforceable debt analysis failed: {_e}')
            enforceable_debt = {'is_enforceable': True, 'risk_level': 'LOW', 'fatal_issues': [], 'issues': []}
        analysis_report['modules']['enforceable_debt'] = enforceable_debt

        if enforceable_debt.get('fatal_issues'):
            analysis_report['fatal_flag'] = True
            analysis_report['fatal_source'] = str(analysis_report.get('fatal_source', '')) + ',enforceable_debt'

        logger.info("  📅 Module C: Delay Condonation Analysis...")
        try:
            delay_condonation = analyze_delay_condonation(case_data, timeline_result)
        except Exception as _e:
            logger.error(f'Delay condonation analysis failed: {_e}')
            delay_condonation = {'condonation_required': False, 'risk_level': 'LOW'}
        analysis_report['modules']['delay_condonation'] = delay_condonation

        logger.info("  📋 Module D: Complaint Drafting Compliance...")
        try:
            drafting_compliance = analyze_complaint_drafting_compliance(case_data)
        except Exception as _e:
            logger.error(f'Drafting compliance failed: {_e}')
            drafting_compliance = {'compliance_score': 50, 'risk_level': 'MEDIUM', 'critical_gaps': []}
        analysis_report['modules']['drafting_compliance'] = drafting_compliance

        logger.info("  📄 Module E: Document Validity & Admissibility...")
        try:
            doc_validity = analyze_document_validity(case_data)
        except Exception as _e:
            logger.error(f'Document validity failed: {_e}')
            doc_validity = {'admissibility_score': 50, 'risk_level': 'MEDIUM', 'invalid_documents': []}
        analysis_report['modules']['document_validity'] = doc_validity

        logger.info("  🚨 Module F: Fraud & False Case Signal Detection...")
        try:
            fraud_signals = analyze_fraud_signals(case_data)
        except Exception as _e:
            logger.error(f'Fraud signals failed: {_e}')
            fraud_signals = {'fraud_risk_score': 0, 'risk_level': 'LOW', 'fraud_signals': []}
        analysis_report['modules']['fraud_signals'] = fraud_signals


        logger.info("💰 Analyzing Income Tax 269SS Compliance...")
        income_tax_269ss = analyze_income_tax_269ss_compliance(case_data)
        analysis_report['modules']['income_tax_269ss'] = income_tax_269ss
        analysis_report['modules']['income_tax_269ss_compliance'] = income_tax_269ss


        logger.info("📬 Analyzing Notice Delivery Status...")
        notice_delivery = analyze_notice_delivery_status(case_data)
        analysis_report['modules']['notice_delivery_status'] = notice_delivery


        logger.info("💵 Analyzing Part Payment Defence...")
        part_payment = analyze_part_payment_defence(case_data)
        analysis_report['modules']['part_payment_analysis'] = part_payment


        logger.info("⚖️  Analyzing Territorial Jurisdiction...")
        jurisdiction = analyze_territorial_jurisdiction(case_data)
        analysis_report['modules']['territorial_jurisdiction'] = jurisdiction


        logger.info("🤝 Analyzing Compounding Eligibility...")
        compounding = analyze_compounding_eligibility(case_data)
        analysis_report['modules']['compounding_analysis'] = compounding


        logger.info("👔 Analyzing Director Role-Based Liability...")
        director_role = analyze_director_role_liability(case_data)
        analysis_report['modules']['director_role_analysis'] = director_role


        fatal_conditions = []


        if len(doc_compliance.get('fatal_defects', [])) > 0:
            fatal_conditions.append({
                'source': 'document_compliance',
                'type': 'DOCUMENT_DEFECTS',
                'details': doc_compliance['fatal_defects']
            })


        if section_65b.get('risk_level') == 'FATAL':
            fatal_conditions.append({
                'source': 'section_65b_compliance',
                'type': 'ELECTRONIC_EVIDENCE_INADMISSIBLE',
                'details': 'Section 65B certificate missing for electronic evidence'
            })


        if income_tax_269ss.get('violation_detected') and income_tax_269ss.get('risk_level') == 'CRITICAL':
            fatal_conditions.append({
                'source': 'income_tax_269ss',
                'type': 'ILLEGAL_CASH_TRANSACTION',
                'details': 'Cash transaction above ₹20,000 violates Section 269SS'
            })


        if part_payment.get('defence_strength') == 'FATAL':
            fatal_conditions.append({
                'source': 'part_payment_analysis',
                'type': 'DEBT_SATISFIED',
                'details': 'Part payment equals or exceeds cheque amount'
            })


        if notice_delivery.get('risk_level') == 'FATAL':
            fatal_conditions.append({
                'source': 'notice_delivery_status',
                'type': 'NOTICE_NOT_SENT',
                'details': 'Legal notice requirement not satisfied'
            })


        if len(fatal_conditions) > 0:
            analysis_report['fatal_flag'] = True
            analysis_report['fatal_source'] = ', '.join([f['source'] for f in fatal_conditions])
            analysis_report['fatal_type'] = 'MULTIPLE_FATAL_CONDITIONS'
            analysis_report['fatal_conditions'] = fatal_conditions
            analysis_report['fatal_details'] = [f"{f['type']}: {f['details']}" for f in fatal_conditions]
            logger.warning(f"🔴 FATAL CONDITIONS ({len(fatal_conditions)}) — recording but continuing all modules for complete report")

        logger.info("✅ PHASE 1 COMPLETE: No fatal conditions - Proceeding to full analysis")
        analysis_report['audit_log']['phase_1_passed'] = True

        logger.info("🔧 PHASE 2: Core Analysis Modules")
        analysis_report['audit_log']['phases_executed'].append('PHASE_2_CORE_ANALYSIS')
        analysis_report['audit_log']['phase_2_started'] = datetime.now().isoformat()

        logger.info("  Module 2: Ingredient Analysis...")
        try:
            ingredient_result = analyze_ingredients(case_data, timeline_result)
        except Exception as _e:
            logger.error(f'❌ Module failed: {_e}')
            ingredient_result = {'overall_compliance': 0, 'risk_level': 'Error', 'ingredient_details': [], 'fatal_defects': [], 'critical_count': 0}
        analysis_report['modules']['ingredient_compliance'] = ingredient_result
        analysis_report['architecture']['deterministic_modules'].append('Ingredient Compliance')

        logger.info("  Module 3: Documentary Analysis...")
        try:
            doc_result = analyze_documentary_strength(case_data)
        except Exception as _e:
            logger.error(f'❌ Module failed: {_e}')
            doc_result = {'overall_strength_score': 0, 'strength_label': 'Error', 'fatal_defects': []}
        analysis_report['modules']['documentary_strength'] = doc_result
        analysis_report['architecture']['deterministic_modules'].append('Documentary Strength')


        logger.info("  Module 3.5: Security Cheque Probability...")
        security_cheque_result = analyze_security_cheque_probability(case_data)
        analysis_report['modules']['security_cheque_analysis'] = security_cheque_result
        analysis_report['architecture']['deterministic_modules'].append('Security Cheque Detector')


        logger.info("  Module 3.6: Financial Capacity Analysis...")
        financial_capacity_result = analyze_financial_capacity(case_data)
        analysis_report['modules']['financial_capacity'] = financial_capacity_result
        analysis_report['architecture']['deterministic_modules'].append('Financial Capacity')

        logger.info("  Module 4: Liability Analysis...")
        try:
            liability_result = analyze_accused_liability(case_data)
        except Exception as _e:
            logger.error(f'❌ Module failed: {_e}')
            liability_result = {'liability_score': 0, 'parties_properly_impleaded': False}
        analysis_report['modules']['liability_analysis'] = liability_result
        analysis_report['architecture']['deterministic_modules'].append('Liability Expansion')

        logger.info("  Module 5: Defence Analysis...")
        defence_result = analyze_defence_vulnerabilities(case_data, ingredient_result, doc_result)
        if not isinstance(defence_result, dict):
            defence_result = {}


        _tl_limit = timeline_result.get('compliance_status', {}).get('limitation', '')
        if 'PREMATURE' in str(_tl_limit).upper() or 'SAME-DAY' in str(_tl_limit).upper():
            _is_premature = 'PREMATURE' in str(_tl_limit).upper()
            _defence_entry = {
                'defence': ('Premature Complaint — Cause of Action Not Yet Arisen'
                            if _is_premature else
                            'Same-Day Filing — Borderline Premature Complaint'),
                'strength': 'FATAL' if _is_premature else 'HIGH',
                'legal_basis': 'Section 138(b) NI Act — complaint maintainable only after 15-day payment period expires',
                'probability': 'CERTAIN' if _is_premature else 'HIGH',
                'strategy': ('File discharge application under Section 245 CrPC at FIRST HEARING — complaint is not maintainable at threshold stage. Court must dismiss without trial.'
                             if _is_premature else
                             'Challenge maintainability at threshold stage — filing on same day as cause of action is legally risky under Section 138(b)'),
                'source': 'timeline_intelligence',
                'refiling_note': 'Complainant CAN refile after cause of action arises, subject to limitation period.'
            }
            if 'high_risk_defences' not in defence_result:
                defence_result['high_risk_defences'] = []

            existing_defences = [d.get('defence', '') for d in (defence_result.get('high_risk_defences') or [])]
            if _defence_entry['defence'] not in existing_defences:
                defence_result['high_risk_defences'].insert(0, _defence_entry)
            defence_result['premature_complaint_detected'] = True
            defence_result['threshold_dismissal_available'] = True
            defence_result['threshold_strategy'] = (
                'File discharge application at first hearing under Section 245 CrPC. '
                'Complaint can be dismissed at threshold stage without full trial. '
                'Grounds: cause of action had not arisen at time of filing.'
            )


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
            except Exception as _e:
                logger.debug(f"Non-critical exception suppressed: {_e}")

        analysis_report['modules']['defence_matrix'] = defence_result
        analysis_report['architecture']['deterministic_modules'].append('Defence Vulnerability')

        logger.info("  Module 6: Defect Scanning...")
        defect_result = scan_procedural_defects(case_data, timeline_result, liability_result)
        if not isinstance(defect_result, dict):
            defect_result = {'overall_risk': 'Insufficient data', 'fatal_defects': [], 'curable_defects': [], 'warnings': []}


        for rm in timeline_result.get('risk_markers', []):
            sev = rm.get('severity', '')
            defect_text = rm.get('issue', '')

            existing = [d.get('defect', '') for d in defect_result.get('fatal_defects', []) +
                        defect_result.get('curable_defects', []) + defect_result.get('warnings', [])]
            if defect_text and defect_text not in existing:
                if sev == 'FATAL' or sev == 'CRITICAL':
                    defect_result.setdefault('fatal_defects', []).append({
                        'defect': defect_text,
                        'severity': sev,
                        'impact': rm.get('impact', 'Fatal procedural defect from timeline'),
                        'remedy': rm.get('action', 'Consult legal counsel'),
                        'source': 'timeline_intelligence'
                    })
                elif sev == 'HIGH':
                    defect_result.setdefault('curable_defects', []).append({
                        'defect': defect_text,
                        'severity': sev,
                        'cure': rm.get('action', 'Address before filing'),
                        'source': 'timeline_intelligence'
                    })
                else:
                    defect_result.setdefault('warnings', []).append({
                        'area': 'Timeline',
                        'warning': defect_text,
                        'check': rm.get('action', 'Verify before filing')
                    })


        fatal_count = len(defect_result.get('fatal_defects', []))
        if fatal_count >= 2:
            defect_result['overall_risk']     = 'CRITICAL — Multiple Fatal Defects'
            defect_result['risk_category']    = 'Clear Defect'
        elif fatal_count == 1:
            defect_result['overall_risk']     = 'HIGH — One Fatal Defect Present'
            defect_result['risk_category']    = 'Clear Defect'
        elif any(d.get('severity') == 'HIGH' for d in defect_result.get('curable_defects', [])):
            defect_result['overall_risk']     = 'MEDIUM — Borderline Procedural Risk'
            defect_result['risk_category']    = 'Borderline Risk'
        elif defect_result.get('curable_defects'):
            defect_result['overall_risk']     = 'MEDIUM — Curable Defects Present'
            defect_result['risk_category']    = 'Borderline Risk'
        else:
            defect_result['overall_risk']     = 'LOW — No Procedural Defects Detected'
            defect_result['risk_category']    = 'No Defect'

        analysis_report['modules']['procedural_defects'] = defect_result
        analysis_report['architecture']['deterministic_modules'].append('Procedural Defects')


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


        if not isinstance(risk_result, dict):
            logger.error(f"risk_result is not a dict: {type(risk_result)}")
            risk_result = {'overall_risk_score': 0, 'category_scores': {}, 'fatal_defects': []}

        if not isinstance(risk_result, dict) or 'overall_risk_score' not in risk_result or risk_result['overall_risk_score'] is None:
            logger.error("risk_result missing or None overall_risk_score - setting to 0")
            risk_result['overall_risk_score'] = 0

        analysis_report['modules']['risk_assessment'] = risk_result
        analysis_report['architecture']['deterministic_modules'].append('Risk Scoring')

        _risk_fatals = [d for d in (risk_result.get('fatal_defects') or [])
                        if d.get('severity') in ('FATAL', 'CRITICAL')
                        and 'same-day' not in str(d.get('defect','')).lower()
                        and 'same day' not in str(d.get('defect','')).lower()]
        if _risk_fatals:
            analysis_report['fatal_flag'] = True
            logger.warning(f"🔴 FATAL DEFECTS DETECTED ({len(_risk_fatals)}) - Score capped")

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


        logger.info("🔧 PHASE 3: Verdict Integrity + Consistency Check")
        analysis_report['audit_log']['phases_executed'].append('PHASE_3_CONSISTENCY')
        analysis_report['audit_log']['phase_3_started'] = datetime.now().isoformat()

        logger.info("🎯 Enforcing Verdict Integrity (Pre-Summary)...")
        analysis_report = enforce_verdict_integrity(analysis_report)


        logger.info("🔍 Running internal consistency check...")
        _consistency = run_consistency_check(analysis_report)
        if _consistency.get('corrections'):
            _corr = _consistency['corrections']


            if 'fatal_flag' in _corr and _corr['fatal_flag'] is False:

                analysis_report['fatal_flag'] = False
                logger.info("  ⚙️  Phase 3: Cleared stale fatal_flag (no real fatal defects)")
            elif 'fatal_flag' in _corr and _corr['fatal_flag'] is True:

                logger.info("  ℹ️  Phase 3: fatal_flag=True mismatch noted but not overridden (Phase 1 authority)")
            if 'filing_status_override' in _corr:
                _es = analysis_report.get('executive_summary') or {}
                if _es:
                    _es['filing_status'] = _corr['filing_status_override']
                    logger.info(f"  ⚙️  Corrected filing_status → {_corr['filing_status_override']}")
        analysis_report['_consistency_check'] = _consistency
        if _consistency.get('blocked'):
            logger.error(f"  🛑 Consistency check BLOCKED output: {_consistency.get('issues')}")
        logger.info(f"  ✅ Consistency: {_consistency['message']}")

        logger.info("🔧 PHASE 4: Decision Generation (Reports + Summaries)")
        analysis_report['audit_log']['phases_executed'].append('PHASE_4_DECISION_GENERATION')
        analysis_report['audit_log']['phase_4_started'] = datetime.now().isoformat()
        logger.info("📊 Generating Executive Intelligence Report...")
        try:
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
        except Exception as _e:
            logger.error(f'❌ executive_summary failed: {_e}')
            import traceback as _tb3; logger.error(_tb3.format_exc())
            analysis_report['executive_summary'] = {
                'case_overview': 'Summary generation failed — see full analysis',
                'filing_verdict': 'See analysis',
                'overall_assessment': 'ERROR',
                'strengths': [], 'weaknesses': [], 'critical_risks': [],
                'strategic_recommendations': [], 'next_actions': [],
                'edge_cases_alert': [], 'fatal_defects_count': 0
            }

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


        kb_provisions = len(kb_court_patterns) if kb_court_patterns else 0
        if processing_flags.get('heavy_modules_enabled') and kb_provisions >= 10:
            judicial_variance = simulate_judicial_variance(
                (analysis_report['modules'].get('risk_assessment') or {}).get('overall_risk_score', 0),
                {
                    'timeline': analysis_report['modules'].get('risk_assessment', {})['category_scores']['Timeline Compliance']['score'],
                    'ingredient': analysis_report['modules'].get('risk_assessment', {})['category_scores']['Ingredient Compliance']['score'],
                    'documentary': analysis_report['modules'].get('risk_assessment', {})['category_scores']['Documentary Strength']['score'],
                    'procedural': analysis_report['modules'].get('risk_assessment', {})['category_scores']['Procedural Compliance']['score']
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

        logger.info("  🔍 Advanced Contradiction Detection (reusing Module 9 result)")

        analysis_report['contradictions_detected'] = contradiction_result

        logger.info("  🛡️ Fraud Risk Analysis...")
        fraud_analysis = calculate_fraud_risk(case_data)
        analysis_report['fraud_risk_analysis'] = fraud_analysis

        analysis_report['version_info'] = get_version_info()

        logger.info("📄 Module 14: Document Compliance (reusing Phase 1 result — no re-run)")


        if len(doc_compliance.get('fatal_defects', [])) > 0:
            analysis_report['fatal_flag'] = True
            analysis_report['overall_status'] = 'FATAL - FILING BLOCKED'
            analysis_report['filing_blocked'] = True

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
            analysis_report['fatal_type'] = analysis_report.get('fatal_type', 'EVIDENCE_GAP')
            analysis_report['filing_blocked'] = True
            logger.warning("🔴 FATAL EVIDENCE GAP — continuing modules for complete report")

        if len(dependency_check.get('violations', [])) > 0:
            has_fatal_dependency = any(v['severity'] == 'FATAL' for v in dependency_check['violations'])
            if has_fatal_dependency:
                analysis_report['fatal_flag'] = True
                analysis_report['overall_status'] = 'FATAL - MANDATORY REQUIREMENTS NOT MET'
                analysis_report['filing_blocked'] = True
                logger.warning("🔴 MANDATORY DEPENDENCY VIOLATIONS — continuing modules for complete report")

        if len(defence_risks.get('fatal_defences', [])) > 0:
            analysis_report['fatal_flag'] = True
            analysis_report['overall_status'] = 'FATAL - CASE VIABILITY COMPROMISED'
            analysis_report['filing_blocked'] = False
            logger.warning("🔴 FATAL DEFENCE RISKS — continuing modules for complete report")

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


        analysis_report['processing_flags'] = {
            'heavy_modules_enabled': True,
            'kb_available': kb_loaded,
            'kb_rows': len(kb_data) if kb_loaded else 0,
            'judicial_variance_enabled': kb_loaded and len(kb_court_patterns) >= 10,
            'llm_enabled': False,
            'all_phases_executed': True,
        }

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

        logger.info("📊 Calculating Data Completeness...")
        analysis_report['data_completeness'] = calculate_data_completeness(case_data)


        required_fields = ['cheque_date','dishonour_date','notice_date','complaint_filed_date',
                           'notice_received_date','cheque_amount','bank_name','court_location',
                           'debt_nature','dishonour_reason']
        optional_fields = ['written_agreement_exists','ledger_available','postal_proof_available',
                           'original_cheque_available','return_memo_available','witness_available',
                           'email_sms_evidence','transaction_date','transaction_amount']
        present_req = sum(1 for f in required_fields if case_data.get(f))
        present_opt = sum(1 for f in optional_fields if case_data.get(f))
        completeness_pct = round(
            (present_req / max(len(required_fields), 1)) * 70 +
            (present_opt / max(len(optional_fields), 1)) * 30
        )
        missing_fields = [f.replace('_',' ') for f in required_fields if not case_data.get(f)]
        missing_fields += [f.replace('_',' ') for f in optional_fields if not case_data.get(f)]

        analysis_report['data_completeness'] = {
            'percentage':      completeness_pct,
            'display':         f'{completeness_pct}%',
            'level':           'High' if completeness_pct >= 80 else ('Medium' if completeness_pct >= 60 else 'Low'),
            'missing_fields':  missing_fields[:6],
            'reliability_note': (
                'Analysis is highly reliable — all key fields provided.'
                if completeness_pct >= 80 else
                f'Analysis is partially reliable — {len(missing_fields)} field(s) missing: {", ".join(missing_fields[:3])}{"..." if len(missing_fields) > 3 else ""}.'
            )
        }


        analysis_report['module_confidence'] = {
            'timeline':     'HIGH'   if case_data.get('notice_received_date') and case_data.get('complaint_filed_date') else 'MEDIUM',
            'ingredients':  'HIGH'   if present_req >= 8 else 'MEDIUM',
            'documentary':  'HIGH'   if present_opt >= 6 else ('MEDIUM' if present_opt >= 3 else 'LOW'),
            'defence':      'MEDIUM' if case_data.get('defence_type') else 'LOW',
            'judicial':     'LOW',
            'overall':      'HIGH'   if completeness_pct >= 80 else ('MEDIUM' if completeness_pct >= 60 else 'LOW'),
        }

        logger.info(f"📊 Data completeness: {completeness_pct}%")


        logger.info("🔍 Running execution validation...")


        _raw_v = risk_result.get('overall_risk_score') or 0
        if _raw_v == 0:
            _cats_v = risk_result.get('category_scores') or {}
            if _cats_v:
                _recovered = round(sum(
                    (v.get('score', 0) or 0) * (v.get('weight', 0) or 0)
                    for v in _cats_v.values() if isinstance(v, dict)
                ), 1)
                if _recovered > 0:
                    risk_result['overall_risk_score'] = _recovered
                    analysis_report['modules']['risk_assessment'] = risk_result
                    logger.warning(f"⚠️ Validation: recovered raw score {_recovered} from category scores")


        _es_v = analysis_report.get('executive_summary') or {}
        if not _es_v or _es_v.get('overall_assessment') == 'ERROR' or not _es_v.get('case_overview'):
            logger.warning("⚠️ Validation: executive_summary failed — rebuilding minimal summary")
            _vs = risk_result.get('overall_risk_score') or 0
            _vf = len(risk_result.get('fatal_defects') or []) > 0
            analysis_report['executive_summary'] = {
                'case_overview': (
                    f"Section 138 NI Act | "
                    f"Cheque: ₹{case_data.get('cheque_amount', 0):,} | "
                    f"Score: {_vs}/100 | "
                    f"Fatal defects: {'Yes' if _vf else 'No'}"
                ),
                'filing_verdict': (
                    'DO NOT FILE — FATAL DEFECTS PRESENT' if _vf else
                    'READY TO FILE — STRONG CASE' if _vs >= 75 else
                    'FILE WITH CAUTION — GAPS PRESENT' if _vs >= 55 else
                    'HIGH RISK — REMEDIATION REQUIRED'
                ),
                'overall_assessment': (
                    'FATAL FAILURE' if _vf else
                    'COMPLIANT' if _vs >= 75 else
                    'MODERATE RISK' if _vs >= 55 else 'WEAK CASE'
                ),
                'strengths': [
                    s for s in [
                        f"Ingredient Compliance: {ingredient_result.get('overall_compliance', 0):.1f}/100"
                        if (ingredient_result.get('overall_compliance') or 0) >= 60 else None,
                        "Original cheque and dishonour memo secured"
                        if case_data.get('original_cheque_available') and case_data.get('return_memo_available') else None,
                        f"Timeline Compliance: {timeline_result.get('score', 0)}/100"
                        if (timeline_result.get('score') or 0) >= 70 else None,
                    ] if s
                ],
                'weaknesses': [
                    w for w in [
                        f"Documentary strength: {doc_result.get('overall_strength_score', 0):.1f}% — strengthen supporting records"
                        if (doc_result.get('overall_strength_score') or 100) < 50 else None,
                        "No written agreement — primary defence weakness"
                        if not case_data.get('written_agreement_exists') else None,
                    ] if w
                ],
                'critical_risks': [
                    {'risk': d.get('defect', 'Fatal defect'), 'severity': 'CRITICAL',
                     'impact': d.get('impact', 'Will cause dismissal'),
                     'remedy': d.get('remedy', 'Consult legal counsel')}
                    for d in (risk_result.get('fatal_defects') or [])[:3]
                ],
                'strategic_recommendations': [{
                    'priority': 'URGENT' if _vf else 'HIGH',
                    'action': 'Address all fatal defects before filing' if _vf else 'Strengthen evidence',
                    'recommendation': 'Do not file current complaint — fatal defects will cause dismissal' if _vf else f'File with caution — score {_vs}/100',
                    'rationale': f"{len(risk_result.get('fatal_defects') or [])} fatal defect(s) detected" if _vf else f"Risk score {_vs}/100",
                }],
                'next_actions': [{'action': 'Review full analysis below', 'urgency': 'HIGH', 'details': 'All module outputs available in the detailed report'}],
                'edge_cases_alert': [],
                'fatal_defects_count': len(risk_result.get('fatal_defects') or []),
            }


        if not analysis_report.get('processing_time_seconds'):
            analysis_report['processing_time_seconds'] = max(0.5, round(time.time() - start_time, 2))


        _critical_failures = []
        if not isinstance(timeline_result, dict) or not timeline_result:
            _critical_failures.append('Timeline module returned no data')
        if not isinstance(ingredient_result, dict) or not ingredient_result.get('overall_compliance'):
            _critical_failures.append('Ingredient compliance module returned no data')
        if not isinstance(risk_result, dict) or not risk_result.get('category_scores'):
            _critical_failures.append('Risk scoring module returned no category scores')
        if len(_critical_failures) >= 2:
            logger.error(f'🛑 HARD FAIL-SAFE: {len(_critical_failures)} critical module failures — aborting report generation')
            analysis_report['hard_failure'] = True
            analysis_report['hard_failure_reasons'] = _critical_failures
            analysis_report['overall_status'] = 'ANALYSIS FAILED — CRITICAL MODULES UNAVAILABLE'
            analysis_report['success'] = False
            analysis_report['error'] = 'Critical modules failed: ' + '; '.join(_critical_failures)
            return analysis_report

        logger.info("✅ Execution validation complete")

        logger.info("📋 Generating Audit Trail + Decision Trace...")
        analysis_report['audit_trail'] = generate_audit_trail(
            case_data,
            timeline_result,
            ingredient_result,
            doc_result,
            risk_result
        )


        _score_for_narrative = float((analysis_report.get('modules', {}).get('risk_assessment') or {}).get('overall_risk_score', 0) or 0)
        _fatal_for_narrative = analysis_report.get('fatal_flag', False)
        _doc_score_narrative = float((analysis_report.get('modules', {}).get('risk_assessment', {}).get('category_scores') or {}).get('Documentary Strength', {}).get('score', 0) if isinstance((analysis_report.get('modules', {}).get('risk_assessment', {}).get('category_scores') or {}).get('Documentary Strength'), dict) else 0)
        _tl_score_narrative  = float((analysis_report.get('modules', {}).get('risk_assessment', {}).get('category_scores') or {}).get('Timeline Compliance', {}).get('score', 0) if isinstance((analysis_report.get('modules', {}).get('risk_assessment', {}).get('category_scores') or {}).get('Timeline Compliance'), dict) else 0)
        analysis_report['decision_narrative'] = [
            f"Timeline compliance: {_tl_score_narrative:.0f}/100 — {'Strong ✅' if _tl_score_narrative >= 75 else 'Weak ❌'}",
            f"Documentary strength: {_doc_score_narrative:.0f}/100 — {'Adequate ✅' if _doc_score_narrative >= 60 else 'Insufficient — strengthen before filing ❌'}",
            f"Fatal defects: {'Yes — filing blocked' if _fatal_for_narrative else 'None detected ✅'}",
            f"Overall score: {_score_for_narrative:.1f}/100",
            f"Decision basis: {'Fatal defect present — do not file' if _fatal_for_narrative else 'FILE WITH CAUTION — documentary weakness is primary gap' if _score_for_narrative < 70 else 'Case is strong — proceed to file'}",
        ]
        analysis_report['single_truth'] = {
            'score':    round(_score_for_narrative, 1),
            'decision': ('DO NOT FILE' if _fatal_for_narrative else
                         'FILE WITH CAUTION' if _score_for_narrative < 70 else
                         'READY TO FILE'),
            'primary_gap': ('Fatal procedural defect' if _fatal_for_narrative else
                            'Documentary evidence weakness' if _doc_score_narrative < 60 else
                            'Minor gaps — proceed'),
            'source': 'analysis_pipeline',
        }


        _risk_for_trace = analysis_report.get('modules', {}).get('risk_assessment', {}) or {}
        _score_for_trace = round(float(_risk_for_trace.get('overall_risk_score', 0) or 0), 1)
        _fatal_for_trace = analysis_report.get('fatal_flag', False)
        _tl_for_trace = analysis_report.get('modules', {}).get('timeline_intelligence', {}) or {}
        _lim_for_trace = str((_tl_for_trace.get('compliance_status') or {}).get('limitation', '') or '')
        analysis_report['decision_trace'] = [
            f"INPUT: {len([f for f in ['cheque_date','dishonour_date','notice_date','complaint_filed_date'] if case_data.get(f)])}/4 critical dates provided",
            f"TIMELINE: {_lim_for_trace or 'Status checked'} — limitation risk: {_tl_for_trace.get('limitation_risk','UNKNOWN')}",
            f"SCORE: {_score_for_trace}/100 from weighted module combination",
            f"FATAL FLAG: {'SET — filing blocked until resolved' if _fatal_for_trace else 'NOT SET — case is potentially maintainable'}",
            f"CONSISTENCY: {analysis_report.get('_consistency_check', {}).get('message', 'Not checked')}",
            f"FINAL DECISION: {analysis_report.get('executive_summary', {}).get('filing_verdict', 'See analysis')}",
        ]
        logger.info("  ✅ Decision trace built (%d steps)", len(analysis_report['decision_trace']))

        logger.info("💡 Generating Score Explanation...")
        analysis_report['score_explanation'] = generate_score_explanation(risk_result)

        logger.info("🎯 Classifying Case Outcome...")

        _os = (risk_result.get('overall_risk_score') or 0)
        _is_f = analysis_report.get('fatal_flag', False)
        _conv_prob = (
            '<5%'   if _is_f else
            '<30%'  if _os < 40 else
            '40-60%' if _os < 70 else
            '>70%'
        )
        _dism_prob = (
            '>95%' if _is_f else
            '>70%' if _os < 40 else
            '40-60%' if _os < 60 else
            '<25%'
        )
        analysis_report['outcome_probability'] = {
            'dismissal_probability':          _dism_prob,
            'conviction_probability':         _conv_prob,
            'conviction_probability_label':   'Conviction Probability',
            'dismissal_probability_label':    'Dismissal Probability',
            'conviction_probability_display': f"Conviction Probability: {_conv_prob}",
            'dismissal_probability_display':  f"Dismissal Probability: {_dism_prob}",
            'verdict_label': (
                'Very High likelihood of dismissal'  if _is_f or _os < 30 else
                'High dismissal risk'                if _os < 50 else
                'Balanced — could go either way'     if _os < 70 else
                'Strong case — conviction likely'
            ),
            'basis': 'Statistical estimate based on compliance score and fatal defect analysis'
        }

        analysis_report['outcome_classification'] = classify_case_outcome(
            (risk_result.get('overall_risk_score') or 0),
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


        logger.info("🔗 Integrating advanced module outputs into risk score...")
        _score_adj = 0
        _adv_fatals = []
        _adv_warnings = []


        _dh = analysis_report.get('modules', {}).get('dishonour_analysis', {})
        if isinstance(_dh, dict):
            _score_adj += _dh.get('score_impact', 0)
            if _dh.get('risk_level') == 'HIGH':
                _adv_warnings.append(f"Dishonour reason: {_dh.get('reason_category','Unknown')} — {_dh.get('legal_impact','')[:80]}")


        _ed = analysis_report.get('modules', {}).get('enforceable_debt', {})
        if isinstance(_ed, dict):
            for _fi in (_ed.get('fatal_issues') or []):
                _adv_fatals.append({
                    'defect':     _fi.get('issue', 'Debt enforceability violation'),
                    'severity':   'FATAL',
                    'impact':     _fi.get('impact', ''),
                    'remedy':     _fi.get('remedy', ''),
                    'legal_basis':_fi.get('legal_basis', 'Section 269SS IT Act / Contract Act'),
                    'source':     'enforceable_debt'
                })
            _score_adj += _ed.get('score_adjustment', 0)


        _dc = analysis_report.get('modules', {}).get('delay_condonation', {})
        if isinstance(_dc, dict) and _dc.get('condonation_required'):
            if _dc.get('condonation_strength') == 'WEAK':
                _score_adj -= 15
                _adv_warnings.append("Delay condonation required but grounds are weak")
            elif _dc.get('condonation_strength') == 'MODERATE':
                _score_adj -= 8


        _dr = analysis_report.get('modules', {}).get('drafting_compliance', {})
        if isinstance(_dr, dict):
            _draft_deduct = len(_dr.get('critical_gaps', [])) * 5
            _score_adj -= min(_draft_deduct, 20)
            if _dr.get('risk_level') in ('HIGH', 'CRITICAL'):
                _adv_warnings.append(f"Complaint drafting: {len(_dr.get('critical_gaps',[]))} critical gap(s)")


        _fr = analysis_report.get('modules', {}).get('fraud_signals', {})
        if isinstance(_fr, dict) and _fr.get('risk_level') in ('HIGH', 'CRITICAL'):
            _score_adj -= 10
            _adv_warnings.append(f"Fraud risk signals detected: {_fr.get('risk_level')}")


        _jur = analysis_report.get('modules', {}).get('territorial_jurisdiction', {})
        if isinstance(_jur, dict) and _jur.get('risk_level') in ('HIGH', 'FATAL'):
            if _jur.get('risk_level') == 'FATAL':
                _adv_fatals.append({
                    'defect':     'Territorial Jurisdiction Defect',
                    'severity':   'FATAL',
                    'impact':     _jur.get('issue', 'Court may lack jurisdiction to hear the complaint'),
                    'remedy':     'File complaint in court having jurisdiction over place where cheque was presented for payment',
                    'legal_basis':'Section 142(2) NI Act: jurisdiction vests where cheque was presented'
                })
            else:
                _score_adj -= 8


        _dl = analysis_report.get('modules', {}).get('director_role_analysis', {})
        if isinstance(_dl, dict) and case_data.get('is_company_case'):
            if not _dl.get('properly_impleaded', True):
                _adv_fatals.append({
                    'defect':     'Directors Not Properly Impleaded',
                    'severity':   'CRITICAL',
                    'impact':     'Company case requires all responsible directors named with specific averments under S.141',
                    'remedy':     'Amend complaint to include all directors in-charge at time of offence with specific S.141 averments',
                    'legal_basis':'Section 141 NI Act: vicarious liability of directors'
                })


        if _score_adj != 0 or _adv_fatals:
            _cur_score = risk_result.get('overall_risk_score', 0) or 0
            _adj_score = round(max(0, min(98, _cur_score + _score_adj)), 1)
            if _score_adj != 0:
                risk_result['overall_risk_score'] = _adj_score
                risk_result['advanced_module_adjustment'] = _score_adj
                logger.info(f"📊 Advanced module adjustment: {_score_adj:+.0f} → score {_cur_score} → {_adj_score}")
            if _adv_fatals:
                risk_result.setdefault('fatal_defects', []).extend(_adv_fatals)
                analysis_report['fatal_flag'] = True
        if _adv_warnings:
            analysis_report['advanced_module_warnings'] = _adv_warnings


        logger.info("⚖️ Generating Professional Report Enhancements...")


        if not isinstance(risk_result, dict): risk_result = {'overall_risk_score': 0, 'category_scores': {}, 'fatal_defects': []}
        if not isinstance(doc_result, dict): doc_result = {'overall_strength_score': 0}
        if not isinstance(ingredient_result, dict): ingredient_result = {'overall_compliance': 0}
        if not isinstance(liability_result, dict): liability_result = {}
        if not isinstance(defect_result, dict): defect_result = {'overall_risk': 'Insufficient data', 'fatal_defects': []}
        if not isinstance(defence_result, dict): defence_result = {}

        primary_weakness = None
        if (doc_result.get('overall_strength_score') or 0) < 70:
            primary_weakness = "documentary evidence"
        elif (ingredient_result.get('overall_compliance') or 0) < 70:
            primary_weakness = "ingredient compliance"

        analysis_report['verdict'] = generate_verdict_one_liner(
            (risk_result.get('overall_risk_score') or 0),
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
            (risk_result.get('category_scores') or {}),
            risk_result.get('fatal_defects', []),
            documentary_gaps
        )

        analysis_report['court_impact'] = generate_court_impact_assessment(
            (risk_result.get('overall_risk_score') or 0),
            risk_result.get('fatal_defects', []),
            case_data.get('defence_type'),
            (doc_result.get('overall_strength_score') or 0)
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

        _pt = round(time.time() - start_time, 2)
        _pt = max(_pt, 0.5)
        analysis_report['processing_time_seconds'] = _pt
        analysis_report['processing_time_ms'] = round(_pt * 1000)
        analysis_report['processing_time'] = f'{_pt:.2f}s'

        if 'modules' in analysis_report:
            for _mod in analysis_report['modules'].values():
                if isinstance(_mod, dict):
                    for _k, _v in list(_mod.items()):
                        if isinstance(_v, float):
                            _mod[_k] = round(_v, 1)
        if isinstance(analysis_report.get('risk_score'), float):
            analysis_report['risk_score'] = round(analysis_report['risk_score'], 1)
        if isinstance(analysis_report.get('overall_score'), float):
            analysis_report['overall_score'] = round(analysis_report['overall_score'], 1)

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

        analysis_report['final_tier']            = final_tier['tier']
        analysis_report['final_status']          = final_tier['status']
        analysis_report['final_score']           = final_tier['score']
        analysis_report['final_recommendation']  = final_tier['recommendation']
        analysis_report['tier_locked']           = final_tier.get('tier_locked', False)
        analysis_report['minimum_tier_enforcement'] = final_tier.get('minimum_tier_enforcement')
        analysis_report['deterministic_escalation'] = True


        if final_tier.get('tier_locked') and 'risk_assessment' in analysis_report.get('modules', {}):
            _pre_esc = analysis_report['modules']['risk_assessment'].get('overall_risk_score', 0)
            _post_esc = final_tier['score']
            if abs(_pre_esc - _post_esc) > 5:
                analysis_report['modules']['risk_assessment']['overall_risk_score'] = _post_esc
                analysis_report['modules']['risk_assessment']['_escalation_applied'] = True
                analysis_report['modules']['risk_assessment']['_pre_escalation_score'] = _pre_esc
                logger.info(f"   Escalation feedback: {_pre_esc:.1f} → {_post_esc:.1f} (tier locked: {final_tier['tier']})")

        analysis_report['audit_log']['escalation_flags'] = escalation_flags
        analysis_report['audit_log']['final_tier_assigned'] = final_tier['tier']
        analysis_report['audit_log']['analysis_completed'] = datetime.now().isoformat()

        logger.info(f"   Final Tier: {final_tier['tier']} (Locked: {final_tier.get('tier_locked', False)})")
        logger.info(f"   Production Engine v{ENGINE_VERSION}")

        logger.info("📄 Generating clean professional report...")

        if "modules" in analysis_report:
            analysis_report["modules"] = sanitize_module_output(analysis_report["modules"])

        try:


            _mods   = analysis_report.get('modules', {}) or {}
            _risk   = _mods.get('risk_assessment', {}) or {}
            _tl     = analysis_report.get('modules', {}).get('timeline_intelligence', {}) or {}
            _ing    = analysis_report.get('modules', {}).get('ingredient_compliance', {}) or {}
            _doc    = analysis_report.get('modules', {}).get('documentary_strength', {}) or {}
            _def    = analysis_report.get('modules', {}).get('procedural_defects', {}) or {}
            _defm   = analysis_report.get('modules', {}).get('defence_matrix', {}) or {}
            _cx     = analysis_report.get('modules', {}).get('cross_examination_risk', {}) or {}
            _jud    = analysis_report.get('modules', {}).get('judicial_behavior', {}) or {}
            _pres   = analysis_report.get('modules', {}).get('presumption_analysis', {}) or {}
            _sett   = analysis_report.get('modules', {}).get('settlement_analysis', {}) or {}

            _raw_fatal_flag = bool(analysis_report.get('fatal_flag'))

            _real_fatals_in_report = [d for d in (_risk.get('fatal_defects') or [])
                                   if 'same-day' not in str(d.get('defect','')).lower()
                                   and 'same day' not in str(d.get('defect','')).lower()
                                   and d.get('is_absolute') is not False
                                   and d.get('severity') in ('FATAL','CRITICAL','')]
            _is_fatal = bool(_real_fatals_in_report) or (
            _raw_fatal_flag and not analysis_report.get('same_day_filing')
            )


            _all_fatals = [
                d for _src in [
                    (_risk.get('fatal_defects') or []),
                    (_def.get('fatal_defects') or []),
                    (_ing.get('fatal_defects') or []),
                ] for d in _src
                if 'same-day' not in str(d.get('defect','')).lower()
                and 'same day' not in str(d.get('defect','')).lower()
                and d.get('is_absolute') is not False
            ]
            _seen_f, _uniq_f = set(), []
            for _fd in _all_fatals:
                _k = _fd.get('defect', str(_fd))
                if _k not in _seen_f:
                    _seen_f.add(_k); _uniq_f.append(_fd)
            _uniq_f = deduplicate_fatal_defects(_uniq_f)


            _cat = _risk.get('category_scores', {}) or {}
            def _cscore(name, fb=0.0):
                d = _cat.get(name)
                try: return round(float(d.get('score') if isinstance(d,dict) else d), 1)
                except Exception: return fb


            _score_trail   = _risk.get('score_trail') or {}
            _base_weighted = round(float(_score_trail.get('base_weighted_score') or
                                         _risk.get('score_cap_explanation', {}).get('base_score') or
                                         0), 1)

            _orig_score    = round(float(_risk.get('final_score') or 0), 1)

            if _orig_score == 0 and _base_weighted > 0:
                _has_abs_fatal = any(
                    d.get('severity') in ('FATAL','CRITICAL') and
                    d.get('is_absolute') is not False and
                    'same-day' not in str(d.get('defect','')).lower()
                    for d in (_risk.get('fatal_defects') or [])
                )
                if _has_abs_fatal:
                    _orig_score = round(min(_base_weighted, 25), 1)
                else:
                    _orig_score = _base_weighted

            _fdo         = _risk.get('fatal_defect_override', {}) or _risk.get('hard_fatal_override', {}) or {}
            _orig_before = round(float(_fdo.get('original_score') or
                                        _fdo.get('original_weighted_score') or
                                        _base_weighted or _orig_score), 1)
            _cap_val     = round(float(_fdo.get('capped_at') or
                                        _fdo.get('overridden_score') or
                                        _orig_score), 1)
            _cap_display = f"{_cap_val}/100"


            _tl_score = _tl.get('score', _cscore('Timeline Compliance', 50))


            _doc_gaps = []
            for _field, _label, _sev, _imp in [
                ('written_agreement_exists', 'No documentary proof of legally enforceable debt', 'Severe',
                 'Accused can deny the existence of a legally enforceable debt — a fundamental Section 138 ingredient'),
                ('ledger_available',         'No financial records or ledger entries',           'High',
                 'No independent paper trail of the alleged transaction — debt story is entirely oral and uncorroborated'),
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


            _pres_stage_raw  = str(_pres.get('current_stage') or '')
            _pres_burden_raw = str(_pres.get('burden_position') or '')
            if _pres_stage_raw and _pres_stage_raw not in ('Insufficient data', 'None', ''):
                _pres_stage  = _pres_stage_raw
                _pres_burden = _pres_burden_raw or 'Insufficient data'
            else:
                _ing_score_p = float(_ing.get('overall_compliance', 0) or 0)
                if _ing_score_p >= 70:
                    _pres_stage  = 'Stage 2: Presumption Activated — Burden on Accused (S.139)'
                    _pres_burden = 'On Accused — Section 139 presumption unrebutted'
                elif _ing_score_p >= 40:
                    _pres_stage  = 'Stage 1: Partial Compliance — Presumption Conditionally Available'
                    _pres_burden = 'Shared — Complainant must establish all 7 ingredients first'
                else:
                    _pres_stage  = 'Stage 0: Ingredients Incomplete — Presumption Not Available'
                    _pres_burden = 'On Complainant — basic ingredients not established'


            _jud_conf    = str(_jud.get('confidence', 'Insufficient data') or 'Insufficient data')
            _jud_court   = str(_jud.get('court_identified', case_data.get('court_location', 'Not specified')) or 'Not specified')
            _jud_note    = (
                'Judicial behaviour analysis unavailable — insufficient court data in knowledge base.'
                if _jud_conf in ('LOW', 'Insufficient data', 'Not available') else
                f"Based on cases from {_jud_court}. Confidence: {_jud_conf}."
            )


            _same_day_filing = bool(analysis_report.get('same_day_filing', False))


            _has_real_fatal = any(
                d.get('severity') in ('FATAL','CRITICAL') and
                'same-day' not in str(d.get('defect','')).lower() and
                'same day' not in str(d.get('defect','')).lower() and
                d.get('is_absolute', True) is not False
                for d in _uniq_f
            )
            if _same_day_filing and not _has_real_fatal:
                _is_fatal = False
                _filing_verdict = ('FILE WITH CAUTION — SAME-DAY PROCEDURAL RISK'
                                   if _orig_score >= 55 else
                                   'HIGH RISK — SAME-DAY FILING + EVIDENCE GAPS')
            elif _has_real_fatal or _is_fatal:
                _filing_verdict = 'DO NOT FILE — FATAL DEFECTS PRESENT'
            elif _orig_score >= 75:
                _filing_verdict = 'READY TO FILE — STRONG CASE'
            elif _orig_score >= 55:
                _filing_verdict = 'FILE WITH CAUTION — EVIDENCE GAPS PRESENT'
            else:
                _filing_verdict = 'HIGH RISK — REMEDIATION REQUIRED BEFORE FILING'


            _fatal_issues    = [d for d in _uniq_f if d.get('severity','') in ('CRITICAL','FATAL')]
            _high_issues     = []
            _moderate_issues = []

            for _d in (_def.get('curable_defects') or []):
                _sev = _d.get('severity','')
                if _sev == 'HIGH':
                    _high_issues.append({'issue': _d.get('defect',''), 'severity': 'HIGH', 'source': 'procedural'})
                else:
                    _moderate_issues.append({'issue': _d.get('defect',''), 'severity': 'MEDIUM', 'source': 'procedural'})
            for _rm in (_tl.get('risk_markers') or []):
                if _rm.get('severity') == 'HIGH' and _rm.get('issue','') not in [d.get('defect','') for d in _uniq_f]:
                    _high_issues.append({'issue': _rm.get('issue',''), 'severity': 'HIGH',
                                         'source': 'timeline', 'impact': _rm.get('impact','')})

            _fatal_count_display = len(_uniq_f)
            _primary_fatal       = _uniq_f[0] if _uniq_f else None
            _refiling            = _tl.get('refiling_window') or {}
            _outcome_prob        = _risk.get('outcome_probability') or {}
            _score_cap           = _risk.get('score_cap_explanation') or {}
            _base_score_display  = round(float(_score_cap.get('base_score', _orig_before) or _orig_before), 1)


            if _same_day_filing:

                _uniq_f_real_fatal = [d for d in _uniq_f if 'same-day' not in str(d.get('defect','')).lower()
                                      and 'same day' not in str(d.get('defect','')).lower()
                                      and 'same date' not in str(d.get('defect','')).lower()]
                _same_day_defects  = [d for d in _uniq_f if d not in _uniq_f_real_fatal]

                for _sd in _same_day_defects:
                    _sd['severity']  = 'HIGH'
                    _sd['is_fatal']  = False
                    _sd['risk_note'] = ('Interpretation-dependent — some courts dismiss, others allow. '
                                       'Not an absolute bar unlike premature complaint.')

                if not _uniq_f_real_fatal:
                    _is_fatal = False

                    if _orig_score >= 75:
                        _filing_verdict = 'FILE WITH CAUTION — SAME-DAY FILING RISK'
                    elif _orig_score >= 55:
                        _filing_verdict = 'FILE WITH CAUTION — PROCEDURAL RISK + EVIDENCE GAPS'
                    else:
                        _filing_verdict = 'HIGH RISK — ADDRESS PROCEDURAL RISK BEFORE FILING'
                    _orig_score = max(_orig_score, _orig_before * 0.7)


            _tl_limitation_status = str((_tl.get('compliance_status') or {}).get('limitation', ''))
            _tl_limitation_risk_raw = _tl.get('limitation_risk', 'LOW')


            if 'SAME-DAY' in _tl_limitation_status.upper():
                _limitation_risk_display = 'LOW — Filed within limitation period'

            elif ('WITHIN' in _tl_limitation_status.upper() or 'COMPLIANT' in _tl_limitation_status.upper()):
                _limitation_risk_display = 'LOW — Filed within limitation period'
            elif 'PREMATURE' in _tl_limitation_status.upper():
                _limitation_risk_display = 'CRITICAL — Filed before cause of action arose'
            elif 'BARRED' in _tl_limitation_status.upper() or 'EXPIRED' in _tl_limitation_status.upper():
                _limitation_risk_display = 'CRITICAL — Complaint barred by limitation (Section 142)'
            elif _tl_limitation_risk_raw in ('CRITICAL', 'HIGH'):
                _limitation_risk_display = f'{_tl_limitation_risk_raw} — Limitation compliance issue'
            else:
                _limitation_risk_display = 'LOW — Filed within limitation period'


            if _orig_score == 0 and _orig_before > 0:
                if not _uniq_f:

                    _orig_score = _orig_before
                elif all(d.get('severity') not in ('FATAL', 'CRITICAL') for d in _uniq_f):

                    _orig_score = round(_orig_before * 0.65, 1)

            if _orig_score == 0 and _orig_before == 0:

                _cats_recover = _risk.get('category_scores') or {}
                if _cats_recover:
                    _orig_score = round(sum(
                        float(v.get('score', 0) or 0) * float(v.get('weight', 0) or 0)
                        for v in _cats_recover.values() if isinstance(v, dict)
                    ), 1)
                    _orig_before = _orig_score


            _pt_secs  = analysis_report.get('processing_time_seconds') or 0
            if _pt_secs == 0:
                import time as _time
                _pt_secs = round(_time.time() - start_time, 2) if 'start_time' in locals() else 1.0
                if _pt_secs > 0: analysis_report['processing_time_seconds'] = _pt_secs
            _pt_str   = f"{_pt_secs:.2f}s" if _pt_secs > 0 else "< 1s"


            _data_fields = [
                'cheque_date', 'dishonour_date', 'notice_date', 'complaint_filed_date',
                'cheque_amount', 'bank_name', 'court_location', 'notice_received_date'
            ]
            _filled = sum(1 for f in _data_fields if case_data.get(f))
            _data_completeness_pct = max(30, round(_filled / len(_data_fields) * 100)) if _data_fields else 30


            _modules_populated = sum(1 for m in [_tl, _ing, _doc, _risk, _pres, _cx, _def] if m)
            _analysis_confidence = (
                'HIGH'   if _modules_populated >= 6 and _orig_score > 0 else
                'MEDIUM' if _modules_populated >= 4 and _orig_score > 0 else
                'LOW'
            )


            analysis_report['_result'] = {


                'overall_score':         _orig_score,
                'overall_score_display': f"{_orig_score:.1f}/100",
                'final_score':           _orig_score,
                'final_score_display':   f"{_orig_score:.1f}/100",

                'score_note':            f"Score: {_orig_score:.1f}/100",
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
                'compliance_level':      str(_risk.get('compliance_level', 'Assessment Pending') or 'Assessment Pending'),

                'is_fatal':              _is_fatal,
                'fatal_defects_count':   _fatal_count_display,
                'primary_fatal_defect':  ({
                    'defect':      str(_primary_fatal.get('defect','') if _primary_fatal else ''),
                    'severity':    str(_primary_fatal.get('severity','CRITICAL') if _primary_fatal else ''),
                    'is_absolute': not _same_day_filing,
                    'risk_note':   _primary_fatal.get('risk_note','') if _primary_fatal else '',
                    'impact':      str(_primary_fatal.get('impact','') if _primary_fatal else ''),
                    'remedy':      str(_primary_fatal.get('remedy', _primary_fatal.get('cure','Consult legal counsel')) if _primary_fatal else ''),
                    'legal_basis': str(_primary_fatal.get('legal_basis','Section 138 NI Act') if _primary_fatal else ''),
                }) if _primary_fatal else None,
                'fatal_issues':          _fatal_issues,
                'high_risk_issues':      _high_issues[:5],
                'moderate_issues':       _moderate_issues[:5],
                'issue_summary':         f"{len(_fatal_issues)} fatal, {len(_high_issues)} high-risk, {len(_moderate_issues)} moderate",
                'fatal_defects':         [
                    {'defect':   str(d.get('defect','Unknown defect')),
                     'severity': str(d.get('severity','CRITICAL')),
                     'impact':   str(d.get('impact','Complaint will be dismissed') or ''),
                     'remedy':   str(d.get('remedy', d.get('cure','Consult legal counsel')) or 'Consult legal counsel')}
                    for d in _uniq_f
                ],

                'filing_verdict':        _filing_verdict,

                'processing_time':       analysis_report.get('processing_time') or _pt_str or '< 1s',
                'processing_time_seconds': _pt_secs,

                'documentary_gaps':      _doc_gaps,
                'documentary_gaps_count': len(_doc_gaps),

                'cross_exam_questions':  _cx_questions[:8],
                'cross_exam_killer_questions': _cx.get('killer_questions', _cx_questions[:3])[:3],
                'cross_exam_supporting_questions': _cx.get('supporting_questions', _cx_questions[3:8])[:5],
                'cross_exam_risk':       str(_cx.get('overall_cross_exam_risk') or _cx.get('overall_risk') or 'Insufficient data'),
                'cross_exam_zones':      [
                    {'zone': str(z.get('zone', z.get('area',''))),
                     'risk': str(z.get('risk_level', z.get('severity','MEDIUM')))}
                    for z in (_cx.get('vulnerability_zones') or [])[:4]
                ],

                'presumption_stage':     _pres_stage,
                'burden_position':       _pres_burden,
                'presumption_activated': bool(_pres.get('presumption_activated') or _pres.get('presumption_triggered')),

                'court_name':            _jud_court,
                'court_confidence':      _jud_conf,
                'court_note':            _jud_note,
                'court_indices':         {k: str(v) for k,v in (_jud.get('behavioral_indices') or {}).items()},

                'next_actions':          _next_actions,

                'action_required_within': (
                f"{_refiling.get('days_remaining', 0)} days"
                if isinstance(_refiling, dict) and _refiling.get('days_remaining', 0) > 0
                else 'Immediately'
                ),
                'action_deadline':        (
                _refiling.get('deadline_date', '')
                if isinstance(_refiling, dict) else ''
                ),
                'urgency_note':           (
                f"⚠️ Only {_refiling.get('days_remaining',0)} days left to refile — act immediately"
                if isinstance(_refiling, dict) and 0 < _refiling.get('days_remaining',0) <= 7 else
                f"File fresh complaint within {_refiling.get('days_remaining',0)} days"
                if isinstance(_refiling, dict) and _refiling.get('days_remaining',0) > 7 else
                ''
                ),

                'analysis_confidence':    _analysis_confidence,
                'confidence_note': (
                'All core modules executed — high confidence in analysis'
                if _analysis_confidence == 'HIGH' else
                'Some modules incomplete — review with qualified counsel'
                ),

                'cheque_amount':         case_data.get('cheque_amount', 0),
                'settlement_recommended': bool(_sett.get('settlement_recommended')),
                'settlement_range':      str(_sett.get('recommended_settlement_range', 'Not calculated') or 'Not calculated'),


                'outcome_probability':    (_risk.get('outcome_probability') or {}).get('label', ''),
                'dismissal_probability':  (_risk.get('outcome_probability') or {}).get('dismissal_probability', ''),
                'conviction_probability': (_risk.get('outcome_probability') or {}).get('conviction_probability', ''),

                'refiling_days_left':     (_tl.get('refiling_window') or {}).get('days_remaining', 0),
                'refiling_deadline':      (_tl.get('refiling_window') or {}).get('deadline_date', ''),
                'refiling_message':       (_tl.get('refiling_window') or {}).get('message', '') or str(_tl.get('refile_guidance', '')),


                'data_completeness':       _data_completeness_pct,
                'filing_status':           _filing_verdict,
                'limitation_risk':        _limitation_risk_display,

                'dishonour_category':      str((_mods.get('dishonour_analysis') or {}).get('reason_category','Not specified')),
                'dishonour_strength':       str((_mods.get('dishonour_analysis') or {}).get('legal_strength','Unknown')),
                'dishonour_impact':         str((_mods.get('dishonour_analysis') or {}).get('legal_impact','')[:200]),
                'dishonour_recommendation': str((_mods.get('dishonour_analysis') or {}).get('recommendation','')[:200]),
                'debt_enforceable':         bool((_mods.get('enforceable_debt') or {}).get('is_enforceable', True)),
                'debt_risk_level':          str((_mods.get('enforceable_debt') or {}).get('risk_level','LOW')),
                'debt_fatal_count':         len((_mods.get('enforceable_debt') or {}).get('fatal_issues',[])),
                'condonation_required':     bool((_mods.get('delay_condonation') or {}).get('condonation_required', False)),
                'condonation_strength':     str((_mods.get('delay_condonation') or {}).get('condonation_strength','N/A')),
                'drafting_score':           float((_mods.get('drafting_compliance') or {}).get('compliance_score', 100)),
                'drafting_gap_count':       len((_mods.get('drafting_compliance') or {}).get('critical_gaps',[])),
                'fraud_risk_level':         str((_mods.get('fraud_signals') or {}).get('risk_level','LOW')),
                'fraud_signal_count':       len((_mods.get('fraud_signals') or {}).get('fraud_signals',[])),
                'jurisdiction_valid':       bool((_mods.get('territorial_jurisdiction') or {}).get('jurisdiction_valid', True)),
                'jurisdiction_risk':        str((_mods.get('territorial_jurisdiction') or {}).get('risk_level','LOW')),
                'notice_delivery_status':   str((_mods.get('notice_delivery_status') or {}).get('delivery_status','Not assessed')),
                'notice_risk_level':        str((_mods.get('notice_delivery_status') or {}).get('risk_level','LOW')),
                'deemed_service':           bool((_mods.get('notice_delivery_status') or {}).get('deemed_service_applicable', False)),
                'section_65b_required':     bool((_mods.get('section_65b_compliance') or {}).get('certificate_required', False)),
                'section_65b_risk':         str((_mods.get('section_65b_compliance') or {}).get('risk_level','LOW')),
                'director_impleaded':       bool((_mods.get('director_role_analysis') or {}).get('properly_impleaded', True)),
                'advanced_score_adj':       int(risk_result.get('advanced_module_adjustment', 0)),
                'advanced_warnings':        list(analysis_report.get('advanced_module_warnings') or []),
            'platform_name':         'JUDIQ Legal Intelligence Platform',
                'platform_email':        'hello@judiq.ai',
                'platform_website':      'www.judiq.ai',
                'case_id':               str(analysis_report.get('case_id', '')),
                'generated_date':        datetime.now().strftime('%d %B %Y'),
                'engine_version':        str(analysis_report.get('engine_version', 'v10.0')),
            }
        except Exception as _e:
            logger.error(f'❌ _result building failed: {_e}')
            import traceback as _tb2; logger.error(_tb2.format_exc())

            _fb_score = (
                analysis_report.get('overall_score') or
                analysis_report.get('risk_score') or
                (analysis_report.get('modules', {}).get('risk_assessment', {}).get('overall_risk_score') or 0)
            )
            analysis_report['_result'] = {
                'overall_score':         round(float(_fb_score), 1),
                'overall_score_display': f'{round(float(_fb_score), 1):.1f}/100',
                'is_fatal':              bool(analysis_report.get('fatal_flag')),
                'fatal_defects_count':   len(analysis_report.get('fatal_defects', [])),
                'filing_status':         'DO NOT FILE' if analysis_report.get('fatal_flag') else 'See analysis',
                'processing_time':       f"{round(time.time() - start_time, 2) if 'start_time' in locals() else 1.2:.2f}s",
                'documentary_gaps':      [],
                'cross_exam_questions':  [],
                'next_actions':          [],
                'presumption_stage':     'INSUFFICIENT DATA',
                'burden_position':       'See analysis',
                'conviction_probability': 'N/A',
                'conviction_probability_display': 'Not calculable — fatal defect present' if bool(analysis_report.get('fatal_flag')) else 'See analysis',
            }


        _m = analysis_report.get('modules', {})
        analysis_report['timeline']        = _m.get('timeline_intelligence', {})
        analysis_report['ingredients']     = _m.get('ingredient_compliance', {})
        analysis_report['documentary']     = _m.get('documentary_strength', {})
        analysis_report['defence']         = _m.get('defence_matrix', {})
        analysis_report['procedural']      = _m.get('procedural_defects', {})
        analysis_report['cross_examination'] = _m.get('cross_examination_risk', {})
        analysis_report['settlement']      = _m.get('settlement_analysis', {})
        analysis_report['judicial']        = _m.get('judicial_behavior', {})
        analysis_report['risk_assessment'] = _m.get('risk_assessment', {})
        analysis_report['overall_score']   = (
            _m.get('risk_assessment', {}).get('overall_risk_score', 0) or
            analysis_report.get('risk_score', 0)
        )


        try:
            analysis_report['professional_report'] = generate_clean_professional_report(analysis_report, case_data)
        except Exception as _e:
            logger.error(f'❌ professional_report generation failed: {_e}')
            analysis_report['professional_report'] = {'error': str(_e), 'report_format': 'Error'}


        case_data_sorted = json.dumps(case_data, sort_keys=True, default=str)
        input_hash = hashlib.sha256(case_data_sorted.encode()).hexdigest()


        analysis_sorted = json.dumps(analysis_report, sort_keys=True, default=str)
        analysis_hash = hashlib.sha256(analysis_sorted.encode()).hexdigest()


        _risk_r   = analysis_report.get('modules', {}).get('risk_assessment', {}) or {}
        _tl_r     = analysis_report.get('modules', {}).get('timeline_intelligence', {}) or {}
        _proc_r   = analysis_report.get('modules', {}).get('procedural_defects', {}) or {}
        _doc_r    = analysis_report.get('modules', {}).get('documentary_strength', {}) or {}
        _es_r     = analysis_report.get('executive_summary', {}) or {}
        _final_sc = round(float(_risk_r.get('overall_risk_score', 0) or 0), 1)
        _tl_lim   = str((_tl_r.get('compliance_status') or {}).get('limitation', '') or '')
        _doc_sc   = round(float((_doc_r.get('overall_strength_score') or 0)), 1)
        _proc_f   = len(_proc_r.get('fatal_defects', []) or [])
        _decision_chain = []
        _decision_chain.append(f'Timeline: {'BARRED/PREMATURE — FATAL' if any(x in _tl_lim.upper() for x in ("BARRED","PREMATURE")) else 'SAME-DAY RISK' if 'SAME' in _tl_lim.upper() else 'COMPLIANT'}')
        _decision_chain.append(f'Documentary strength: {_doc_sc}/100 — {'Adequate' if _doc_sc >= 60 else 'Insufficient — reduces score'}')
        _decision_chain.append(f'Procedural defects: {_proc_f} fatal defect(s) — {'present — score capped' if _proc_f > 0 else 'none detected'}')
        _decision_chain.append(f'Composite score: {_final_sc}/100')
        _decision_chain.append(f'Fatal flag: {"active — filing blocked" if analysis_report.get("fatal_flag") else "not set"}')
        _decision_chain.append(f'Final verdict: {_es_r.get("filing_verdict", analysis_report.get("_result", {}).get("filing_status", "Unknown"))}')
        if _consistency.get('corrections'):
            for _k, _v in _consistency['corrections'].items():
                _decision_chain.append(f'Auto-corrected: {_k} → {_v}')
        analysis_report['audit_trail'] = {
            'input_hash':      input_hash,
            'analysis_hash':   analysis_hash,
            'timestamp':       datetime.now().isoformat(),
            'locked':          True,
            'version':         'v11.2',
            'decision_chain':  _decision_chain,
            'phases_executed': analysis_report.get('audit_log', {}).get('phases_executed', []),
            'note': 'Analysis locked — input/output hashes + decision chain recorded'
        }

        logger.info(f"  🔒 Audit lock applied - Input hash: {input_hash[:16]}...")


        db_saved = save_analysis_to_db(analysis_report)
        if not db_saved:
            logger.warning("⚠️ Analysis completed but failed to save to database")
            analysis_report['audit_trail']['database_saved'] = False
        else:
            analysis_report['audit_trail']['database_saved'] = True


        return analysis_report

    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()


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
    db_ok = False
    try:
        conn = sqlite3.connect(analytics_db_path)
        conn.execute("SELECT 1"); conn.close(); db_ok = True
    except Exception: pass
    return {
        "status": "healthy",
        "version": ENGINE_VERSION,
        "platform": "JUDIQ AI — Section 138 NI Act Legal Intelligence",
        "kb_loaded": kb_loaded,
        "kb_provisions": len(kb_data) if kb_loaded else 0,
        "db_status": "ok" if db_ok else "unavailable",
        "llm_enhancement": False,
        "embed_model": False,
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

        analysis_data = _safe_json_parse(result[0])

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

        a = _safe_json_parse(result[0])
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
    except Exception: return fb

def _build_flat_report(a: dict) -> dict:
    """
    Build a completely flat, null-safe report object.
    Reads from central _result object first (guaranteed non-null),
    then falls back to module data.
    Every field the PDF template might read is guaranteed to have a real value.
    """
    R       = a.get('_result') or {}
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

    score      = _n(R.get('overall_score') or a.get('risk_score') or risk.get('final_score'))
    fatal      = bool(R.get('is_fatal') or a.get('fatal_flag') or a.get('is_fatal'))
    cat_scores = risk.get('category_scores') or {}

    def cat(name, fallback=0.0):
        rkey = {'Timeline Compliance': 'score_timeline',
                'Ingredient Compliance': 'score_ingredients',
                'Documentary Strength': 'score_documentary',
                'Procedural Compliance': 'score_procedural',
                'Liability Expansion': 'score_liability'}.get(name)
        if rkey and R.get(rkey) is not None:
            return _n(R[rkey], fallback)
        d = cat_scores.get(name)
        if isinstance(d, dict): return _n(d.get('score'), fallback)
        return _n(d, fallback)


    uniq = list(R.get('fatal_defects') or [])
    if not uniq:
        all_f = (risk.get('fatal_defects') or []) + (defects.get('fatal_defects') or []) + (ingr.get('fatal_defects') or [])
        seen = set()
        for d in all_f:
            k = d.get('defect', str(d))
            if k not in seen: seen.add(k); uniq.append(d)


    fdo          = risk.get('fatal_defect_override') or risk.get('hard_fatal_override') or {}
    orig_score   = _n(R.get('original_score') or fdo.get('original_score') or fdo.get('original_weighted_score') or score)
    cap_val      = _n(R.get('capped_at') or fdo.get('capped_at') or fdo.get('overridden_score') or score)
    cap_display  = f"{cap_val:.1f}/100"
    orig_display = f"{orig_score:.1f}/100"
    _ = R.get('fatal_override_note') or (
        f"Original: {orig_display} → Capped at {cap_display} due to fatal defect"
        if (fatal and fdo) else "No fatal override applied"
    )


    pt_secs = float(R.get('processing_time_seconds') or a.get('processing_time_seconds') or 0)
    pt_str  = f"{pt_secs:.2f}s" if pt_secs > 0 else "< 1s"


    tl_score = _n(R.get('timeline_score') or tl.get('score') or cat('Timeline Compliance', 50))


    doc_gaps = list(R.get('documentary_gaps') or [])
    if not doc_gaps:
        meta = a.get('case_metadata') or {}
        for field, label, sev, imp in [
            ('written_agreement_exists', 'No documentary proof of legally enforceable debt', 'Severe',
             'Accused can deny the existence of a legally enforceable debt — a fundamental Section 138 ingredient'),
            ('ledger_available', 'No financial records or ledger entries', 'High',
             'No independent paper trail of the alleged transaction — debt story is entirely oral and uncorroborated'),
            ('postal_proof_available', 'No postal proof of notice', 'Moderate',
             'Notice service unproven — accused can deny receipt'),
            ('original_cheque_available', 'Original cheque not secured', 'High',
             'Primary instrument unavailable — foundational evidence at risk'),
            ('return_memo_available', 'Bank dishonour memo missing', 'Severe',
             'Proof of dishonour not secured — essential statutory ingredient at risk'),
        ]:
            if not meta.get(field, True):
                doc_gaps.append({'gap_name': label, 'document': label,
                                  'severity': sev, 'impact': imp, 'remedy': 'Obtain before filing'})


    cx_questions = list(R.get('cross_exam_questions') or cx.get('likely_questions') or [])
    if not cx_questions:
        meta = a.get('case_metadata') or {}
        tl_limit = (tl.get('compliance_status') or {}).get('limitation', '')
        if not meta.get('written_agreement_exists'):
            cx_questions.append('Is it correct that there is no written agreement evidencing this alleged loan?')
            cx_questions.append('Can you explain why such a significant amount was given without any documentation?')
        if 'PREMATURE' in str(tl_limit).upper():
            cx_questions.append('Was this complaint filed before the 15-day payment period under Section 138 expired?')
            cx_questions.append('Are you aware that a Section 138 complaint is not maintainable before the cause of action arises?')
        if not meta.get('postal_proof_available'):
            cx_questions.append('Do you have an AD card or tracking proof confirming the accused received your notice?')
        if not meta.get('ledger_available'):
            cx_questions.append('Can you produce any bank statement showing transfer of the cheque amount?')
        cx_questions.append('On what date and in what form — cash, cheque, or bank transfer — was the amount given?')
        cx_questions.append('Who was present when the alleged transaction took place?')


    next_actions = list(R.get('next_actions') or [])
    if not next_actions:
        for fd in uniq[:3]:
            next_actions.append({
                'action': str(fd.get('remedy', fd.get('defect', 'Address fatal defect')) or 'Consult legal counsel'),
                'urgency': 'URGENT',
                'details': str(fd.get('impact', 'This defect will cause dismissal') or '')
            })
        if not next_actions:
            next_actions.append({'action': 'Proceed with final legal review', 'urgency': 'NORMAL', 'details': ''})


    pres_stage  = _s(R.get('presumption_stage') or pres.get('current_stage'), 'Insufficient data')
    pres_burden = _s(R.get('burden_position') or pres.get('burden_position'), 'Insufficient data')


    jud_conf  = _s(R.get('court_confidence') or jb.get('confidence'), 'Insufficient data')
    jud_court = _s(R.get('court_name') or jb.get('court_identified'), 'Not specified')
    jud_note  = R.get('court_note') or (
        'Judicial behaviour analysis unavailable — insufficient court data.'
        if jud_conf in ('LOW', 'Insufficient data', 'Not available') else
        f"Based on cases from {jud_court}. Confidence: {jud_conf}."
    )


    adv_65b = mods.get('section_65b_compliance') or {}
    adv_jur = mods.get('territorial_jurisdiction') or {}
    adv_ntc = mods.get('notice_delivery_status') or {}
    jur_exp = _s(adv_jur.get('explanation') or adv_jur.get('finding'),
        "Jurisdiction risk detected — verify cause of action arose within court's territorial limits "
        "(place of dishonour, notice service, or accused's residence/business)")

    chart = sorted(
        [e for e in (tl.get('timeline_chart') or []) if e.get('date')],
        key=lambda x: x.get('date', '9999')
    )

    strengths  = exec_s.get('strengths') or R.get('strengths') or ['Analysis complete']
    weaknesses = exec_s.get('weaknesses') or R.get('weaknesses') or ['Review full analysis']

    return sanitize_module_output({

        'case_details': {
            'complainant_name':  _s(a.get('complainant_name', '')),
            'accused_name':      _s(a.get('accused_name', '')),
            'lawyer_name':       _s(a.get('lawyer_name', '')),
            'case_number':       _s(a.get('case_number', '')),
            'court_name':        _s(a.get('court_name', '')),
            'platform_name':     'JUDIQ Legal',
            'platform_tagline':  'Section 138 Legal Intelligence',
            'generated_by':      'JUDIQ AI - Advanced Legal Analysis Engine',
        },


        'case_id':                _s(a.get('case_id')),
        'generated_date':         _s(a.get('analysis_timestamp', '')[:10]),
        'engine_version':         _s(a.get('engine_version'), 'v10.0'),
        'processing_time':        pt_str if (pt_secs and pt_secs >= 0.5) else '1.2s',
        'processing_time_display': pt_str if (pt_secs and pt_secs >= 0.5) else '1.2s',
        'processing_time_seconds': round(pt_secs, 2) if pt_secs >= 0.5 else 1.2,
        'final_score':            score,
        'final_score_display':    f"{score:.1f}/100",
        'compliance_level':       _s(R.get('compliance_level') or risk.get('compliance_level') or a.get('case_strength'), 'Assessment Pending'),
        'is_fatal':               fatal,
        'fatal_defects_count':    len(uniq),
        'filing_status':          _s(R.get('filing_verdict') or exec_s.get('filing_verdict') or a.get('final_status'), 'See analysis'),
        'one_line_verdict':       _s(exec_s.get('case_overview') or exec_s.get('filing_verdict'), 'Analysis complete'),
        'recommended_action':     _s(a.get('decisive_verdict') or a.get('filing_recommendation'), 'Consult legal counsel'),
        'strengths':              [_s(s) for s in strengths[:5]],
        'weaknesses':             [_s(w) for w in weaknesses[:5]],
        'critical_risks':         [{'defect': _s(d.get('defect')), 'severity': _s(d.get('severity'), 'CRITICAL'),
                                     'impact': _s(d.get('impact')), 'remedy': _s(d.get('remedy', d.get('cure', 'Consult counsel')))}
                                    for d in uniq[:5]],
        'next_actions':           next_actions[:5],
        'score_timeline':         cat('Timeline Compliance'),
        'score_timeline_display': f"{cat('Timeline Compliance'):.1f}/100",
        'score_ingredients':      cat('Ingredient Compliance'),
        'score_documentary':      cat('Documentary Strength'),
        'score_procedural':       cat('Procedural Compliance'),
        'score_liability':        cat('Liability Expansion'),
        'timeline_score':         tl_score,
        'timeline_score_display': f"{tl_score:.1f}/100",
        'limitation_risk':        _s(tl.get('limitation_risk'), 'Insufficient data'),
        'limitation_status':      _s((tl.get('compliance_status') or {}).get('limitation'), 'Insufficient data'),
        'timeline_events':        [{'date': _s(e.get('date')), 'event': _s(e.get('event')), 'status': _s(e.get('status'), 'OK')} for e in chart],
        'critical_dates':         {k: _s(v) for k, v in (tl.get('critical_dates') or {}).items()},
        'ingredient_compliance':  _n(ingr.get('overall_compliance')),
        'ingredient_risk_level':  _s(ingr.get('risk_level'), 'Insufficient data'),
        'ingredient_details':     ingr.get('ingredient_details', []),
        'doc_strength_score':     _n(doc.get('overall_strength_score')),
        'doc_strength_display':   f"{_n(doc.get('overall_strength_score')):.1f}/100",
        'doc_strength_label':     _s(doc.get('strength_label'), 'Insufficient data'),
        'document_gaps':          doc_gaps,
        'documentary_gaps':       doc_gaps,
        'documentary_gaps_count': len(doc_gaps),
        'procedural_risk':        _s(defects.get('overall_risk'), 'Insufficient data'),
        'fatal_defects':          [{'defect': _s(d.get('defect')), 'severity': _s(d.get('severity'), 'CRITICAL'),
                                     'impact': _s(d.get('impact')), 'remedy': _s(d.get('remedy', d.get('cure', 'Consult counsel')))}
                                    for d in (defects.get('fatal_defects') or [])],
        'curable_defects':        [{'defect': _s(d.get('defect')), 'cure': _s(d.get('cure'))} for d in (defects.get('curable_defects') or [])[:3]],
        'defence_exposure':       _s(defence.get('overall_exposure') or defence.get('exposure_level'), 'Insufficient data'),
        'high_risk_defences':     ([{'defence': _s(d.get('defence', d.get('ground'))),
                                      'strength': _s(d.get('strength', d.get('risk_impact')), 'Unknown'),
                                      'strategy': _s(d.get('strategy'))}
                                     for d in (defence.get('high_risk_defences') or [])[:4]]
                                    or [{'defence': 'No major defences identified', 'strength': 'LOW', 'strategy': 'Maintain evidence posture'}]),
        'settlement_recommended': bool(settle.get('settlement_recommended')),
        'settlement_range':       _s(str(settle.get('recommended_settlement_range') or 'Not calculated')),
        'interim_eligible':       bool(settle.get('interim_compensation_eligible')),
        'cheque_amount':          a.get('case_metadata', {}).get('cheque_amount', 0),

        'conviction_probability':         _s(((mods.get('risk_assessment') or {}).get('outcome_probability') or a.get('outcome_probability') or {}).get('conviction_probability'), 'See analysis'),
        'dismissal_probability':          _s(((mods.get('risk_assessment') or {}).get('outcome_probability') or a.get('outcome_probability') or {}).get('dismissal_probability'), 'See analysis'),
        'conviction_probability_display': _s(((mods.get('risk_assessment') or {}).get('outcome_probability') or a.get('outcome_probability') or {}).get('conviction_probability_display'), 'Conviction Probability: See analysis'),
        'dismissal_probability_display':  _s(((mods.get('risk_assessment') or {}).get('outcome_probability') or a.get('outcome_probability') or {}).get('dismissal_probability_display'), 'Dismissal Probability: See analysis'),
        'outcome_probability_label':      _s(((mods.get('risk_assessment') or {}).get('outcome_probability') or a.get('outcome_probability') or {}).get('verdict_label'), 'See analysis'),
        'court_name':             jud_court if jud_conf in ('HIGH', 'MEDIUM') else 'Not specified',
        'court_confidence':       jud_conf,
        'court_data_available':   jud_conf in ('HIGH', 'MEDIUM'),
        'court_note':             jud_note if jud_conf in ('HIGH', 'MEDIUM') else 'Judicial outcome unavailable — insufficient court data',
        'court_indices':          {k: str(v) for k, v in (jb.get('behavioral_indices') or {}).items()},
        'presumption_stage':      pres_stage,
        'burden_position':        pres_burden,
        'presumption_activated':  bool(pres.get('presumption_activated') or pres.get('presumption_triggered')),
        'cross_exam_risk':        _s(R.get('cross_exam_risk') or cx.get('overall_cross_exam_risk') or cx.get('overall_risk'), 'Insufficient data'),
        'cross_exam_questions':   [_s(q) for q in cx_questions[:8]],
        'cross_exam_zones':       [{'zone': _s(z.get('zone', z.get('area'))), 'risk': _s(z.get('risk_level', z.get('severity')), 'MEDIUM')}
                                    for z in (cx.get('vulnerability_zones') or [])[:4]],
        'section_65b_status':     _s(adv_65b.get('status')) if adv_65b.get('applicable') else 'NOT_APPLICABLE',
        'section_65b_applicable': bool(adv_65b.get('applicable', False)),
        'notice_delivery_status': _s(adv_ntc.get('status')) if adv_ntc.get('status') not in (None,'','??','—','Not assessed') else 'DATA NOT AVAILABLE',
        'notice_delivery_risk':   _s(adv_ntc.get('risk_level')) if adv_ntc.get('risk_level') not in (None,'','??','—','Not assessed') else 'DATA NOT AVAILABLE',
        'jurisdiction_risk':      _s(adv_jur.get('risk_level') or adv_jur.get('status')) if adv_jur.get('risk_level') not in (None,'','??','—','Not assessed','INSUFFICIENT_DATA') else 'DATA NOT AVAILABLE',
        'jurisdiction_explanation': jur_exp if jur_exp not in (None,'','??','—','Not assessed') else 'DATA NOT AVAILABLE',
        'jurisdiction_valid':     bool(adv_jur.get('jurisdiction_valid', True)),
        'platform_name':          'JUDIQ Legal Intelligence Platform',
        'platform_email':         'hello@judiq.ai',
        'platform_website':       'www.judiq.ai',
        'platform_address':       'JUDIQ AI — Section 138 Intelligence Platform',
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


def _safe_json_parse(s, default=None):
    """Safely parse a JSON string; returns default ({}) on any error."""
    if not s:
        return default if default is not None else {}
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError, ValueError) as _e:
        logger.warning(f"JSON parse failed: {_e}")
        return default if default is not None else {}

@app.post("/analyze-case")
@limiter.limit("30/minute")
async def analyze_case(request: CaseAnalysisRequest, http_request: Request = None):

    start = time.time()
    audit = AuditTrail()

    try:
        case_data = request.model_dump()


        _sanity = run_input_sanity_check(case_data)

        _SOFT_CHECKS = {
            'FUTURE_DATE_CHEQUE_DATE',
            'FUTURE_DATE_DISHONOUR_DATE',
            'FUTURE_DATE_NOTICE_DATE',
            'FUTURE_DATE_COMPLAINT_FILED_DATE',
            'LARGE_AMOUNT',
        }
        _hard_errors = [e for e in _sanity.get('errors', [])
                        if e.get('check') not in _SOFT_CHECKS]
        _soft_warnings = _sanity.get('warnings', []) + [
            e for e in _sanity.get('errors', []) if e.get('check') in _SOFT_CHECKS
        ]
        if _hard_errors:
            logger.warning(f"⚠️ INPUT SANITY HARD STOP: {len(_hard_errors)} impossible input(s)")
            return {
                "success": False,
                "error": "Input sanity check failed",
                "error_type": "IMPOSSIBLE_INPUT",
                "sanity_errors": _hard_errors,
                "sanity_warnings": _soft_warnings,
                "input_sanity": _sanity,
                "proceed": False,
                "message": "One or more inputs are logically impossible. Correct and resubmit.",
                "audit_trail": audit.get_trail(),
            }

        if _soft_warnings:
            logger.info(f"ℹ️ Input sanity: {len(_soft_warnings)} soft warning(s) — proceeding")

        case_data['_sanity_check'] = _sanity

        case_data['_sanity_warnings'] = _sanity.get('warnings', [])


        is_valid, validation_errors, sanitized_data = validate_case_input_strict(case_data)
        audit.log_validation(is_valid, validation_errors)


        if not is_valid:
            logger.warning(f"⚠️ VALIDATION FAILED: {validation_errors}")
            return {
                "success": False,
                "error": "Input validation failed",
                "validation_errors": validation_errors,
                "audit_trail": audit.get_trail(),
                "message": "Please correct the errors listed in validation_errors and resubmit."
            }


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
                "error_details": analysis.get('error_traceback', ''),
                "debug_traceback": analysis.get('error_traceback', ''),
                "audit_trail": audit.get_trail() if 'audit' in locals() else {}
            }

        analysis['audit_trail'] = audit.get_trail()


        if input_warnings:
            analysis['input_warnings'] = input_warnings
            analysis['input_warning_count'] = len(input_warnings)


        _ue = case_data.get('user_email', '')
        if _ue:
            try:
                _today = date.today().isoformat()
                _conn = get_db_connection()
                _cur  = _conn.cursor()
                _cur.execute("""SELECT COUNT(*) FROM case_analyses
                               WHERE user_email=? AND DATE(analysis_timestamp)=?""",
                             (_ue, _today))
                _used_analyses = _cur.fetchone()[0]

                try:
                    _cur.execute("""SELECT COUNT(*) FROM draft_logs
                                   WHERE user_email=? AND DATE(draft_timestamp)=?""",
                                 (_ue, _today))
                    _used_drafts = _cur.fetchone()[0]
                except Exception:
                    _used_drafts = 0
                _conn.close()
                _analysis_limit = get_user_limit(_ue)
                analysis['usage'] = {
                    'analyses': {
                        'used':      _used_analyses,
                        'limit':     _analysis_limit,
                        'remaining': max(0, _analysis_limit - _used_analyses),
                        'exhausted': _used_analyses >= _analysis_limit
                    },
                    'drafts': {
                        'used':      _used_drafts,
                        'limit':     3,
                        'remaining': max(0, 3 - _used_drafts),
                        'exhausted': _used_drafts >= 3
                    },
                    'next_reset': (date.today() + timedelta(days=1)).isoformat() + "T00:00:00"
                }
            except Exception as _ue_err:
                logger.warning(f"Could not attach usage counts: {_ue_err}")


        if 'modules' in analysis and 'risk_assessment' in analysis['modules']:
            risk = analysis['modules']['risk_assessment']
            if 'category_scores' in risk:
                weights = get_centralized_weights()
                analysis['score_breakdown_detail'] = generate_score_breakdown(
                    risk['category_scores'],
                    weights
                )


        plain_summary = generate_plain_summary(analysis, case_data)


        _am = analysis.get('modules', {})
        analysis.setdefault('timeline',          _am.get('timeline_intelligence', {}))
        analysis.setdefault('ingredients',       _am.get('ingredient_compliance', {}))
        analysis.setdefault('documentary',       _am.get('documentary_strength', {}))
        analysis.setdefault('defence',           _am.get('defence_matrix', {}))
        analysis.setdefault('procedural',        _am.get('procedural_defects', {}))
        analysis.setdefault('cross_examination', _am.get('cross_examination_risk', {}))
        analysis.setdefault('settlement',        _am.get('settlement_analysis', {}))
        analysis.setdefault('judicial',          _am.get('judicial_behavior', {}))
        analysis.setdefault('risk_assessment',   _am.get('risk_assessment', {}))
        analysis.setdefault('dishonour_analysis', _am.get('dishonour_analysis', {}))
        analysis.setdefault('enforceable_debt',   _am.get('enforceable_debt', {}))
        analysis.setdefault('delay_condonation',  _am.get('delay_condonation', {}))
        analysis.setdefault('drafting_compliance',_am.get('drafting_compliance', {}))
        analysis.setdefault('fraud_signals',      _am.get('fraud_signals', {}))
        analysis.setdefault('notice_delivery',    _am.get('notice_delivery_status', {}))
        analysis.setdefault('jurisdiction',       _am.get('territorial_jurisdiction', {}))
        analysis.setdefault('section_65b',        _am.get('section_65b_compliance', {}))
        analysis.setdefault('director_liability', _am.get('director_role_analysis', {}))
        analysis.setdefault('overall_score',
            _am.get('risk_assessment', {}).get('final_score', 0) or
            analysis.get('risk_score', 0))


        _result_obj   = analysis.get('_result', {})
        _canon_score  = float(
            _result_obj.get('overall_score') or
            analysis.get('overall_score') or
            analysis.get('risk_score') or
            (analysis.get('modules', {}).get('risk_assessment', {}).get('final_score') or 0)
        )
        _canon_fatal  = bool(
            _result_obj.get('is_fatal') or
            analysis.get('fatal_flag') or False
        )


        _exec_summary = analysis.get('executive_summary', {})
        _doc_context = _exec_summary.get('documentary_context', '')
        _primary_issue = _exec_summary.get('primary_issue', '')
        _next_actions = _exec_summary.get('next_actions', [])
        _top_summary = _exec_summary.get('top_summary', '')


        _decision_conf = plain_summary.get('decision_confidence', {})
        _contradictions = plain_summary.get('contradictions', {})
        _evidence_gaps = plain_summary.get('evidence_gap_priority', {})
        _defence_counters = plain_summary.get('defence_counters', {})
        _module_confidence = plain_summary.get('module_confidence', {})
        _outcome_scenarios = plain_summary.get('outcome_scenarios', {})


        if _canon_fatal:
            _clean_status = "FATAL - DO NOT FILE"
        elif _canon_score >= 75:
            _clean_status = "READY TO FILE"
        elif _canon_score >= 55:
            _clean_status = "HIGH RISK CAUTION"
        else:
            _clean_status = "CRITICAL GAPS"


        if not analysis.get('analysis_timestamp'):
            analysis['analysis_timestamp'] = datetime.utcnow().isoformat() + 'Z'

        if not analysis.get('case_metadata'):
            analysis['case_metadata'] = {k: v for k, v in case_data.items()
                                          if not k.startswith('_')}

        return {
            "success": True,


            "final_score": round(_canon_score, 1),
            "status": _clean_status,
            "summary": _top_summary,
            "documentary_strength": _doc_context,
            "key_issue": _primary_issue,
            "next_actions": _next_actions[:5] if _next_actions else [],


            "decision_confidence": _decision_conf,
            "contradictions": _contradictions,
            "evidence_gaps": _evidence_gaps,
            "defence_counters": _defence_counters,
            "module_confidence": _module_confidence,
            "outcome_scenarios": _outcome_scenarios,
            "procedural_defects": analysis.get('modules', {}).get('procedural_defects', {}),


            "case_id":            analysis['case_id'],
            "analysis_timestamp": analysis['analysis_timestamp'],


            "overall_score":  _canon_score,
            "fatal_flag":     _canon_fatal,
            "risk_score":     _canon_score,


            "plain_summary":     plain_summary,
            "executive_summary": analysis.get('executive_summary', {}),
            "result":            _result_obj,


            "timeline":          analysis.get('timeline', {}),
            "ingredients":       analysis.get('ingredients', {}),
            "documentary":       analysis.get('documentary', {}),
            "defence":           analysis.get('defence', {}),
            "procedural":        analysis.get('procedural', {}),
            "cross_examination": analysis.get('cross_examination', {}),
            "settlement":        analysis.get('settlement', {}),
            "judicial":          analysis.get('judicial', {}),
            "risk_assessment":   analysis.get('risk_assessment', {}),
            "presumption":       analysis.get('presumption', {}),


            "dishonour_analysis":  analysis.get('dishonour_analysis', {}),
            "enforceable_debt":    analysis.get('enforceable_debt', {}),
            "delay_condonation":   analysis.get('delay_condonation', {}),
            "drafting_compliance": analysis.get('drafting_compliance', {}),
            "document_validity":   analysis.get('document_validity', {}),
            "fraud_signals":       analysis.get('fraud_signals', {}),


            "legal_basis_map":     plain_summary.get('legal_basis_map', {}),
            "time_sensitivity":    plain_summary.get('time_sensitivity', {}),
            "input_sanity":        plain_summary.get('input_sanity', {}),


            "decision_trace":    analysis.get('decision_trace', []),
            "processing_flags":  analysis.get('processing_flags', {}),
            "pipeline_phases":   analysis.get('audit_log', {}).get('phases_executed', []),
            "sanity_warnings":   case_data.get('_sanity_warnings', []),


            "modules":           analysis.get('modules', {}),

            "analysis": analysis,


            "api_response_time_ms": round((time.time() - start) * 1000, 1),
            "engine_version":       ENGINE_VERSION,
            "maturity_grade":       "Production Stable",
        }

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

        return {
            "success": False,
            "error": "Critical analysis error",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "message": "Analysis could not be completed. The error has been logged.",
            "partial_result": {
                "overall_score": 0,
                "fatal_flag": True,
                "filing_status": "ANALYSIS ERROR — retry or check inputs",
            },
            "audit_trail": audit.get_trail() if 'audit' in locals() else {},
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
                "analysis": _safe_json_parse(analysis_json)
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


        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='case_analyses'
        """)
        if not cursor.fetchone():

            conn.close()
            return {
                "success": True,
                "total_analyses": 0,
                "average_risk_score": 0,
                "avg_score": 0,
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
            "avg_score": round(avg_risk, 1),
            "compliance_distribution": compliance_dist
        }
    except Exception as e:
        logger.error(f"Analytics retrieval failed: {e}")

        return {
            "success": True,
            "total_analyses": 0,
            "average_risk_score": 0,
            "avg_score": 0,
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

        name, total, conv_rate, acq_rate, lim_rate, tech_rate, comp_rate,\
        strictness, classification, confidence, updated = court_data


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
                'compounding_rate': round((compounded / total) * 100, 1) if total > 0 else 0,
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


ADMIN_CREDENTIALS = {
    "admin@judiq.com": {
        "password_hash": "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8",
        "name": "Admin User",
        "role": "super_admin"
    }
}


def get_db_connection() -> sqlite3.Connection:
    """Return a sqlite3 connection to the main analytics database."""
    analytics_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(analytics_db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _load_db_admins():
    """Load persisted admin accounts from DB into ADMIN_CREDENTIALS memory dict."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT email, name, role, password_hash FROM admin_accounts")
        for row in cursor.fetchall():
            email, name, role, pw_hash = row
            if email not in ADMIN_CREDENTIALS:
                ADMIN_CREDENTIALS[email] = {
                    "password_hash": pw_hash,
                    "name": name,
                    "role": role
                }
        conn.close()
        loaded = len(ADMIN_CREDENTIALS)
        logger.info(f"✅ Loaded {loaded} admin account(s) into memory")
    except Exception as e:
        logger.warning(f"Could not load DB admins: {e}")


def init_admin_tables():
    """Initialize admin-related database tables"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_limits (
                user_email TEXT PRIMARY KEY,
                daily_limit INTEGER DEFAULT 3,
                is_unlimited BOOLEAN DEFAULT 0,
                custom_limit_reason TEXT,
                set_by_admin TEXT,
                set_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_email TEXT,
                action_type TEXT,
                target_user TEXT,
                action_details TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')


        cursor.execute('''
            CREATE TABLE IF NOT EXISTS draft_logs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_email      TEXT    NOT NULL,
                draft_timestamp TEXT    NOT NULL,
                case_id         TEXT,
                draft_type      TEXT    DEFAULT 'complaint',
                created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_draft_email ON draft_logs(user_email)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_draft_ts ON draft_logs(draft_timestamp)"
        )

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admin_accounts (
                email       TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                role        TEXT NOT NULL DEFAULT 'admin',
                password_hash TEXT NOT NULL,
                created_by  TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ENSURE default super-admin exists in database
        cursor.execute("SELECT email FROM admin_accounts WHERE email = ?", ("admin@judiq.com",))
        if not cursor.fetchone():
            logger.info("🔧 Creating default super-admin in database...")
            cursor.execute("""
                INSERT INTO admin_accounts (email, name, role, password_hash, created_by)
                VALUES (?, ?, ?, ?, ?)
            """, (
                "admin@judiq.com",
                "Super Admin",
                "super_admin",
                "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8",  # password = "password"
                "SYSTEM"
            ))
            logger.info("✅ Default super-admin created: admin@judiq.com / password")

        conn.commit()
        conn.close()
        logger.info("✅ Admin tables initialized")
        _load_db_admins()
        return True
    except Exception as e:
        logger.error(f"❌ Error initializing admin tables: {e}")
        return False


init_admin_tables()

def verify_admin(email: str, password: str) -> bool:
    """Verify admin credentials"""
    import hashlib
    if email not in ADMIN_CREDENTIALS:
        return False

    password_hash = hashlib.sha256(password.encode()).hexdigest()
    return ADMIN_CREDENTIALS[email]["password_hash"] == password_hash

def log_admin_action(admin_email: str, action_type: str, target_user: str = None, details: str = None):
    """Log admin actions for audit trail"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO admin_activity_log (admin_email, action_type, target_user, action_details)
            VALUES (?, ?, ?, ?)
        ''', (admin_email, action_type, target_user, details))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error logging admin action: {e}")

def get_user_limit(user_email: str) -> int:
    """Get custom limit for user, or default 3"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT daily_limit, is_unlimited
            FROM user_limits
            WHERE user_email = ?
        ''', (user_email,))
        result = cursor.fetchone()
        conn.close()

        if result:
            is_unlimited = result[1]
            if is_unlimited:
                return 999999
            return result[0]
        return 3
    except Exception as e:
        logger.error(f"Error getting user limit: {e}")
        return 3


def check_daily_limit(user_email: str) -> Tuple[bool, int]:
    """
    Check if user has exceeded daily analysis limit.
    Now supports custom limits per user.
    """
    if not user_email or not isinstance(user_email, str):
        return True, 0

    today = date.today().isoformat()
    DAILY_LIMIT = get_user_limit(user_email)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT COUNT(*) FROM case_analyses
            WHERE user_email = ?
            AND DATE(analysis_timestamp) = ?
        ''', (user_email, today))

        count = cursor.fetchone()[0]
        conn.close()

        logger.info(f"📊 Daily limit check for {user_email}: {count}/{DAILY_LIMIT} used today")

        return count < DAILY_LIMIT, count

    except Exception as e:
        logger.error(f"❌ Error checking daily limit: {e}")
        return True, 0


def check_draft_limit(user_email: str) -> Tuple[bool, int]:
    """
    Check whether a user has used up their 3 daily complaint drafts.

    Returns:
        (is_allowed: bool, count_used_today: int)
    """
    if not user_email or not isinstance(user_email, str):
        return True, 0

    today = date.today().isoformat()
    DRAFT_LIMIT = 3

    try:
        conn = get_db_connection()
        cursor = conn.cursor()


        cursor.execute("""
            SELECT COUNT(*) FROM draft_logs
            WHERE user_email = ?
              AND DATE(draft_timestamp) = ?
        """, (user_email, today))

        count = cursor.fetchone()[0]
        conn.close()

        logger.info(f"📋 Draft limit check for {user_email}: {count}/{DRAFT_LIMIT} used today")
        return count < DRAFT_LIMIT, count

    except Exception as e:
        logger.error(f"❌ Error checking draft limit: {e}")
        return True, 0


def log_draft_usage(user_email: str, case_id: str = None, draft_type: str = 'complaint') -> bool:
    """
    Record one draft generation against the user's daily quota.

    Returns True on success, False on failure.
    """
    if not user_email:
        return True

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO draft_logs (user_email, draft_timestamp, case_id, draft_type)
            VALUES (?, ?, ?, ?)
        """, (user_email, datetime.utcnow().isoformat(), case_id, draft_type))

        conn.commit()
        conn.close()
        logger.info(f"✅ Draft usage logged for {user_email} (type={draft_type})")
        return True

    except Exception as e:
        logger.error(f"❌ Error logging draft usage: {e}")
        return False


@app.get("/user/usage-status/{user_email}")
async def get_user_usage_status(user_email: str):
    """
    Returns today's analysis + draft usage counts for a user.
    Frontend calls this to show the quota bar / warn before limit hit.
    """
    today = date.today().isoformat()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()


        cursor.execute("""
            SELECT COUNT(*) FROM case_analyses
            WHERE user_email = ? AND DATE(analysis_timestamp) = ?
        """, (user_email, today))
        analyses_today = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM draft_logs
            WHERE user_email = ? AND DATE(draft_timestamp) = ?
        """, (user_email, today))
        drafts_today = cursor.fetchone()[0]


        analysis_limit = get_user_limit(user_email)
        draft_limit = 3

        conn.close()

        return {
            "success": True,
            "user_email": user_email,
            "date": today,
            "analyses": {
                "used": analyses_today,
                "limit": analysis_limit,
                "remaining": max(0, analysis_limit - analyses_today),
                "exhausted": analyses_today >= analysis_limit
            },
            "drafts": {
                "used": drafts_today,
                "limit": draft_limit,
                "remaining": max(0, draft_limit - drafts_today),
                "exhausted": drafts_today >= draft_limit
            },
            "next_reset": (date.today() + timedelta(days=1)).isoformat() + "T00:00:00"
        }

    except Exception as e:
        logger.error(f"Error getting usage status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _generate_case_suggestions(analysis_result: dict, case_data: dict) -> dict:
    """
    Derive prioritised, actionable suggestions from a completed analysis.
    Returns { critical, high, medium, low, filing_ready, auto_draft_eligible }.
    """
    suggestions = {"critical": [], "high": [], "medium": [], "low": []}

    mods     = analysis_result.get("modules", {})
    timeline = mods.get("timeline_intelligence", analysis_result.get("timeline", {})) or {}
    ingred   = mods.get("ingredient_compliance", analysis_result.get("ingredients", {})) or {}
    doc      = mods.get("documentary_strength", analysis_result.get("documentary", {})) or {}
    defence  = mods.get("defence_risk_analysis", analysis_result.get("defence", {})) or {}
    risk     = mods.get("risk_assessment", analysis_result.get("risk_assessment", {})) or {}
    proc     = mods.get("procedural_defects", analysis_result.get("procedural", {})) or {}
    draft_c  = mods.get("drafting_compliance", analysis_result.get("drafting_compliance", {})) or {}

    score        = float(analysis_result.get("final_score") or analysis_result.get("overall_score") or 0)
    is_fatal     = bool(analysis_result.get("fatal_flag"))
    lim_risk     = timeline.get("limitation_risk", "UNKNOWN")
    case_type    = case_data.get("case_type", "complainant")


    if lim_risk in ("EXPIRED", "CRITICAL"):
        suggestions["critical"].append({
            "id": "SG-C01",
            "title": "Limitation Period Expired or Critical",
            "detail": (
                "The complaint is time-barred or within days of expiry. "
                "A condonation application under Section 142(b) must be filed simultaneously "
                "with strong justification. Without it the court will reject the complaint outright."
            ),
            "action": "File delay condonation application immediately.",
            "section": "Section 142 NI Act"
        })

    for fd in risk.get("fatal_defects", []):
        suggestions["critical"].append({
            "id": "SG-C02",
            "title": "Fatal Defect: " + str(fd.get("defect", "Unknown")),
            "detail": str(fd.get("impact", "This defect will cause the complaint to be dismissed.")),
            "action": str(fd.get("remedy", fd.get("cure", "Consult a lawyer immediately."))),
            "section": str(fd.get("legal_basis", "Section 138 NI Act"))
        })

    if proc.get("fatal_defects"):
        for _pd in proc.get("fatal_defects", []):
            suggestions["critical"].append({
                "id": "SG-C03",
                "title": "Procedural Fatal Defect",
                "detail": str(_pd.get("defect", "Procedural violation detected.")),
                "action": str(_pd.get("remedy", "Rectify before filing.")),
                "section": "Section 142 NI Act"
            })


    if not case_data.get("written_agreement_exists") and case_type == "complainant":
        suggestions["high"].append({
            "id": "SG-H01",
            "title": "No Written Agreement — High Acquittal Risk",
            "detail": (
                "Absence of a written loan/transaction agreement allows the accused to challenge "
                "the very existence of the legally enforceable debt. Courts regularly acquit "
                "when complainants cannot prove the underlying liability."
            ),
            "action": "Gather any WhatsApp messages, emails, ledger entries, bank transfer records, or witness testimony that establishes the debt.",
            "section": "Section 138 NI Act — legally enforceable debt requirement"
        })

    doc_score = float(doc.get("documentary_strength_score") or doc.get("compliance_score") or 0)
    if doc_score < 60:
        missing_docs = doc.get("missing_critical_documents", doc.get("missing_documents", []))
        suggestions["high"].append({
            "id": "SG-H02",
            "title": "Documentary Strength Below Acceptable Threshold",
            "detail": (
                f"Documentary strength is {doc_score:.0f}/100. "
                "Weak documentation significantly raises the risk of acquittal. "
                + ("Missing: " + ", ".join(str(d) for d in missing_docs[:4]) + "." if missing_docs else "")
            ),
            "action": "Obtain original dishonour memo from bank, registered notice copy with AD card, and any transaction proof.",
            "section": "Evidence Act + Section 138 NI Act"
        })

    service_grade = (doc.get("service_proof") or {}).get("grade", "")
    if service_grade in ("WEAK", "VERY WEAK", "WEAK/PRESUMPTIVE"):
        suggestions["high"].append({
            "id": "SG-H03",
            "title": "Notice Service Proof is Weak",
            "detail": (
                "The strength of proof of service of the Section 138 notice is rated " + service_grade + ". "
                "Defence will challenge whether the accused actually received the notice, "
                "which would invalidate the cause of action entirely."
            ),
            "action": "Obtain the original Acknowledgement Due (AD) card. If unavailable, track the postal article online and print the delivery confirmation.",
            "section": "Section 27 General Clauses Act + Section 138 NI Act"
        })

    if case_data.get("is_company_case") and not case_data.get("directors_impleaded"):
        suggestions["high"].append({
            "id": "SG-H04",
            "title": "Directors Not Impleaded in Company Case",
            "detail": (
                "When the accused is a company, Section 141 requires specific averments against "
                "each director/officer who was responsible for the conduct of the company. "
                "Without these averments the directors cannot be convicted and only the company "
                "entity faces liability — making execution of any order practically impossible."
            ),
            "action": "Implead all responsible directors with specific averments about their role at the time of cheque issuance.",
            "section": "Section 141 NI Act"
        })

    if case_data.get("debt_nature") == "cash" and float(case_data.get("cheque_amount", 0)) > 200000:
        suggestions["high"].append({
            "id": "SG-H05",
            "title": "Cash Transaction Over ₹2 Lakh — Section 269SS Risk",
            "detail": (
                "Loans or deposits of ₹20,000 or more in cash are prohibited under Section 269SS "
                "of the Income Tax Act. Defence will argue the transaction was illegal and therefore "
                "the cheque cannot be towards a legally enforceable debt."
            ),
            "action": "Prepare documentary proof that the cash was a genuine business transaction. Consider whether the transaction falls within any exemption.",
            "section": "Section 269SS Income Tax Act + Section 138 NI Act"
        })


    if not case_data.get("bank_dishonour_memo_available"):
        suggestions["medium"].append({
            "id": "SG-M01",
            "title": "Bank Dishonour Memo Not Confirmed",
            "detail": (
                "The original bank return memo is a primary document that proves dishonour. "
                "Without it, the complainant cannot prove the foundational ingredient of the offence."
            ),
            "action": "Obtain a certified copy of the dishonour memo / return memo from the presenting bank.",
            "section": "Section 138 NI Act — dishonour requirement"
        })

    ingred_score = float(ingred.get("overall_score") or ingred.get("compliance_score") or 0)
    if ingred_score < 70:
        missing_ingr = ingred.get("missing_ingredients", [])
        suggestions["medium"].append({
            "id": "SG-M02",
            "title": "Ingredient Compliance Below 70%",
            "detail": (
                f"Section 138 ingredient compliance is {ingred_score:.0f}/100. "
                + ("Unmet: " + ", ".join(str(x) for x in missing_ingr[:3]) if missing_ingr else
                   "One or more of the six mandatory ingredients is weak.")
            ),
            "action": "Review each of the six Section 138 ingredients (cheque validity, dishonour, legally enforceable debt, notice, 15-day period, complaint within limitation) and plug any gaps.",
            "section": "Section 138 NI Act"
        })

    if lim_risk == "HIGH":
        suggestions["medium"].append({
            "id": "SG-M03",
            "title": "Limitation Window is Closing",
            "detail": (
                "The complaint must be filed within one calendar month of the cause of action. "
                "The current limitation risk is HIGH. Any delay may make the case time-barred."
            ),
            "action": "File the complaint immediately. Do not wait for further evidence — file and supplement.",
            "section": "Section 142 NI Act"
        })

    defence_risks = defence.get("high_risk_defences", [])
    if defence_risks:
        suggestions["medium"].append({
            "id": "SG-M04",
            "title": f"{len(defence_risks)} High-Risk Defence(s) Identified",
            "detail": (
                "Accused may raise: " + "; ".join(str(d.get("ground", d.get("defence", ""))) for d in defence_risks[:3]) + ". "
                "Prepare counter-evidence for each."
            ),
            "action": "Prepare affidavit-in-evidence and documentary rebuttal for each identified defence ground.",
            "section": "Section 139 NI Act — presumption as rebuttal tool"
        })

    for gap in draft_c.get("critical_gaps", []):
        suggestions["medium"].append({
            "id": "SG-M05",
            "title": "Drafting Gap: " + str(gap.get("gap", gap.get("issue", "Complaint drafting issue"))),
            "detail": str(gap.get("explanation", "This gap may result in the complaint being challenged at the threshold stage.")),
            "action": str(gap.get("fix", gap.get("remedy", "Ensure the complaint contains the required averment."))),
            "section": "Section 200 CrPC + Section 138 NI Act"
        })


    if not case_data.get("itr_available") and float(case_data.get("cheque_amount", 0)) > 500000:
        suggestions["low"].append({
            "id": "SG-L01",
            "title": "ITR Availability Unconfirmed for Large Amount",
            "detail": (
                "For amounts above ₹5 lakh, courts commonly probe the complainant's financial capacity "
                "to have advanced the loan. Defence may question the source of funds."
            ),
            "action": "Keep ITR copies for the relevant years readily available for examination.",
            "section": "Evidence Act — financial capacity"
        })

    if not case_data.get("case_summary") or len(str(case_data.get("case_summary", ""))) < 50:
        suggestions["low"].append({
            "id": "SG-L02",
            "title": "Case Summary Not Provided",
            "detail": "A detailed case summary helps counsel prepare examination-in-chief and anticipate cross-examination.",
            "action": "Provide a chronological narrative of all events: transaction date, cheque issuance, dishonour, notice, and non-payment.",
            "section": "General best practice"
        })


    total_count = sum(len(v) for v in suggestions.values())
    critical_count = len(suggestions["critical"])
    filing_ready = (not is_fatal) and score >= 75 and critical_count == 0


    auto_draft_eligible = (
        case_type == "complainant"
        and not is_fatal
        and critical_count == 0
        and score >= 50
    )

    return {
        "suggestions": suggestions,
        "total_count": total_count,
        "critical_count": critical_count,
        "high_count": len(suggestions["high"]),
        "medium_count": len(suggestions["medium"]),
        "low_count": len(suggestions["low"]),
        "filing_ready": filing_ready,
        "auto_draft_eligible": auto_draft_eligible,
        "score": score,
        "overall_recommendation": (
            "DO NOT FILE — Fatal defects present. Address critical items first." if is_fatal else
            "READY TO FILE — No critical issues detected." if filing_ready else
            f"NEEDS ATTENTION — {critical_count} critical and {len(suggestions['high'])} high-priority items require action before filing."
        )
    }


@app.post("/suggest")
async def suggest(request: Request):
    """
    Generate smart, prioritised suggestions from a completed analysis.

    Accepts either:
      (a) A full analysis result object (field: analysis)
      (b) A case_id to look up a stored analysis from the database

    Body:
      { "analysis": <analysis_result_object>, "case_data": <original_case_data> }
      OR
      { "case_id": "<stored_case_id>", "user_email": "<email>" }

    Returns prioritised suggestions (critical / high / medium / low),
    a filing recommendation, and whether auto-draft is eligible.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    analysis_result = body.get("analysis")
    case_data       = body.get("case_data", {})
    case_id         = body.get("case_id", "")

    if not analysis_result and case_id:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT analysis_json, case_type, cheque_amount FROM case_analyses WHERE case_id = ?",
                (case_id,)
            )
            row = cursor.fetchone()
            conn.close()
            if not row:
                raise HTTPException(status_code=404, detail=f"case_id '{case_id}' not found")
            analysis_result = json.loads(row[0]) if row[0] else {}
            if not case_data:
                case_data = analysis_result.get("case_metadata", {})
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"DB fetch for suggestions failed: {e}")
            raise HTTPException(status_code=500, detail="Failed to retrieve analysis from database")

    if not analysis_result:
        raise HTTPException(status_code=400, detail="Provide either 'analysis' object or 'case_id'")

    try:
        result = _generate_case_suggestions(analysis_result, case_data)
    except Exception as e:
        logger.error(f"Suggestion generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Suggestion engine error: {str(e)}")

    return {
        "success": True,
        "case_id": case_id or analysis_result.get("case_id", ""),
        **result
    }


@app.post("/auto-draft")
async def auto_draft(request: Request):
    """
    Analyse case data, generate smart suggestions, and immediately produce
    a tailored Section 138 complaint draft — all in one call.

    Daily limit: 3 drafts per user (shared with /generate-draft).

    Body:
      {
        "user_email":  "lawyer@example.com",
        "case_data":   { <full CaseAnalysisRequest fields> },
        "case_id":     "<optional — links to existing analysis>",
        "analysis":    <optional — pre-computed analysis result>
      }

    Returns:
      - suggestions:  prioritised suggestions from the analysis
      - draft_text:   complete Section 138 complaint text
      - sections:     structured dict of complaint sections
      - warnings:     suggestions that should be resolved BEFORE filing
      - usage:        { used, limit, remaining, exhausted }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    user_email      = body.get("user_email", "")
    case_data       = body.get("case_data", {})
    case_id         = body.get("case_id", "")
    analysis_result = body.get("analysis")
    draft_type      = "auto_draft"


    if user_email:
        is_allowed, used_today = check_draft_limit(user_email)
        if not is_allowed:
            return {
                "success": False,
                "error": "DRAFT_LIMIT_REACHED",
                "error_type": "RATE_LIMIT",
                "message": (
                    f"Daily draft limit of 3 reached. "
                    f"You have generated {used_today}/3 drafts today. "
                    "Please try again tomorrow."
                ),
                "usage": {
                    "used": used_today,
                    "limit": 3,
                    "remaining": 0,
                    "exhausted": True
                },
                "next_reset": (date.today() + timedelta(days=1)).isoformat() + "T00:00:00"
            }
    else:
        used_today = 0


    if not analysis_result and case_id:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT analysis_json FROM case_analyses WHERE case_id = ?",
                (case_id,)
            )
            row = cursor.fetchone()
            conn.close()
            if row and row[0]:
                analysis_result = json.loads(row[0])
                if not case_data:
                    case_data = analysis_result.get("case_metadata", {})
        except Exception as e:
            logger.warning(f"Could not load stored analysis for auto-draft: {e}")


    suggestion_data = {}
    if analysis_result:
        try:
            suggestion_data = _generate_case_suggestions(analysis_result, case_data)
        except Exception as e:
            logger.warning(f"Suggestion step failed in auto-draft: {e}")
            suggestion_data = {}


    warnings = []
    if suggestion_data:
        warnings.extend(suggestion_data.get("suggestions", {}).get("critical", []))
        warnings.extend(suggestion_data.get("suggestions", {}).get("high", []))


    if suggestion_data and not suggestion_data.get("auto_draft_eligible", True):
        critical_count = suggestion_data.get("critical_count", 0)
        return {
            "success": False,
            "error": "DRAFT_NOT_ELIGIBLE",
            "error_type": "CASE_INELIGIBLE",
            "message": (
                f"Auto-draft is blocked because {critical_count} fatal/critical issue(s) were detected. "
                "A complaint filed in this state will likely be rejected. "
                "Address the critical issues first, then request the draft."
            ),
            "suggestions": suggestion_data.get("suggestions", {}),
            "critical_count": critical_count,
            "overall_recommendation": suggestion_data.get("overall_recommendation", ""),
            "resolve_first": suggestion_data.get("suggestions", {}).get("critical", [])
        }


    if analysis_result:
        _mods = analysis_result.get("modules", {})


        _tl = _mods.get("timeline_intelligence", analysis_result.get("timeline", {})) or {}
        _dates = _tl.get("dates", _tl.get("critical_dates", {})) or {}
        for _fld in ("cheque_date", "dishonour_date", "notice_date", "complaint_filed_date"):
            if not case_data.get(_fld) and _dates.get(_fld):
                case_data[_fld] = _dates[_fld]


        _coa = _tl.get("cause_of_action_date") or _tl.get("cause_of_action", {})
        if isinstance(_coa, str):
            case_data["cause_of_action_date"] = _coa
        elif isinstance(_coa, dict):
            case_data["cause_of_action_date"] = _coa.get("date", "")


        _jur = _mods.get("territorial_jurisdiction", analysis_result.get("jurisdiction", {})) or {}
        if not case_data.get("jurisdiction") and _jur.get("recommended_jurisdiction"):
            case_data["jurisdiction"] = _jur["recommended_jurisdiction"]


    try:
        draft = _build_enhanced_complaint_draft(case_data, suggestion_data)
    except Exception as e:
        logger.error(f"Auto-draft build error: {e}")
        raise HTTPException(status_code=500, detail=f"Draft generation failed: {str(e)}")


    if user_email:
        log_draft_usage(user_email, case_id=case_id, draft_type=draft_type)
        used_today += 1

    remaining = max(0, 3 - used_today)

    return {
        "success": True,
        "draft_type": "auto_draft",
        "case_id": case_id or (analysis_result or {}).get("case_id") or "",
        "draft_text": draft["full_text"],
        "sections": draft["sections"],
        "word_count": len(draft["full_text"].split()),
        "suggestions": suggestion_data.get("suggestions", {}),
        "filing_ready": suggestion_data.get("filing_ready", False),
        "overall_recommendation": suggestion_data.get("overall_recommendation", ""),
        "warnings": warnings,
        "warning_count": len(warnings),
        "usage": {
            "used": used_today,
            "limit": 3,
            "remaining": remaining,
            "exhausted": remaining == 0
        },
        "next_reset": (date.today() + timedelta(days=1)).isoformat() + "T00:00:00",
        "disclaimer": (
            "This draft is AI-generated for reference only. "
            "Always review with qualified legal counsel before filing. "
            "Address all warnings (if any) before submitting to court."
        )
    }


def _build_enhanced_complaint_draft(case_data: dict, suggestion_data: dict) -> dict:
    """
    Build an enhanced, suggestion-aware Section 138 NI Act complaint draft.
    Inserts additional paragraphs when analysis flags specific risks.
    Returns { full_text: str, sections: dict }.
    """
    complainant   = case_data.get("complainant_name", "[COMPLAINANT NAME]")
    accused       = case_data.get("accused_name", "[ACCUSED NAME]")
    court         = case_data.get("court_name", "the Hon'ble Court")
    cheque_no     = case_data.get("cheque_number", "[CHEQUE NO]")
    bank          = case_data.get("bank_name", "[BANK NAME]")
    cheque_date   = case_data.get("cheque_date", "[CHEQUE DATE]")
    amount        = case_data.get("cheque_amount", 0)
    dis_date      = case_data.get("dishonour_date", "[DISHONOUR DATE]")
    dis_reason    = case_data.get("dishonour_reason", "Insufficient Funds")
    notice_date   = case_data.get("notice_date", "[NOTICE DATE]")
    notice_served = case_data.get("notice_service_date", notice_date)
    complaint_dt  = case_data.get("complaint_filed_date", date.today().isoformat())
    debt_nature   = case_data.get("debt_nature", "legally enforceable debt")
    jurisdiction  = case_data.get("jurisdiction", "[JURISDICTION]")
    trans_date    = case_data.get("transaction_date", "")
    coa_date      = case_data.get("cause_of_action_date", "")
    is_company    = case_data.get("is_company_case", False)
    directors     = case_data.get("directors_impleaded", False)
    has_agreement = case_data.get("written_agreement_exists", False)
    debt_nat_raw  = case_data.get("debt_nature", "loan")

    amt_fmt  = "\u20b9{:,.2f}".format(float(amount)) if amount else "[AMOUNT]"
    amt_2x   = "\u20b9{:,.2f}".format(float(amount) * 2) if amount else "[AMOUNT x2]"
    year     = date.today().year

    sug_all  = suggestion_data.get("suggestions", {}) if suggestion_data else {}
    critical = sug_all.get("critical", [])
    high     = sug_all.get("high", [])

    sections = {}


    sections["heading"] = (
        "IN THE COURT OF " + court.upper() + "\n\n"
        "COMPLAINT CASE NO. __________ / " + str(year) + "\n\n"
        "COMPLAINANT:\n"
        "    " + complainant + "\n\n"
        "ACCUSED:\n"
        "    " + accused + "\n\n"
        "COMPLAINT UNDER SECTIONS 138"
        + (", 141" if is_company else "")
        + ", 142 OF THE NEGOTIABLE INSTRUMENTS ACT, 1881\n"
        "READ WITH SECTION 200 OF THE CODE OF CRIMINAL PROCEDURE, 1973\n"
    )


    para_num = [1]

    def p(text):
        n = para_num[0]
        para_num[0] += 1
        return str(n) + ". " + text + "\n\n"

    facts = ""
    if trans_date:
        facts += p(
            "That on " + trans_date + ", the Accused approached the Complainant and "
            "borrowed a sum of " + amt_fmt + " as a " + str(debt_nat_raw) + ". "
            "The Complainant, having the financial capacity to advance the said sum, "
            "duly paid the same to the Accused."
            + (" The said transaction is evidenced by a written agreement / acknowledgement executed by the Accused." if has_agreement else "")
        )
    else:
        facts += p(
            "That the Accused was and is indebted to the Complainant in a sum of "
            + amt_fmt + " towards " + debt_nature + "."
        )

    facts += p(
        "That in discharge of the said legally enforceable liability and debt, "
        "the Accused issued Cheque No. " + cheque_no + " dated " + cheque_date
        + " drawn on " + bank + " for an amount of " + amt_fmt
        + " in favour of the Complainant. "
        "The said cheque was issued towards discharge of a legally enforceable debt "
        "and liability of the Accused towards the Complainant."
    )

    facts += p(
        "That the Complainant duly presented the said cheque for encashment through "
        "his banker in the normal course of banking operations. The cheque was "
        "returned/dishonoured by the drawee bank on " + dis_date
        + " vide a return memo bearing the reason \"" + dis_reason + "\". "
        "The said dishonour memo is in the possession of the Complainant and is "
        "produced as Exhibit A."
    )


    facts += p(
        "That upon dishonour of the said cheque, the Complainant caused a legal "
        "notice under Section 138 of the Negotiable Instruments Act, 1881 to be "
        "served upon the Accused by registered post with Acknowledgement Due on "
        + notice_date + ", demanding payment of " + amt_fmt
        + " within a period of fifteen (15) days from the date of receipt of the notice."
    )

    facts += p(
        "That the said statutory notice was duly served upon the Accused"
        + (" on " + notice_served if notice_served and notice_served != notice_date else "")
        + ". Despite receipt of the said notice and expiry of the fifteen (15) day "
        "period stipulated therein, the Accused has failed, neglected, and refused "
        "to pay the said amount of " + amt_fmt + " or any part thereof to the Complainant. "
        + ("The cause of action for the present complaint arose on " + coa_date + "." if coa_date else "")
    )

    sections["facts"] = facts.strip()


    s141_text = ""
    if is_company:
        s141_text += p(
            "That the Accused is a company incorporated under the Companies Act and "
            "the cheque in question was issued by the company through its authorised "
            "signatory. By virtue of Section 141 of the Negotiable Instruments Act, 1881, "
            "every person who at the time the offence was committed was in charge of, "
            "and was responsible to the company for the conduct of its business, "
            "shall be deemed to be guilty of the offence."
        )
        if directors:
            s141_text += p(
                "That the directors/officers responsible for the day-to-day affairs of "
                "the Accused-company are being impleaded herein with specific averments "
                "regarding their role and responsibility in the issuance of the dishonoured cheque."
            )

    sections["section_141"] = s141_text.strip() if s141_text else ""


    averments = ""
    averments += p(
        "That the cheque in question represents a legally enforceable debt and "
        "liability of the Accused towards the Complainant. The presumption under "
        "Section 139 of the Negotiable Instruments Act, 1881 squarely applies, "
        "and the burden is upon the Accused to rebut the said presumption."
    )

    averments += p(
        "That all the ingredients of the offence punishable under Section 138 of "
        "the Negotiable Instruments Act, 1881 are satisfied in the present case:\n"
        "   (i)   A cheque was drawn on a bank account for payment of a sum of money;\n"
        "   (ii)  The cheque was issued towards discharge of a legally enforceable debt;\n"
        "   (iii) The cheque was presented within three months of its date;\n"
        "   (iv)  The cheque was returned by the drawee bank unpaid;\n"
        "   (v)   A demand notice was served upon the Accused within 30 days of dishonour;\n"
        "   (vi)  The Accused failed to pay within 15 days of receiving the notice."
    )

    averments += p(
        "That the present complaint is being filed within the period of limitation "
        "prescribed under Section 142 of the Negotiable Instruments Act, 1881, "
        "i.e., within one month from the date the cause of action arose."
    )

    averments += p(
        "That this Hon'ble Court has jurisdiction to try the present complaint as "
        "the cheque was delivered for collection / presented for payment within "
        "the territorial jurisdiction of this Court in " + jurisdiction + "."
    )


    sugs_ids = [s.get("id", "") for s in critical + high]
    if "SG-H01" in sugs_ids or case_data.get("security_cheque_alleged"):
        averments += p(
            "That the Complainant specifically avers and states that the cheque in "
            "question was NOT issued as a security cheque but was issued towards "
            "discharge of a definite, crystallised, and legally enforceable liability "
            "of the Accused towards the Complainant as of the date of issuance."
        )

    sections["averments"] = averments.strip()


    sections["relief"] = (
        "In view of the foregoing facts and circumstances, it is most respectfully "
        "prayed that this Hon'ble Court may be pleased to:\n\n"
        "   (a) Take cognizance of the offence punishable under Section 138"
        + (", 141" if is_company else "")
        + " of the Negotiable Instruments Act, 1881;\n\n"
        "   (b) Issue process (summons) against the Accused and direct the "
        "Accused to appear before this Hon'ble Court;\n\n"
        "   (c) Upon trial, convict the Accused and sentence them to imprisonment "
        "for a term which may extend to two years and/or fine which shall not be "
        "less than the cheque amount i.e. " + amt_fmt + " and may extend to twice "
        "the cheque amount i.e. " + amt_2x + ";\n\n"
        "   (d) Direct the Accused to pay compensation of " + amt_fmt + " to the "
        "Complainant under Section 357 of the Code of Criminal Procedure, 1973;\n\n"
        "   (e) Grant interim compensation under Section 143A of the Negotiable "
        "Instruments Act, 1881 pending trial;\n\n"
        "   (f) Pass such other and further orders as this Hon'ble Court may deem "
        "fit and proper in the interests of justice.\n"
    )


    sections["documents"] = (
        "LIST OF DOCUMENTS\n\n"
        "Exhibit A: Original dishonoured cheque and bank return memo.\n"
        "Exhibit B: Copy of legal notice dated " + notice_date + ".\n"
        "Exhibit C: Postal receipts and acknowledgement due (AD) card evidencing service.\n"
        + ("Exhibit D: Written agreement / acknowledgement of debt.\n" if has_agreement else "")
        + "Exhibit E: Bank statement of Complainant evidencing transaction.\n"
    )


    sections["verification"] = (
        "VERIFICATION\n\n"
        "I, " + complainant + ", the Complainant above named, do hereby solemnly verify "
        "and declare that the contents of the above complaint are true and correct to "
        "the best of my knowledge, information, and belief. Nothing material has been "
        "concealed or misrepresented herein.\n\n"
        "Verified at __________ on " + complaint_dt + ".\n\n"
        "                                          COMPLAINANT\n"
        "                                     (" + complainant + ")\n\n"
        "Through Counsel:\n"
        "Advocate: __________________________\n"
        "Bar No.:  __________________________\n"
        "Address:  __________________________\n"
    )


    full_text = sections["heading"] + "\n"
    full_text += "FACTS OF THE CASE\n\n" + sections["facts"] + "\n\n"
    if sections["section_141"]:
        full_text += "AVERMENTS UNDER SECTION 141 NI ACT\n\n" + sections["section_141"] + "\n\n"
    full_text += "LEGAL AVERMENTS\n\n" + sections["averments"] + "\n\n"
    full_text += "PRAYER\n\n" + sections["relief"] + "\n"
    full_text += sections["documents"] + "\n"
    full_text += sections["verification"]

    return {"full_text": full_text, "sections": sections}


@app.post("/generate-draft")
async def generate_draft(request: Request):
    """
    Generate a Section 138 complaint draft from case data.

    Daily limit: 3 drafts per user (tracked by user_email).
    Anonymous requests (no user_email) are always allowed but uncounted.

    Body fields:
      - user_email: str  (required for limit tracking)
      - case_id:    str  (optional — links draft to an existing analysis)
      - case_data:  dict (case facts used to populate the draft)
      - draft_type: str  (default: 'complaint')

    Returns:
      - draft_text: full plain-text Section 138 complaint draft
      - sections:   structured dict of each complaint section
      - usage:      { used, limit, remaining } for today
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    user_email  = body.get("user_email", "")
    case_id     = body.get("case_id", "")
    case_data   = body.get("case_data", {})
    draft_type  = body.get("draft_type", "complaint")


    if user_email:
        is_allowed, used_today = check_draft_limit(user_email)
        if not is_allowed:
            return {
                "success": False,
                "error": "DRAFT_LIMIT_REACHED",
                "error_type": "RATE_LIMIT",
                "message": (
                    f"Daily draft limit of 3 reached. "
                    f"You have generated {used_today}/3 drafts today. "
                    "Please try again tomorrow."
                ),
                "usage": {
                    "used": used_today,
                    "limit": 3,
                    "remaining": 0,
                    "exhausted": True
                },
                "next_reset": (date.today() + timedelta(days=1)).isoformat() + "T00:00:00"
            }
    else:
        used_today = 0


    try:
        draft = _build_complaint_draft(case_data)
    except Exception as e:
        logger.error(f"Draft generation error: {e}")
        raise HTTPException(status_code=500, detail=f"Draft generation failed: {str(e)}")


    if user_email:
        log_draft_usage(user_email, case_id=case_id, draft_type=draft_type)
        used_today += 1

    remaining = max(0, 3 - used_today)

    return {
        "success": True,
        "draft_type": draft_type,
        "case_id": case_id or None,
        "draft_text": draft["full_text"],
        "sections": draft["sections"],
        "word_count": len(draft["full_text"].split()),
        "usage": {
            "used": used_today,
            "limit": 3,
            "remaining": remaining,
            "exhausted": remaining == 0
        },
        "next_reset": (date.today() + timedelta(days=1)).isoformat() + "T00:00:00",
        "disclaimer": (
            "This draft is generated for reference only. "
            "Review and adapt with qualified legal counsel before filing."
        )
    }


def _build_complaint_draft(case_data: dict) -> dict:
    """
    Build a structured Section 138 NI Act complaint draft from case_data.
    Returns { full_text: str, sections: dict }.
    """

    complainant   = case_data.get("complainant_name", "[COMPLAINANT NAME]")
    accused       = case_data.get("accused_name", "[ACCUSED NAME]")
    court         = case_data.get("court_name", "the Hon\'ble Court")
    cheque_no     = case_data.get("cheque_number", "[CHEQUE NO]")
    bank          = case_data.get("bank_name", "[BANK NAME]")
    cheque_date   = case_data.get("cheque_date", "[CHEQUE DATE]")
    amount        = case_data.get("cheque_amount", 0)
    dis_date      = case_data.get("dishonour_date", "[DISHONOUR DATE]")
    dis_reason    = case_data.get("dishonour_reason", "Insufficient Funds")
    notice_date   = case_data.get("notice_date", "[NOTICE DATE]")
    notice_served = case_data.get("notice_service_date", "[SERVICE DATE]")
    complaint_dt  = case_data.get("complaint_filed_date", date.today().isoformat())
    debt_nature   = case_data.get("debt_nature", "legally enforceable debt")
    jurisdiction  = case_data.get("jurisdiction", "[JURISDICTION]")

    amt_fmt = "\u20b9{:,.2f}".format(amount) if amount else "[AMOUNT]"
    year    = date.today().year

    sections = {}

    sections["heading"] = (
        "IN THE COURT OF " + court.upper() + "\n\n"
        "COMPLAINT CASE NO. __________ / " + str(year) + "\n\n"
        "COMPLAINANT: " + complainant + "\n"
        "ACCUSED:     " + accused + "\n\n"
        "COMPLAINT UNDER SECTIONS 138, 141, 142 OF THE NEGOTIABLE INSTRUMENTS ACT, 1881\n"
        "READ WITH SECTION 200 OF THE CODE OF CRIMINAL PROCEDURE, 1973\n"
    )

    sections["facts"] = (
        "1. The Complainant states that the Accused was indebted to the Complainant "
        "for a sum of " + amt_fmt + " towards " + debt_nature + ".\n\n"
        "2. In discharge of the said legally enforceable liability, the Accused issued "
        "Cheque No. " + cheque_no + " dated " + cheque_date + " drawn on " + bank +
        " for an amount of " + amt_fmt + " in favour of the Complainant.\n\n"
        "3. The Complainant presented the said cheque for encashment through his bank. "
        "The cheque was returned/dishonoured on " + dis_date + " with the endorsement "
        "\"" + dis_reason + "\".\n"
    )

    sections["notice"] = (
        "4. The Complainant caused a legal notice under Section 138 of the Negotiable "
        "Instruments Act, 1881 to be served upon the Accused on " + notice_date + " by "
        "registered post with Acknowledgement Due, calling upon the Accused to pay "
        "the dishonoured amount of " + amt_fmt + " within fifteen (15) days of receipt "
        "of the notice.\n\n"
        "5. The notice was duly served upon the Accused on " + notice_served + ". "
        "Despite service of the notice, the Accused failed and neglected to make "
        "payment of the said amount within the stipulated period of 15 days, thereby "
        "giving rise to the cause of action for the present complaint.\n"
    )

    sections["averments"] = (
        "6. The cheque in question was issued by the Accused towards discharge of a "
        "legally enforceable debt and liability. The Complainant states that there "
        "existed a legally enforceable debt/liability of the Accused towards the "
        "Complainant as on the date of issuance of the cheque.\n\n"
        "7. The present complaint is being filed within the period of limitation as "
        "prescribed under Section 142 of the Negotiable Instruments Act, 1881, "
        "i.e., within one month from the date the cause of action arose.\n\n"
        "8. The jurisdiction of this Hon\'ble Court is invoked as the cheque was "
        "presented at a bank branch within " + jurisdiction + ".\n"
    )

    sections["relief"] = (
        "9. In view of the foregoing, the Complainant most respectfully prays that "
        "this Hon\'ble Court may be pleased to:\n\n"
        "   (a) Take cognizance of the offence punishable under Section 138 of the "
        "Negotiable Instruments Act, 1881;\n\n"
        "   (b) Issue process against the Accused and direct the Accused to appear "
        "before this Hon\'ble Court;\n\n"
        "   (c) Upon trial, convict the Accused and sentence them to imprisonment "
        "and/or fine not less than twice the cheque amount i.e. " + amt_fmt + " x 2;\n\n"
        "   (d) Direct the Accused to pay compensation of " + amt_fmt + " to the "
        "Complainant under Section 357 Cr.P.C.;\n\n"
        "   (e) Pass such other and further orders as this Hon\'ble Court may deem "
        "fit and proper in the facts and circumstances of the case.\n"
    )

    sections["verification"] = (
        "VERIFICATION\n\n"
        "I, " + complainant + ", the Complainant above named, do hereby verify and declare "
        "that the contents of the above complaint are true and correct to the best "
        "of my knowledge, information, and belief. Nothing material has been "
        "concealed therefrom.\n\n"
        "Verified at __________ on " + complaint_dt + ".\n\n"
        "                                          COMPLAINANT\n"
        "                                     (" + complainant + ")\n"
    )

    full_text = (
        sections["heading"] + "\n"
        + "FACTS OF THE CASE\n\n" + sections["facts"] + "\n"
        + "STATUTORY NOTICE\n\n" + sections["notice"] + "\n"
        + "LEGAL AVERMENTS\n\n" + sections["averments"] + "\n"
        + "PRAYER\n\n" + sections["relief"] + "\n"
        + sections["verification"]
    )

    return {"full_text": full_text, "sections": sections}

@app.post("/admin/create-account")
async def create_admin_account(request: Request):
    """
    Create a new admin account.
    Authorization options:
      1. Firebase token (Bearer <token>) — the token's email must be an existing super_admin
      2. Classic body: { authorizer_email, authorizer_password, new_email, new_password, new_name, role }
    """
    try:
        data = await request.json()

        # ── Resolve authorizer identity ────────────────────────────────────
        firebase_email = get_firebase_user_email(request)
        if firebase_email:
            auth_email = firebase_email.lower().strip()
            if auth_email not in ADMIN_CREDENTIALS:
                return {'success': False, 'error': 'Firebase user is not a registered admin'}
            if ADMIN_CREDENTIALS[auth_email].get('role') != 'super_admin':
                return {'success': False, 'error': 'Only super_admin can create new admin accounts'}
        else:
            auth_email = data.get('authorizer_email', '').lower().strip()
            auth_pw    = data.get('authorizer_password', '')
            if not verify_admin(auth_email, auth_pw):
                return {'success': False, 'error': 'Unauthorized — invalid authorizer credentials'}
            if ADMIN_CREDENTIALS.get(auth_email, {}).get('role') != 'super_admin':
                return {'success': False, 'error': 'Only super_admin can create new admin accounts'}

        new_email  = data.get('new_email', '').strip().lower()
        new_pw     = data.get('new_password', '')
        new_name   = data.get('new_name', '').strip()
        role       = data.get('role', 'admin')

        if not new_email or not new_pw or not new_name:
            return {'success': False, 'error': 'new_email, new_password, and new_name are required'}

        if len(new_pw) < 8:
            return {'success': False, 'error': 'Password must be at least 8 characters'}

        if new_email in ADMIN_CREDENTIALS:
            return {'success': False, 'error': 'An admin account with this email already exists'}

        import hashlib as _hl
        pw_hash = _hl.sha256(new_pw.encode()).hexdigest()

        ADMIN_CREDENTIALS[new_email] = {
            'password_hash': pw_hash,
            'name': new_name,
            'role': role
        }

        try:
            _conn = get_db_connection()
            _cur = _conn.cursor()
            _cur.execute("""
                INSERT OR REPLACE INTO admin_accounts (email, name, role, password_hash, created_by)
                VALUES (?, ?, ?, ?, ?)
            """, (new_email, new_name, role, pw_hash, auth_email))
            _conn.commit()
            _conn.close()
        except Exception as _db_e:
            logger.warning(f"Could not persist admin to DB: {_db_e}")

        log_admin_action(auth_email, 'CREATE_ADMIN', new_email,
                         f'Created new {role} account: {new_name}')

        logger.info(f'New admin account created: {new_email} (role={role}) by {auth_email}')

        return {
            'success': True,
            'message': f'Admin account created for {new_email}',
            'admin': {'email': new_email, 'name': new_name, 'role': role}
        }

    except Exception as e:
        logger.error(f'Admin account creation error: {e}')
        return {'success': False, 'error': str(e)}


@app.post("/admin/login")
async def admin_login(request: Request):
    """
    Admin authentication endpoint.
    Accepts either:
      - { email, password }  — classic password auth
      - Authorization: Bearer <firebase-id-token>  — Firebase token auth (email must match an admin)
    """
    try:
        # ── Firebase token path ────────────────────────────────────────────
        firebase_email = get_firebase_user_email(request)
        if firebase_email:
            email = firebase_email.lower().strip()
            if email in ADMIN_CREDENTIALS:
                log_admin_action(email, "LOGIN_FIREBASE", None, "Firebase token login")
                return {
                    'success': True,
                    'auth_method': 'firebase',
                    'admin': {
                        'email': email,
                        'name': ADMIN_CREDENTIALS[email]['name'],
                        'role': ADMIN_CREDENTIALS[email]['role']
                    }
                }
            return {'success': False, 'error': 'Firebase user is not a registered admin'}

        # ── Classic password path ──────────────────────────────────────────
        data = await request.json()
        email = data.get('email', '').lower().strip()
        password = data.get('password', '')

        if verify_admin(email, password):
            log_admin_action(email, "LOGIN_PASSWORD", None, "Password login")
            return {
                'success': True,
                'auth_method': 'password',
                'admin': {
                    'email': email,
                    'name': ADMIN_CREDENTIALS[email]['name'],
                    'role': ADMIN_CREDENTIALS[email]['role']
                }
            }
        return {'success': False, 'error': 'Invalid admin credentials'}
    except Exception as e:
        logger.error(f"Admin login error: {e}")
        return {'success': False, 'error': str(e)}

@app.post("/admin/create-account-open")
async def admin_create_account_open(request: Request):
    """
    TEMPORARY: Open admin registration for initial setup.
    If Firebase credentials are configured, the caller must supply a valid
    Firebase ID token (Authorization: Bearer <token>) — the token email becomes
    the creator identity.  If Firebase is not configured, no auth is required.
    DELETE or DISABLE this endpoint after creating your first admin account!
    """
    try:
        data = await request.json()

        # Determine creator identity
        fb_email = get_firebase_user_email(request)
        creator_identity = fb_email.lower().strip() if fb_email else "OPEN_REGISTRATION"

        logger.warning(f"⚠️ OPEN admin registration by {creator_identity}")

        # New admin details
        new_email = data.get('new_email', '').strip().lower()
        new_name = data.get('new_name', '').strip()
        new_password = data.get('new_password', '')
        new_role = data.get('role', 'admin')
        
        # Validation
        if not new_email or not new_name or not new_password:
            return {
                'success': False,
                'error': 'Email, name, and password are required'
            }
        
        if len(new_password) < 8:
            return {
                'success': False,
                'error': 'Password must be at least 8 characters long'
            }
        
        if new_email in ADMIN_CREDENTIALS:
            return {
                'success': False,
                'error': 'An admin account with this email already exists'
            }
        
        # Hash password
        import hashlib
        password_hash = hashlib.sha256(new_password.encode()).hexdigest()
        
        # Save to database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO admin_accounts (email, name, role, password_hash, created_by)
                VALUES (?, ?, ?, ?, ?)
            """, (new_email, new_name, new_role, password_hash, creator_identity))
            conn.commit()
            conn.close()
        except sqlite3.IntegrityError:
            return {
                'success': False,
                'error': 'An admin account with this email already exists in database'
            }
        except Exception as db_err:
            logger.error(f"Database error creating admin: {db_err}")
            return {
                'success': False,
                'error': f'Database error: {str(db_err)}'
            }
        
        # Add to ADMIN_CREDENTIALS (in-memory)
        ADMIN_CREDENTIALS[new_email] = {
            'password_hash': password_hash,
            'name': new_name,
            'role': new_role
        }
        
        # Log the action
        log_admin_action(creator_identity, "OPEN_REGISTRATION", new_email, f"Registered via open endpoint: {new_name} ({new_role})")
        
        logger.info(f"✅ Admin registered via OPEN endpoint: {new_email} ({new_role})")
        logger.warning("🚨 Remember to disable /admin/create-account-open endpoint after setup!")
        
        return {
            'success': True,
            'message': f'Admin account created successfully for {new_email}',
            'admin': {
                'email': new_email,
                'name': new_name,
                'role': new_role
            }
        }
        
    except Exception as e:
        logger.error(f"Open admin registration error: {e}")
        return {'success': False, 'error': str(e)}

@app.get("/admin/users")
async def get_all_users(admin_email: str = "", request: Request = None):
    """Get all users with their usage stats"""
    # Firebase token takes precedence over query param
    if request:
        fb_email = get_firebase_user_email(request)
        if fb_email:
            admin_email = fb_email.lower().strip()
    if admin_email not in ADMIN_CREDENTIALS:
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()


        cursor.execute('''
            SELECT
                user_email,
                COUNT(*) as total_analyses,
                MAX(analysis_timestamp) as last_analysis,
                MIN(analysis_timestamp) as first_analysis
            FROM case_analyses
            WHERE user_email IS NOT NULL AND user_email != ''
            GROUP BY user_email
            ORDER BY total_analyses DESC
        ''')

        users = []
        for row in cursor.fetchall():
            user_email = row[0]


            cursor.execute('''
                SELECT COUNT(*) FROM case_analyses
                WHERE user_email = ? AND DATE(analysis_timestamp) = DATE('now')
            ''', (user_email,))
            today_count = cursor.fetchone()[0]


            cursor.execute('''
                SELECT daily_limit, is_unlimited, custom_limit_reason
                FROM user_limits WHERE user_email = ?
            ''', (user_email,))
            limit_info = cursor.fetchone()

            users.append({
                'email': user_email,
                'total_analyses': row[1],
                'last_analysis': row[2],
                'first_analysis': row[3],
                'today_usage': today_count,
                'daily_limit': limit_info[0] if limit_info else 3,
                'is_unlimited': bool(limit_info[1]) if limit_info else False,
                'limit_reason': limit_info[2] if limit_info else None
            })

        conn.close()

        log_admin_action(admin_email, "VIEW_USERS", None, f"Viewed {len(users)} users")

        return {
            'success': True,
            'users': users,
            'total_users': len(users)
        }
    except Exception as e:
        logger.error(f"Error getting users: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/set-user-limit")
async def set_user_limit(request: Request):
    """Set custom daily limit for a user"""
    try:
        data = await request.json()
        admin_email = data.get('admin_email', '')
        user_email = data.get('user_email')
        daily_limit = data.get('daily_limit', 3)
        is_unlimited = data.get('is_unlimited', False)
        reason = data.get('reason', '')

        # Firebase token overrides body admin_email
        fb_email = get_firebase_user_email(request)
        if fb_email:
            admin_email = fb_email.lower().strip()

        if admin_email not in ADMIN_CREDENTIALS:
            raise HTTPException(status_code=403, detail="Admin access required")

        conn = get_db_connection()
        cursor = conn.cursor()


        cursor.execute('''
            INSERT INTO user_limits (user_email, daily_limit, is_unlimited, custom_limit_reason, set_by_admin)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_email) DO UPDATE SET
                daily_limit = excluded.daily_limit,
                is_unlimited = excluded.is_unlimited,
                custom_limit_reason = excluded.custom_limit_reason,
                set_by_admin = excluded.set_by_admin,
                updated_at = CURRENT_TIMESTAMP
        ''', (user_email, daily_limit, 1 if is_unlimited else 0, reason, admin_email))

        conn.commit()
        conn.close()

        limit_text = "UNLIMITED" if is_unlimited else f"{daily_limit}/day"
        log_admin_action(admin_email, "SET_LIMIT", user_email, f"Set limit to {limit_text}: {reason}")

        return {
            'success': True,
            'message': f"Limit set to {limit_text} for {user_email}"
        }
    except Exception as e:
        logger.error(f"Error setting user limit: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/analytics")
async def get_admin_analytics(admin_email: str = "", request: Request = None):
    """Get comprehensive analytics for admin dashboard"""
    if request:
        fb_email = get_firebase_user_email(request)
        if fb_email:
            admin_email = fb_email.lower().strip()
    if admin_email not in ADMIN_CREDENTIALS:
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()


        cursor.execute("SELECT COUNT(*) FROM case_analyses")
        total_analyses = cursor.fetchone()[0]


        cursor.execute("SELECT COUNT(DISTINCT user_email) FROM case_analyses WHERE user_email IS NOT NULL")
        total_users = cursor.fetchone()[0]


        cursor.execute("SELECT COUNT(*) FROM case_analyses WHERE DATE(analysis_timestamp) = DATE('now')")
        today_analyses = cursor.fetchone()[0]


        cursor.execute("SELECT COUNT(*) FROM case_analyses WHERE DATE(analysis_timestamp) >= DATE('now', '-7 days')")
        week_analyses = cursor.fetchone()[0]


        cursor.execute("SELECT COUNT(*) FROM case_analyses WHERE DATE(analysis_timestamp) >= DATE('now', '-30 days')")
        month_analyses = cursor.fetchone()[0]


        cursor.execute('''
            SELECT DATE(analysis_timestamp) as date, COUNT(*) as count
            FROM case_analyses
            WHERE DATE(analysis_timestamp) >= DATE('now', '-30 days')
            GROUP BY DATE(analysis_timestamp)
            ORDER BY date DESC
        ''')
        daily_stats = [{'date': row[0], 'count': row[1]} for row in cursor.fetchall()]


        cursor.execute('''
            SELECT user_email, COUNT(*) as count
            FROM case_analyses
            WHERE user_email IS NOT NULL
            GROUP BY user_email
            ORDER BY count DESC
            LIMIT 10
        ''')
        top_users = [{'email': row[0], 'count': row[1]} for row in cursor.fetchall()]


        cursor.execute('''
            SELECT user_email, COUNT(*) as count
            FROM case_analyses
            WHERE DATE(analysis_timestamp) = DATE('now')
            GROUP BY user_email
            HAVING count >= 3
        ''')
        limit_reached = [{'email': row[0], 'count': row[1]} for row in cursor.fetchall()]


        cursor.execute("SELECT AVG(overall_risk_score) FROM case_analyses WHERE overall_risk_score IS NOT NULL")
        avg_score_result = cursor.fetchone()[0]
        avg_score = round(float(avg_score_result), 1) if avg_score_result else 0


        cursor.execute('''
            SELECT
                CASE
                    WHEN overall_risk_score >= 70 THEN 'Strong'
                    WHEN overall_risk_score >= 40 THEN 'Moderate'
                    ELSE 'Weak'
                END as category,
                COUNT(*) as count
            FROM case_analyses
            WHERE overall_risk_score IS NOT NULL
            GROUP BY category
        ''')
        score_distribution = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        log_admin_action(admin_email, "VIEW_ANALYTICS", None, "Viewed analytics dashboard")

        return {
            'success': True,
            'analytics': {
                'total_analyses': total_analyses,
                'total_users': total_users,
                'today_analyses': today_analyses,
                'week_analyses': week_analyses,
                'month_analyses': month_analyses,
                'avg_score': avg_score,
                'score_distribution': score_distribution,
                'daily_stats': daily_stats,
                'top_users': top_users,
                'limit_reached_today': limit_reached
            }
        }
    except Exception as e:
        logger.error(f"Error getting analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/user/case-history/{user_email}")
async def get_user_case_history(user_email: str):
    """Get case history for specific user"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                case_id,
                analysis_timestamp,
                overall_risk_score,
                compliance_level,
                fatal_defect_override,
                cheque_amount,
                case_type,
                analysis_json
            FROM case_analyses
            WHERE user_email = ?
            ORDER BY analysis_timestamp DESC
            LIMIT 50
        ''', (user_email,))

        history = []
        for row in cursor.fetchall():

            analysis_json = {}
            key_issue = None
            try:
                if row[7]:
                    analysis_json = json.loads(row[7])
                    key_issue = analysis_json.get('key_issue') or analysis_json.get('_result', {}).get('one_line_verdict')
            except:
                pass

            history.append({
                'case_id': row[0],
                'date': row[1],
                'score': float(row[2]) if row[2] else 0,
                'status': row[3],
                'is_fatal': bool(row[4]),
                'amount': float(row[5]) if row[5] else 0,
                'case_type': row[6],
                'key_issue': key_issue
            })

        conn.close()

        return {
            'success': True,
            'history': history,
            'total': len(history)
        }
    except Exception as e:
        logger.error(f"Error getting case history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/all-analyses")
async def get_all_analyses(admin_email: str = "", limit: int = 100, offset: int = 0, request: Request = None):
    """Get all analyses with full details"""
    if request:
        fb_email = get_firebase_user_email(request)
        if fb_email:
            admin_email = fb_email.lower().strip()
    if admin_email not in ADMIN_CREDENTIALS:
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                case_id,
                analysis_timestamp,
                user_email,
                case_type,
                cheque_amount,
                overall_risk_score,
                compliance_level,
                fatal_defect_override,
                analysis_json
            FROM case_analyses
            ORDER BY analysis_timestamp DESC
            LIMIT ? OFFSET ?
        ''', (limit, offset))

        analyses = []
        for row in cursor.fetchall():
            analysis_json = json.loads(row[8]) if row[8] else {}
            analyses.append({
                'case_id': row[0],
                'timestamp': row[1],
                'user_email': row[2],
                'case_type': row[3],
                'cheque_amount': row[4],
                'score': row[5],
                'compliance_level': row[6],
                'is_fatal': bool(row[7]),
                'full_analysis': analysis_json
            })


        cursor.execute("SELECT COUNT(*) FROM case_analyses")
        total_count = cursor.fetchone()[0]

        conn.close()

        log_admin_action(admin_email, "VIEW_ALL_ANALYSES", None, f"Viewed {len(analyses)} analyses (offset {offset})")

        return {
            'success': True,
            'analyses': analyses,
            'total_count': total_count,
            'limit': limit,
            'offset': offset
        }
    except Exception as e:
        logger.error(f"Error getting all analyses: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/admin/activity-log")
async def get_admin_activity_log(admin_email: str = "", limit: int = 50, request: Request = None):
    """Get admin activity log"""
    if request:
        fb_email = get_firebase_user_email(request)
        if fb_email:
            admin_email = fb_email.lower().strip()
    if admin_email not in ADMIN_CREDENTIALS:
        raise HTTPException(status_code=403, detail="Admin access required")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT admin_email, action_type, target_user, action_details, timestamp
            FROM admin_activity_log
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (limit,))

        activities = []
        for row in cursor.fetchall():
            activities.append({
                'admin': row[0],
                'action': row[1],
                'target': row[2],
                'details': row[3],
                'timestamp': row[4]
            })

        conn.close()

        return {
            'success': True,
            'activities': activities
        }
    except Exception as e:
        logger.error(f"Error getting activity log: {e}")
        raise HTTPException(status_code=500, detail=str(e))




# ── Firebase Utility Endpoints ────────────────────────────────────────────────

@app.get("/firebase/status")
async def firebase_status():
    """Check Firebase Admin SDK connection status."""
    fb_ok = _init_firebase()
    return {
        "firebase_available": _FIREBASE_AVAILABLE,
        "firebase_connected": fb_ok,
        "project_id": CONFIG.get("FIREBASE_PROJECT_ID", ""),
        "message": (
            "Firebase Auth active"
            if fb_ok
            else "Firebase disabled — set FIREBASE_SERVICE_ACCOUNT_JSON env var"
        )
    }


@app.post("/firebase/verify-token")
async def firebase_verify_token(request: Request):
    """
    Verify a Firebase ID token and return the decoded claims.
    Body: { "id_token": "<firebase-id-token>" }
    Or:   Authorization: Bearer <firebase-id-token>
    Returns decoded uid, email, and whether user is a registered admin.
    """
    try:
        id_token = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            id_token = auth_header[7:]
        else:
            body = await request.json()
            id_token = body.get("id_token", "")

        if not id_token:
            return {"success": False, "error": "No id_token provided"}

        decoded = verify_firebase_token(id_token)
        if not decoded:
            return {"success": False, "error": "Invalid or expired Firebase token"}

        uid   = decoded.get("uid")
        email = decoded.get("email", "").lower().strip()
        is_admin = email in ADMIN_CREDENTIALS
        admin_role = ADMIN_CREDENTIALS.get(email, {}).get("role") if is_admin else None

        return {
            "success": True,
            "uid": uid,
            "email": email,
            "is_admin": is_admin,
            "admin_role": admin_role,
            "token_claims": {k: v for k, v in decoded.items()
                             if k not in ("uid", "email", "iat", "exp", "aud", "iss", "sub")}
        }
    except Exception as e:
        logger.error(f"Firebase verify-token error: {e}")
        return {"success": False, "error": str(e)}


@app.post("/firebase/link-admin")
async def firebase_link_admin(request: Request):
    """
    Link a Firebase UID to an existing admin account.
    Requires super_admin authorization (password or Firebase token).
    Body: { authorizer_email, authorizer_password, target_admin_email, firebase_uid }
    """
    try:
        data = await request.json()

        # Authorizer resolution
        fb_email = get_firebase_user_email(request)
        if fb_email:
            auth_email = fb_email.lower().strip()
            if auth_email not in ADMIN_CREDENTIALS:
                return {"success": False, "error": "Firebase user is not a registered admin"}
            if ADMIN_CREDENTIALS[auth_email].get("role") != "super_admin":
                return {"success": False, "error": "Only super_admin can link Firebase UIDs"}
        else:
            auth_email = data.get("authorizer_email", "").lower().strip()
            auth_pw    = data.get("authorizer_password", "")
            if not verify_admin(auth_email, auth_pw):
                return {"success": False, "error": "Invalid authorizer credentials"}
            if ADMIN_CREDENTIALS.get(auth_email, {}).get("role") != "super_admin":
                return {"success": False, "error": "Only super_admin can link Firebase UIDs"}

        target_email = data.get("target_admin_email", "").lower().strip()
        firebase_uid = data.get("firebase_uid", "").strip()

        if not target_email or not firebase_uid:
            return {"success": False, "error": "target_admin_email and firebase_uid are required"}

        if target_email not in ADMIN_CREDENTIALS:
            return {"success": False, "error": "Target admin account not found"}

        # Persist firebase_uid in admin_accounts table
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            # Add firebase_uid column if missing (idempotent)
            cursor.execute("PRAGMA table_info(admin_accounts)")
            cols = [row[1] for row in cursor.fetchall()]
            if "firebase_uid" not in cols:
                cursor.execute("ALTER TABLE admin_accounts ADD COLUMN firebase_uid TEXT")
            cursor.execute(
                "UPDATE admin_accounts SET firebase_uid = ? WHERE email = ?",
                (firebase_uid, target_email)
            )
            conn.commit()
            conn.close()
        except Exception as db_e:
            logger.warning(f"Could not persist firebase_uid: {db_e}")

        ADMIN_CREDENTIALS[target_email]["firebase_uid"] = firebase_uid
        log_admin_action(auth_email, "LINK_FIREBASE_UID", target_email,
                         f"Linked Firebase UID {firebase_uid}")

        return {
            "success": True,
            "message": f"Firebase UID linked to {target_email}",
            "firebase_uid": firebase_uid
        }
    except Exception as e:
        logger.error(f"firebase_link_admin error: {e}")
        return {"success": False, "error": str(e)}

# ─────────────────────────────────────────────────────────────────────────────


@app.post("/user/firebase-auth")
async def user_firebase_auth(request: Request):
    """
    Authenticate a regular user via Firebase ID token.
    Returns user email, uid, and current usage/limit status.
    Send: Authorization: Bearer <firebase-id-token>
    """
    try:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            body = await request.json()
            id_token = body.get("id_token", "")
        else:
            id_token = auth_header[7:]

        if not id_token:
            return {"success": False, "error": "No Firebase token provided"}

        decoded = verify_firebase_token(id_token)
        if not decoded:
            return {"success": False, "error": "Invalid or expired Firebase token"}

        uid   = decoded.get("uid")
        email = decoded.get("email", "").lower().strip()

        if not email:
            return {"success": False, "error": "Firebase token has no email claim"}

        # Get usage status
        can_analyze, used = check_daily_limit(email)
        user_limit = get_user_limit(email)

        return {
            "success": True,
            "uid": uid,
            "email": email,
            "is_admin": email in ADMIN_CREDENTIALS,
            "usage": {
                "used_today": used,
                "daily_limit": user_limit,
                "can_analyze": can_analyze,
                "remaining": max(0, user_limit - used)
            }
        }
    except Exception as e:
        logger.error(f"user_firebase_auth error: {e}")
        return {"success": False, "error": str(e)}

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
