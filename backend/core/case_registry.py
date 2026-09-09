import logging
from typing import Dict, Type, Optional, List
from core.base_domain_engine import BaseDomainEngine

logger = logging.getLogger(__name__)

class CaseRegistry:
    """
    Central Registry pattern managing domain engine registrations and routing.
    Eliminates ad-hoc if/elif branching in central orchestrators.
    """

    def __init__(self):
        self._engines: Dict[str, BaseDomainEngine] = {}

    def register(self, domain_key: str, engine_instance: BaseDomainEngine) -> None:
        key = domain_key.lower().strip()
        self._engines[key] = engine_instance
        logger.info(f"[CASE_REGISTRY] Registered domain engine '{engine_instance.domain_name}' under key '{key}'.")

    def get(self, case_type: str) -> Optional[BaseDomainEngine]:
        key = (case_type or "").lower().strip()
        if key in ("sarfaesi", "drt", "securitisation", "criminal", "ipc", "bns", "crpc", "bnss", "civil", "cpc", "commercial", "composite", "multi_track", "multitrack", "composite_recovery", "unified_npa", "unified", "all"):
            logger.warning(f"[CASE_REGISTRY] Domain engine for '{key}' is currently disabled. Falling back to Section 138 Cheque Bounce engine.")
            return self._engines.get("cheque_bounce")
        if key in self._engines:
            return self._engines[key]
        # Fallback to Section 138 / Cheque Bounce
        return self._engines.get("cheque_bounce")

    def list_registered_domains(self) -> List[str]:
        return list(self._engines.keys())

case_registry = CaseRegistry()

# Initialize built-in domain engines: ONLY Section 138 Cheque Bounce is active
try:
    from cheque_bounce.cheque_bounce_engine import ChequeBounceEngine
    case_registry.register("cheque_bounce", ChequeBounceEngine())
except Exception as _e:
    logger.warning(f"Could not auto-register ChequeBounceEngine: {_e}")

# Note: Criminal, SARFAESI, Civil, and Composite engines are intentionally disabled in this release.
# Only Section 138 NI Act litigation engine is enabled.


