from fastapi import APIRouter
import analysis, caseroom, verification, documents, cases, telemetry
from security import SecurityManager
import uuid
api_router = APIRouter()
@api_router.post("/auth/anonymous", tags=["Authentication"])
def create_anonymous_session():
    user_id = f"ANON_{uuid.uuid4().hex[:12]}"
    token = SecurityManager.create_access_token(data={"sub": user_id})
    return {"access_token": token, "token_type": "bearer", "user_id": user_id}
from admin_router import router as admin_control_router, user_quota_router
from counsel_router import router as counsel_router
from analytics_router import router as analytics_router
from client_portal_router import router as client_portal_router
from deadline_router import router as deadline_router
from case_manager import router as case_manager_router
from client_manager import router as client_manager_router
from document_manager import router as document_manager_router
from draft_workflow import router as draft_workflow_router
from team_manager import router as team_manager_router
from communication import router as communication_router

api_router.include_router(analysis.router, prefix="/analyze", tags=["Section 138 Analysis"])
api_router.include_router(counsel_router, prefix="/intel/counsel", tags=["Opposing Counsel Intel"])
api_router.include_router(analytics_router, prefix="/analytics", tags=["Analytics Dashboard"])
api_router.include_router(client_portal_router, prefix="/portal", tags=["Client Portal"])
api_router.include_router(deadline_router, prefix="/deadlines", tags=["Deadline Tracker"])
api_router.include_router(caseroom.router, prefix="/caseroom", tags=["Caseroom"])
api_router.include_router(verification.router, prefix="/verify", tags=["Verification"])
api_router.include_router(documents.router, prefix="/documents", tags=["Documents"])
api_router.include_router(cases.router, prefix="/cases", tags=["Cases"])
api_router.include_router(telemetry.router, prefix="/telemetry", tags=["Telemetry"])
api_router.include_router(admin_control_router, prefix="/admin", tags=["Admin Control"])
api_router.include_router(user_quota_router, prefix="/user", tags=["User Quota"])

# ── CMS Routers ──────────────────────────────────────────────
api_router.include_router(case_manager_router, prefix="/cms", tags=["Case Management"])
api_router.include_router(client_manager_router, prefix="/cms", tags=["Client Management"])
api_router.include_router(document_manager_router, prefix="/cms", tags=["Document Management"])
api_router.include_router(draft_workflow_router, prefix="/cms", tags=["Draft Workflow"])
api_router.include_router(team_manager_router, prefix="/cms", tags=["Team Management"])
api_router.include_router(communication_router, prefix="/cms", tags=["Communication & Audit"])

from knowledge_pipeline import PrecedentIngestionPayload, PrecedentIngestionService
from fastapi import Body, Depends
from security import require_admin

@api_router.post("/ingest/precedents", tags=["Knowledge Pipeline"])
def ingest_precedent_endpoint(
    payload: PrecedentIngestionPayload = Body(...),
    admin_user: str = Depends(require_admin)
):
    return PrecedentIngestionService.ingest_precedent(payload)

