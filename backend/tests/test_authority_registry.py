import pytest
import os
import sys

# Ensure backend root is in sys.path
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from citation.citation_verifier import CitationVerifierEngine

def test_authority_object_structure_verified_judgment():
    auth = CitationVerifierEngine.verify_citation("United Bank of India v. Satyawati Tondon (2010)")
    assert auth["status"] in ["VERIFIED", "DISTINGUISHABLE"]
    assert "source" in auth
    assert auth["source"]["official_url"] is not None
    assert auth["source"]["integrity_status"] == "PENDING_LIVE_HTTP_RETRIEVAL"
    assert auth["verification"]["primary_source_verified"] is True
    assert auth["verification"]["document_integrity_verified"] is False

def test_authority_object_rbi_master_circular():
    auth = CitationVerifierEngine.verify_citation("RBI IRAC Master Circular 2025")
    assert auth["type"] == "RBI_DIRECTION"
    assert auth["source"]["source_domain"] == "rbi.org.in"
    assert auth["source"]["official_url"] == "https://rbi.org.in/scripts/NotificationUser.aspx?Id=12822"
    assert auth["source"]["integrity_status"] == "PENDING_LIVE_HTTP_RETRIEVAL"
    assert auth["verification"]["primary_source_verified"] is True
    assert auth["verification"]["proposition_mapped"] is True
    assert auth["verification"]["proposition_verified"] is False

def test_authority_object_superseded_trap():
    auth = CitationVerifierEngine.verify_citation("Mardia Chemicals 2004 pre-deposit rule for Sec 17")
    assert auth["status"] == "SUPERSEDED"
    assert auth["treatment"] == "SUPERSEDED"
    assert auth["verification"]["proposition_mapped"] is False
    assert auth["verification"]["current_treatment_checked"] is True

def test_authority_object_unknown_citation():
    auth = CitationVerifierEngine.verify_citation("Fake Case v. Unknown Bank AIR 2099 SC 9999")
    assert auth["status"] == "UNKNOWN"
    assert auth["source"]["official_url"] is None
    assert auth["source"]["document_hash"] is None
    assert auth["verification"]["primary_source_verified"] is False
    assert auth["verification"]["document_integrity_verified"] is False

def test_primary_source_retriever_interface():
    from citation.source_retriever import PrimarySourceRetriever
    res = PrimarySourceRetriever.fetch_and_verify_primary_source("invalid_url")
    assert res["success"] is False
    assert res["document_integrity_verified"] is False
    assert res["document_hash"] is None

def test_production_readiness_endpoints_and_headers():
    from fastapi.testclient import TestClient
    from main import app
    client = TestClient(app)

    # Health, Readiness, and Liveness probes
    res_health = client.get("/health")
    assert res_health.status_code == 200
    assert res_health.json()["status"] == "healthy"

    res_ready = client.get("/ready")
    assert res_ready.status_code == 200
    assert res_ready.json()["status"] == "ready"

    res_live = client.get("/live")
    assert res_live.status_code == 200
    assert res_live.json()["status"] == "alive"

    # Enterprise security headers
    headers = res_live.headers
    assert headers.get("x-content-type-options") == "nosniff"
    assert headers.get("x-frame-options") == "SAMEORIGIN"
    assert headers.get("referrer-policy") == "strict-origin-when-cross-origin"
    assert headers.get("x-xss-protection") == "1; mode=block"

    # Protected precedent ingestion route (rejects unauthenticated)
    res_ingest = client.post("/api/v1/ingest/precedents", json={"domain": "ni_act", "citation": "TEST"})
    assert res_ingest.status_code in (401, 403)
