import re
from fastapi import APIRouter, HTTPException

from .models import SLAGenerateRequest, SLAEsignRequest
from .service import generate_and_store, send_esign_request
from .config import MONTHS

router = APIRouter()


@router.post(
    "/generate",
    summary="Generate and store a filled SLA Word document",
    description="""
Accepts contract details as JSON, generates the filled `.docx`, uploads it to
Supabase Storage, records it in `sla_documents`, and returns a JSON response
with a signed download URL.

**Required fields:** `company_name`, `company_address`, `contract_day/month/year`,
`num_beneficiaries`, `premium_naira`, `premium_words`, `plans` (1+),
`start_day/month/year`, `end_day/month/year`.

**Response:** `{ id, company_name, download_url, storage_path }`
    """,
)
def generate_sla(req: SLAGenerateRequest, generated_by: str = ""):
    # Validate month names
    for field, val in [
        ("contract_month", req.contract_month),
        ("start_month",    req.start_month),
        ("end_month",      req.end_month),
    ]:
        if val not in MONTHS:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid {field} '{val}'. Must be a full month name e.g. 'March'.",
            )

    try:
        result = generate_and_store(req, generated_by=generated_by)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Document generation failed: {exc}")

    return result


@router.post(
    "/send-esign",
    summary="Generate SLA and send for e-signature via Dropbox Sign",
    description="""
Generates the SLA document, uploads it to Supabase Storage, dispatches a
Dropbox Sign multi-signer request, and records everything in `sla_documents`.

**Response:** `{ id, company_name, download_url, storage_path, signature_request_id, test_mode }`
    """,
)
def send_esign(req: SLAEsignRequest):
    for field, val in [
        ("contract_month", req.contract_month),
        ("start_month",    req.start_month),
        ("end_month",      req.end_month),
    ]:
        if val not in MONTHS:
            raise HTTPException(
                status_code=422,
                detail=f"Invalid {field} '{val}'. Must be a full month name e.g. 'March'.",
            )

    try:
        result = send_esign_request(req, generated_by=req.generated_by)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"E-sign request failed: {exc}")

    return result
