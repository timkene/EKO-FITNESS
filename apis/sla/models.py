from pydantic import BaseModel, Field
from typing import List


class PlanItem(BaseModel):
    plan_type:   str = Field(..., description="e.g. 'Gold', 'Silver'")
    description: str = Field("",  description="Plan description / benefit details")
    num_lives:   str = Field(..., description="Number of lives on this plan, e.g. '50'")
    amount:      str = Field(..., description="Annual premium for this plan, e.g. '3000000'")


class SLAGenerateRequest(BaseModel):
    # Client
    company_name:    str = Field(..., description="Full legal company name (will be uppercased)")
    company_address: str = Field(..., description="Registered office address")

    # Agreement date (cover page + opening paragraph)
    contract_day:   int = Field(..., ge=1, le=31)
    contract_month: str = Field(..., description="Full month name, e.g. 'March'")
    contract_year:  str = Field(..., description="4-digit year, e.g. '2026'")

    # Premium
    num_beneficiaries: str = Field(..., description="Total lives covered, e.g. '150'")
    premium_naira:     str = Field(..., description="Premium amount, e.g. '6,000,000'")
    premium_words:     str = Field(..., description="e.g. 'Six Million Naira Only'")

    # Plans table
    plans: List[PlanItem] = Field(..., min_length=1)

    # Period of cover
    start_day:   int = Field(..., ge=1, le=31)
    start_month: str
    start_year:  str
    end_day:     int = Field(..., ge=1, le=31)
    end_month:   str
    end_year:    str


class SLAEsignRequest(SLAGenerateRequest):
    director_name:    str   = Field(..., description="Name of the director signer")
    director_email:   str   = Field(..., description="Email of the director signer")
    legal_head_name:  str   = Field(..., description="Name of the legal head signer")
    legal_head_email: str   = Field(..., description="Email of the legal head signer")
    hr_email:         str   = Field("",  description="HR email (receives a CC copy)")
    client_name:      str   = Field("",  description="Client contact name (optional)")
    client_email:     str   = Field("",  description="Client contact email (optional)")
    test_mode:        bool  = Field(True, description="Dropbox Sign test mode — not legally binding")
    generated_by:     str   = Field("",  description="Staff member who triggered this")
