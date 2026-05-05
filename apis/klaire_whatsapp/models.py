from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime


class TermiiInbound(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    text: str
    type: str = "incoming"


class EnrolleeIdentity(BaseModel):
    memberid: str
    legacycode: str
    firstname: str
    lastname: str
    genderid: int
    dateofbirth: Optional[str] = None


class AftercarContext(BaseModel):
    panumber: str
    diagnosis: str
    drugs: List[str]
    procedures: List[str]
    hospital: str
    turn: int = 1


class FeedbackEntry(BaseModel):
    enrollee_id: str
    panumber: str
    hospital: str
    rating: Optional[int] = None
    comment: Optional[str] = None
    adherence_flag: bool = False
    escalated: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)


class OutreachEntry(BaseModel):
    enrollee_id: str
    panumber: str
    contacted_at: datetime = Field(default_factory=datetime.utcnow)
    responded: bool = False
