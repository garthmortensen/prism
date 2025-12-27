"""Data models for ACA risk calculator."""

from datetime import date

from pydantic import BaseModel, Field


class MemberInput(BaseModel):
    """Input data for a single member.

    Attributes:
        member_id: Unique identifier for the member
        date_of_birth: Member's date of birth
        gender: 'M' for male, 'F' for female
        metal_level: Plan metal level (platinum, gold, silver, bronze, catastrophic)
        diagnoses: List of ICD-10-CM diagnosis codes
        enrollment_months: Number of months enrolled in benefit year (1-12)
    """

    member_id: str
    date_of_birth: date
    gender: str = Field(pattern="^[MF]$")
    metal_level: str = Field(default="silver")
    diagnoses: list[str] = Field(default_factory=list)
    ndc_codes: list[str] = Field(default_factory=list)
    enrollment_months: int = Field(default=12, ge=1, le=12)


class ScoreOutput(BaseModel):
    """Output from risk score calculation.

    Attributes:
        member_id: Member identifier (from input)
        risk_score: Calculated risk score
        hcc_list: List of HCCs after hierarchy application
        details: Dictionary with calculation details
    """

    member_id: str
    risk_score: float
    hcc_list: list[str]
    details: dict = Field(default_factory=dict)
