"""Data models for ACA risk calculator."""

from datetime import date

from pydantic import BaseModel, Field


class ScoreComponent(BaseModel):
    """Individual score component for audit trail.
    
    Attributes:
        component_type: Type of component ('demographic', 'hcc', 'rxc', 'hcc_group')
        component_code: Variable name (e.g., 'MAGE_LAST_55_59', 'HHS_HCC019', 'RXC_01', 'G01')
        coefficient: Coefficient value for this component
        source_data: Source ICD-10 codes (for HCCs) or NDC codes (for RXCs) that triggered this component
        superseded_by: If this component was zeroed out by hierarchy, the dominant component code
        supersedes: List of component codes that this component superseded via hierarchy
        grouped_into: If this HCC was grouped (e.g., into 'G01'), the group variable name
        table_references: Dictionary tracking which DIY tables were used
    """
    
    component_type: str = Field(..., pattern="^(demographic|hcc|rxc|hcc_group)$")
    component_code: str
    coefficient: float
    source_data: list[str] = Field(default_factory=list)
    superseded_by: str | None = None
    supersedes: list[str] = Field(default_factory=list)
    grouped_into: str | None = None
    table_references: dict = Field(default_factory=dict)


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
        components: List of individual score components for audit trail
    """

    member_id: str
    risk_score: float
    hcc_list: list[str]
    details: dict = Field(default_factory=dict)
    components: list[ScoreComponent] = Field(default_factory=list)
