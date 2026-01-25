"""
Module: config_schemas.py
Description:
    Defines the structured configuration models for Dagster assets.
    - Includes Enums for fixed choices (MetalLevel, CSRVariant).
    - Defines Dagster Config classes for type-safe resource and asset configuration.
"""
from enum import Enum
from dagster import Config


# -----------------------------------------------------------------------------
# Scoring Configuration
# -----------------------------------------------------------------------------

class InvalidGenderOption(str, Enum):
    skip = "skip"
    # Randomly assign M or F. Law of large numbers suggests ~50/50 split without state tracking.
    random = "random"
    error = "error"


class MetalLevel(str, Enum):
    platinum = "platinum"
    gold = "gold"
    silver = "silver"
    bronze = "bronze"
    catastrophic = "catastrophic"


class CSRVariant(str, Enum):
    none = "none"
    csr_73 = "csr_73"
    csr_87 = "csr_87"
    csr_94 = "csr_94"
    limited = "limited"


ModelYearOption = Enum(
    "ModelYearOption",
    {str(y): y for y in range(2021, 2026)},
    type=int,
)


class ScoringConfig(Config):
    # DIY tables year (controls coefficients/mappings/hierarchies/etc.).
    diy_model_year: ModelYearOption = ModelYearOption(2024)
    # Year used for DOB-based age calculation (age as-of 12/31 of this year).
    # Typically aligned with diy_model_year +/- 2 years.
    member_age_basis_year: str | None = None
    group_id: int | None = None
    group_description: str | None = None
    run_description: str = "ACA scoring run"
    trigger_source: str = "dagster"
    blueprint_id: str | None = None
    invalid_gender: InvalidGenderOption = InvalidGenderOption.skip
    metal_level: MetalLevel | None = None
    csr_variant: CSRVariant | None = None
    allow_telehealth: bool = True
    # Optional: override where scoring inputs come from
    claims_view: str | None = None
    enrollments_view: str | None = None
    members_view: str | None = None


# -----------------------------------------------------------------------------
# Comparison Configuration
# -----------------------------------------------------------------------------

class PopulationMode(str, Enum):
    INTERSECTION = "intersection"
    UNION = "union"
    A_ONLY = "a_only"
    B_ONLY = "b_only"


class MetricType(str, Enum):
    MEAN = "mean"
    SUM = "sum"


class ComparisonConfig(Config):
    run_ref_a: str
    run_ref_b: str
    run_description: str | None = None
    metric: MetricType = MetricType.MEAN
    population_mode: PopulationMode = PopulationMode.INTERSECTION
    group_id: int | None = None
    group_description: str | None = None


# -----------------------------------------------------------------------------
# Dashboard Configuration
# -----------------------------------------------------------------------------

class DashboardConfig(Config):
    run_ref: str
    run_description: str = "Dashboard Analysis"


class ComparisonDashboardConfig(Config):
    run_ref: str


# -----------------------------------------------------------------------------
# Decomposition Configuration
# -----------------------------------------------------------------------------

class DecompositionComponent(Config):
    name: str
    run_ref: str
    description: str | None = None
    population_mode: str | None = None


class DecompositionConfig(Config):
    baseline_run_ref: str
    actual_run_ref: str
    # Currently only 'intersection' is supported by logic, but schema permits string
    population_mode: str = "intersection"
    components: list[DecompositionComponent] = []

    # Common Run Metadata
    group_id: int | None = None
    group_description: str | None = None
    run_description: str = "Decomposition Analysis"
    trigger_source: str = "dagster"
    blueprint_id: str | None = None

