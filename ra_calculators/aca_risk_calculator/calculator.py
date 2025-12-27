"""ACA HHS-HCC Risk Score Calculator.

This module implements the main calculator class that:
1. Determines the appropriate model (Adult/Child/Infant) based on age
2. Maps ICD-10 diagnoses to Condition Categories (CCs)
3. Applies hierarchies to get final HCCs
4. Applies HCC groupings where applicable
5. Sums demographic factor + HCC coefficients

The calculator loads official CMS DIY tables from:
    diy_tables/cy202*_diy_tables/
"""

from datetime import date

from ra_calculators.aca_risk_calculator.hierarchies import apply_hierarchies
from ra_calculators.aca_risk_calculator.models import MemberInput, ScoreOutput
from ra_calculators.aca_risk_calculator.table_loader import (
    load_coefficients,
    load_hcc_groups,
    load_icd_to_cc,
    load_model_exclusions,
)


class ACACalculator:
    """HHS-HCC Risk Score Calculator for ACA markets.

    Implements the basic risk scoring algorithm:
    1. Determine model type (Adult/Child/Infant) from age
    2. Calculate demographic factor from age/sex
    3. Map ICD-10 diagnoses to Condition Categories (CCs)
    4. Apply hierarchies to get final HCCs
    5. Apply HCC groupings
    6. Sum demographic factor + HCC/group coefficients

    Example:
        >>> from datetime import date
        >>> calculator = ACACalculator(model_year="2024")
        >>> member = MemberInput(
        ...     member_id="M001",
        ...     date_of_birth=date(1965, 3, 15),
        ...     gender="M",
        ...     metal_level="silver",
        ...     diagnoses=["E1165", "I10", "F329"],
        ... )
        >>> result = calculator.score(member)
        >>> print(f"Risk Score: {result.risk_score:.3f}")
    """

    def __init__(self, model_year: str = "2024"):
        """Initialize calculator with model year.

        Args:
            model_year: Model year (e.g., "2024"). Must have corresponding
                DIY tables in diy_tables/cy{model_year}_diy_tables/
        """
        self.model_year = model_year

        # Load tables (cached after first load)
        self._icd_to_cc = load_icd_to_cc(model_year)
        self._coefficients = load_coefficients(model_year)
        self._exclusions = load_model_exclusions(model_year)

    def _calculate_age(self, dob: date, as_of: date | None = None) -> int:
        """Calculate age in years as of a given date.

        Args:
            dob: Date of birth
            as_of: Date to calculate age as of (defaults to today)

        Returns:
            Age in completed years
        """
        as_of = as_of or date.today()
        age = as_of.year - dob.year
        if (as_of.month, as_of.day) < (dob.month, dob.day):
            age -= 1
        return max(0, age)

    def _get_model_type(self, age: int) -> str:
        """Determine model type from age.

        Args:
            age: Age in years

        Returns:
            "Adult", "Child", or "Infant"
        """
        if age >= 21:
            return "Adult"
        elif age >= 2:
            return "Child"
        else:
            return "Infant"

    def _get_demographic_variable(self, model: str, gender: str, age: int) -> str:
        """Get the demographic variable name for coefficient lookup.

        Args:
            model: Model type ("Adult", "Child", "Infant")
            gender: "M" or "F"
            age: Age in years

        Returns:
            Variable name (e.g., "MAGE_LAST_55_59")
        """
        prefix = "M" if gender == "M" else "F"

        if model == "Adult":
            if age <= 24:
                return f"{prefix}AGE_LAST_21_24"
            elif age <= 29:
                return f"{prefix}AGE_LAST_25_29"
            elif age <= 34:
                return f"{prefix}AGE_LAST_30_34"
            elif age <= 39:
                return f"{prefix}AGE_LAST_35_39"
            elif age <= 44:
                return f"{prefix}AGE_LAST_40_44"
            elif age <= 49:
                return f"{prefix}AGE_LAST_45_49"
            elif age <= 54:
                return f"{prefix}AGE_LAST_50_54"
            elif age <= 59:
                return f"{prefix}AGE_LAST_55_59"
            else:
                return f"{prefix}AGE_LAST_60_GT"

        elif model == "Child":
            if age <= 4:
                return f"{prefix}AGE_LAST_2_4"
            elif age <= 9:
                return f"{prefix}AGE_LAST_5_9"
            elif age <= 14:
                return f"{prefix}AGE_LAST_10_14"
            else:
                return f"{prefix}AGE_LAST_15_20"

        else:  # Infant
            # Infant model has different structure - simplified here
            return f"{prefix}AGE_LAST_0_1"

    def _get_coefficient(
        self,
        model: str,
        variable: str,
        metal_level: str,
    ) -> float:
        """Look up coefficient for a variable.

        Args:
            model: Model type ("Adult", "Child", "Infant")
            variable: Variable name (e.g., "HHS_HCC001", "MAGE_LAST_55_59")
            metal_level: Metal level (platinum, gold, silver, bronze, catastrophic)

        Returns:
            Coefficient value, or 0.0 if not found
        """
        key = (model, variable)
        if key not in self._coefficients:
            return 0.0

        metal = metal_level.lower()
        return self._coefficients[key].get(metal, 0.0)

    def _map_diagnoses_to_ccs(self, diagnoses: list[str]) -> set[str]:
        """Map ICD-10 codes to Condition Categories.

        Args:
            diagnoses: List of ICD-10-CM diagnosis codes

        Returns:
            Set of CC numbers (as strings, e.g., "19", "35_1")
        """
        ccs: set[str] = set()

        for dx in diagnoses:
            # Normalize: remove dots, uppercase
            dx_clean = dx.replace(".", "").upper()

            # Try exact match first
            if dx_clean in self._icd_to_cc:
                ccs.update(self._icd_to_cc[dx_clean])
                continue

            # Try progressively shorter prefixes (handle code variations)
            for length in [6, 5, 4, 3]:
                prefix = dx_clean[:length]
                if prefix in self._icd_to_cc:
                    ccs.update(self._icd_to_cc[prefix])
                    break

        return ccs

    def _apply_groupings(
        self,
        hccs: set[str],
        model: str,
    ) -> tuple[set[str], set[str]]:
        """Apply HCC groupings for the model.

        Some HCCs are grouped together (e.g., G01 = HCC 19, 20, 21).
        When any HCC in a group is present, the group variable is set
        and individual HCCs are zeroed.

        Args:
            hccs: Set of HCCs after hierarchy application
            model: Model type ("Adult", "Child")

        Returns:
            Tuple of (remaining HCCs, group variables triggered)
        """
        if model == "Infant":
            # Infant model uses severity levels, not groups
            return hccs, set()

        groups = load_hcc_groups(self.model_year, model)
        remaining_hccs = hccs.copy()
        triggered_groups: set[str] = set()

        for group_var, group_hccs in groups.items():
            # Check if any HCC in this group is present
            for hcc in group_hccs:
                if hcc in remaining_hccs:
                    triggered_groups.add(group_var)
                    # Remove all HCCs in this group from individual scoring
                    for h in group_hccs:
                        remaining_hccs.discard(h)
                    break

        return remaining_hccs, triggered_groups

    def _exclude_model_hccs(self, hccs: set[str], model: str) -> set[str]:
        """Remove HCCs that are excluded from this model.

        Args:
            hccs: Set of HCCs
            model: Model type

        Returns:
            HCCs with model-specific exclusions removed
        """
        excluded = self._exclusions.get(model, set())
        return hccs - excluded

    def score(self, member: MemberInput, prediction_year: int | None = None) -> ScoreOutput:
        """Calculate risk score for a single member.

        Args:
            member: Member input data
            prediction_year: Year to calculate age as of (defaults to model_year).
                           Use this to simulate scores for different years.

        Returns:
            ScoreOutput with risk score and calculation details
        """
        # Step 1: Determine model type from age
        # HHS uses age as of the last day of the benefit year
        target_year = prediction_year if prediction_year is not None else int(self.model_year)
        benefit_year_end = date(target_year, 12, 31)

        age = self._calculate_age(member.date_of_birth, as_of=benefit_year_end)
        model = self._get_model_type(age)

        # Step 2: Get demographic factor
        demo_var = self._get_demographic_variable(model, member.gender, age)
        demographic_factor = self._get_coefficient(model, demo_var, member.metal_level)

        # Step 3: Map diagnoses to CCs
        raw_ccs = self._map_diagnoses_to_ccs(member.diagnoses)

        # Step 4: Apply hierarchies
        hccs_after_hierarchy = apply_hierarchies(raw_ccs, self.model_year)

        # Step 5: Remove model-excluded HCCs
        hccs_filtered = self._exclude_model_hccs(hccs_after_hierarchy, model)

        # Step 6: Apply groupings
        remaining_hccs, triggered_groups = self._apply_groupings(hccs_filtered, model)

        # Step 7: Calculate HCC score
        hcc_score = 0.0
        hcc_details: dict[str, float] = {}

        # Score individual HCCs
        for hcc in remaining_hccs:
            var_name = f"HHS_HCC{hcc.zfill(3)}" if hcc.isdigit() else f"HHS_HCC{hcc}"
            coef = self._get_coefficient(model, var_name, member.metal_level)
            if coef != 0.0:
                hcc_score += coef
                hcc_details[var_name] = coef

        # Score group variables
        for group in triggered_groups:
            coef = self._get_coefficient(model, group, member.metal_level)
            if coef != 0.0:
                hcc_score += coef
                hcc_details[group] = coef

        # Step 8: Calculate total score
        total_score = demographic_factor + hcc_score

        # Build final HCC list (includes groups for transparency)
        final_hccs = sorted(remaining_hccs) + sorted(triggered_groups)

        return ScoreOutput(
            member_id=member.member_id,
            risk_score=round(total_score, 4),
            hcc_list=final_hccs,
            details={
                "model": model,
                "age": age,
                "gender": member.gender,
                "metal_level": member.metal_level,
                "demographic_variable": demo_var,
                "demographic_factor": round(demographic_factor, 4),
                "raw_ccs": sorted(raw_ccs),
                "hccs_after_hierarchy": sorted(hccs_after_hierarchy),
                "hccs_filtered": sorted(hccs_filtered),
                "remaining_hccs": sorted(remaining_hccs),
                "triggered_groups": sorted(triggered_groups),
                "hcc_coefficients": hcc_details,
                "hcc_score": round(hcc_score, 4),
                "model_year": self.model_year,
            },
        )

    def score_batch(
        self, members: list[MemberInput], prediction_year: int | None = None
    ) -> list[ScoreOutput]:
        """Calculate risk scores for multiple members.

        Args:
            members: List of member inputs
            prediction_year: Optional year override for age calculation

        Returns:
            List of score outputs in same order as inputs
        """
        return [self.score(member, prediction_year) for member in members]
