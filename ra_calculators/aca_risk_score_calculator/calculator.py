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

from ra_calculators.aca_risk_score_calculator.hierarchies import apply_hierarchies
from ra_calculators.aca_risk_score_calculator.models import MemberInput, ScoreComponent, ScoreOutput
from ra_calculators.aca_risk_score_calculator.table_loader import (
    load_coefficients,
    load_hcc_groups,
    load_hcc_labels,
    load_icd_to_cc,
    load_model_exclusions,
    load_ndc_to_rxc,
    load_rxc_hierarchies,
    load_rxc_labels,
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
        self._ndc_to_rxc = load_ndc_to_rxc(model_year)
        self._rxc_hierarchies = load_rxc_hierarchies(model_year)
        self._hcc_labels = load_hcc_labels(model_year)
        self._rxc_labels = load_rxc_labels(model_year)

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

        # Normalize CCs to match hierarchy and coefficient table formats
        # e.g., "21.0" -> "21", "35.1" -> "35_1"
        normalized_ccs: set[str] = set()
        for cc in ccs:
            # Remove .0 suffix
            if cc.endswith(".0"):
                cc = cc[:-2]
            # Replace remaining dots with underscores
            cc = cc.replace(".", "_")
            normalized_ccs.add(cc)

        return normalized_ccs

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

    def _map_ndcs_to_rxcs(self, ndc_codes: list[str]) -> set[str]:
        """Map NDC codes to RXCs.

        Args:
            ndc_codes: List of NDC codes

        Returns:
            Set of RXC numbers (as strings)
        """
        rxcs: set[str] = set()

        for ndc in ndc_codes:
            # Normalize: remove dashes, ensure 11 digits if possible?
            # For now, just strip whitespace
            ndc_clean = ndc.replace("-", "").strip()

            if ndc_clean in self._ndc_to_rxc:
                rxcs.update(self._ndc_to_rxc[ndc_clean])
            # Try padding to 11 digits if it's numeric and shorter (e.g. integer input)
            # DIY tables use 11-digit NDCs (e.g., 00003196401). If input data stores NDCs
            # as integers or strings without leading zeros (e.g., 3196401), they wouldn't match
            elif ndc_clean.isdigit() and len(ndc_clean) < 11:
                padded = ndc_clean.zfill(11)
                if padded in self._ndc_to_rxc:
                    rxcs.update(self._ndc_to_rxc[padded])

        return rxcs

    def _apply_rxc_hierarchies(self, rxcs: set[str]) -> set[str]:
        """Apply RXC hierarchies.

        Args:
            rxcs: Set of RXCs

        Returns:
            Set of RXCs after hierarchy application
        """
        final_rxcs = rxcs.copy()
        for dominant, superseded_list in self._rxc_hierarchies.items():
            if dominant in final_rxcs:
                for superseded in superseded_list:
                    final_rxcs.discard(superseded)
        return final_rxcs

    def _find_source_icds_for_hcc(self, hcc: str, diagnoses: list[str]) -> list[str]:
        """Find which ICD codes contributed to a specific HCC.

        Args:
            hcc: HCC code (e.g., "19", "35_1")
            diagnoses: List of ICD-10 diagnosis codes

        Returns:
            List of ICD codes that mapped to this HCC
        """
        source_icds = []
        for dx in diagnoses:
            dx_ccs = self._map_diagnoses_to_ccs([dx])
            if hcc in dx_ccs:
                source_icds.append(dx)
        return source_icds

    def _find_source_ndcs_for_rxc(self, rxc: str, ndc_codes: list[str]) -> list[str]:
        """Find which NDC codes contributed to a specific RXC.

        Args:
            rxc: RXC code (e.g., "1", "01")
            ndc_codes: List of NDC codes

        Returns:
            List of NDC codes that mapped to this RXC
        """
        source_ndcs = []
        for ndc in ndc_codes:
            ndc_rxcs = self._map_ndcs_to_rxcs([ndc])
            if rxc in ndc_rxcs:
                source_ndcs.append(ndc)
        return source_ndcs

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

        # Initialize components list for audit trail
        components: list[ScoreComponent] = []

        # Track demographic component
        components.append(
            ScoreComponent(
                component_type="demographic",
                component_code=demo_var,
                coefficient=demographic_factor,
                source_data=[f"age={age}", f"gender={member.gender}"],
                table_references={
                    "table": "table_9",
                    "model": model,
                    "metal_level": member.metal_level,
                },
            )
        )

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

        # Track which HCCs were superseded by hierarchy
        # superseded_hccs = set(raw_ccs) - set(hccs_after_hierarchy)
        # Track which HCCs were filtered by model exclusions
        # model_excluded_hccs = set(hccs_after_hierarchy) - set(hccs_filtered)
        # Track which HCCs were grouped
        grouped_hccs = set(hccs_filtered) - set(remaining_hccs)

        # Score individual HCCs
        for hcc in remaining_hccs:
            if hcc.isdigit():
                var_name = f"HHS_HCC{hcc.zfill(3)}"
            else:
                # Handle cases like "35_1" -> "HHS_HCC035_1"
                parts = hcc.split("_")
                if len(parts) == 2 and parts[0].isdigit():
                    var_name = f"HHS_HCC{parts[0].zfill(3)}_{parts[1]}"
                else:
                    var_name = f"HHS_HCC{hcc}"

            coef = self._get_coefficient(model, var_name, member.metal_level)
            if coef != 0.0:
                hcc_score += coef
                hcc_details[var_name] = coef

                # Track HCC component for audit trail
                source_icds = self._find_source_icds_for_hcc(hcc, member.diagnoses)
                components.append(
                    ScoreComponent(
                        component_type="hcc",
                        component_code=var_name,
                        description=self._hcc_labels.get(hcc),
                        coefficient=coef,
                        source_data=source_icds,
                        table_references={
                            "icd_mapping": "table_3",
                            "hierarchy": "table_4",
                            "coefficient": "table_9",
                            "model": model,
                            "metal_level": member.metal_level,
                        },
                    )
                )

        # Score group variables
        for group in triggered_groups:
            coef = self._get_coefficient(model, group, member.metal_level)
            if coef != 0.0:
                hcc_score += coef
                hcc_details[group] = coef

                # Find which HCCs were grouped into this group variable
                from ra_calculators.aca_risk_score_calculator.table_loader import load_hcc_groups

                groups_map = load_hcc_groups(self.model_year, model)
                group_member_hccs = [
                    hcc for hcc in groups_map.get(group, []) if hcc in grouped_hccs
                ]

                # Track group component for audit trail
                components.append(
                    ScoreComponent(
                        component_type="hcc_group",
                        component_code=group,
                        coefficient=coef,
                        source_data=group_member_hccs,
                        table_references={
                            "grouping": f"table_{'6' if model == 'Adult' else '7'}",
                            "coefficient": "table_9",
                            "model": model,
                            "metal_level": member.metal_level,
                        },
                    )
                )

        # Step 7b: Adult Enrollment Duration Factor (EDF)
        # Per HHS/HCC EDF logic: if ENROLDURATION = N and HCC_CNT > 0 then HCC_EDN = 1
        # Here we define HCC_CNT as the count of payment HCCs (post-hierarchy/exclusions/grouping)
        # plus any triggered group variables.
        edf_var: str | None = None
        edf_factor = 0.0

        hcc_cnt = 0
        # Count final payment HCCs (post-hierarchy, post-exclusions, post-grouping)
        hcc_cnt += len(remaining_hccs)
        # Count group variables G*
        hcc_cnt += len(triggered_groups)

        if model == "Adult":
            enrollment_months = int(getattr(member, "enrollment_months", 12) or 12)
            # 2024 DIY table_9 includes HCC_ED1..HCC_ED6 for Adult EDF.
            if 1 <= enrollment_months <= 6 and hcc_cnt > 0:
                edf_var = f"HCC_ED{enrollment_months}"
                edf_factor = self._get_coefficient(model, edf_var, member.metal_level)
                if edf_factor != 0.0:
                    hcc_score += edf_factor
                    hcc_details[edf_var] = edf_factor
                    components.append(
                        ScoreComponent(
                            component_type="edf",
                            component_code=edf_var,
                            description=(
                                f"Enrollment Duration {enrollment_months} months, at least one HCC"
                            ),
                            coefficient=edf_factor,
                            source_data=[
                                f"enrollment_months={enrollment_months}",
                                f"hcc_cnt={hcc_cnt}",
                            ],
                            table_references={
                                "coefficient": "table_9",
                                "model": model,
                                "metal_level": member.metal_level,
                            },
                        )
                    )

        # Step 8: RXC Scoring
        raw_rxcs = self._map_ndcs_to_rxcs(member.ndc_codes)
        rxcs_after_hierarchy = self._apply_rxc_hierarchies(raw_rxcs)

        # Track which RXCs were superseded by hierarchy
        # superseded_rxcs = raw_rxcs - rxcs_after_hierarchy

        rxc_score = 0.0
        rxc_details: dict[str, float] = {}

        for rxc in rxcs_after_hierarchy:
            # Try different variable name formats
            # RXC codes are typically 2 digits (e.g., "01", "02")
            candidates = [
                f"RXC_{rxc.zfill(2)}",
                f"RXC_{rxc.zfill(3)}",
                f"RXC_{rxc}",
                rxc,
            ]

            coef = 0.0
            var_name = ""
            for candidate in candidates:
                c = self._get_coefficient(model, candidate, member.metal_level)
                if c != 0.0:
                    coef = c
                    var_name = candidate
                    break

            if coef != 0.0:
                rxc_score += coef
                rxc_details[var_name] = coef

                # Track RXC component for audit trail
                source_ndcs = self._find_source_ndcs_for_rxc(rxc, member.ndc_codes)
                components.append(
                    ScoreComponent(
                        component_type="rxc",
                        component_code=var_name,
                        description=self._rxc_labels.get(rxc),
                        coefficient=coef,
                        source_data=source_ndcs,
                        table_references={
                            "ndc_mapping": "table_10a",
                            "hierarchy": "table_11",
                            "coefficient": "table_9",
                            "model": model,
                            "metal_level": member.metal_level,
                        },
                    )
                )

        # Step 9: Calculate total score
        total_score = demographic_factor + hcc_score + rxc_score

        # Build final HCC list (includes groups for transparency)
        final_hccs = sorted(remaining_hccs) + sorted(triggered_groups)
        final_rxcs = sorted(rxcs_after_hierarchy)

        return ScoreOutput(
            member_id=member.member_id,
            risk_score=total_score,
            hcc_list=final_hccs,
            components=components,
            details={
                "model": model,
                "age": age,
                "gender": member.gender,
                "metal_level": member.metal_level,
                "enrollment_months": member.enrollment_months,
                "demographic_variable": demo_var,
                "demographic_factor": demographic_factor,
                "raw_ccs": sorted(raw_ccs),
                "hccs_after_hierarchy": sorted(hccs_after_hierarchy),
                "hccs_filtered": sorted(hccs_filtered),
                "remaining_hccs": sorted(remaining_hccs),
                "triggered_groups": sorted(triggered_groups),
                "hcc_cnt": hcc_cnt,
                "edf_variable": edf_var,
                "edf_factor": edf_factor,
                "hcc_coefficients": hcc_details,
                "hcc_score": hcc_score,
                "raw_rxcs": sorted(raw_rxcs),
                "rxcs_after_hierarchy": sorted(final_rxcs),
                "rxc_coefficients": rxc_details,
                "rxc_score": rxc_score,
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
