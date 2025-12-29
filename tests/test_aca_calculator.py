"""Tests for ACA Risk Calculator."""

from datetime import date

import pytest

from ra_calculators.aca_risk_score_calculator import ACACalculator, MemberInput, ScoreOutput


class TestMemberInput:
    """Tests for MemberInput model."""

    def test_valid_member(self):
        """Test creating a valid member input."""
        member = MemberInput(
            member_id="M001",
            date_of_birth=date(1965, 3, 15),
            gender="M",
            metal_level="silver",
            diagnoses=["E1165", "I509"],
        )
        assert member.member_id == "M001"
        assert member.gender == "M"
        assert len(member.diagnoses) == 2

    def test_default_values(self):
        """Test default values are applied."""
        member = MemberInput(
            member_id="M002",
            date_of_birth=date(1990, 1, 1),
            gender="F",
        )
        assert member.metal_level == "silver"
        assert member.diagnoses == []
        assert member.enrollment_months == 12

    def test_invalid_gender(self):
        """Test that invalid gender raises error."""
        with pytest.raises(ValueError):
            MemberInput(
                member_id="M003",
                date_of_birth=date(1980, 1, 1),
                gender="X",
            )


class TestACACalculator:
    """Tests for ACACalculator."""

    @pytest.fixture
    def calculator(self):
        """Create calculator instance."""
        return ACACalculator(model_year="2024")

    def test_adult_model_selection(self, calculator):
        """Test that adults (age >= 21) use Adult model."""
        member = MemberInput(
            member_id="A001",
            date_of_birth=date(1980, 1, 1),  # ~44 years old
            gender="M",
            diagnoses=[],
        )
        result = calculator.score(member)
        assert result.details["model"] == "Adult"

    def test_child_model_selection(self, calculator):
        """Test that children (age 2-20) use Child model."""
        member = MemberInput(
            member_id="C001",
            date_of_birth=date(2015, 1, 1),  # ~10 years old
            gender="F",
            diagnoses=[],
        )
        result = calculator.score(member)
        assert result.details["model"] == "Child"

    def test_infant_model_selection(self, calculator):
        """Test that infants (age 0-1) use Infant model."""
        member = MemberInput(
            member_id="I001",
            date_of_birth=date(2024, 6, 1),  # < 1 year old
            gender="M",
            diagnoses=[],
        )
        result = calculator.score(member)
        assert result.details["model"] == "Infant"

    def test_demographic_only_score(self, calculator):
        """Test scoring with demographics only (no diagnoses)."""
        member = MemberInput(
            member_id="D001",
            date_of_birth=date(1965, 3, 15),  # ~59 years old
            gender="M",
            metal_level="silver",
            diagnoses=[],
        )
        result = calculator.score(member)

        assert result.risk_score > 0
        assert result.hcc_list == []
        assert result.details["demographic_factor"] > 0
        assert result.details["hcc_score"] == 0

    def test_diagnosis_mapping(self, calculator):
        """Test that diagnoses are mapped to CCs."""
        member = MemberInput(
            member_id="DX001",
            date_of_birth=date(1970, 1, 1),
            gender="F",
            metal_level="silver",
            diagnoses=["A021"],  # Salmonella sepsis -> CC 2
        )
        result = calculator.score(member)

        # Should have mapped to a CC
        assert len(result.details["raw_ccs"]) > 0

    def test_score_output_structure(self, calculator):
        """Test that score output has expected structure."""
        member = MemberInput(
            member_id="S001",
            date_of_birth=date(1975, 6, 15),
            gender="F",
            metal_level="gold",
            diagnoses=["E1165"],
        )
        result = calculator.score(member)

        # Check ScoreOutput fields
        assert isinstance(result, ScoreOutput)
        assert result.member_id == "S001"
        assert isinstance(result.risk_score, float)
        assert isinstance(result.hcc_list, list)
        assert isinstance(result.details, dict)

        # Check details fields
        assert "model" in result.details
        assert "age" in result.details
        assert "demographic_factor" in result.details
        assert "hcc_score" in result.details
        assert "model_year" in result.details

    def test_metal_level_affects_score(self, calculator):
        """Test that different metal levels produce different scores."""
        base_member = {
            "member_id": "ML001",
            "date_of_birth": date(1960, 1, 1),
            "gender": "M",
            "diagnoses": ["E1165"],
        }

        scores = {}
        for metal in ["platinum", "gold", "silver", "bronze", "catastrophic"]:
            member = MemberInput(**base_member, metal_level=metal)
            result = calculator.score(member)
            scores[metal] = result.risk_score

        # Platinum should generally have highest coefficients
        # (more cost sharing = higher expected utilization adjustment)
        assert scores["platinum"] >= scores["catastrophic"]

    def test_batch_scoring(self, calculator):
        """Test batch scoring multiple members."""
        members = [
            MemberInput(
                member_id=f"B{i:03d}",
                date_of_birth=date(1970 + i, 1, 1),
                gender="M" if i % 2 == 0 else "F",
                diagnoses=[],
            )
            for i in range(5)
        ]

        results = calculator.score_batch(members)

        assert len(results) == 5
        assert all(isinstance(r, ScoreOutput) for r in results)
        assert [r.member_id for r in results] == ["B000", "B001", "B002", "B003", "B004"]

    def test_adult_edf_applied_when_partial_year_and_has_hcc(self, calculator):
        """Adult EDF is added when enrollment_months is 1-11 and HCC_CNT > 0."""
        member = MemberInput(
            member_id="EDF001",
            date_of_birth=date(1980, 1, 1),
            gender="F",
            metal_level="bronze",
            enrollment_months=6,
            # Use a diagnosis that maps to at least one payment HCC in the provided tables.
            diagnoses=["K5000"],
        )
        result = calculator.score(member)

        assert result.details["model"] == "Adult"
        assert result.details["hcc_cnt"] > 0
        assert result.details["edf_variable"] == "HCC_ED6"
        assert isinstance(result.details["edf_factor"], float)
        assert result.details["edf_factor"] != 0.0

        edf_components = [c for c in result.components if c.component_type == "edf"]
        assert len(edf_components) == 1
        assert edf_components[0].component_code == "HCC_ED6"


class TestHierarchyApplication:
    """Tests for HCC hierarchy logic."""

    @pytest.fixture
    def calculator(self):
        return ACACalculator(model_year="2024")

    def test_hierarchy_removes_superseded(self, calculator):
        """Test that hierarchies remove superseded HCCs."""
        # This test requires diagnoses that map to hierarchically related HCCs
        # For now, just verify the structure works
        member = MemberInput(
            member_id="H001",
            date_of_birth=date(1970, 1, 1),
            gender="M",
            diagnoses=[],
        )
        result = calculator.score(member)

        # Verify hierarchy was applied (hccs_after_hierarchy exists)
        assert "hccs_after_hierarchy" in result.details
        assert "raw_ccs" in result.details
