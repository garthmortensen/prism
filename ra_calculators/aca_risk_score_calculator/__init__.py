"""ACA HHS-HCC Risk Score Calculator.

Implements the CMS HHS-HCC risk adjustment model for ACA markets.
Loads official DIY tables from diy_tables/cy202*_diy_tables/.
"""

from ra_calculators.aca_risk_score_calculator.calculator import ACACalculator
from ra_calculators.aca_risk_score_calculator.models import MemberInput, ScoreOutput

__all__ = ["ACACalculator", "MemberInput", "ScoreOutput"]
