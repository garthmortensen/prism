from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from sqlalchemy import create_engine, text

from ra_calculators.aca_risk_score_calculator.postgresdb_to_csv import score_from_postgres_to_csv


def test_postgresdb_to_csv_runner_smoke(tmp_path: Path) -> None:
    """Smoke test for the PostgreSQL to CSV runner."""
    # Use in-memory SQLite for testing (SQLAlchemy compatible)
    database_url = "sqlite:///:memory:"
    out_csv = tmp_path / "scores.csv"

    engine = create_engine(database_url)
    
    with engine.connect() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS main_intermediate"))
        con.execute(
            text("""
            CREATE TABLE main_intermediate.int_aca_risk_input (
                member_id TEXT,
                date_of_birth DATE,
                gender TEXT,
                metal_level TEXT,
                enrollment_months INTEGER,
                diagnoses TEXT,
                ndc_codes TEXT
            )
            """)
        )
        con.commit()

        con.execute(
            text("""
            INSERT INTO main_intermediate.int_aca_risk_input 
            (member_id, date_of_birth, gender, metal_level, enrollment_months, diagnoses, ndc_codes)
            VALUES
                (:m1, :dob1, :g1, :ml1, :em1, :d1, :n1),
                (:m2, :dob2, :g2, :ml2, :em2, :d2, :n2)
            """),
            {
                "m1": "M1",
                "dob1": date(1980, 1, 1),
                "g1": "M",
                "ml1": "silver",
                "em1": 12,
                "d1": '["A021"]',
                "n1": '[]',
                "m2": "M2",
                "dob2": date(2015, 1, 1),
                "g2": "F",
                "ml2": "silver",
                "em2": 12,
                "d2": '[]',
                "n2": '[]',
            },
        )
        con.commit()

    written = score_from_postgres_to_csv(
        database_url=database_url,
        output_csv_path=str(out_csv),
        model_year="2024",
        prediction_year=None,
        schema="main_intermediate",
        table="int_aca_risk_input",
        limit=None,
    )

    assert written == 2
    assert out_csv.exists()

    with out_csv.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    assert {r["member_id"] for r in rows} == {"M1", "M2"}

    # Column guarantee: details_json is not exported
    assert "details_json" not in rows[0]

    # Pipeline guarantee: risk_score is present and numeric-ish
    for r in rows:
        assert float(r["risk_score"]) > 0
        assert r["hcc_list"] is not None


def test_postgresdb_to_csv_runner_coerces_invalid_gender(tmp_path: Path) -> None:
    """Test that invalid genders are coerced when configured."""
    database_url = "sqlite:///:memory:"
    out_csv = tmp_path / "scores.csv"

    engine = create_engine(database_url)
    
    with engine.connect() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS main_intermediate"))
        con.execute(
            text("""
            CREATE TABLE main_intermediate.int_aca_risk_input (
                member_id TEXT,
                date_of_birth DATE,
                gender TEXT,
                metal_level TEXT,
                enrollment_months INTEGER,
                diagnoses TEXT,
                ndc_codes TEXT
            )
            """)
        )
        con.commit()

        con.execute(
            text("""
            INSERT INTO main_intermediate.int_aca_risk_input
            (member_id, date_of_birth, gender, metal_level, enrollment_months, diagnoses, ndc_codes)
            VALUES
                (:m1, :dob1, :g1, :ml1, :em1, :d1, :n1),
                (:m2, :dob2, :g2, :ml2, :em2, :d2, :n2)
            """),
            {
                "m1": "M1",
                "dob1": date(1980, 1, 1),
                "g1": "O",  # Invalid
                "ml1": "silver",
                "em1": 12,
                "d1": '[]',
                "n1": '[]',
                "m2": "M2",
                "dob2": date(2015, 1, 1),
                "g2": "F",
                "ml2": "silver",
                "em2": 12,
                "d2": '[]',
                "n2": '[]',
            },
        )
        con.commit()

    written = score_from_postgres_to_csv(
        database_url=database_url,
        output_csv_path=str(out_csv),
        model_year="2024",
        prediction_year=None,
        schema="main_intermediate",
        table="int_aca_risk_input",
        limit=None,
        invalid_gender="coerce",
        coerce_gender="M",
    )

    assert written == 2
    assert out_csv.exists()

    with out_csv.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 2
    assert {r["member_id"] for r in rows} == {"M1", "M2"}


def test_postgresdb_to_csv_runner_skips_invalid_gender(tmp_path: Path) -> None:
    """Test that invalid genders are skipped when configured."""
    database_url = "sqlite:///:memory:"
    out_csv = tmp_path / "scores.csv"

    engine = create_engine(database_url)
    
    with engine.connect() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS main_intermediate"))
        con.execute(
            text("""
            CREATE TABLE main_intermediate.int_aca_risk_input (
                member_id TEXT,
                date_of_birth DATE,
                gender TEXT,
                metal_level TEXT,
                enrollment_months INTEGER,
                diagnoses TEXT,
                ndc_codes TEXT
            )
            """)
        )
        con.commit()

        con.execute(
            text("""
            INSERT INTO main_intermediate.int_aca_risk_input
            (member_id, date_of_birth, gender, metal_level, enrollment_months, diagnoses, ndc_codes)
            VALUES
                (:m1, :dob1, :g1, :ml1, :em1, :d1, :n1),
                (:m2, :dob2, :g2, :ml2, :em2, :d2, :n2)
            """),
            {
                "m1": "GOOD",
                "dob1": date(1980, 1, 1),
                "g1": "M",
                "ml1": "silver",
                "em1": 12,
                "d1": '[]',
                "n1": '[]',
                "m2": "BAD",
                "dob2": date(1980, 1, 1),
                "g2": "O",
                "ml2": "silver",
                "em2": 12,
                "d2": '[]',
                "n2": '[]',
            },
        )
        con.commit()

    written = score_from_postgres_to_csv(
        database_url=database_url,
        output_csv_path=str(out_csv),
        model_year="2024",
        invalid_gender="skip",
    )

    assert written == 1

    with out_csv.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert [r["member_id"] for r in rows] == ["GOOD"]

    # Column guarantee: details_json is not exported
    assert "details_json" not in rows[0]
