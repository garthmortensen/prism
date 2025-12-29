from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

import duckdb

from ra_calculators.aca_risk_score_calculator.duckdb_to_csv import score_from_duckdb_to_csv


def test_duckdb_to_csv_runner_smoke(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "risk_adjustment.duckdb"
    out_csv = tmp_path / "scores.csv"

    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("CREATE SCHEMA main_intermediate")
        con.execute(
            """
            CREATE TABLE main_intermediate.int_aca_risk_input (
                member_id VARCHAR,
                date_of_birth DATE,
                gender VARCHAR,
                metal_level VARCHAR,
                enrollment_months INTEGER,
                diagnoses VARCHAR[],
                ndc_codes VARCHAR[]
            )
            """
        )

        con.execute(
            """
            INSERT INTO main_intermediate.int_aca_risk_input VALUES
                (?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "M1",
                date(1980, 1, 1),
                "M",
                "silver",
                12,
                ["A021"],
                [],
                "M2",
                date(2015, 1, 1),
                "F",
                "silver",
                12,
                [],
                [],
            ],
        )
    finally:
        con.close()

    written = score_from_duckdb_to_csv(
        duckdb_path=str(duckdb_path),
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

    # Pipeline guarantee: risk_score is present and numeric-ish
    for r in rows:
        assert float(r["risk_score"]) > 0
        assert r["hcc_list"] is not None


def test_duckdb_to_csv_runner_skips_invalid_gender(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "risk_adjustment.duckdb"
    out_csv = tmp_path / "scores.csv"

    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("CREATE SCHEMA main_intermediate")
        con.execute(
            """
            CREATE TABLE main_intermediate.int_aca_risk_input (
                member_id VARCHAR,
                date_of_birth DATE,
                gender VARCHAR,
                metal_level VARCHAR,
                enrollment_months INTEGER,
                diagnoses VARCHAR[],
                ndc_codes VARCHAR[]
            )
            """
        )

        con.execute(
            """
            INSERT INTO main_intermediate.int_aca_risk_input VALUES
                (?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "GOOD",
                date(1980, 1, 1),
                "M",
                "silver",
                12,
                [],
                [],
                "BAD",
                date(1980, 1, 1),
                "O",
                "silver",
                12,
                [],
                [],
            ],
        )
    finally:
        con.close()

    written = score_from_duckdb_to_csv(
        duckdb_path=str(duckdb_path),
        output_csv_path=str(out_csv),
        model_year="2024",
        invalid_gender="skip",
    )

    assert written == 1

    with out_csv.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert [r["member_id"] for r in rows] == ["GOOD"]
