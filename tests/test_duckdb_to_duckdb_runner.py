from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
from ra_calculators.aca_risk_score_calculator.pipeline import export_duckdb_scores_to_table


def test_duckdb_to_duckdb_writer_replace(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "risk_adjustment.duckdb"

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

    written, stats = export_duckdb_scores_to_table(
        duckdb_path=str(duckdb_path),
        output_duckdb_path=str(duckdb_path),
        input_schema="main_intermediate",
        input_table="int_aca_risk_input",
        output_schema="main_intermediate",
        output_table="aca_risk_scores",
        write_mode="replace",
        model_year="2024",
        prediction_year=None,
    )

    assert stats.skipped == 0
    assert written == 2

    con = duckdb.connect(str(duckdb_path))
    try:
        rows = con.execute(
            "SELECT member_id, risk_score, hcc_list "
            "FROM main_intermediate.aca_risk_scores "
            "ORDER BY member_id"
        ).fetchall()
    finally:
        con.close()

    assert [r[0] for r in rows] == ["M1", "M2"]
    assert all(float(r[1]) > 0 for r in rows)
    assert all(r[2] is not None for r in rows)


def test_duckdb_to_duckdb_writer_skips_invalid_gender(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "risk_adjustment.duckdb"

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

    written, stats = export_duckdb_scores_to_table(
        duckdb_path=str(duckdb_path),
        output_duckdb_path=str(duckdb_path),
        output_schema="main_intermediate",
        output_table="aca_risk_scores",
        write_mode="replace",
        model_year="2024",
        invalid_gender="skip",
    )

    assert written == 1
    assert stats.skipped == 1

    con = duckdb.connect(str(duckdb_path))
    try:
        rows = con.execute("SELECT member_id FROM main_intermediate.aca_risk_scores").fetchall()
    finally:
        con.close()

    assert [r[0] for r in rows] == ["GOOD"]
