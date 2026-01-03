from __future__ import annotations

from pathlib import Path

import duckdb
from dagster import build_asset_context

from ra_dagster.assets.decomposition import decompose_runs
from ra_dagster.db.bootstrap import ensure_prism_warehouse
from ra_dagster.resources.duckdb_resource import DuckDBResource


def test_decomposition_writes_interaction_row(tmp_path: Path) -> None:
    db_path = tmp_path / "test.duckdb"

    con = duckdb.connect(str(db_path))
    try:
        ensure_prism_warehouse(con)

        # Minimal metadata row for the "actual" run.
        con.execute(
            """
            INSERT INTO main_runs.run_registry (run_id, model_version, benefit_year, data_effective)
            VALUES (?, ?, ?, ?)
            """,
            ["ACTUAL", "hhs_2025", 2025, None],
        )

        # Two members; baseline -> actual total mean delta = 2.0
        con.executemany(
            """
            INSERT INTO main_runs.risk_scores (run_id, member_id, risk_score)
            VALUES (?, ?, ?)
            """,
            [
                ("BASE", "M1", 10.0),
                ("BASE", "M2", 20.0),
                ("ACTUAL", "M1", 12.0),
                ("ACTUAL", "M2", 22.0),
                # Two components whose effects sum to 1.5, leaving interaction 0.5
                ("MODEL", "M1", 11.0),
                ("MODEL", "M2", 21.0),
                ("POP", "M1", 10.5),
                ("POP", "M2", 20.5),
            ],
        )
    finally:
        con.close()

    cfg = {
        "scenarios": {
            "baseline": "BASE",
            "actual": "ACTUAL",
            "model": "MODEL",
            "pop": "POP",
        },
        "analysis": {
            "baseline": "baseline",
            "actual": "actual",
            "method": "marginal",
            "metric": "mean",
            "population_mode": "intersection",
            "components": [
                {"name": "Model Change", "scenario": "model"},
                {"name": "Population Mix", "scenario": "pop"},
            ],
        },
    }

    ctx = build_asset_context(asset_config=cfg)
    resource = DuckDBResource(path=str(db_path))

    decompose_runs(ctx, resource)

    batch_id = ctx.run_id

    con = duckdb.connect(str(db_path))
    try:
        row = con.execute(
            """
            SELECT impact_value
            FROM main_analytics.decomposition_scenarios
            WHERE batch_id = ? AND driver_name = 'Interaction'
            """,
            [batch_id],
        ).fetchone()
    finally:
        con.close()

    assert row is not None
    assert abs(float(row[0]) - 0.5) < 1e-9
