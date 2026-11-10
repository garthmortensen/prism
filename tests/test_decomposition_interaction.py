from __future__ import annotations

from pathlib import Path

from dagster import build_asset_context
from sqlalchemy import create_engine, text

from ra_dagster.assets.decomposition import decompose_runs
from ra_dagster.db.bootstrap import ensure_prism_warehouse
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource


def test_decomposition_writes_interaction_row(tmp_path: Path) -> None:
    """Test that decomposition correctly writes interaction rows."""
    db_path = tmp_path / "test.duckdb"
    url = f"duckdb:///{db_path}"
    engine = create_engine(url)

    with engine.connect() as con:
        ensure_prism_warehouse(con)

        # Minimal metadata row for the "actual" run.
        con.execute(
            text("""
            INSERT INTO runs.run_registry (run_id, model_version, benefit_year, data_effective)
            VALUES (:run_id, :model_version, :benefit_year, :data_effective)
            """),
            {"run_id": "ACTUAL", "model_version": "hhs_2025", "benefit_year": 2025, "data_effective": None},
        )

        # Two members; baseline -> actual total mean delta = 2.0
        con.execute(
            text("""
            INSERT INTO runs.risk_scores (run_id, member_id, risk_score)
            VALUES (:run_id, :member_id, :risk_score)
            """),
            [
                {"run_id": "BASE", "member_id": "M1", "risk_score": 10.0},
                {"run_id": "BASE", "member_id": "M2", "risk_score": 20.0},
                {"run_id": "ACTUAL", "member_id": "M1", "risk_score": 12.0},
                {"run_id": "ACTUAL", "member_id": "M2", "risk_score": 22.0},
                # Two components whose effects sum to 1.5, leaving interaction 0.5
                {"run_id": "MODEL", "member_id": "M1", "risk_score": 11.0},
                {"run_id": "MODEL", "member_id": "M2", "risk_score": 21.0},
                {"run_id": "POP", "member_id": "M1", "risk_score": 10.5},
                {"run_id": "POP", "member_id": "M2", "risk_score": 20.5},
            ],
        )
        con.commit()

    cfg = {
        "baseline_run_id": "BASE",
        "actual_run_id": "ACTUAL",
        "population_mode": "intersection",
        "components": [
            {"name": "Model Change", "run_id": "MODEL"},
            {"name": "Population Mix", "run_id": "POP"},
        ],
    }

    ctx = build_asset_context(asset_config=cfg)
    # Mock resource
    # We can't easily mock ConfigurableResource with just path, we need to set env var or override get_engine
    # But SqlAlchemyResource uses DATABASE_URL.
    # We can subclass or just set env var.
    import os
    os.environ["DATABASE_URL"] = url
    resource = SqlAlchemyResource(database="dev")

    decompose_runs(ctx, resource)

    batch_id = ctx.run_id

    with engine.connect() as con:
        row = con.execute(
            text("""
            SELECT impact_value
            FROM analytics.decomposition_scenarios
            WHERE batch_id = :batch_id AND driver_name = 'Interaction'
            """),
            {"batch_id": batch_id},
        ).fetchone()

    assert row is not None
    assert abs(float(row[0]) - 0.5) < 1e-9
