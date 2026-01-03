from __future__ import annotations

from pathlib import Path

import typer

from ra_dagster.db.bootstrap import ensure_prism_warehouse
from ra_dagster.resources.duckdb_resource import DuckDBResource

app = typer.Typer(no_args_is_help=True, help="Prism CLI - Database and orchestration utilities")

DEFAULT_DUCKDB_PATH = str(
    (Path(__file__).resolve().parents[2] / "risk_adjustment.duckdb").resolve()
)


@app.command(name="db-bootstrap")
def db_bootstrap(
    duckdb_path: str = typer.Option(DEFAULT_DUCKDB_PATH, "--duckdb-path"),
) -> None:
    """Create core Prism schemas + tables in DuckDB.

    Creates: `main_intermediate`, `main_runs`, `main_analytics`.
    """

    res = DuckDBResource(path=duckdb_path)
    con = res.get_connection().connect()
    try:
        ensure_prism_warehouse(con)
    finally:
        con.close()

    typer.echo(f"Bootstrapped warehouse at {Path(duckdb_path).resolve()}")


if __name__ == "__main__":
    app()
