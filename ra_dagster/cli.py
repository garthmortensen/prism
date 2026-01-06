"""
Module: cli.py
Description:
    Command-line interface for the Prism application.
    - Exposes database management utilities (bootstrap).
    - Entry point for ad-hoc administrative tasks.

Usage:
    Run via `prism` or `python -m ra_dagster.cli`.
"""
from __future__ import annotations

from pathlib import Path

import typer

from ra_dagster.db.bootstrap import ensure_prism_warehouse
from ra_dagster.resources.sqlalchemy_resource import SqlAlchemyResource
from ra_agent.cli import app as agent_app

app = typer.Typer(no_args_is_help=True, help="Prism CLI - Database and orchestration utilities")

app.add_typer(agent_app, name="agent", help="AI Agent commands")


@app.command(name="db-bootstrap")
def db_bootstrap(
    database: str = typer.Option("dev", "--database", "-d", help="Database environment: dev or prod"),
) -> None:
    """Create core Prism schemas + tables.

    Creates: `main_intermediate`, `main_runs`, `main_analytics`.
    """

    res = SqlAlchemyResource(database=database)
    engine = res.get_engine()
    con = engine.connect()
    try:
        ensure_prism_warehouse(con)
        con.commit()
    finally:
        con.close()

    typer.echo(f"Bootstrapped warehouse for environment: {database}")


if __name__ == "__main__":
    app()
