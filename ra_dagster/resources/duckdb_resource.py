from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
from dagster import ConfigurableResource


@dataclass(frozen=True)
class DuckDBConnection:
    path: Path

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.path))


class DuckDBResource(ConfigurableResource):
    """Dagster resource for connecting to the Prism DuckDB warehouse."""

    path: str = str((Path(__file__).resolve().parents[2] / "risk_adjustment.duckdb").resolve())

    def get_connection(self) -> DuckDBConnection:
        return DuckDBConnection(path=Path(self.path).expanduser().resolve())
