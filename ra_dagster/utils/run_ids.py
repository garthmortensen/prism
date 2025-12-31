from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4


def generate_run_id() -> str:
    return str(uuid4())


def generate_run_timestamp(now: datetime | None = None) -> str:
    """Return YYYYMMDDHHMMSSUUUU where UUUU is 1/10,000th of a second.

    README asks for 4-digit microseconds (YYYYMMDDHHMMSSUUUU) to avoid collisions.
    We derive UUUU by truncating Python's microseconds (0-999999) to 4 digits.
    """

    now = now or datetime.now(UTC)
    uuuu = now.microsecond // 100  # 0-9999
    return now.strftime("%Y%m%d%H%M%S") + f"{uuuu:04d}"


@dataclass(frozen=True)
class GitProvenance:
    branch: str | None
    commit: str | None
    commit_short: str | None
    clean: bool | None


def get_git_provenance(cwd: str | None = None) -> GitProvenance:
    """Return current git commit + whether the working tree is clean.

    "Clean" is determined via `git status --porcelain`, which treats untracked files as dirty.
    """

    def _run_git(args: list[str]) -> str | None:
        try:
            completed = subprocess.run(
                ["git", *args],
                cwd=cwd,
                check=True,
                capture_output=True,
                text=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None
        return completed.stdout.strip()

    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"])
    commit = _run_git(["rev-parse", "HEAD"])
    commit_short = _run_git(["rev-parse", "--short", "HEAD"])

    if not commit:
        return GitProvenance(branch=None, commit=None, commit_short=None, clean=None)

    status = _run_git(["status", "--porcelain"])
    if status is None:
        clean: bool | None = None
    else:
        clean = status == ""

    return GitProvenance(branch=branch, commit=commit, commit_short=commit_short, clean=clean)


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), default=str)


def extract_launchpad_config(
    *,
    context: Any,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Best-effort extraction of the Dagster Launchpad run config.

    Dagster surfaces this in slightly different places depending on context type and version.
    We try a few common attributes and fall back to a caller-provided dict.
    """

    dagster_run = getattr(context, "dagster_run", None)
    if dagster_run is not None:
        run_config = getattr(dagster_run, "run_config", None)
        if isinstance(run_config, dict) and run_config:
            return run_config

        run_config_yaml = getattr(dagster_run, "run_config_yaml", None)
        if isinstance(run_config_yaml, str) and run_config_yaml.strip():
            return {"run_config_yaml": run_config_yaml}

    run_config = getattr(context, "run_config", None)
    if isinstance(run_config, dict) and run_config:
        return run_config

    op_config = getattr(context, "op_config", None)
    if isinstance(op_config, dict) and op_config:
        return {"op_config": op_config}

    return fallback or {}
