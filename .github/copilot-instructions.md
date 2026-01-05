# Quick Agent Guide — Prism

Focused guidance for AI coding agents and contributors. Keep this file lean, high-signal, and in sync with code changes.

## Purpose & Scope
LLM-driven agent that works for Health Insurer's risk adjustment team. 
It
## Tools & Extension Guidelines
Adding a tool:
1. Define an `async` function with clear docstring (first sentence = one-line description).
1. Prefer LangChain tools (`@tool`) for agent-callable functions.
2. Accept simple primitives; for complex lists (filters) pass JSON-encoded string, parse internally.
3. Return JSON‑serializable result (stringified dict is fine) including context (row count, dimensions used).
4. Log: start, key parameters, row count, elapsed time, errors.
Avoid: heavy blocking operations without pagination; silent failures; printing instead of repo logger.

## Data Access Conventions
- Reflection only: never hard-code column names.
- Filters: dict list supporting operators (`=`, `in`, `between`, `ilike`, `is null`, `is not null`).
- Query compilation uses `literal_binds=True` for auditability; preserve this for debugging.
- `top_n` reserved for previewing high-cardinality groupings (limit ≤100 suggested).
- Return structure keys: `data`, `sql`, `tables`, `config_summary` (consumed by tools & reports).

## Code updates

- DO NOT maintain backwards compatibility. This project is under active development; breakages are acceptable if they improve functionality.

- New libraries are added via `uv add`.

- Code executions should be asynchronous where possible.

- Execute from CLI using `uv run` command.
