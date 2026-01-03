## TODO

## Version Milestones

| Version | Phase | Feature                       | Compete |
| ------- | ----- | ----------------------------- | ------- |
| v0.1.0  | 1     | Calculator + dbt foundation   | x       |
| v0.2.0  | 2     | Dagster scoring job + lineage | x       |
| v0.3.0  | 3     | Comparison + decomposition    | x       |
| v0.4.0  | 4     | Containers                    |         |
| v0.5.0  | 5     | LLM agents (future)           |         |

## Add Agents

**Goal**: LLM-powered narrative generation for decomposition results.

This phase is deferred as a nice-to-have enhancement. When ready:

- [ ] Add `agents/` folder with FastAPI service
- [ ] Implement `POST /narrate/decomposition` endpoint
- [ ] Integrate LangChain for prompt management
- [ ] Add `platform/assets/agents.py` for HTTP integration
- [ ] Create `prompts/decomposition_narrative.txt`

## Web forms

Refactor dashboard such that users can select UUID and process/load dashboard.

Refactor comparison/decomp such that users can select UUIDs and process/load metrics.

Create run tracker for adding additional metadata about runs (2025 final EDGE submission).

## Refactor

- start with building blocks/bricks:

  - dbg (snowflake)
  - config
  - choose calculator
  - dagster
    - jobs
      - scoring
      - comparison
      - decomposition
      - trend analysis
    - visualizations

  ***Given these building blocks, build simplest, most direct codebase***
