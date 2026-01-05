# Feature: Agent

This iteration will add a new epic feature called "Agent" to the project. 

- Lang Chain framework will serve as the backbone.

- OpenAI SDK will be used for language model interactions.

## Objectives

- Implement the Agent feature using Lang Chain and OpenAI SDK.

- LangChain tools (e.g., `@tool`) will be utilized to define and manage functions within the Agent.

- database interactions should use sqlalchemy, with recognition that duckdb and snowflake may be interchanged.

- Ensure seamless integration with existing project components.

- Existing project components will be updated, replaced, or refactored to support the new Agent feature. Again, backwards compatibility is not required.

## Architecture

The Agent feature will follow a loop architecture to facilitate continuous interaction and improvement. Overall, each iteration should include a hypthesis, experiment, and analysis phase. The loop will consist of the following stages:

1. Input Processing: The Agent will receive input from users or other system components.

2. Function Execution: Based on the input, the Agent will determine which functions to execute using LangChain tools.

3. Output Generation: The Agent will generate output based on the results of the function executions.

4. Feedback Loop: The Agent will incorporate feedback to refine its operations and improve future interactions.

5. Iteration: The loop will repeat, allowing the Agent to continuously learn and adapt to new inputs and scenarios.

6. Archive: All iterations and their results will be archived for future reference and analysis.

## Implementation Steps

Create ra_agent/ module structure with agent.py (main loop), tools/ subpackage, prompts/, and Pydantic models for iteration tracking in a new ra_agent/ directory following patterns in ra_calculators.

Define core tools by wrapping existing functions with LangChain tools (`@tool`):

Scoring: wrap ACACalculator.score() and score_members_aca
Query: create new DuckDB query tools using DuckDBResource patterns
Analysis: wrap compare_runs and decompose_runs
Implement agent loop in ra_agent/agent.py using LangChain's AgentExecutor with OpenAI backend, following the Input → Function Execution → Output → Feedback → Archive cycle from feature_agent.md.

Create iteration archive system extending RunRecord pattern with new main_agent.iterations table storing hypothesis, experiment config, analysis results, and parent run_id linkage.

Add CLI commands to cli.py using Typer: prism agent chat (interactive), prism agent run --hypothesis "..." (single iteration), and update Makefile accordingly.

Configure environment by adding OpenAI API key handling to existing patterns and creating system prompt template in ra_agent/prompts/system_prompt.txt.

### Further Considerations

LangChain vs OpenAI Agents SDK? For this project, prefer LangChain/LangGraph for the agent loop and tool definitions (`@tool`). Use OpenAI models via `langchain-openai`.

Async execution model? Per guidelines, agent tools should be async—should blocking Dagster jobs be dispatched via background tasks or run synchronously with progress streaming?

Archive storage location? Options: (A) DuckDB table main_agent.iterations, (B) JSON files in compute_logs alongside existing run logs, (C) Both for redundancy—recommend Option A for queryability.
