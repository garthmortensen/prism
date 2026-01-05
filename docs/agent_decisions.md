# Agent Implementation Decisions

## Decision: LangChain/LangGraph + OpenAI

Build the Agent feature with:

- **LangChain/LangGraph**: the agent loop (reasoning steps, tool routing, memory)
- **OpenAI models** via **`langchain-openai`**: the model provider (talks to OpenAI)
- **LangChain tools** via **`@tool`**: how we expose Prism functions to the agent

Do **not** use the OpenAI Agents SDK decorator `@function_tool` in this project.

## What Is an "Agent"?

An **agent** is an LLM (like GPT-4) that can **decide which tools to call** based on a conversation. Instead of you writing code like "if user asks X, call function Y", the LLM figures that out.

```
┌─────────────────────────────────────────────────────────────┐
│  Traditional Code                                           │
│  ─────────────────                                          │
│  if "score" in user_input:                                  │
│      score_members()                                        │
│  elif "compare" in user_input:                              │
│      compare_runs()                                         │
│                                                             │
│  Agent (LLM decides)                                        │
│  ───────────────────                                        │
│  User: "What changed between last month and this month?"    │
│  LLM thinks: "I need to compare two runs → call compare_runs│
│              with run_id_a=... and run_id_b=..."            │
└─────────────────────────────────────────────────────────────┘
```

---

## How Do You Interact With an Agent?

**Short answer**: Through a chat interface—either CLI, web UI, or API.

| Interface | How It Works | Best For |
|-----------|--------------|----------|
| **CLI (recommended to start)** | Terminal prompt, type questions | Development, scripting |
| **Web UI** | Browser chat box (like ChatGPT) | Business users |
| **API** | HTTP POST with messages | Integrations, automation |
| **Config YAML** | Pre-defined "missions" the agent runs | Batch jobs, scheduled tasks |

### Decision: Start with CLI, Add Web Later

```bash
# Interactive chat
uv run prism agent chat

# Single question (scriptable)
uv run prism agent ask "Why did risk scores increase in Q4?"

# Run a pre-defined analysis from YAML
uv run prism agent run --config configs/agent/lag_investigation.yaml
```

---

## LangChain Basics

LangChain is a Python library that connects LLMs to your code. Think of it as **plumbing** between ChatGPT and your functions.

### Core Concepts

```python
# 1. LLM - The brain (OpenAI's GPT-4)
from langchain_openai import ChatOpenAI
llm = ChatOpenAI(model="gpt-4o")

# 2. Tool - A function the LLM can call
from langchain_core.tools import tool

@tool
def get_risk_score(member_id: str) -> str:
    """Look up risk score for a member."""  # <-- LLM reads this docstring!
    score = db.query(f"SELECT risk_score FROM scores WHERE member_id = '{member_id}'")
    return f"Member {member_id} has risk score {score}"

# 3. Agent - LLM + Tools + Memory
from langgraph.prebuilt import create_react_agent
agent = create_react_agent(llm, tools=[get_risk_score])

# 4. Run it
response = agent.invoke({"messages": [("human", "What's the risk score for MBR001?")]})
# LLM decides to call get_risk_score("MBR001") automatically
```

### What Makes a Good Tool?

The LLM **only sees the function name, docstring, and parameter types**. It uses these to decide when/how to call it.

```python
# BAD - LLM won't know when to use this
@tool
def f(x: str) -> str:
    """Process data."""
    ...

# GOOD - Clear name, detailed docstring, typed parameters
@tool  
def query_risk_scores(
    run_id: str,
    min_score: float = 0.0,
    limit: int = 100
) -> str:
    """
    Query risk scores from a specific scoring run.
    
    Use this to analyze member-level risk scores, find high-risk members,
    or get score distributions. Returns JSON with member_id, risk_score,
    hcc_list, and demographic details.
    
    Args:
        run_id: The UUID of the scoring run (from list_runs tool)
        min_score: Filter to scores >= this value (default: 0.0)
        limit: Max rows to return (default: 100, max: 1000)
    """
    ...
```

---

## How Agent Fits With dbt + Dagster

Current Prism architecture:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   dbt       │───▶│  Dagster    │───▶│ Warehouse   │
│ (SQL models)│    │ (orchestrate│    │ (DuckDB or  │
│             │    │             │    │  Snowflake) │
└─────────────┘    │  + assets)  │    └─────────────┘
                   └─────────────┘
```

With Agent added:

```
                         ┌─────────────────────────────────┐
                         │         AGENT LAYER             │
                         │  (LangChain + OpenAI GPT-4)     │
                         │                                 │
                         │  "Why did scores change?"       │
                         │         │                       │
                         │         ▼                       │
                         │  Tools: query_scores,           │
                         │         compare_runs,           │
                         │         trigger_scoring_job     │
                         └────────────┬────────────────────┘
                                      │ calls
                                      ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   dbt       │───▶│  Dagster    │───▶│ Warehouse   │
│ (SQL models)│    │ (orchestrate│    │ (DuckDB or  │
│             │    │             │    │  Snowflake) │
└─────────────┘    │  + assets)  │    └─────────────┘
                   └─────────────┘
```

### What Changes?

| Component | Before Agent | After Agent |
|-----------|--------------|-------------|
| **dbt** | No change | No change (still transforms SQL) |
| **Dagster** | Triggered via CLI/UI/YAML | Also triggered by Agent tools |
| **Warehouse (DuckDB/Snowflake)** | Queried by assets | Also queried by Agent tools |
| **User** | Writes YAML configs, clicks buttons | Asks questions in natural language |

### Key Integration Points

1. **Agent reads from the warehouse** (same database as Dagster)
2. **Agent can trigger Dagster jobs** (via Python API, not CLI)
3. **Agent archives its work** to the warehouse (new `main_agent` schema)

---

## Decision: Tool Categories

We'll create these tool groups:

### 1. Query Tools (read-only, fast)
```python
@tool
def list_runs(analysis_type: str = "scoring", limit: int = 10) -> str:
    """List recent runs. Returns run_id, timestamp, description, status."""

@tool  
def query_risk_scores(run_id: str, filters: str = "{}") -> str:
    """Query member-level risk scores from a run."""

@tool
def get_run_summary(run_id: str) -> str:
    """Get aggregate statistics for a scoring run."""
```

### 2. Analysis Tools (trigger Dagster jobs)
```python
@tool
def compare_two_runs(run_id_a: str, run_id_b: str, description: str) -> str:
    """Compare two scoring runs and compute deltas."""

@tool
def decompose_score_changes(run_ids: str, description: str) -> str:
    """Decompose score changes across multiple runs into drivers."""
```

### 3. Scoring Tools (heavyweight, creates new data)
```python
@tool
def score_population(config_name: str) -> str:
    """Trigger a new scoring run using a predefined config."""
```

---

## Decision: Data Access Strategy

To ensure efficiency and avoid overwhelming the context window, the agent must follow a strict data access hierarchy:

1. **Mart Data First**: Always start by querying aggregated "mart" tables (e.g., `mart_risk_scores`, `mart_hcc_summary`). These provide high-level trends and summaries.
2. **Drill Down Second**: Only query member-level tables (e.g., `int_claims`, `raw_eligibility`) if the mart data indicates a specific anomaly or if the user explicitly requests granular details.

**Why?**
- **Performance**: Mart tables are pre-aggregated and faster to query.
- **Context**: Member-level data is high-cardinality and can easily exceed token limits.
- **Cost**: Reduces the number of rows processed by the LLM.

---

## Decision: Conversation Memory

Agents can "remember" the conversation. Options:

| Memory Type | Behavior | Use Case |
|-------------|----------|----------|
| **None** | Each message is independent | Simple Q&A |
| **Buffer** | Remember last N messages | Short investigations |
| **Summary** | LLM summarizes old context | Long sessions |

### Decision: Use Buffer Memory (last 20 messages)

```python
from langgraph.checkpoint.memory import MemorySaver

memory = MemorySaver()
agent = create_react_agent(llm, tools, checkpointer=memory)

# Each conversation gets a thread_id
config = {"configurable": {"thread_id": "investigation-001"}}
agent.invoke({"messages": [("human", "List recent runs")]}, config)
agent.invoke({"messages": [("human", "Show me the first one")]}, config)  # Remembers context
```

---

## Decision: The Investigation Loop

Per `feature_agent.md`, each analysis iteration has: **Hypothesis → Experiment → Analysis → Archive**

```python
# Stored in the warehouse (DuckDB or Snowflake): main_agent.iterations
{
    "iteration_id": "iter-001",
    "thread_id": "investigation-001", 
    "hypothesis": "Risk scores increased due to new HCC mappings",
    "experiment": {
        "tool_calls": [
            {"tool": "compare_two_runs", "args": {"run_id_a": "...", "run_id_b": "..."}},
            {"tool": "query_risk_scores", "args": {"run_id": "...", "filters": "{\"min_score\": 5.0}"}}
        ]
    },
    "analysis": "Comparison shows 12% increase in HCC captures, primarily in diabetes codes...",
    "conclusion": "SUPPORTED - New mappings added 847 HCC captures",
    "timestamp": "2026-01-05T10:30:00Z"
}
```

### How It Works in Practice

```
User: "I think Q4 scores went up because we got more complete claims data"

Agent: I'll test that hypothesis. Let me:
       1. Find the Q3 and Q4 scoring runs
       2. Compare them to see the delta
       3. Check if HCC capture rates changed
       
       [Calls list_runs, compare_two_runs, query_risk_scores]
       
       Analysis: Q4 scores are 8% higher. HCC capture rate increased 
       from 2.1 to 2.4 per member. Claims volume up 15%.
       
       Your hypothesis appears SUPPORTED. The additional claims data 
       resulted in more diagnoses being captured.
       
       [Archives iteration to main_agent.iterations]
```

---

## Decision: Configuration Approach

### Environment Variables (secrets)
```bash
# .env (git-ignored)
OPENAI_API_KEY=sk-...
```

### YAML Configs (reusable investigations)
```yaml
# configs/agent/quarterly_review.yaml
name: "Quarterly Score Review"
system_prompt: |
  You are analyzing risk adjustment scores for a health insurer.
  Focus on: score trends, HCC capture rates, and data completeness.
  
initial_questions:
  - "List the last 4 quarterly scoring runs"
  - "Compare Q3 to Q4 and summarize changes"
  - "What are the top 10 HCCs driving score increases?"

archive: true  # Save all iterations
```

### CLI Usage
```bash
# Interactive
uv run prism agent chat

# Run predefined investigation  
uv run prism agent run --config configs/agent/quarterly_review.yaml

# One-shot question
uv run prism agent ask "What's the average risk score for run abc123?"
```

---

## Directory Structure

```
ra_agent/
├── __init__.py
├── agent.py              # Main agent setup (LLM + tools + memory)
├── cli.py                # Typer commands (chat, run, ask)
├── tools/
│   ├── __init__.py       # Exports all tools
│   ├── query_tools.py    # list_runs, query_risk_scores, get_run_summary
│   ├── analysis_tools.py # compare_two_runs, decompose_score_changes  
│   └── scoring_tools.py  # score_population
├── prompts/
│   └── system.txt        # Default system prompt
├── models.py             # Pydantic models (Iteration, ToolCall, etc.)
└── archive.py            # Save/load iterations to the warehouse
```

---

## Minimal Working Example

Here's the simplest possible agent to prove the concept works:

```python
# ra_agent/agent.py
import os
from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, func, select

# Uses SQLAlchemy so we can point to DuckDB (dev) or Snowflake (prod)
DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///risk_adjustment.duckdb")
engine = create_engine(DATABASE_URL)

@tool
def list_scoring_runs(limit: int = 5) -> str:
    """List recent risk scoring runs with their IDs and descriptions."""
    md = MetaData()
    run_registry = Table("run_registry", md, schema="main_runs", autoload_with=engine)

    stmt = (
        select(run_registry.c.run_id, run_registry.c.run_timestamp, run_registry.c.status)
        .where(run_registry.c.analysis_type == "scoring")
        .order_by(run_registry.c.run_timestamp.desc())
        .limit(limit)
    )

    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    return df.to_markdown(index=False)

@tool
def get_score_stats(run_id: str) -> str:
    """Get aggregate risk score statistics for a scoring run."""
    md = MetaData()
    risk_scores = Table("risk_scores", md, schema="main_runs", autoload_with=engine)

    stmt = select(
        func.count().label("member_count"),
        func.avg(risk_scores.c.risk_score).label("avg_score"),
        func.min(risk_scores.c.risk_score).label("min_score"),
        func.max(risk_scores.c.risk_score).label("max_score"),
    ).where(risk_scores.c.run_id == run_id)

    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
    return df.to_markdown(index=False)

def create_agent():
    llm = ChatOpenAI(model="gpt-4o", api_key=os.getenv("OPENAI_API_KEY"))
    tools = [list_scoring_runs, get_score_stats]
    return create_react_agent(llm, tools)

if __name__ == "__main__":
    agent = create_agent()
    
    # Simple CLI loop
    print("Prism Agent (type 'quit' to exit)")
    while True:
        user_input = input("\nYou: ")
        if user_input.lower() == 'quit':
            break
        
        response = agent.invoke({"messages": [("human", user_input)]})
        print(f"\nAgent: {response['messages'][-1].content}")
```

Run it:
```bash
export OPENAI_API_KEY=sk-...
    export DATABASE_URL='duckdb:///risk_adjustment.duckdb'
uv run python -m ra_agent.agent
```

---

## Next Steps (Implementation Order)

1. **Week 1**: Minimal agent with 3 query tools (list_runs, query_scores, get_stats)
2. **Week 2**: Add analysis tools (compare, decompose) that wrap Dagster
3. **Week 3**: Add iteration archiving to the warehouse
4. **Week 4**: Polish CLI, add YAML config support, error handling

---

## Open Questions

1. **Should agent trigger dbt runs?** Current thinking: No, dbt should run on schedule or manually. Agent only triggers Dagster assets that consume already-built dbt models.

2. **Cost controls?** GPT-4o is ~$5/1M input tokens. Add token counting and budget limits per session.

3. **Streaming responses?** Nice UX but adds complexity. Defer to v2.

4. **Multi-user?** Current design is single-user CLI. Web UI would need auth. Defer to v2.
