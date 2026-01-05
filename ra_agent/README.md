# Prism Risk Adjustment Agent

This module provides an AI-powered agent for analyzing risk adjustment data. It uses LangChain and OpenAI to query the data warehouse, run comparisons, and explain results.

## Usage

### Interactive Chat
Start a conversational session with the agent:
```bash
uv run prism agent chat
```

### Single Question
Ask a specific question and get a direct answer:
```bash
uv run prism agent ask "What is the average risk score for the latest run?"
```

## Configuration

The agent configuration is located in `config_infra/infrastructure.yaml`.

```yaml
agent:
  model: "gpt-4o"  # Options: gpt-4o, gpt-4-turbo, etc.
```

## Tools

The agent has access to the following tools:
- **Run Registry**: List recent scoring and decomposition runs.
- **Run Summaries**: Get aggregate statistics (scores, HCCs, RXCs).
- **Distributions**: View risk score distributions.
- **Comparisons**: Analyze differences between runs by dimension.
- **Decomposition**: Retrieve results from decomposition analyses.
- **Drill-down**: Query member-level risk scores (used sparingly).
