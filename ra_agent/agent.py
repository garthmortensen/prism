import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from ra_agent.tools import (
    list_runs, 
    get_run_summary, 
    get_hcc_summary, 
    get_score_distribution, 
    get_rxc_summary,
    get_score_by_dimension,
    get_comparison_by_dimension,
    get_decomposition_results,
    query_risk_scores,
    compare_two_runs,
    configure_scoring_run,
    execute_scoring_run,
    get_last_execution_error,
    describe_hccs,
    describe_rxcs
)

# Load environment variables from .env file if present
load_dotenv()

def load_config():
    """Load configuration from config_infra/infrastructure.yaml."""
    # Assuming running from project root, but let's be safe and find the root relative to this file
    # ra_agent/agent.py -> .../prism/ra_agent/agent.py
    project_root = Path(__file__).parent.parent
    config_path = project_root / "config_infra" / "infrastructure.yaml"
    
    if config_path.exists():
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    return {}

def create_agent():
    """Create the Prism Risk Adjustment Agent."""
    
    # Load config
    config = load_config()
    model_name = config.get("agent", {}).get("model", "gpt-4o")

    # Initialize LLM
    # Ensure OPENAI_API_KEY is set in environment
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY not found in environment variables. "
            "Please set it in your .env file or export it in your shell."
        )

    llm = ChatOpenAI(model=model_name, api_key=api_key)
    
    # Define tools
    tools = [
        list_runs, 
        get_run_summary, 
        get_hcc_summary, 
        get_score_distribution, 
        get_rxc_summary,
        get_score_by_dimension,
        get_comparison_by_dimension,
        get_decomposition_results,
        query_risk_scores,
        compare_two_runs,
        configure_scoring_run,
        execute_scoring_run,
        get_last_execution_error,
        describe_hccs,
        describe_rxcs
    ]
    
    # Initialize memory
    memory = MemorySaver()
    
    # Create agent
    agent = create_react_agent(llm, tools, checkpointer=memory)
    
    return agent
