import json
import pandas as pd
from langchain_core.tools import tool
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, JSON, select
from ra_agent.db import engine

@tool
def list_runs(analysis_type: str = "scoring", limit: int = 10) -> str:
    """
    List recent runs. Returns run_id, timestamp, description, status.
    
    Args:
        analysis_type: Type of run to list (default: "scoring")
        limit: Number of runs to return (default: 10)
    """
    md = MetaData()
    # Define table explicitly to avoid reflection issues with DuckDB/SQLAlchemy versions
    run_registry = Table(
        "run_registry", 
        md, 
        Column("run_id", String),
        Column("run_timestamp", String),
        Column("status", String),
        Column("run_description", String),
        Column("analysis_type", String),
        schema="main_runs"
    )

    stmt = (
        select(run_registry.c.run_id, run_registry.c.run_timestamp, run_registry.c.status, run_registry.c.run_description)
        .where(run_registry.c.analysis_type == analysis_type)
        .order_by(run_registry.c.run_timestamp.desc())
        .limit(limit)
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_registry: {str(e)}"
    
    if df.empty:
        return "No runs found."
        
    return df.to_markdown(index=False)

@tool
def get_run_summary(run_id: str) -> str:
    """
    Get aggregate statistics for a scoring run.
    Uses precomputed views in the data mart for performance.
    
    Args:
        run_id: The UUID of the scoring run
    """
    md = MetaData()
    # Explicitly define table to avoid reflection issues
    summary_table = Table(
        "run_score_summary", 
        md, 
        Column("run_id", String),
        Column("member_count", Integer),
        Column("avg_score", Float),
        Column("min_score", Float),
        Column("max_score", Float),
        Column("p50", Float),
        Column("p90", Float),
        Column("p99", Float),
        Column("avg_age", Float),
        schema="main_analytics"
    )

    # Select all columns from the summary view for this run
    stmt = select(summary_table).where(summary_table.c.run_id == run_id)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_score_summary: {str(e)}"
    
    if df.empty:
        return "No summary found for this run_id."

    return df.to_markdown(index=False)

@tool
def get_hcc_summary(run_id: str, limit: int = 20) -> str:
    """
    Get top HCCs (Hierarchical Condition Categories) for a run.
    Returns HCC codes, descriptions, and prevalence/counts.
    
    Args:
        run_id: The UUID of the scoring run
        limit: Max number of HCCs to return (default: 20)
    """
    md = MetaData()
    hcc_table = Table(
        "run_hcc_summary", 
        md, 
        Column("run_id", String),
        Column("hcc_code", String),
        Column("member_count", Integer),
        Column("prevalence", Float),
        Column("avg_score_contribution", Float),
        Column("total_score_contribution", Float),
        schema="main_analytics"
    )

    stmt = (
        select(hcc_table)
        .where(hcc_table.c.run_id == run_id)
        .order_by(hcc_table.c.member_count.desc())
        .limit(limit)
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_hcc_summary: {str(e)}"
        
    if df.empty:
        return "No HCC summary data found for this run."
        
    return df.to_markdown(index=False)

@tool
def get_score_distribution(run_id: str) -> str:
    """
    Get the distribution of risk scores for a run (binned).
    Useful for understanding the population risk profile.
    
    Args:
        run_id: The UUID of the scoring run
    """
    md = MetaData()
    dist_table = Table(
        "run_score_distribution", 
        md, 
        Column("run_id", String),
        Column("score_bucket", Float),
        Column("member_count", Integer),
        Column("pct_members", Float),
        Column("avg_score", Float),
        Column("min_score", Float),
        Column("max_score", Float),
        Column("avg_hcc_score", Float),
        Column("avg_rxc_score", Float),
        Column("avg_demographic_score", Float),
        schema="main_analytics"
    )

    stmt = (
        select(dist_table)
        .where(dist_table.c.run_id == run_id)
        .order_by(dist_table.c.score_bucket)
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_score_distribution: {str(e)}"
        
    if df.empty:
        return "No distribution data found for this run."
        
    return df.to_markdown(index=False)

@tool
def get_rxc_summary(run_id: str, limit: int = 20) -> str:
    """
    Get top RXCs (Prescription Drug Categories) for a run.
    Returns RXC codes, prevalence, and score contributions.
    
    Args:
        run_id: The UUID of the scoring run
        limit: Max number of RXCs to return (default: 20)
    """
    md = MetaData()
    rxc_table = Table(
        "run_rxc_summary", 
        md, 
        Column("run_id", String),
        Column("rxc_code", String),
        Column("member_count", Integer),
        Column("prevalence", Float),
        Column("avg_score_contribution", Float),
        Column("total_score_contribution", Float),
        schema="main_analytics"
    )

    stmt = (
        select(rxc_table)
        .where(rxc_table.c.run_id == run_id)
        .order_by(rxc_table.c.member_count.desc())
        .limit(limit)
    )

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_rxc_summary: {str(e)}"
        
    if df.empty:
        return "No RXC summary data found for this run."
        
    return df.to_markdown(index=False)

@tool
def get_score_by_dimension(run_id: str, dimension: str = None) -> str:
    """
    Get risk scores aggregated by a specific dimension (e.g., 'age_band', 'gender').
    
    Args:
        run_id: The UUID of the scoring run
        dimension: Optional dimension to filter by (e.g. 'age_band', 'gender'). 
                   If None, returns all dimensions.
    """
    md = MetaData()
    dim_table = Table(
        "run_score_by_dim", 
        md, 
        Column("run_id", String),
        Column("dimension", String),
        Column("dimension_value", String),
        Column("member_count", Integer),
        Column("avg_score", Float),
        Column("min_score", Float),
        Column("max_score", Float),
        schema="main_analytics"
    )

    stmt = select(dim_table).where(dim_table.c.run_id == run_id)
    
    if dimension:
        stmt = stmt.where(dim_table.c.dimension == dimension)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_score_by_dim: {str(e)}"
        
    if df.empty:
        return "No dimension data found for this run."
        
    return df.to_markdown(index=False)

@tool
def get_comparison_by_dimension(run_id_a: str, run_id_b: str, dimension: str = None) -> str:
    """
    Get comparison of two runs aggregated by dimension.
    Requires that a comparison has already been run between these two IDs.
    
    Args:
        run_id_a: The first run UUID (baseline)
        run_id_b: The second run UUID (comparison)
        dimension: Optional dimension to filter by.
    """
    md = MetaData()
    comp_table = Table(
        "run_comparison_by_dim", 
        md, 
        Column("batch_id", String),
        Column("run_id_a", String),
        Column("run_id_b", String),
        Column("dimension", String),
        Column("dimension_value", String),
        Column("member_count", Integer),
        Column("avg_score_a", Float),
        Column("avg_score_b", Float),
        Column("avg_diff", Float),
        schema="main_analytics"
    )

    stmt = select(comp_table).where(
        (comp_table.c.run_id_a == run_id_a) & 
        (comp_table.c.run_id_b == run_id_b)
    )
    
    if dimension:
        stmt = stmt.where(comp_table.c.dimension == dimension)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying run_comparison_by_dim: {str(e)}"
        
    if df.empty:
        stmt = stmt.where(comp_table.c.dimension == dimension)

    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
        
    if df.empty:
        return "No comparison data found for these runs. A comparison job may need to be run first."
        
    return df.to_markdown(index=False)


@tool
def get_decomposition_results(batch_id: str) -> str:
    """
    Get decomposition results for a specific batch.
    Returns driver names and their impact values.
    
    Args:
        batch_id: The UUID of the decomposition batch (from list_runs(analysis_type='decomposition'))
    """
    md = MetaData()
    scenarios = Table(
        "decomposition_scenarios", 
        md, 
        Column("batch_id", String),
        Column("driver_name", String),
        Column("impact_value", Float),
        Column("run_id", String),
        schema="main_analytics"
    )

    stmt = select(scenarios).where(scenarios.c.batch_id == batch_id)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying decomposition_scenarios: {str(e)}"
        
    if df.empty:
        return "No decomposition results found for this batch_id."
        
    return df.to_markdown(index=False)


@tool
def query_risk_scores(run_id: str, filters: str = "{}", limit: int = 100) -> str:
    """
    Query member-level risk scores from a run.
    
    WARNING: This queries granular member-level data. 
    Prefer using get_run_summary or other aggregate tools first.
    
    Args:
        run_id: The UUID of the scoring run
        filters: JSON string of filters (e.g. '{"min_score": 1.0}')
        limit: Max rows to return (default: 100)
    """
    try:
        filter_dict = json.loads(filters)
    except json.JSONDecodeError:
        return "Error: filters must be a valid JSON string."

    md = MetaData()
    risk_scores = Table(
        "risk_scores", 
        md, 
        Column("run_id", String),
        Column("member_id", String),
        Column("risk_score", Float),
        Column("hcc_list", JSON),
        schema="main_runs"
    )

    stmt = select(
        risk_scores.c.member_id, 
        risk_scores.c.risk_score,
        risk_scores.c.hcc_list
    ).where(risk_scores.c.run_id == run_id)

    # Apply filters
    if "min_score" in filter_dict:
        stmt = stmt.where(risk_scores.c.risk_score >= float(filter_dict["min_score"]))
    if "max_score" in filter_dict:
        stmt = stmt.where(risk_scores.c.risk_score <= float(filter_dict["max_score"]))
    if "member_id" in filter_dict:
        stmt = stmt.where(risk_scores.c.member_id == filter_dict["member_id"])

    stmt = stmt.limit(limit)

    try:
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn)
    except Exception as e:
        return f"Error querying risk_scores: {str(e)}"
    
    if df.empty:
        return "No records found matching criteria."

    return df.to_markdown(index=False)
