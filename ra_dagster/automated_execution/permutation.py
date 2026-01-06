"""
Configuration Permutation Generator (CLI Tool).

This script automatically generates exhaustive configuration permutations for testing purposes.
It inspects the Pydantic configuration schemas defined in the project and creates a specific
configuration file for every possible combination of enumerated options.

Purpose:
    - To facilitate "parameter sweeps" for Risk Adjustment scoring.
    - To ensure comprehensive testing of all valid configuration states.
    - To generate inputs for batch execution via `launch_analyses.py`.

Usage:
    Run from the workspace root:
    $ python ra_dagster/automated_execution/permutation.py

    Output:
    YAML files written to `ra_dagster/configs/permutations/`.
"""

import itertools
import os
import sys
import yaml
import shutil
from pathlib import Path
from typing import Type, Any, Dict, List, Union, get_origin, get_args
import importlib.util
import inspect
from enum import Enum
import types

# Add workspace root to python path
sys.path.append(os.getcwd())

# Load config_schemas directly to avoid circular imports via ra_dagster/__init__.py
spec = importlib.util.spec_from_file_location("config_schemas_standalone", "ra_dagster/config_schemas.py")
module = importlib.util.module_from_spec(spec)
sys.modules["config_schemas_standalone"] = module # Mock the module to solve relative import issues if any
spec.loader.exec_module(module)

ScoringConfig = module.ScoringConfig
ComparisonConfig = module.ComparisonConfig
DecompositionConfig = module.DecompositionConfig
DecompositionComponent = module.DecompositionComponent

# Mapping of Op/Asset Name to Config Class
CONFIG_MAPPING = {
    "score_runs": ScoringConfig,
    "compare_runs": ComparisonConfig,
    "decompose_runs": DecompositionConfig,
}

OUTPUT_DIR = Path("ra_dagster/configs/permutations")

def is_enum(type_obj):
    return inspect.isclass(type_obj) and issubclass(type_obj, Enum)

def try_extract_enum_and_optionality(annotation):
    """
    Check if annotation is an Enum or Optional[Enum] / Enum | None.
    Returns (EnumClass, is_optional)
    """
    # Direct Enum
    if is_enum(annotation):
        return annotation, False
        
    # Check for Union (Optional or |)
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)
        found_enum = None
        is_opt = type(None) in args
        
        for arg in args:
            if is_enum(arg):
                found_enum = arg
                break
        
        if found_enum:
            return found_enum, is_opt
    
    return None, False

def get_enum_values(enum_cls):
    return [e.value for e in enum_cls]

def generate_permutations(op_name: str, config_cls: Type[Any]):
    # Pydantic model inspection
    # Dagster 1.5+ uses Pydantic 2 compat layer, or exposes model_fields as dict
    try:
        fields = config_cls.model_fields
    except AttributeError:
        # Fallback for Pydantic v1
        fields = config_cls.__fields__
    
    varying_fields = {}
    static_fields = {}
    
    iterator = fields.items() if isinstance(fields, dict) else fields.items()
    
    for name, field in iterator:
        # dagster.Config uses pydantic.fields.FieldInfo
        # In Pydantic v1 vs v2 this differs. Assuming v2 style access or compatible.
        # But wait, Dagster might wrap things.
        
        # Let's check the annotation type
        annotation = field.annotation
        
        # Helper to unwrap Optional[T] if needed, but for now assuming direct Enums
        # Most of our Enums are direct types in the properties
        
        enum_cls, is_opt = try_extract_enum_and_optionality(annotation)
        if enum_cls:
            vals = get_enum_values(enum_cls)
            if is_opt:
                vals.append(None)
            varying_fields[name] = vals
        elif annotation is bool:
            # Vary bools explicitly
            varying_fields[name] = [True, False]
        else:
            # Handle Required vs Default
            if field.is_required():
                # Provide placeholder
                if annotation == str:
                    static_fields[name] = "PLACEHOLDER"
                elif annotation == int:
                    static_fields[name] = 0
                elif annotation == list:
                    static_fields[name] = []
                else:
                    static_fields[name] = None
            else:
                # Value is optional, we can skip it or use default
                # field.default might be PydanticUndefined if required
                # if not required, it has a default
                static_fields[name] = field.default

    # Generate Cartesian Product
    keys = list(varying_fields.keys())
    values = list(varying_fields.values())
    
    print(f"DEBUG: {op_name} varying keys: {keys}")
    for k, v in varying_fields.items():
        print(f"DEBUG:   {k} ({len(v)} values): {v}")

    permutations = list(itertools.product(*values))
    
    print(f"Generating {len(permutations)} permutations for {op_name}...")
    
    for idx, combo in enumerate(permutations):
        config_dict = static_fields.copy()
        for k, v in zip(keys, combo):
            config_dict[k] = v
            
        # Expansion logic: Create multiple variations from this single permutation if needed
        variations = [config_dict]
        
        # 1. Expand ScoringConfig: member_age_basis_year = diy_model_year +/- 2
        if op_name == "score_runs" and "diy_model_year" in config_dict:
            model_year = int(config_dict["diy_model_year"])
            base_config = config_dict
            variations = []
            for offset in range(-2, 3): # -2, -1, 0, 1, 2
                new_conf = base_config.copy()
                new_conf["member_age_basis_year"] = str(model_year + offset)
                variations.append(new_conf)

        for var_config in variations:
            # Skip if invalid_gender is set to error (explicit user request)
            if var_config.get("invalid_gender") == "error":
                continue

            # Construct YAML structure
            yaml_data = {
                "ops": {
                    op_name: {
                        "config": var_config
                    }
                }
            }
            
            # Create descriptive filename from varying fields + specific expansions
            filename_parts = []
            
            # Add the standard varying fields
            # We need to extract them from var_config because keys/combo is for the base permutation
            for k in keys:
                filename_parts.append(f"{k}-{var_config[k]}")
            
            # Add custom expansion fields
            if op_name == "score_runs":
                filename_parts.append(f"age_basis-{var_config['member_age_basis_year']}")

            if filename_parts:
                filename = f"{op_name}__" + "__".join(filename_parts) + ".yaml"
            else:
                filename = f"{op_name}_default.yaml"
            
            filepath = OUTPUT_DIR / filename
            
            with open(filepath, "w") as f:
                f.write(f"# Auto-generated permutation based on {idx+1} for {op_name}\n")
                yaml.dump(yaml_data, f, sort_keys=False, default_flow_style=False)

def main():
    # Clean output dir
    if OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.glob("*.yaml"):
            f.unlink()
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
    for op_name, config_cls in CONFIG_MAPPING.items():
        generate_permutations(op_name, config_cls)
        
    print(f"Done. Files written to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
