"""
Asset: config_generator.py

Generates Dagster run configurations dynamically based on a control manifest.
This asset allows for "parameter sweep" style execution management directly within
the Dagster environment.

Features:
    - Reads control logic from `ra_dagster/configs/permutations.yaml`.
    - Introspects `ScoringConfig` (and others) to discover valid options.
    - Generates Cartesian products of enabled parameters (e.g. Model Year x Metal Level).
    - Writes ready-to-execute YAML configs to `ra_dagster/configs/permutations/`.

Usage:
    Materialize the `generate_permutation_configs` asset in Dagster.
"""

import itertools
import shutil
import yaml
from pathlib import Path
from typing import Any, Type, Union, get_origin, get_args
import types
from enum import Enum

from dagster import asset, AssetExecutionContext

# Import config schemas directly since we are inside the package
from ra_dagster.config_schemas import (
    ScoringConfig,
    ComparisonConfig,
    DecompositionConfig,
    DecompositionComponent,
)

# Configuration
PERMUTATIONS_CONFIG_PATH = Path("ra_dagster/configs/permutations.yaml")
OUTPUT_DIR = Path("ra_dagster/configs/permutations")

# Mappings
CONFIG_MAPPING = {
    "score_runs": ScoringConfig,
    "compare_runs": ComparisonConfig,
    "decompose_runs": DecompositionConfig,
}

def is_enum(type_obj):
    return isinstance(type_obj, type) and issubclass(type_obj, Enum)

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

@asset
def generate_permutation_configs(context: AssetExecutionContext) -> None:
    """
    Generates Dagster run configuration files based on ra_dagster/configs/permutations.yaml.
    
    This asset:
    1. Reads the permutations control file.
    2. Inspects Pydantic config schemas for available fields and options.
    3. Filters options based on the control file (enabled flags and specific value lists).
    4. Generates Cartesian products of all valid options.
    5. Writes YAML files to ra_dagster/configs/permutations/.
    """
    
    if not PERMUTATIONS_CONFIG_PATH.exists():
        raise FileNotFoundError(f"Permutations config not found at {PERMUTATIONS_CONFIG_PATH}")

    with open(PERMUTATIONS_CONFIG_PATH, "r") as f:
        perm_settings = yaml.safe_load(f) or {}

    # Clean output dir
    if OUTPUT_DIR.exists():
        for f in OUTPUT_DIR.glob("*.yaml"):
            f.unlink()
    else:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    generated_count = 0

    for op_name, config_cls in CONFIG_MAPPING.items():
        op_settings = perm_settings.get(op_name, {})
        
        # Introspect Schema
        try:
            fields = config_cls.model_fields
        except AttributeError:
            fields = config_cls.__fields__
            
        varying_fields = {}
        static_fields = {}
        
        iterator = fields.items() if isinstance(fields, dict) else fields.items()
        
        for name, field in iterator:
            annotation = field.annotation
            
            # Check if this field is enabled in settings
            enable_key = f"enable_{name}"
            
            # Check for Enum or Bool
            enum_cls, is_opt = try_extract_enum_and_optionality(annotation)
            
            # Potential values from Schema
            schema_values = []
            if enum_cls:
                schema_values = get_enum_values(enum_cls)
                if is_opt:
                    schema_values.append(None)
            elif annotation is bool:
                schema_values = [True, False]
            
            if schema_values:
                # This is a field we can vary
                
                # Check control file
                is_enabled = op_settings.get(enable_key, True) # Default to True if not specified? Or False?
                # User prompt said "a header called enable_: True/False"
                # If key missing in yaml, assume not varying (use default)? 
                # Or assume enabled if it's an Enum?
                # Let's check if the *key* exists in settings. If not, we might fall back to default behavior.
                # But to follow the "permissions.yaml" logic strictly:
                
                if enable_key in op_settings and not op_settings[enable_key]:
                    # Explicitly disabled variation. Use default.
                    static_fields[name] = field.default
                    continue

                if enable_key not in op_settings:
                    # Not configured in yaml -> Use default static
                    if field.default is not None: # PydanticUndefined check needed ideally
                         static_fields[name] = field.default
                    else:
                         static_fields[name] = None
                    continue

                # It is enabled. Check for specific values list.
                user_values = op_settings.get(name)
                
                if user_values and isinstance(user_values, list) and len(user_values) > 0:
                    # User restricted values
                    # Validate against schema?
                    # valid_set = set(schema_values)
                    # For None, yaml loads as null.
                    
                    final_values = []
                    for v in user_values:
                        # Simple validation/conversion could go here
                        final_values.append(v)
                    varying_fields[name] = final_values
                else:
                    # Blank or empty list -> Use ALL schema values
                    varying_fields[name] = schema_values
                    
            else:
                # Static field (str, int)
                if field.is_required():
                    if annotation == str:
                        static_fields[name] = "PLACEHOLDER"
                    elif annotation == int:
                        static_fields[name] = 0
                    elif annotation == list:
                        static_fields[name] = []
                    else:
                        static_fields[name] = None
                else:
                    static_fields[name] = field.default

        # Generate Cartesian Product
        keys = list(varying_fields.keys())
        values = list(varying_fields.values())
        
        permutations = list(itertools.product(*values))
        
        context.log.info(f"Generating {len(permutations)} base permutations for {op_name}...")
        
        for idx, combo in enumerate(permutations):
            config_dict = static_fields.copy()
            for k, v in zip(keys, combo):
                config_dict[k] = v
            
            # Expansion logic (Age Basis)
            # Retaining the logic from the script
            variations = [config_dict]
            
            if op_name == "score_runs" and "diy_model_year" in config_dict:
                # Only expand if we have a valid model year
                if config_dict["diy_model_year"]:
                    model_year = int(config_dict["diy_model_year"])
                    base_conf_copy = config_dict
                    variations = []
                    for offset in range(-2, 3): 
                        new_conf = base_conf_copy.copy()
                        new_conf["member_age_basis_year"] = str(model_year + offset)
                        variations.append(new_conf)

            for var_config in variations:
                # Construct YAML structure
                yaml_data = {
                    "ops": {
                        op_name: {
                            "config": var_config
                        }
                    }
                }
                
                # Naming
                filename_parts = []
                for k in keys:
                    filename_parts.append(f"{k}-{var_config[k]}")
                
                if op_name == "score_runs" and "member_age_basis_year" in var_config:
                    filename_parts.append(f"age_basis-{var_config['member_age_basis_year']}")

                # Filename safety
                safe_parts = [str(p).replace(" ", "_").replace("/", "-") for p in filename_parts]
                
                if safe_parts:
                    filename = f"{op_name}__" + "__".join(safe_parts) + ".yaml"
                else:
                    filename = f"{op_name}_default_{idx}.yaml"
                
                filepath = OUTPUT_DIR / filename
                
                with open(filepath, "w") as f:
                    f.write(f"# Auto-generated via permutation_generator asset\n")
                    yaml.dump(yaml_data, f, sort_keys=False, default_flow_style=False)
                    generated_count += 1

    context.log.info(f"Successfully generated {generated_count} config files in {OUTPUT_DIR}")

