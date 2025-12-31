import polars as pl
import json
from pathlib import Path
import re


def generate_scenarios():
    # Base path for tables
    tables_dir = Path(__file__).parent / "diy_tables/cy2024_diy_tables"

    # Load tables
    print("Loading tables...")
    df_coeffs = pl.read_parquet(tables_dir / "table_9.parquet")
    df_icd = pl.read_parquet(tables_dir / "table_3.parquet")
    df_ndc = pl.read_parquet(tables_dir / "table_10a.parquet")

    # 1. Build Reverse Maps

    # Map CC (normalized) -> List of ICDs
    # Table 3 'cc' column format: "19.0", "35.1", "135.0"
    # We want to map "019" -> "19.0", "035_1" -> "35.1"
    cc_to_icd = {}

    # Helper to normalize Table 3 CCs to match Table 9 variable suffix style (roughly)
    # But actually, we want to go from Table 9 Variable -> Table 3 CC
    # Table 9: HHS_HCC019 -> 19.0
    # Table 9: HHS_HCC035_1 -> 35.1

    # Let's build a map from "clean" CC to ICDs first
    # Clean CC: "19", "35_1", "135"

    print("Building ICD map...")
    for row in df_icd.iter_rows(named=True):
        icd = row["icd10"]
        # Check all CC columns
        for col in ["cc", "second_cc", "third_cc"]:
            val = row.get(col)
            if val:
                # val is like "19.0" or "35.1"
                # Normalize to "019" or "035_1" style for matching?
                # Let's normalize to "19", "35_1", "135" (no leading zeros, underscores for decimals)
                val_str = str(val).strip()
                if val_str.endswith(".0"):
                    norm = val_str[:-2]
                else:
                    norm = val_str.replace(".", "_")

                if norm not in cc_to_icd:
                    cc_to_icd[norm] = []
                cc_to_icd[norm].append(icd)

    # Map RXC -> List of NDCs
    # Table 10a 'rxc' column: "1", "01", "6"
    rxc_to_ndc = {}
    print("Building NDC map...")
    for row in df_ndc.iter_rows(named=True):
        ndc = row["ndc"]
        rxc = str(row["rxc"]).strip()
        # Normalize RXC to simple string "1", "6", "10"
        rxc_norm = str(int(rxc)) if rxc.isdigit() else rxc

        if rxc_norm not in rxc_to_ndc:
            rxc_to_ndc[rxc_norm] = []
        rxc_to_ndc[rxc_norm].append(ndc)

    # 2. Identify Variables from Coefficients Table
    print("Processing variables...")
    variables = df_coeffs["variable"].unique().to_list()

    scenarios = []

    # Regex for HCCs: HHS_HCC(\d+(_\d+)?)
    hcc_pattern = re.compile(r"HHS_HCC(\d+(?:_\d+)?)")

    # Regex for RXCs: RXC_(\d+)
    rxc_pattern = re.compile(r"RXC_(\d+)")

    processed_hccs = set()
    processed_rxcs = set()

    for var in variables:
        # Check for HCC
        hcc_match = hcc_pattern.search(var)
        if hcc_match:
            raw_suffix = hcc_match.group(1)  # e.g. "019", "035_1"

            # Normalize for lookup: remove leading zeros from parts
            # "019" -> "19"
            # "035_1" -> "35_1"
            parts = raw_suffix.split("_")
            norm_parts = [str(int(p)) for p in parts]
            lookup_key = "_".join(norm_parts)

            if lookup_key not in processed_hccs:
                if lookup_key in cc_to_icd:
                    # Create Scenario
                    icd_example = cc_to_icd[lookup_key][0]
                    scenarios.append(
                        {
                            "name": f"Generated: {var} ({lookup_key})",
                            "specialty": "Internal Medicine",
                            "diagnoses": [icd_example],
                            "procedures": [],
                            "drugs": [],
                            "service_category": "Outpatient",
                            "cost_range": [1000.0, 5000.0],
                            "length_of_stay_range": [0, 0],
                            "age_range": {"min": 40, "max": 80},
                            "metadata": {"type": "HCC", "code": var},
                        }
                    )
                    processed_hccs.add(lookup_key)

        # Check for RXC
        rxc_match = rxc_pattern.search(var)
        if rxc_match:
            raw_suffix = rxc_match.group(1)  # e.g. "01", "06"
            lookup_key = str(int(raw_suffix))  # "1", "6"

            if lookup_key not in processed_rxcs:
                if lookup_key in rxc_to_ndc:
                    ndc_example = rxc_to_ndc[lookup_key][0]
                    scenarios.append(
                        {
                            "name": f"Generated: {var} ({lookup_key})",
                            "specialty": "Pharmacy",
                            "diagnoses": [],  # RXCs don't strictly require dx for the score component itself (though interactions do)
                            "procedures": [],
                            "drugs": [ndc_example],
                            "service_category": "Pharmacy",
                            "cost_range": [500.0, 2000.0],
                            "length_of_stay_range": [0, 0],
                            "age_range": {"min": 40, "max": 80},
                            "metadata": {"type": "RXC", "code": var},
                        }
                    )
                    processed_rxcs.add(lookup_key)

    # 3. Output
    output_data = {"generated_comprehensive": scenarios}

    print(f"Generated {len(scenarios)} scenarios.")

    output_path = Path(__file__).parent / "scenarios_comprehensive.json"
    with open(output_path, "w") as f:
        json.dump(output_data, f, indent=2)
    print(f"Wrote to {output_path}")


if __name__ == "__main__":
    generate_scenarios()
