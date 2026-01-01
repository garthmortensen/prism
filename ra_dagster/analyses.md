# Risk Adjustment Exploratory Analyses

***Using a platform like Prism, the following analyses are quick and simple.***

## Analyses

This document outlines high-value exploratory analyses that can be performed using the `score_members_aca` asset by manipulating input views (`claims_view`, `enrollments_view`) or configuration parameters (`diy_model_year`).

| Analysis          | Variable Changed                | Config Field       |
| ----------------- | ------------------------------- | ------------------ |
| Coding Intensity  | Claims Data                     | `claims_view`      |
| Claims Lag        | Claims Data (Time filtered)     | `claims_view`      |
| Metal Level Sim   | Enrollment Data (Plan ID)       | `enrollments_view` |
| Audit Risk        | Claims Data (Provider filtered) | `claims_view`      |
| Regulatory Change | Scoring Logic                   | `diy_model_year`   |

## 1. "What-If" Coding Intensity & Chart Review Impact

Simulate the impact of aggressive vs. passive chart reviews or coding improvement initiatives.

*   **The Setup:** Create a view `main_raw.raw_claims_2024_enhanced` where you artificially add diagnosis codes (e.g., adding "Diabetes with complications" where only "Diabetes" existed, or adding codes suspected from lab data).
*   **The Comparison:** Run scoring on `raw_claims_2024` vs. `raw_claims_2024_enhanced`.
*   **The Insight:** "If we successfully capture the missing specificity for 10% of our diabetics, our RAF score increases by 0.02."
*   **Config Change:** `claims_view`

## 2. Lag/Runout Analysis (IBNR Simulation)
Risk scores mature over time as claims lag settles. Quantify the value of "completeness."

*   **The Setup:** Create views based on `paid_date`.
    *   `claims_2024_3months`: Claims incurred in 2024, paid by March 2024.
    *   `claims_2024_6months`: Claims incurred in 2024, paid by June 2024.
    *   `claims_2024_9months`: Claims incurred in 2024, paid by September 2024.
    *   `claims_2024_final`: Full runout.
*   **The Comparison:** Score all three against the same model.
*   **The Insight:** "We are currently 85% complete on risk capture compared to our 6-month projection. We need to accrue for the remaining 15%."
*   **Config Change:** `claims_view`

## 3. Benefit Design Impact (The "Metal Level" Effect)
ACA risk adjustment models (HHS-HCC) behave differently depending on the plan's metal level (Platinum, Gold, Silver, Bronze, Catastrophic) because the coefficients change (induced demand).

*   **The Setup:** Create a view `members_simulated_silver` where you force every member's metal level to Silver, and `members_simulated_bronze` where everyone is Bronze.
*   **The Comparison:** Score the *exact same population* with the *exact same diagnoses* against different metal tiers.
*   **The Insight:** "Our population is sicker than average; moving them to Bronze plans would lower premiums but the risk transfer payment drop might outweigh the premium savings."
*   **Config Change:** `members_view`

## 4. Truncation / Capping Analysis (Audit Risk)
Assess the impact of "outliers" or data quality issues, such as diagnoses from high-risk provider types.

*   **The Setup:** Create a claims view that filters out diagnoses from specific provider types (e.g., "exclude telehealth" or "exclude urgent care") or specific claim types.
*   **The Comparison:** Score `all_claims` vs. `high_confidence_claims`.
*   **The Insight:** "15% of our risk score is driven by diagnoses that only appear on telehealth visits, which are at high risk of RADV audit failure."
*   **Config Change:** `claims_view`

## 5. Model Version Cross-Walking (The "Regulatory Cliff")
Analyze the impact of regulatory model changes (e.g., V24 to V28).

*   **The Setup:** Use the existing `diy_model_year` configuration.
*   **The Comparison:** Run the *exact same 2024 data* through `diy_model_year: "2023"` (V24) and `diy_model_year: "2024"` (V28).
*   **The Insight:** "The transition to V28 is causing a 4% drop in scores specifically due to the removal of HCC 47 (Diabetes w/o complications). We need a clinical program to address specificity."
*   **Config Change:** `diy_model_year`

## 6. Monte Carlo RADV Simulation
Quantify uncertainty by simulating the probability of claim rejection during an audit (RADV).

*   **The Setup:**
    1.  Create a "Stacked" view (`claims_monte_carlo_radv`) that replicates the population $N$ times (e.g., 50 simulations).
    2.  Assign a confidence score to each claim type (e.g., Inpatient=0.98, Telehealth=0.60).
    3.  In each simulation, randomly drop claims based on their confidence score.
    4.  Create matching stacked views for members and enrollments to ensure join keys match (`member_id + '_sim_' + sim_id`).

    **Example SQL for Claims View:**
    ```sql
    with raw_claims as (
        select *,
            -- Assign confidence based on claim type (simplified logic)
            case 
                when claim_type = 'inpatient' then 0.98
                when claim_type = 'professional' then 0.85
                else 0.70 
            end as audit_confidence
        from {{ ref('raw_claims') }}
    ),
    simulations as (
        -- Generate 50 simulation IDs
        select unnest(generate_series(1, 50)) as sim_id
    )
    select 
        -- Create unique member IDs for each simulation so the scorer treats them as distinct
        c.member_id || '_sim_' || s.sim_id as member_id,
        c.claim_id,
        c.diagnosis_code,
        c.claim_type,
        c.clean_claim_out,
        s.sim_id
    from raw_claims c
    cross join simulations s
    -- The Monte Carlo Step: Keep claim if random roll is less than confidence
    where random() < c.audit_confidence
    ```

    **Example SQL for Members View:**
    ```sql
    select 
        m.member_id || '_sim_' || s.sim_id as member_id,
        m.gender,
        m.dob,
        s.sim_id
    from {{ ref('raw_members') }} m
    cross join (select unnest(generate_series(1, 50)) as sim_id) s
    ```

*   **The Execution:** Run a single scoring job on this "exploded" population.
*   **The Analysis:** Group the results by `sim_id` to generate a distribution of RAF scores (Mean, P5, P95).
*   **The Insight:** "Our raw score is 1.25, but after accounting for audit risk, our P95 floor is 1.18."
*   **Config Change:** `claims_view`, `members_view`, `enrollments_view`
