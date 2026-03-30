# Data Audit Summary

## Scope

This summary captures current audit findings and operational guidance for the marts and analytics outputs used in reporting.

## Current Data Behavior

### 1) Revenue YoY Outliers

Revenue YoY percentages can be very large when prior-year revenue is very small. These values are mathematically valid and expected from the formula.

Operational guidance:

- Use studio-level pre-aggregated fields from analytics views for dashboard visuals.
- Keep the raw values available in tooltips or drill-through for transparency.

### 2) Budget And Revenue Scaling Corrections

Correction logic is applied in intermediate model int_movie_company_bridge.

Implemented patterns:

- both tiny values (budget and revenue in 1-999): scale both by 1,000,000
- tiny budget with high revenue: scale budget by 1,000,000
- mid budget with extreme revenue ratio: scale budget by 1,000

Correction outcomes are tracked in budget_correction_flag.

### 3) High-Value Studio Recommendation Logic

Current decision logic in vw_high_value_studio_performance:

- Inactive override:
    - latest_year <= 2013 -> recommendation Inactive

- Weighted score components:
    - latest band score: 45%
    - dominant band score: 25%
    - years observed score: 10%
    - activity recency score: 20%

- Action thresholds:
    - Incentivise: consistency_ratio > 0.15 and weighted_decision_score >= 0.55
    - Review: consistency_ratio > 0.15 and weighted_decision_score >= 0.30
    - Renegotiate: remaining active studios not meeting thresholds
