-- Budget Correction Audit
-- Purpose: Identify all movies where budget/revenue were auto-corrected due to scaling errors
-- Use this to validate the correction logic and spot false positives
-- 
-- To run: dbt run-operation audit_budget_corrections

SELECT
    imc.movie_id,
    imc.title,
    imc.release_year,
    imc.production_company,
    CASE
        WHEN imc.budget_correction_flag = 'both_scaled_×1m' THEN imc.revenue_amount / 1000000
        ELSE imc.revenue_amount
    END AS revenue_original,
    imc.revenue_amount AS revenue_corrected,
    CASE 
        WHEN imc.budget_correction_flag != 'original'
        THEN (
            CASE
                WHEN imc.budget_correction_flag IN ('both_scaled_×1m', 'budget_scaled_×1m') THEN imc.budget_amount / 1000000
                WHEN imc.budget_correction_flag = 'budget_scaled_×1k' THEN imc.budget_amount / 1000
                ELSE imc.budget_amount
            END
        )
        ELSE imc.budget_amount
    END AS budget_original,
    imc.budget_amount AS budget_corrected,
    ROUND(CASE WHEN imc.budget_amount > 0 THEN (imc.revenue_amount / imc.budget_amount) ELSE NULL END, 2) AS roi_corrected,
    imc.budget_correction_flag,
    imc.record_loaded_at
FROM {{ ref('int_movie_company_bridge') }} AS imc
WHERE imc.budget_correction_flag != 'original'
ORDER BY imc.budget_correction_flag, imc.production_company, imc.release_year, imc.title
