/*
 * Model: fct_company_year_performance
 * Layer: marts (fact)
 * Grain: one row per (production_company, release_year)
 *
 * Purpose:
 *   - Store company-year performance metrics for trend and YoY analysis.
 *   - Support portfolio-level consistency scoring and growth diagnostics.
 *
 * Behavior:
 *   - Uses intermediate company-year metrics.
 *   - Joins with production company dimension for stable surrogate key and date foreign key.
 *   - Flags years where both revenue and rating improved.
 */
SELECT
    d.production_company_key,
    i.production_company,
    (i.release_year::TEXT || '0101')::INT AS release_year_start_date_key,
    MAKE_DATE(i.release_year, 1, 1) AS release_year_start_date,
    i.release_year,
    i.movies_released,
    ROUND(i.total_revenue, 2) AS total_revenue,
    ROUND(i.avg_revenue, 2) AS avg_revenue,
    ROUND(i.weighted_avg_rating, 4) AS weighted_avg_rating,
    ROUND(i.avg_vote_count, 2) AS avg_vote_count,
    ROUND(i.prior_year_revenue, 2) AS prior_year_revenue,
    ROUND(i.prior_year_rating, 4) AS prior_year_rating,
    ROUND(i.revenue_yoy_pct, 2) AS revenue_yoy_pct,
    ROUND(i.rating_yoy_delta, 4) AS rating_yoy_delta,
    CASE
        WHEN i.revenue_yoy_pct IS NOT NULL
             AND i.rating_yoy_delta IS NOT NULL
             AND i.revenue_yoy_pct > 0
             AND i.rating_yoy_delta >= 0
            THEN TRUE
        ELSE FALSE
    END AS growth_and_rating_improved,
    i.record_loaded_at
FROM {{ ref('int_company_year_metrics') }} AS i
JOIN {{ ref('dim_production_companies') }} AS d
    ON LOWER(TRIM(i.production_company)) = LOWER(TRIM(d.production_company))
