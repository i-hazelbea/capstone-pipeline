/*
 * Model: vw_company_year_trends
 * Layer: analytics (view)
 * Grain: one row per (production_company, release_year)
 *
 * Purpose:
 *   - Provide BI-ready yearly trend output for company performance analysis.
 *   - Classify annual performance into business-friendly trend bands.
 *
 * Source:
 *   - Derived from fct_company_year_performance.
 */

WITH sequenced AS (
    SELECT
        f.production_company_key,
        f.production_company,
        f.release_year_start_date_key,
        f.release_year,
        f.movies_released,
        f.total_revenue,
        f.avg_revenue,
        f.weighted_avg_rating,
        f.avg_vote_count,
        f.revenue_yoy_pct,
        f.rating_yoy_delta,
        f.growth_and_rating_improved,
        f.record_loaded_at,
        LAG(f.release_year) OVER (
            PARTITION BY f.production_company_key
            ORDER BY f.release_year
        ) AS prior_observed_year
    FROM {{ ref('fct_company_year_performance') }} AS f
    WHERE f.release_year BETWEEN 1990 AND 2024
)
SELECT
    s.production_company_key,
    s.production_company,
    s.release_year_start_date_key,
    s.release_year,
    s.prior_observed_year,
    ROUND(s.total_revenue, 2) AS total_revenue,
    ROUND(s.avg_revenue, 2) AS avg_revenue,
    ROUND(s.weighted_avg_rating, 4) AS weighted_avg_rating,
    ROUND(s.avg_vote_count, 2) AS avg_vote_count,
    ROUND(s.revenue_yoy_pct, 2) AS revenue_yoy_pct,
    ROUND(s.rating_yoy_delta, 4) AS rating_yoy_delta,
    s.growth_and_rating_improved,
    CASE
        WHEN s.prior_observed_year IS NULL THEN 'no_baseline'
        WHEN s.revenue_yoy_pct >= 20 AND s.rating_yoy_delta >= 0 THEN 'outperforming'
        WHEN s.revenue_yoy_pct >= 0 AND s.rating_yoy_delta >= 0 THEN 'stable_positive'
        WHEN s.revenue_yoy_pct < 0 AND s.rating_yoy_delta >= 0 THEN 'rating_up_revenue_down'
        WHEN s.revenue_yoy_pct >= 0 AND s.rating_yoy_delta < 0 THEN 'revenue_up_rating_down'
        ELSE 'declining'
    END AS performance_band,
    s.record_loaded_at
FROM sequenced AS s
ORDER BY s.production_company, s.release_year
