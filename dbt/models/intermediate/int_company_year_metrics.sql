/*
 * Model: int_company_year_metrics
 * Layer: intermediate
 * Grain: one row per (production_company, release_year)
 *
 * Purpose:
 *   - Build reusable company-year performance metrics.
 *   - Prepare prior-year baselines for YoY trend analysis.
 *
 * Behavior:
 *   - Aggregates bridge rows by company and year.
 *   - Computes revenue, weighted ratings, and vote-volume metrics.
 *   - Uses window lag to compute prior-year references and YoY deltas.
 */

WITH company_year AS (
    SELECT
        production_company,
        release_year,
        COUNT(DISTINCT movie_id) AS movies_released,
        ROUND(SUM(CASE WHEN revenue_amount > 0 THEN revenue_amount ELSE 0 END), 2) AS total_revenue,
        ROUND(AVG(NULLIF(revenue_amount, 0)), 2) AS avg_revenue,
        CASE
            WHEN SUM(COALESCE(total_ratings, 0)) > 0 THEN
                ROUND(
                    SUM(COALESCE(avg_rating, 0) * COALESCE(total_ratings, 0))
                    / SUM(COALESCE(total_ratings, 0)),
                    4
                )
            ELSE ROUND(AVG(avg_rating), 4)
        END AS weighted_avg_rating,
        ROUND(AVG(COALESCE(total_ratings, 0)), 2) AS avg_vote_count,
        MAX(record_loaded_at) AS record_loaded_at
    FROM {{ ref('int_movie_company_bridge') }}
    WHERE release_year BETWEEN 1990 AND 2024
    GROUP BY production_company, release_year
),
with_lag AS (
    SELECT
        production_company,
        release_year,
        movies_released,
        total_revenue,
        avg_revenue,
        weighted_avg_rating,
        avg_vote_count,
        LAG(total_revenue) OVER (
            PARTITION BY production_company
            ORDER BY release_year
        ) AS prior_year_revenue,
        LAG(weighted_avg_rating) OVER (
            PARTITION BY production_company
            ORDER BY release_year
        ) AS prior_year_rating,
        record_loaded_at
    FROM company_year
)
SELECT
    production_company,
    release_year,
    movies_released,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(avg_revenue, 2) AS avg_revenue,
    ROUND(weighted_avg_rating, 4) AS weighted_avg_rating,
    ROUND(avg_vote_count, 2) AS avg_vote_count,
    ROUND(prior_year_revenue, 2) AS prior_year_revenue,
    ROUND(prior_year_rating, 4) AS prior_year_rating,
    CASE
        WHEN prior_year_revenue IS NULL OR prior_year_revenue = 0 THEN NULL
        ELSE ROUND(((total_revenue - prior_year_revenue) / prior_year_revenue) * 100.0, 2)
    END AS revenue_yoy_pct,
    CASE
        WHEN prior_year_rating IS NULL THEN NULL
        ELSE ROUND(weighted_avg_rating - prior_year_rating, 4)
    END AS rating_yoy_delta,
    record_loaded_at
FROM with_lag
