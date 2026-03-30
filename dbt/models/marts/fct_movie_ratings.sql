/*
 * Model: fct_movie_ratings
 * Layer: marts (fact)
 * Grain: one row per movie_id (latest staged rating record)
 *
 * Purpose:
 *   - Combine movie-level rating signals with dimensional movie attributes.
 *   - Expose normalized rating features for cross-movie comparison.
 *   - Compute performance metric ROI at fact layer (not dimension layer).
 */
SELECT
    r.movie_id,
    d.title,
    d.release_date_key,
    d.release_date,
    EXTRACT(YEAR FROM d.release_date)::INT AS release_year,
    EXTRACT(MONTH FROM d.release_date)::INT AS release_month,
    d.genres,
    d.production_countries,
    d.budget_amount,
    d.revenue_amount,
    CASE
        WHEN d.budget_amount IS NULL OR d.budget_amount = 0 OR d.revenue_amount IS NULL THEN NULL
        ELSE ROUND(((d.revenue_amount - d.budget_amount) / d.budget_amount) * 100.0, 2)
    END AS roi_pct,
    r.avg_rating,
    r.total_ratings,
    r.std_dev,
    r.avg_rating_minmax,
    r.avg_rating_zscore,
    CASE
        WHEN r.total_ratings >= 10000 THEN 'blockbuster_signal'
        WHEN r.total_ratings >= 1000 THEN 'high_interest'
        WHEN r.total_ratings >= 100 THEN 'medium_interest'
        ELSE 'low_interest'
    END AS interest_band,
    r.loaded_at AS rating_loaded_at
FROM {{ ref('stg_ratings') }} AS r
LEFT JOIN {{ ref('dim_movies') }} AS d
    ON r.movie_id = d.movie_id
