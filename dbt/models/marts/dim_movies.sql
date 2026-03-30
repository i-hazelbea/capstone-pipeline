/*
 * Model: dim_movies
 * Layer: marts (dimension)
 * Grain: one row per movie_id
 *
 * Purpose:
 *   - Store descriptive movie attributes and stable conformed keys.
 *   - Keep this model descriptive; avoid business performance metrics here.
 *
 * Notes:
 *   - release_date_key links to dim_date.
 *   - ROI is computed in fact models (for clearer dimensional modeling).
 */

SELECT
    m.movie_id,
    m.title,
    CASE
        WHEN m.release_date IS NOT NULL THEN TO_CHAR(m.release_date, 'YYYYMMDD')::INT
        ELSE NULL
    END AS release_date_key,
    m.release_date,
    m.budget_amount,
    m.revenue_amount,
    e.genres,
    e.production_countries,
    e.production_companies,
    e.spoken_languages,
    GREATEST(m.loaded_at, e.loaded_at) AS record_loaded_at
FROM {{ ref('stg_movies_main') }} AS m
LEFT JOIN {{ ref('stg_movie_extended') }} AS e
    ON m.movie_id = e.movie_id
