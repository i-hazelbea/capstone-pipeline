/*
 * Model: stg_ratings
 * Layer: staging
 * Grain: one row per movie_id
 *
 * Purpose:
 *   - Deduplicate staged ratings by latest loaded_at.
 *   - Clean numeric text fields and cast to numeric types.
 *   - Normalize ratings to a consistent 0-10 scale.
 *   - Add distribution-based normalized scores for comparability.
 *
 * Output columns:
 *   - avg_rating: harmonized rating on 0..10 scale
 *   - avg_rating_minmax: min-max normalization to 0..1
 *   - avg_rating_zscore: z-score normalization
 */

WITH src AS (
    SELECT
        CASE
            WHEN NULLIF(TRIM(movie_id), '') ~ '^\d+$' THEN NULLIF(TRIM(movie_id), '')::BIGINT
            ELSE NULL
        END AS movie_id,
        avg_rating,
        total_ratings,
        stg_dev,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY CASE
                WHEN NULLIF(TRIM(movie_id), '') ~ '^\d+$' THEN NULLIF(TRIM(movie_id), '')::BIGINT
                ELSE NULL
            END
            ORDER BY NULLIF(TRIM(loaded_at), '') DESC NULLS LAST
        ) AS rn
    FROM {{ source('mm_staging', 'ratings') }}
),
dedup AS (
    SELECT *
    FROM src
    WHERE rn = 1 AND movie_id IS NOT NULL
),
cleaned AS (
    SELECT
        movie_id,
        NULLIF(REGEXP_REPLACE(avg_rating, '[^0-9.-]', '', 'g'), '') AS avg_rating_raw,
        NULLIF(REGEXP_REPLACE(total_ratings, '[^0-9.-]', '', 'g'), '') AS total_ratings_raw,
        NULLIF(REGEXP_REPLACE(stg_dev, '[^0-9.-]', '', 'g'), '') AS std_dev_raw,
        NULLIF(TRIM(loaded_at), '') AS loaded_at_raw
    FROM dedup
),
typed AS (
    SELECT
        movie_id,
        CASE
            WHEN avg_rating_raw IS NULL THEN NULL
            WHEN avg_rating_raw::NUMERIC < 0 THEN NULL
            WHEN avg_rating_raw::NUMERIC <= 5 THEN avg_rating_raw::NUMERIC * 2
            WHEN avg_rating_raw::NUMERIC <= 10 THEN avg_rating_raw::NUMERIC
            WHEN avg_rating_raw::NUMERIC <= 100 THEN avg_rating_raw::NUMERIC / 10
            ELSE NULL
        END AS avg_rating_10_scale,
        CASE WHEN total_ratings_raw IS NOT NULL THEN total_ratings_raw::INTEGER ELSE NULL END AS total_ratings,
        CASE WHEN std_dev_raw IS NOT NULL THEN std_dev_raw::NUMERIC(10, 4) ELSE NULL END AS std_dev,
        CASE WHEN loaded_at_raw IS NOT NULL THEN loaded_at_raw::TIMESTAMP ELSE NULL END AS loaded_at
    FROM cleaned
),
rating_stats AS (
    SELECT
        MIN(avg_rating_10_scale) AS min_rating,
        MAX(avg_rating_10_scale) AS max_rating,
        AVG(avg_rating_10_scale) AS mean_rating,
        STDDEV_SAMP(avg_rating_10_scale) AS std_rating
    FROM typed
    WHERE avg_rating_10_scale IS NOT NULL
)
SELECT
    t.movie_id,
    CASE WHEN t.avg_rating_10_scale IS NOT NULL THEN ROUND(t.avg_rating_10_scale, 4) ELSE NULL END AS avg_rating,
    t.total_ratings,
    t.std_dev,
    CASE
        WHEN t.avg_rating_10_scale IS NULL OR s.max_rating IS NULL OR s.min_rating IS NULL OR s.max_rating = s.min_rating THEN NULL
        ELSE ROUND((t.avg_rating_10_scale - s.min_rating) / NULLIF(s.max_rating - s.min_rating, 0), 6)
    END AS avg_rating_minmax,
    CASE
        WHEN t.avg_rating_10_scale IS NULL OR s.std_rating IS NULL OR s.std_rating = 0 THEN NULL
        ELSE ROUND((t.avg_rating_10_scale - s.mean_rating) / s.std_rating, 6)
    END AS avg_rating_zscore,
    t.loaded_at
FROM typed AS t
CROSS JOIN rating_stats AS s
