WITH src AS (
    SELECT
        NULLIF(TRIM(movie_id), '') AS movie_id,
        NULLIF(REGEXP_REPLACE(avg_rating, '[^0-9.-]', '', 'g'), '') AS avg_rating_raw,
        NULLIF(REGEXP_REPLACE(total_ratings, '[^0-9.-]', '', 'g'), '') AS total_ratings_raw,
        NULLIF(REGEXP_REPLACE(stg_dev, '[^0-9.-]', '', 'g'), '') AS std_dev_raw,
        NULLIF(TRIM(loaded_at), '') AS loaded_at_raw,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(movie_id), '')
            ORDER BY NULLIF(TRIM(loaded_at), '') DESC NULLS LAST
        ) AS rn
    FROM {{ source('mm_staging', 'ratings') }}
),
dedup AS (
    SELECT *
    FROM src
    WHERE rn = 1 AND movie_id IS NOT NULL
)
SELECT
    movie_id,
    CASE WHEN avg_rating_raw IS NOT NULL THEN avg_rating_raw::NUMERIC(10, 4) ELSE NULL END AS avg_rating,
    CASE WHEN total_ratings_raw IS NOT NULL THEN total_ratings_raw::INTEGER ELSE NULL END AS total_ratings,
    CASE WHEN std_dev_raw IS NOT NULL THEN std_dev_raw::NUMERIC(10, 4) ELSE NULL END AS std_dev,
    CASE WHEN loaded_at_raw IS NOT NULL THEN loaded_at_raw::TIMESTAMP ELSE NULL END AS loaded_at
FROM dedup
