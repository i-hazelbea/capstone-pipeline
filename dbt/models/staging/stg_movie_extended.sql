/*
 * Model: stg_movie_extended
 * Layer: staging
 * Grain: one row per movie_id
 *
 * Purpose:
 *   - Filters out non-numeric id values and handles empty strings using TRIM and NULLIF
 *   - Uses ROW_NUMBER to keep only the most recent record per movie_id based on loaded_at
 *   - Cleans production_companies by replacing pipe delimeters with commas
 *   - Converts the cleaned movie_id to BIGINT and loaded_ta to TIMESTAMP
 *
 * Output columns:
 *   - movie_id - the unique identifier for each file cast
 *   - genres - a string containing movie categories.
 *   - production_countries - list of countries involved in the production
 *   - spoken_languages - languages used within the movie
 *   - loaded_at - the timestamp indicating when the data was ingested into the staging layer
 */

WITH src AS (
    SELECT
        CASE
            WHEN NULLIF(TRIM(id), '') ~ '^\d+$' THEN NULLIF(TRIM(id), '')::BIGINT
            ELSE NULL
        END AS movie_id,
        genres,
        production_countries,
        production_companies,
        spoken_languages,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY CASE
                WHEN NULLIF(TRIM(id), '') ~ '^\d+$' THEN NULLIF(TRIM(id), '')::BIGINT
                ELSE NULL
            END
            ORDER BY loaded_at DESC NULLS LAST
        ) AS rn
    FROM {{ source('mm_staging', 'movie_extended') }}
)
SELECT
    movie_id,
    NULLIF(TRIM(genres), '') AS genres,
    NULLIF(TRIM(production_countries), '') AS production_countries,
    NULLIF(REGEXP_REPLACE(TRIM(production_companies), '\\s*\\|\\s*', ',', 'g'), '') AS production_companies,
    NULLIF(TRIM(spoken_languages), '') AS spoken_languages,
    loaded_at::TIMESTAMP AS loaded_at
FROM src
WHERE rn = 1 AND movie_id IS NOT NULL
