/*
 * Model: stg_movie_extended
 * Layer: staging
 * Grain: one row per movie_id
 *
 * Purpose:
 *   - Utilizes a Common Table Expression (CTE) with ROW_NUMBER to isolate the most recent entry per movie_id based on ingestion time
 *   - Filters out non-numeric or empty strings to ensure movie_id integrity.
 *   - Strips non-numeric characters from budget and revenue fields using regular expressions.
 *   - Applies conditional logic to parse multiple date formats (ISO, US, and European) into a unified date type.
 *   - Converts raw string data into appropriate numeric and timestamp formats for downstream analysis.
 *
 * Output columns:
 *   - movie_id - the unique identifier for each file cast
 *   - title - the cleaned name of the film
 *   - release_date - the standardized date of release
 *   - budget_amount - the numeric representation of the film's budget
 *   - revenue_amount - the numeric representation of the film's revenue
 *   - loaded_at - the timestamp indicating when the data was ingested into the staging layer
 */
WITH src AS (
    SELECT
        CASE
            WHEN NULLIF(TRIM(id), '') ~ '^\d+$' THEN NULLIF(TRIM(id), '')::BIGINT
            ELSE NULL
        END AS movie_id,
        title,
        release_date,
        budget,
        revenue,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY CASE
                WHEN NULLIF(TRIM(id), '') ~ '^\d+$' THEN NULLIF(TRIM(id), '')::BIGINT
                ELSE NULL
            END
            ORDER BY loaded_at DESC NULLS LAST
        ) AS rn
    FROM {{ source('mm_staging', 'movies_main') }}
),
dedup AS (
    SELECT *
    FROM src
    WHERE rn = 1 AND movie_id IS NOT NULL
),
cleaned AS (
    SELECT
        movie_id,
        NULLIF(TRIM(title), '') AS title,
        NULLIF(TRIM(release_date), '') AS release_date_raw,
        NULLIF(REGEXP_REPLACE(budget, '[^0-9.-]', '', 'g'), '') AS budget_raw,
        NULLIF(REGEXP_REPLACE(revenue, '[^0-9.-]', '', 'g'), '') AS revenue_raw,
        loaded_at
    FROM dedup
)
SELECT
    movie_id,
    title,
    CASE
        WHEN release_date_raw ~ '^\d{4}-\d{1,2}-\d{1,2}$' THEN TO_DATE(release_date_raw, 'YYYY-MM-DD')
        WHEN release_date_raw ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(release_date_raw, 'MM/DD/YYYY')
        WHEN release_date_raw ~ '^\d{1,2}-\d{1,2}-\d{4}$' THEN TO_DATE(release_date_raw, 'DD-MM-YYYY')
        ELSE NULL
    END AS release_date,
    CASE WHEN budget_raw IS NOT NULL THEN budget_raw::NUMERIC(18, 2) ELSE NULL END AS budget_amount,
    CASE WHEN revenue_raw IS NOT NULL THEN revenue_raw::NUMERIC(18, 2) ELSE NULL END AS revenue_amount,
    loaded_at::TIMESTAMP AS loaded_at
FROM cleaned
