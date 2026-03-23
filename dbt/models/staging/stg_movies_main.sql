WITH src AS (
    SELECT
        NULLIF(TRIM(id), '') AS movie_id,
        NULLIF(TRIM(title), '') AS title,
        NULLIF(TRIM(release_date), '') AS release_date_raw,
        NULLIF(REGEXP_REPLACE(budget, '[^0-9.-]', '', 'g'), '') AS budget_raw,
        NULLIF(REGEXP_REPLACE(revenue, '[^0-9.-]', '', 'g'), '') AS revenue_raw,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(id), '')
            ORDER BY loaded_at DESC NULLS LAST
        ) AS rn
    FROM {{ source('mm_staging', 'movies_main') }}
),
dedup AS (
    SELECT *
    FROM src
    WHERE rn = 1 AND movie_id IS NOT NULL
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
FROM dedup
