WITH src AS (
    SELECT
        NULLIF(TRIM(id), '') AS movie_id,
        NULLIF(TRIM(genres), '') AS genres,
        NULLIF(TRIM(production_countries), '') AS production_countries,
        NULLIF(TRIM(production_companies), '') AS production_companies,
        NULLIF(TRIM(spoken_languages), '') AS spoken_languages,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(id), '')
            ORDER BY loaded_at DESC NULLS LAST
        ) AS rn
    FROM {{ source('mm_staging', 'movie_extended') }}
)
SELECT
    movie_id,
    genres,
    production_countries,
    production_companies,
    spoken_languages,
    loaded_at::TIMESTAMP AS loaded_at
FROM src
WHERE rn = 1 AND movie_id IS NOT NULL
