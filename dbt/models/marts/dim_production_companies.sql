/*
 * Model: dim_production_companies
 * Layer: marts (dimension)
 * Grain: one row per production_company
 *
 * Purpose:
 *   - Provide a conformed production company dimension with stable surrogate keys.
 *   - Track descriptive temporal boundaries for company activity.
 *
 * Behavior:
 *   - Normalizes company names and creates hashed surrogate keys.
 *   - Derives first/latest observed release year and latest loaded timestamp.
 *   - Uses full movie-company bridge history to avoid capping first release year.
 */
WITH normalized AS (
    SELECT
        LOWER(TRIM(production_company)) AS production_company_norm,
        TRIM(production_company) AS production_company,
        release_year,
        record_loaded_at
    FROM {{ ref('int_movie_company_bridge') }}
    WHERE production_company IS NOT NULL
        AND TRIM(production_company) <> ''
)
SELECT
    MD5(production_company_norm) AS production_company_key,
    MIN(production_company) AS production_company,
    MIN(release_year) AS first_release_year,
    MAX(release_year) AS latest_release_year,
    MAX(record_loaded_at) AS loaded_at
FROM normalized
GROUP BY production_company_norm
