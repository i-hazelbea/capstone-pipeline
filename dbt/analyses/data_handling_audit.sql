-- Data handling audit: row quality and drop impact across pipeline layers.
-- Run after ingest/stage/dbt run.
-- Example:
-- docker exec -i mm-postgres psql -U admin -d movie_db -f /usr/app/analyses/data_handling_audit.sql

WITH
raw_quality AS (
    SELECT
        'raw.movies_main'::text AS relation,
        COUNT(*)::bigint AS total_rows,
        SUM(CASE WHEN id IS NULL OR BTRIM(id) = '' THEN 1 ELSE 0 END)::bigint AS missing_key_rows,
        (COUNT(*) - COUNT(DISTINCT NULLIF(BTRIM(id), '')))::bigint AS duplicate_key_rows
    FROM raw.movies_main

    UNION ALL

    SELECT
        'raw.movie_extended'::text,
        COUNT(*)::bigint,
        SUM(CASE WHEN id IS NULL OR BTRIM(id) = '' THEN 1 ELSE 0 END)::bigint,
        (COUNT(*) - COUNT(DISTINCT NULLIF(BTRIM(id), '')))::bigint
    FROM raw.movie_extended

    UNION ALL

    SELECT
        'raw.ratings'::text,
        COUNT(*)::bigint,
        SUM(CASE WHEN movie_id IS NULL OR BTRIM(movie_id) = '' THEN 1 ELSE 0 END)::bigint,
        (COUNT(*) - COUNT(DISTINCT NULLIF(BTRIM(movie_id), '')))::bigint
    FROM raw.ratings
),
layer_counts AS (
    SELECT
        'movies_main'::text AS table_name,
        (SELECT COUNT(*) FROM raw.movies_main)::bigint AS raw_rows,
        (SELECT COUNT(*) FROM staging.movies_main)::bigint AS py_staging_rows,
        (SELECT COUNT(*) FROM staging.stg_movies_main)::bigint AS dbt_staging_rows

    UNION ALL

    SELECT
        'movie_extended',
        (SELECT COUNT(*) FROM raw.movie_extended)::bigint,
        (SELECT COUNT(*) FROM staging.movie_extended)::bigint,
        (SELECT COUNT(*) FROM staging.stg_movie_extended)::bigint

    UNION ALL

    SELECT
        'ratings',
        (SELECT COUNT(*) FROM raw.ratings)::bigint,
        (SELECT COUNT(*) FROM staging.ratings)::bigint,
        (SELECT COUNT(*) FROM staging.stg_ratings)::bigint
),
ratings_coverage AS (
    SELECT
        (SELECT COUNT(DISTINCT NULLIF(BTRIM(id), '')) FROM raw.movies_main)::bigint AS movie_universe_ids,
        (SELECT COUNT(DISTINCT NULLIF(BTRIM(movie_id), '')) FROM staging.ratings)::bigint AS ratings_ids_kept
),
bridge_filter_impact AS (
    WITH movie_base AS (
        SELECT
            m.movie_id,
            m.release_date,
            e.production_companies
        FROM staging.stg_movies_main m
        LEFT JOIN staging.stg_movie_extended e
            ON m.movie_id = e.movie_id
    ),
    exploded AS (
        SELECT
            movie_id,
            EXTRACT(YEAR FROM release_date)::int AS release_year,
            TRIM(company_name) AS production_company
        FROM movie_base,
        UNNEST(REGEXP_SPLIT_TO_ARRAY(COALESCE(production_companies, ''), '\\s*,\\s*')) AS company_name
    )
    SELECT
        COUNT(*)::bigint AS exploded_rows_before_filter,
        SUM(CASE WHEN production_company = '' OR production_company IS NULL THEN 1 ELSE 0 END)::bigint AS dropped_blank_company_rows,
        SUM(CASE WHEN release_year IS NULL THEN 1 ELSE 0 END)::bigint AS dropped_null_release_year_rows,
        SUM(CASE WHEN production_company <> '' AND release_year IS NOT NULL THEN 1 ELSE 0 END)::bigint AS rows_kept_by_bridge_filter
    FROM exploded
)
SELECT
    'raw_key_quality'::text AS section,
    relation || ':total_rows' AS metric,
    total_rows AS value
FROM raw_quality

UNION ALL

SELECT
    'raw_key_quality',
    relation || ':missing_key_rows',
    missing_key_rows
FROM raw_quality

UNION ALL

SELECT
    'raw_key_quality',
    relation || ':duplicate_key_rows',
    duplicate_key_rows
FROM raw_quality

UNION ALL

SELECT
    'layer_counts',
    table_name || ':raw_rows',
    raw_rows
FROM layer_counts

UNION ALL

SELECT
    'layer_counts',
    table_name || ':python_staging_rows',
    py_staging_rows
FROM layer_counts

UNION ALL

SELECT
    'layer_counts',
    table_name || ':dbt_staging_rows',
    dbt_staging_rows
FROM layer_counts

UNION ALL

SELECT
    'layer_deltas',
    table_name || ':raw_minus_python_staging',
    (raw_rows - py_staging_rows)
FROM layer_counts

UNION ALL

SELECT
    'layer_deltas',
    table_name || ':python_staging_minus_dbt_staging',
    (py_staging_rows - dbt_staging_rows)
FROM layer_counts

UNION ALL

SELECT
    'ratings_coverage',
    'movie_universe_ids',
    movie_universe_ids
FROM ratings_coverage

UNION ALL

SELECT
    'ratings_coverage',
    'ratings_ids_kept',
    ratings_ids_kept
FROM ratings_coverage

UNION ALL

SELECT
    'ratings_coverage',
    'movie_ids_without_ratings_after_stage',
    (movie_universe_ids - ratings_ids_kept)
FROM ratings_coverage

UNION ALL

SELECT
    'bridge_filter_impact',
    'exploded_rows_before_filter',
    exploded_rows_before_filter
FROM bridge_filter_impact

UNION ALL

SELECT
    'bridge_filter_impact',
    'dropped_blank_company_rows',
    dropped_blank_company_rows
FROM bridge_filter_impact

UNION ALL

SELECT
    'bridge_filter_impact',
    'dropped_null_release_year_rows',
    dropped_null_release_year_rows
FROM bridge_filter_impact

UNION ALL

SELECT
    'bridge_filter_impact',
    'rows_kept_by_bridge_filter',
    rows_kept_by_bridge_filter
FROM bridge_filter_impact
ORDER BY section, metric;
