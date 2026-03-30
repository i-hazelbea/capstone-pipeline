/*
 * Model: dim_date
 * Layer: marts (dimension)
 * Grain: one row per calendar date
 *
 * Purpose:
 *   - Provide a conformed date dimension for consistent joins and filtering.
 *   - Ensure gap-free time-series analysis independent of source completeness.
 *
 * Behavior:
 *   - Generates date spine from first source year (or fallback) to current year + 5 years.
 *   - Adds calendar attributes and period boundary flags.
 */

WITH bounds AS (
    SELECT
        COALESCE(
            (SELECT DATE_TRUNC('year', MIN(release_date))::DATE FROM {{ ref('stg_movies_main') }} WHERE release_date IS NOT NULL),
            CAST('1900-01-01' AS DATE)
        ) AS start_date,
        GREATEST(
            COALESCE(
                (SELECT DATE_TRUNC('year', MAX(release_date))::DATE FROM {{ ref('stg_movies_main') }} WHERE release_date IS NOT NULL),
                CURRENT_DATE
            ),
            DATE_TRUNC('year', CURRENT_DATE)::DATE
        ) + INTERVAL '5 years' - INTERVAL '1 day' AS end_date
),
date_spine AS (
    SELECT
        GENERATE_SERIES(start_date, end_date::DATE, INTERVAL '1 day')::DATE AS date_day
    FROM bounds
)
SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_key,
    date_day,
    EXTRACT(YEAR FROM date_day)::INT AS year_number,
    EXTRACT(QUARTER FROM date_day)::INT AS quarter_number,
    EXTRACT(MONTH FROM date_day)::INT AS month_number,
    EXTRACT(DAY FROM date_day)::INT AS day_of_month,
    EXTRACT(DOY FROM date_day)::INT AS day_of_year,
    EXTRACT(WEEK FROM date_day)::INT AS week_of_year,
    EXTRACT(ISODOW FROM date_day)::INT AS iso_day_of_week,
    TRIM(TO_CHAR(date_day, 'Day')) AS day_name,
    TRIM(TO_CHAR(date_day, 'Month')) AS month_name,
    DATE_TRUNC('month', date_day)::DATE AS month_start_date,
    (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end_date,
    DATE_TRUNC('quarter', date_day)::DATE AS quarter_start_date,
    (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS quarter_end_date,
    DATE_TRUNC('year', date_day)::DATE AS year_start_date,
    (DATE_TRUNC('year', date_day) + INTERVAL '1 year' - INTERVAL '1 day')::DATE AS year_end_date,
    CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN date_day = DATE_TRUNC('month', date_day)::DATE THEN TRUE ELSE FALSE END AS is_month_start,
    CASE
        WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
            THEN TRUE
        ELSE FALSE
    END AS is_month_end,
    CASE WHEN date_day = DATE_TRUNC('quarter', date_day)::DATE THEN TRUE ELSE FALSE END AS is_quarter_start,
    CASE
        WHEN date_day = (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months' - INTERVAL '1 day')::DATE
            THEN TRUE
        ELSE FALSE
    END AS is_quarter_end,
    CASE WHEN date_day = DATE_TRUNC('year', date_day)::DATE THEN TRUE ELSE FALSE END AS is_year_start,
    CASE
        WHEN date_day = (DATE_TRUNC('year', date_day) + INTERVAL '1 year' - INTERVAL '1 day')::DATE
            THEN TRUE
        ELSE FALSE
    END AS is_year_end
FROM date_spine