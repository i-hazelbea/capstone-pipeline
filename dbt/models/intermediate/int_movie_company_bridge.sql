/*
 * Model: int_movie_company_bridge
 * Layer: intermediate
 * Grain: one row per (movie_id, production_company)
 *
 * Purpose:
 *   - Reshape movie-level records to movie-company bridge grain.
 *   - Attach rating and release attributes needed for downstream aggregation.
 *   - Standardize company names to consolidate duplicates (e.g., LionsGate variants).
 *   - Handle orphaned articles (e.g., "The") that should be part of company names.
 *
 * Behavior:
 *   - Joins staged movie core, extended, and rating data.
 *   - Explodes comma-separated production companies into one row per company.
 *   - Applies regex-based normalization to consolidate known variants.
 *   - Filters blank companies and null release years.
 */

WITH movie_base AS (
    SELECT
        m.movie_id,
        m.title,
        m.release_date,
        EXTRACT(YEAR FROM m.release_date)::INT AS release_year,
        m.revenue_amount,
        m.budget_amount,
        e.production_companies,
        e.genres,
        e.production_countries,
        r.avg_rating,
        r.total_ratings,
        GREATEST(m.loaded_at, e.loaded_at, COALESCE(r.loaded_at, m.loaded_at)) AS record_loaded_at
    FROM {{ ref('stg_movies_main') }} AS m
    LEFT JOIN {{ ref('stg_movie_extended') }} AS e
        ON m.movie_id = e.movie_id
    LEFT JOIN {{ ref('stg_ratings') }} AS r
        ON m.movie_id = r.movie_id
    WHERE m.release_date IS NOT NULL
),
exploded AS (
    SELECT
        mb.movie_id,
        mb.title,
        mb.release_date,
        mb.release_year,
        mb.revenue_amount,
        mb.budget_amount,
        mb.avg_rating,
        mb.total_ratings,
        mb.genres,
        mb.production_countries,
        c.production_company,
        c.pos,
        mb.record_loaded_at
    FROM movie_base AS mb
    CROSS JOIN LATERAL (
        SELECT
            TRIM(company_name) AS production_company,
            pos
        FROM UNNEST(
            STRING_TO_ARRAY(
                COALESCE(mb.production_companies, ''),
                ','
            )
        ) WITH ORDINALITY AS t(company_name, pos)
        WHERE TRIM(company_name) <> ''
    ) AS c
),
with_article_fix AS (
    -- Handle orphaned articles like "The", "A", "An" that should be part of the next token
    -- Example: "The,Warner Bros." -> "The Warner Bros."
    SELECT
        movie_id,
        title,
        release_date,
        release_year,
        revenue_amount,
        budget_amount,
        avg_rating,
        total_ratings,
        genres,
        production_countries,
        CASE
            WHEN production_company IN ('The', 'A', 'An')
                 AND LEAD(production_company) OVER (PARTITION BY movie_id ORDER BY pos) IS NOT NULL
            THEN production_company || ' ' || LEAD(production_company) OVER (PARTITION BY movie_id ORDER BY pos)
            ELSE production_company
        END AS production_company_raw,
        -- Skip the token that was consumed by a leading article in previous position
        CASE
            WHEN LAG(production_company) OVER (PARTITION BY movie_id ORDER BY pos) IN ('The', 'A', 'An')
            THEN TRUE
            ELSE FALSE
        END AS skip_token,
        record_loaded_at
    FROM exploded
),
budget_corrected AS (
    -- Attempt to fix obvious budget/revenue scaling errors.
    -- For the "both tiny" pattern, correct both budget and revenue.
    SELECT
        movie_id,
        title,
        release_date,
        release_year,
        revenue_amount,
        budget_amount,
        avg_rating,
        total_ratings,
        genres,
        production_countries,
        production_company_raw,
        record_loaded_at,
        -- Correct budget when values imply missing magnitude.
        CASE
            -- Pattern A: BOTH budget and revenue are tiny (1-999) → likely both missing millions
            WHEN budget_amount > 0 
                 AND budget_amount < 1000 
                 AND revenue_amount > 0
                 AND revenue_amount < 1000
            THEN ROUND(budget_amount * 1000000, 2)
            
            -- Pattern B: budget 1-999, revenue > 1M (likely missing millions on budget)
            WHEN budget_amount > 0 
                 AND budget_amount < 1000 
                 AND revenue_amount > 1000000
            THEN ROUND(budget_amount * 1000000, 2)
            
            -- Pattern C: budget 1K-999K, revenue >= 100M (likely missing 1000x on budget)
            WHEN budget_amount >= 1000 
                 AND budget_amount < 1000000 
                 AND revenue_amount >= 100000000
                 AND (revenue_amount / budget_amount) > 100
            THEN ROUND(budget_amount * 1000, 2)
            
            -- Otherwise keep original
            ELSE budget_amount
        END AS budget_amount_corrected,
        -- Correct revenue only for the "both tiny" pattern.
        CASE
            WHEN budget_amount > 0
                 AND budget_amount < 1000
                 AND revenue_amount > 0
                 AND revenue_amount < 1000
            THEN ROUND(revenue_amount * 1000000, 2)
            ELSE revenue_amount
        END AS revenue_amount_corrected,
        -- Flag rows that were corrected for audit trail
        CASE
            WHEN budget_amount > 0 
                 AND budget_amount < 1000 
                 AND revenue_amount > 0
                 AND revenue_amount < 1000
            THEN 'both_scaled_×1m'
            WHEN budget_amount > 0 
                 AND budget_amount < 1000 
                 AND revenue_amount > 1000000
            THEN 'budget_scaled_×1m'
            WHEN budget_amount >= 1000 
                 AND budget_amount < 1000000 
                 AND revenue_amount >= 100000000
                 AND (revenue_amount / budget_amount) > 100
            THEN 'budget_scaled_×1k'
            ELSE 'original'
        END AS budget_correction_flag
    FROM with_article_fix
    WHERE NOT skip_token
),
standardized AS (
    SELECT
        movie_id,
        title,
        release_date,
        release_year,
        revenue_amount_corrected AS revenue_amount,
        budget_amount_corrected AS budget_amount,
        avg_rating,
        total_ratings,
        genres,
        production_countries,
        -- Standardize known company name variants using regex patterns
        CASE
            -- LionsGate family: consolidate Lionsgate, LionsGate, Lions Gate, Lions Gate Films
            WHEN production_company_raw ~* '^lions\s*gate|^lionsgate' THEN 'Lions Gate Films'
            -- Other normalizations can be added here as needed
            ELSE production_company_raw
        END AS production_company,
        budget_correction_flag,
        record_loaded_at
    FROM budget_corrected
)
SELECT
    movie_id,
    title,
    release_date,
    release_year,
    budget_amount,
    revenue_amount,
    avg_rating,
    total_ratings,
    genres,
    production_countries,
    production_company,
    budget_correction_flag,
    record_loaded_at
FROM standardized
WHERE
    production_company <> ''
    AND release_year IS NOT NULL
