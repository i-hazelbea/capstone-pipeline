/*
 * Model: vw_high_value_studio_performance
 * Layer: analytics (view)
 * Grain: one row per production_company
 *
 * Purpose:
 *   - Provide ranked studio consistency metrics for executive/BI consumption.
 *   - Surface high-value studios based on sustained growth and rating trends.
 *
 * Source:
 *   - Aggregated from fct_company_year_performance over a defined year window.
 */

WITH studio_summary AS (
    SELECT
        f.production_company_key,
        f.production_company,
        COUNT(*) AS years_observed,
        SUM(CASE WHEN f.growth_and_rating_improved THEN 1 ELSE 0 END) AS years_improved,
        ROUND(
            SUM(CASE WHEN f.growth_and_rating_improved THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(*), 0),
            4
        ) AS consistency_ratio,
        ROUND(AVG(f.revenue_yoy_pct), 2) AS avg_revenue_yoy_pct,
        ROUND(AVG(f.rating_yoy_delta), 4) AS avg_rating_yoy_delta,
        ROUND(AVG(f.weighted_avg_rating), 4) AS avg_weighted_rating,
        ROUND(AVG(f.total_revenue), 2) AS avg_total_revenue,
        MAX(f.release_year) AS latest_year,
        MAX(f.release_year_start_date_key) AS latest_year_start_date_key
    FROM {{ ref('fct_company_year_performance') }} AS f
    WHERE f.release_year BETWEEN 2000 AND 2024
    GROUP BY f.production_company_key, f.production_company
    HAVING COUNT(*) >= 5
),
band_counts AS (
    SELECT
        t.production_company_key,
        t.performance_band,
        COUNT(*) AS year_count,
        MAX(t.release_year) AS max_year
    FROM {{ ref('vw_company_year_trends') }} AS t
    WHERE t.release_year BETWEEN 2000 AND 2024
      AND t.performance_band <> 'no_baseline'
    GROUP BY t.production_company_key, t.performance_band
),
dominant_band AS (
    SELECT
        bc.production_company_key,
        bc.performance_band,
        ROW_NUMBER() OVER (
            PARTITION BY bc.production_company_key
            ORDER BY bc.year_count DESC, bc.max_year DESC, bc.performance_band ASC
        ) AS band_rank
    FROM band_counts AS bc
),
latest_band AS (
    SELECT
        t.production_company_key,
        t.performance_band,
        ROW_NUMBER() OVER (
            PARTITION BY t.production_company_key
            ORDER BY t.release_year DESC, t.performance_band ASC
        ) AS latest_rank
    FROM {{ ref('vw_company_year_trends') }} AS t
    WHERE t.release_year BETWEEN 2000 AND 2024
      AND t.performance_band <> 'no_baseline'
),
analysis_window AS (
    SELECT
        MAX(f.release_year) AS analysis_max_year
    FROM {{ ref('fct_company_year_performance') }} AS f
    WHERE f.release_year BETWEEN 2000 AND 2024
),
scored AS (
    SELECT
        s.production_company_key,
        s.production_company,
        s.years_observed,
        s.years_improved,
        s.consistency_ratio,
        s.avg_revenue_yoy_pct,
        s.avg_rating_yoy_delta,
        s.avg_weighted_rating,
        s.avg_total_revenue,
        s.latest_year,
        s.latest_year_start_date_key,
        COALESCE(d.performance_band, 'unclassified') AS dominant_performance_band_raw,
        COALESCE(lb.performance_band, 'unclassified') AS latest_performance_band_raw,
        CASE COALESCE(d.performance_band, 'unclassified')
            WHEN 'outperforming' THEN 1.00
            WHEN 'stable_positive' THEN 0.80
            WHEN 'rating_up_revenue_down' THEN 0.50
            WHEN 'revenue_up_rating_down' THEN 0.50
            WHEN 'declining' THEN 0.20
            ELSE 0.00
        END AS dominant_band_score,
        CASE COALESCE(lb.performance_band, 'unclassified')
            WHEN 'outperforming' THEN 1.00
            WHEN 'stable_positive' THEN 0.80
            WHEN 'rating_up_revenue_down' THEN 0.50
            WHEN 'revenue_up_rating_down' THEN 0.50
            WHEN 'declining' THEN 0.20
            ELSE 0.00
        END AS latest_band_score,
        LEAST(s.years_observed::NUMERIC / 15.0, 1.0) AS years_observed_score,
        (aw.analysis_max_year - s.latest_year) AS years_since_latest_release,
        CASE
            WHEN s.latest_year <= 2013 THEN TRUE
            ELSE FALSE
        END AS is_inactive_studio,
        CASE
            WHEN s.latest_year >= (aw.analysis_max_year - 2) THEN TRUE
            ELSE FALSE
        END AS is_recently_active,
        CASE
            WHEN (aw.analysis_max_year - s.latest_year) <= 0 THEN 1.00
            WHEN (aw.analysis_max_year - s.latest_year) <= 2 THEN 0.80
            WHEN (aw.analysis_max_year - s.latest_year) <= 5 THEN 0.50
            ELSE 0.00
        END AS activity_recency_score
    FROM studio_summary AS s
    CROSS JOIN analysis_window AS aw
    LEFT JOIN dominant_band AS d
        ON s.production_company_key = d.production_company_key
       AND d.band_rank = 1
    LEFT JOIN latest_band AS lb
        ON s.production_company_key = lb.production_company_key
       AND lb.latest_rank = 1
)
SELECT
    s.production_company_key,
    s.production_company,
    s.years_observed,
    s.years_improved,
    s.consistency_ratio,
    s.avg_revenue_yoy_pct,
    s.avg_rating_yoy_delta,
    s.avg_weighted_rating,
    s.avg_total_revenue,
    s.latest_year,
    s.latest_year_start_date_key,
    s.years_since_latest_release,
    s.is_inactive_studio,
    s.is_recently_active,
    CASE
        WHEN s.is_inactive_studio THEN 'inactive'
        ELSE s.dominant_performance_band_raw
    END AS dominant_performance_band,
    CASE
        WHEN s.is_inactive_studio THEN 'Inactive'
        ELSE CASE s.latest_performance_band_raw
            WHEN 'outperforming' THEN 'Outperforming'
            WHEN 'stable_positive' THEN 'Stable Positive'
            WHEN 'rating_up_revenue_down' THEN 'Rating Up, Revenue Down'
            WHEN 'revenue_up_rating_down' THEN 'Revenue Up, Rating Down'
            WHEN 'declining' THEN 'Declining'
            ELSE 'Unclassified'
        END
    END AS latest_performance_band,
    CASE
        WHEN s.is_inactive_studio THEN 'Inactive'
        ELSE CASE s.dominant_performance_band_raw
            WHEN 'outperforming' THEN 'Outperforming'
            WHEN 'stable_positive' THEN 'Stable Positive'
            WHEN 'rating_up_revenue_down' THEN 'Rating Up, Revenue Down'
            WHEN 'revenue_up_rating_down' THEN 'Revenue Up, Rating Down'
            WHEN 'declining' THEN 'Declining'
            ELSE 'Unclassified'
        END
    END AS dominant_performance_band_label,
    '⬤' AS performance_dot,
    CASE
        WHEN s.is_inactive_studio THEN '#7A7A7A'
        ELSE CASE s.dominant_performance_band_raw
            WHEN 'declining' THEN '#364B59'
            WHEN 'rating_up_revenue_down' THEN '#C480A7'
            WHEN 'revenue_up_rating_down' THEN '#6E98B5'
            WHEN 'outperforming' THEN '#67002E'
            WHEN 'stable_positive' THEN '#A8C1D3'
            ELSE '#9E9E9E'
        END
    END AS performance_dot_color_hex,
    CASE
        WHEN s.is_inactive_studio THEN 0.0
        ELSE ROUND(
            (s.latest_band_score * 0.45)
            + (s.dominant_band_score * 0.25)
            + (s.years_observed_score * 0.10)
            + (s.activity_recency_score * 0.20),
            4
        )
    END AS weighted_decision_score,
    CASE
        WHEN s.is_inactive_studio THEN 'Inactive'
        WHEN s.consistency_ratio > 0.15
             AND ROUND(
                 (s.latest_band_score * 0.45)
                 + (s.dominant_band_score * 0.25)
                 + (s.years_observed_score * 0.10)
                 + (s.activity_recency_score * 0.20),
                 4
             ) >= 0.40
        THEN 'Incentivise'
        WHEN s.consistency_ratio > 0.15
             AND ROUND(
                 (s.latest_band_score * 0.45)
                 + (s.dominant_band_score * 0.25)
                 + (s.years_observed_score * 0.10)
                 + (s.activity_recency_score * 0.20),
                 4
             ) >= 0.20
        THEN 'Review'
        ELSE 'Renegotiate'
    END AS studio_action_recommendation
FROM scored AS s
ORDER BY s.consistency_ratio DESC, s.avg_revenue_yoy_pct DESC, s.avg_weighted_rating DESC
