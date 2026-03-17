-- ==========================================
-- RAW TABLES
-- ==========================================

CREATE TABLE IF NOT EXISTS raw.movies_main (
    id TEXT,
    title TEXT,
    release_date TEXT,
    budget TEXT,
    revenue TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.movie_extended (
    id TEXT,
    genres TEXT,
    production_countries TEXT,
    production_companies TEXT,
    spoken_languages TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Column ratings_summary flattedned to avg_rating, total_ratings, and std_dev
CREATE TABLE IF NOT EXISTS raw.ratings (
    movie_id TEXT,
    avg_rating TEXT,
    total_ratings TEXT,
    stg_dev TEXT,
    loaded_at TEXT
);


-- ==========================================
-- INDEXES FOR PERFORMANCE
-- ==========================================

CREATE INDEX IF NOT EXISTS idx_movies_main_id
ON raw.movies_main(id);

CREATE INDEX IF NOT EXISTS idx_movie_extended_id
ON raw.movie_extended(id);

CREATE INDEX IF NOT EXISTS idx_ratings_movie_id
ON raw.ratings(movie_id);