"""Build staging schema tables by enriching raw movie data with Kaggle TMDB data."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path


import kagglehub
import pandas as pd

from utils.db_connector import DatabaseConnector
from utils.db_operations import DatabaseOperations
from utils.logger import setup_logger
from utils.pipeline_utils import (
    complete_pipeline_run,
    complete_task_log,
    normalize_mixed_dates,
    prepare_rows,
    start_pipeline_run,
    start_task_log,
)

logger = setup_logger(__name__)


class KaggleMovieEnricher:
    """Loads and serves enrichment columns from Kaggle TMDB dataset."""

    def __init__(
        self,
        dataset_slug: str = "asaniczka/tmdb-movies-dataset-2023-930k-movies",
        kaggle_csv_path: str | None = None,
        min_vote_count: int = 20,
    ) -> None:
        self.dataset_slug = dataset_slug
        self.kaggle_csv_path = kaggle_csv_path
        self.min_vote_count = min_vote_count
        self._frame: pd.DataFrame | None = None

    def _resolve_csv_path(self) -> Path:
        if self.kaggle_csv_path:
            path = Path(self.kaggle_csv_path)
            if not path.exists():
                raise FileNotFoundError(f"Kaggle CSV file not found: {path}")
            return path

        dataset_dir = Path(kagglehub.dataset_download(self.dataset_slug))
        csv_files = list(dataset_dir.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(
                f"No CSV files found in downloaded Kaggle dataset directory: {dataset_dir}"
            )

        # Use the largest CSV file when multiple are present.
        csv_files.sort(key=lambda p: p.stat().st_size, reverse=True)
        return csv_files[0]

    def load(self) -> pd.DataFrame:
        if self._frame is not None:
            return self._frame

        csv_path = self._resolve_csv_path()
        logger.info("Loading Kaggle enrichment file: %s", csv_path)

        use_cols = [
            "id",
            "title",
            "release_date",
            "budget",
            "revenue",
            "genres",
            "production_companies",
            "production_countries",
            "spoken_languages",
            "vote_average",
            "vote_count",
            "status",
        ]
        frame = pd.read_csv(csv_path, usecols=use_cols, dtype=str)
        frame = frame.drop_duplicates(subset=["id"], keep="first")

        frame["release_date_norm"] = normalize_mixed_dates(
            frame["release_date"].fillna(""))
        frame["vote_count_num"] = pd.to_numeric(
            frame["vote_count"], errors="coerce")
        frame["vote_average_num"] = pd.to_numeric(
            frame["vote_average"], errors="coerce")

        self._frame = frame
        return frame


class MovieDataStaging:
    """Enriches raw data using Kaggle TMDB dataset and writes staging tables."""

    def __init__(
        self,
        kaggle_csv_path: str | None = None,
        kaggle_dataset_slug: str = "asaniczka/tmdb-movies-dataset-2023-930k-movies",
        min_vote_count: int = 20,
    ) -> None:
        self.db = DatabaseOperations(DatabaseConnector())
        self.enricher = KaggleMovieEnricher(
            dataset_slug=kaggle_dataset_slug,
            kaggle_csv_path=kaggle_csv_path,
            min_vote_count=min_vote_count,
        )
        self.run_id: int | None = None

    def _read_table(self, table_name: str, schema: str = "raw") -> pd.DataFrame:
        columns_query = """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        columns_result = self.db.execute_query(
            columns_query, (schema, table_name), fetch=True)
        if not columns_result:  # if table doesn't exist, return empty dataframe
            return pd.DataFrame()
        columns = [col[0] for col in columns_result]

        data_query = f"SELECT * FROM {schema}.{table_name}"
        result = self.db.execute_query(data_query, fetch=True)
        if not result:  # if table exists but empty, return dataframe with headers but no rows
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(result, columns=columns)

    def _ensure_staging_tables(self) -> None:
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS staging.movies_main (
                id TEXT,
                title TEXT,
                release_date TEXT,
                budget TEXT,
                revenue TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.movie_extended (
                id TEXT,
                genres TEXT,
                production_countries TEXT,
                production_companies TEXT,
                spoken_languages TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS staging.ratings (
                movie_id TEXT,
                avg_rating TEXT,
                total_ratings TEXT,
                stg_dev TEXT,
                loaded_at TEXT
            );
            """,
        ]
        for query in ddl:
            self.db.execute_query(query)

    def _write_table(self, table_name: str, frame: pd.DataFrame) -> int:
        if frame.empty:
            logger.warning("No rows to write into staging.%s", table_name)
            self.db.truncate_table(table_name, schema="staging")
            return 0

        columns, rows = prepare_rows(frame)
        self.db.truncate_table(table_name, schema="staging")
        return self.db.bulk_insert(table_name, columns, rows, schema="staging")

    @staticmethod
    def _text_is_blank(series: pd.Series) -> pd.Series:
        """Treat null/empty/placeholder text values as blank."""
        normalized = series.fillna("").astype(str).str.strip().str.lower()
        return normalized.isin(["", "none", "null", "nan"])

    def _collapse_duplicate_groups(
        self,
        frame: pd.DataFrame,
        key_col: str,
        signal_cols: list[str],
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        """Collapse duplicate keys while recovering useful non-blank values onto kept rows."""
        if frame.empty or key_col not in frame.columns:
            return frame, {
                "rows_rejected_dedup": 0,
                "keys_recovered_from_duplicates": 0,
                "cells_recovered_from_duplicates": 0,
            }

        # check for duplictes to fail fast
        dup_mask = frame.duplicated(subset=[key_col], keep=False)
        if not dup_mask.any():
            return frame, {
                "rows_rejected_dedup": 0,
                "keys_recovered_from_duplicates": 0,
                "cells_recovered_from_duplicates": 0,
            }

        work = frame.copy()
        existing_signal_cols = [
            col for col in signal_cols if col in work.columns]
        kept_idx = work.groupby(key_col, sort=False).tail(
            1).index  # save index labels

        recovered_cells = 0
        recovered_keys: set[str] = set()

        for col in existing_signal_cols:
            non_blank_values = work[col].where(~self._text_is_blank(work[col]))
            last_non_blank = non_blank_values.groupby(work[key_col]).transform(
                lambda s: s.dropna(
                ).iloc[-1] if not s.dropna().empty else pd.NA
            )

            fill_mask = (
                work.index.isin(kept_idx)
                & self._text_is_blank(work[col])
                & last_non_blank.notna()
            )
            if fill_mask.any():
                work.loc[fill_mask, col] = last_non_blank[fill_mask]
                recovered_cells += int(fill_mask.sum())
                recovered_keys.update(
                    work.loc[fill_mask, key_col].astype(str).tolist())

        collapsed = work.drop_duplicates(subset=[key_col], keep="last")

        return collapsed, {
            "rows_rejected_dedup": int(len(frame) - len(collapsed)),
            "keys_recovered_from_duplicates": int(len(recovered_keys)),
            "cells_recovered_from_duplicates": int(recovered_cells),
        }

    def _dedup_diagnostics(
        self,
        frame: pd.DataFrame,
        key_col: str,
        signal_cols: list[str],
        table_label: str,
    ) -> dict[str, int]:
        """Profile duplicate-key quality and potential information loss before dedup."""
        if frame.empty or key_col not in frame.columns:
            return {
                "duplicate_groups": 0,
                "duplicate_rows": 0,
                "exact_duplicate_groups": 0,
                "conflicting_duplicate_groups": 0,
                "keys_with_potential_value_loss": 0,
                "potential_value_loss_cells": 0,
            }

        dup = frame[frame.duplicated(subset=[key_col], keep=False)].copy()
        if dup.empty:
            return {
                "duplicate_groups": 0,
                "duplicate_rows": 0,
                "exact_duplicate_groups": 0,
                "conflicting_duplicate_groups": 0,
                "keys_with_potential_value_loss": 0,
                "potential_value_loss_cells": 0,
            }

        existing_signal_cols = [
            col for col in signal_cols if col in dup.columns]
        duplicate_groups = int(dup[key_col].nunique())
        duplicate_rows = int(len(dup))

        # creates a unique string 'signature' for each row
        if existing_signal_cols:
            normalized = dup[existing_signal_cols].fillna(
                "").astype(str).apply(lambda s: s.str.strip())
            signature = normalized.agg("||".join, axis=1)
            # 1 = no conflict, 2 = conflict
            sig_counts = signature.groupby(dup[key_col]).nunique()
            conflicting_groups = int((sig_counts > 1).sum())
        else:
            conflicting_groups = 0

        exact_groups = duplicate_groups - conflicting_groups  # exact duplicates

        # Simulate keep='last' and identify where dropped rows contain non-blank values
        # while the kept row is blank for the same field.
        kept = dup.groupby(key_col, sort=False).tail(1)
        dropped = dup.drop(index=kept.index)

        keys_with_loss = 0
        loss_cells = 0

        if existing_signal_cols and not dropped.empty:
            kept_by_key = kept[[key_col] +
                               existing_signal_cols].set_index(key_col)
            dropped_with_kept = dropped[[key_col] + existing_signal_cols].merge(
                kept_by_key,
                left_on=key_col,
                right_index=True,
                suffixes=("_dropped", "_kept"),
                how="left",
            )

            loss_mask = pd.Series(False, index=dropped_with_kept.index)
            for col in existing_signal_cols:
                dropped_has_value = ~self._text_is_blank(
                    dropped_with_kept[f"{col}_dropped"])
                kept_is_blank = self._text_is_blank(
                    dropped_with_kept[f"{col}_kept"])
                col_loss = dropped_has_value & kept_is_blank
                loss_cells += int(col_loss.sum())
                loss_mask = loss_mask | col_loss

            keys_with_loss = int(
                dropped_with_kept.loc[loss_mask, key_col].nunique())

        diagnostics = {
            "duplicate_groups": duplicate_groups,
            "duplicate_rows": duplicate_rows,
            "exact_duplicate_groups": exact_groups,
            "conflicting_duplicate_groups": conflicting_groups,
            "keys_with_potential_value_loss": keys_with_loss,
            "potential_value_loss_cells": loss_cells,
        }

        if duplicate_groups > 0:
            risk_level = "HIGH" if keys_with_loss > 0 else (
                "MEDIUM" if conflicting_groups > 0 else "LOW"
            )
            logger.info(
                "%s dedup check -> duplicate_keys=%d, duplicate_rows=%d, exact_duplicates=%d, conflicting_duplicates=%d, keys_with_possible_data_loss=%d, possible_data_loss_fields=%d, risk=%s",
                table_label,
                duplicate_groups,
                duplicate_rows,
                exact_groups,
                conflicting_groups,
                keys_with_loss,
                loss_cells,
                risk_level,
            )

        if keys_with_loss > 0:
            logger.warning(
                "%s dedup detected potential information loss for %d keys (keep='last').",
                table_label,
                keys_with_loss,
            )

        return diagnostics

    @staticmethod
    def _missing_text_mask(series: pd.Series) -> pd.Series:
        """Return True for values that are null/blank after trimming."""
        return series.fillna("").astype(str).str.strip().eq("")

    @staticmethod
    def _fill_missing_from_source(frame: pd.DataFrame, target_col: str, source_col: str) -> int:
        """Fill one target column from one source column and return fill count."""
        missing_target = MovieDataStaging._missing_text_mask(frame[target_col])
        has_source = frame[source_col].notna()
        fill_mask = missing_target & has_source

        frame.loc[fill_mask, target_col] = frame.loc[fill_mask, source_col]
        return int(fill_mask.sum())

    def _fill_missing_from_mappings(
        self,
        frame: pd.DataFrame,
        mappings: list[tuple[str, str]],
    ) -> int:
        """Fill target columns from source columns and return total filled values."""
        total_filled = 0
        for target_col, source_col in mappings:
            total_filled += self._fill_missing_from_source(
                frame, target_col, source_col)
        return total_filled

    def stage_movies_main(self, kaggle: pd.DataFrame, raw_movies_main: pd.DataFrame) -> tuple[int, int, dict[str, int]]:
        frame = raw_movies_main.copy()  # make a copy for raw schema
        if frame.empty:
            return 0, 0, {
                "rows_touched": 0,
                "values_filled": 0,
                "duplicate_groups": 0,
                "conflicting_duplicate_groups": 0,
                "keys_with_potential_value_loss": 0,
            }

        frame["id"] = frame["id"].astype(str)
        dedup_stats = self._dedup_diagnostics(
            frame,
            key_col="id",
            signal_cols=["title", "release_date", "budget", "revenue"],
            table_label="movies_main",
        )
        frame, dedup_recovery = self._collapse_duplicate_groups(
            frame,
            key_col="id",
            signal_cols=["title", "release_date", "budget", "revenue"],
        )
        rows_rejected_dedup = dedup_recovery["rows_rejected_dedup"]

        kcols = ["id", "title", "release_date_norm", "budget", "revenue"]
        merged = frame.merge(kaggle[kcols], on="id",
                             how="left", suffixes=("", "_kg"))

        values_filled = self._fill_missing_from_mappings(  # returns total count of all values filled
            merged,
            [
                ("title", "title_kg"),
                ("budget", "budget_kg"),
                ("revenue", "revenue_kg"),
            ],
        )

        # Normalize release_date from mixed formats, then fill from Kaggle where still missing.
        merged["release_date"] = normalize_mixed_dates(
            merged["release_date"].fillna(""))
        values_filled += self._fill_missing_from_source(
            merged,
            "release_date",
            "release_date_norm",
        )

        merged["loaded_at"] = datetime.now(timezone.utc)
        output = merged[["id", "title", "release_date",
                         "budget", "revenue", "loaded_at"]]
        rows_out = self._write_table("movies_main", output)

        rows_touched = int(
            (output[["title", "release_date", "budget", "revenue"]].notna().any(axis=1)).sum())
        return rows_out, rows_rejected_dedup, {
            "rows_touched": rows_touched,
            "values_filled": values_filled,
            "duplicate_groups": dedup_stats["duplicate_groups"],
            "conflicting_duplicate_groups": dedup_stats["conflicting_duplicate_groups"],
            "keys_with_potential_value_loss": dedup_stats["keys_with_potential_value_loss"],
            "keys_recovered_from_duplicates": dedup_recovery["keys_recovered_from_duplicates"],
            "cells_recovered_from_duplicates": dedup_recovery["cells_recovered_from_duplicates"],
        }

    def stage_movie_extended(
        self, kaggle: pd.DataFrame, raw_movie_extended: pd.DataFrame
    ) -> tuple[int, int, dict[str, int]]:
        frame = raw_movie_extended.copy()
        if frame.empty:
            return 0, 0, {
                "rows_touched": 0,
                "values_filled": 0,
                "duplicate_groups": 0,
                "conflicting_duplicate_groups": 0,
                "keys_with_potential_value_loss": 0,
            }

        frame["id"] = frame["id"].astype(str)
        dedup_stats = self._dedup_diagnostics(
            frame,
            key_col="id",
            signal_cols=[
                "genres",
                "production_countries",
                "production_companies",
                "spoken_languages",
            ],
            table_label="movie_extended",
        )
        frame, dedup_recovery = self._collapse_duplicate_groups(
            frame,
            key_col="id",
            signal_cols=[
                "genres",
                "production_countries",
                "production_companies",
                "spoken_languages",
            ],
        )
        rows_rejected_dedup = dedup_recovery["rows_rejected_dedup"]

        kcols = [
            "id",
            "genres",
            "production_companies",
            "production_countries",
            "spoken_languages",
        ]
        merged = frame.merge(kaggle[kcols], on="id",
                             how="left", suffixes=("", "_kg"))

        values_filled = self._fill_missing_from_mappings(
            merged,
            [
                ("genres", "genres_kg"),
                ("production_companies", "production_companies_kg"),
                ("production_countries", "production_countries_kg"),
                ("spoken_languages", "spoken_languages_kg"),
            ],
        )

        merged["loaded_at"] = datetime.now(timezone.utc)
        output = merged[
            [
                "id",
                "genres",
                "production_countries",
                "production_companies",
                "spoken_languages",
                "loaded_at",
            ]
        ]
        rows_out = self._write_table("movie_extended", output)

        rows_touched = int(
            (
                output[["genres", "production_companies",
                        "production_countries", "spoken_languages"]]
                .notna()
                .any(axis=1)
            ).sum()
        )
        return rows_out, rows_rejected_dedup, {
            "rows_touched": rows_touched,
            "values_filled": values_filled,
            "duplicate_groups": dedup_stats["duplicate_groups"],
            "conflicting_duplicate_groups": dedup_stats["conflicting_duplicate_groups"],
            "keys_with_potential_value_loss": dedup_stats["keys_with_potential_value_loss"],
            "keys_recovered_from_duplicates": dedup_recovery["keys_recovered_from_duplicates"],
            "cells_recovered_from_duplicates": dedup_recovery["cells_recovered_from_duplicates"],
        }

    def stage_ratings(
        self,
        kaggle: pd.DataFrame,
        raw_ratings: pd.DataFrame,
        raw_movies_main: pd.DataFrame,
    ) -> tuple[int, int, dict[str, int]]:
        movie_ids = raw_movies_main[["id"]].drop_duplicates().rename(columns={
            "id": "movie_id"})  # rename to match ratings table
        movie_ids["movie_id"] = movie_ids["movie_id"].astype(str)

        raw = raw_ratings.copy()
        if raw.empty:
            raw = pd.DataFrame(
                columns=["movie_id", "avg_rating", "total_ratings", "stg_dev", "loaded_at"])
        raw["movie_id"] = raw["movie_id"].astype(str)
        dedup_stats = self._dedup_diagnostics(
            raw,
            key_col="movie_id",
            signal_cols=["avg_rating", "total_ratings", "stg_dev"],
            table_label="ratings",
        )
        raw, dedup_recovery = self._collapse_duplicate_groups(
            raw,
            key_col="movie_id",
            signal_cols=["avg_rating", "total_ratings", "stg_dev"],
        )
        rows_rejected_raw_dedup = dedup_recovery["rows_rejected_dedup"]

        k = kaggle[["id", "vote_average", "vote_count", "status"]].copy()
        k = k.rename(columns={  # match kaggle column names with db column names
                     "id": "movie_id", "vote_average": "kg_avg_rating", "vote_count": "kg_total_ratings"})
        k["movie_id"] = k["movie_id"].astype(str)

        # Keep quality/rejection counting scoped to movies in the pipeline, not the full Kaggle dataset.
        k_for_movies = movie_ids.merge(k, on="movie_id", how="left")
        k_for_movies["kg_total_ratings_num"] = pd.to_numeric(
            k_for_movies["kg_total_ratings"], errors="coerce")

        matched_kaggle_rows = int(k_for_movies["status"].notna().sum())
        k_valid = k_for_movies[
            (k_for_movies["status"].fillna("").str.lower() == "released")
            & (k_for_movies["kg_total_ratings_num"] >= self.enricher.min_vote_count)
        ][["movie_id", "kg_avg_rating", "kg_total_ratings"]]
        rows_rejected_quality = matched_kaggle_rows - len(k_valid)

        merged = movie_ids.merge(raw, on="movie_id", how="left").merge(
            k_valid, on="movie_id", how="left")

        values_filled = self._fill_missing_from_mappings(
            merged,
            [
                ("avg_rating", "kg_avg_rating"),
                ("total_ratings", "kg_total_ratings"),
            ],
        )

        if "stg_dev" not in merged.columns:
            merged["stg_dev"] = None

        now_text = datetime.now(timezone.utc).isoformat()
        merged["loaded_at"] = merged["loaded_at"].fillna(now_text)

        output = merged[["movie_id", "avg_rating",
                         "total_ratings", "stg_dev", "loaded_at"]]
        rows_before_final_filter = len(output)
        output = output[
            output["avg_rating"].fillna("").astype(str).str.strip().ne("")
            | output["total_ratings"].fillna("").astype(str).str.strip().ne("")
        ]
        rows_rejected_final = rows_before_final_filter - len(output)

        rows_out = self._write_table("ratings", output)
        # Count only rows actually removed from final task output as rejected rows.
        # Quality-filtered Kaggle rows are enrichment exclusions, not dropped pipeline rows.
        total_rows_rejected = rows_rejected_raw_dedup + rows_rejected_final

        if total_rows_rejected > 0:
            logger.info(
                "Ratings filtering: rejected %d from raw dedup, %d from quality filter, %d from final filter (total: %d)",
                rows_rejected_raw_dedup,
                rows_rejected_quality,
                rows_rejected_final,
                total_rows_rejected,
            )

        return rows_out, total_rows_rejected, {
            "rows_touched": rows_out,
            "values_filled": values_filled,
            "duplicate_groups": dedup_stats["duplicate_groups"],
            "conflicting_duplicate_groups": dedup_stats["conflicting_duplicate_groups"],
            "keys_with_potential_value_loss": dedup_stats["keys_with_potential_value_loss"],
            "keys_recovered_from_duplicates": dedup_recovery["keys_recovered_from_duplicates"],
            "cells_recovered_from_duplicates": dedup_recovery["cells_recovered_from_duplicates"],
        }

    def stage_all(self) -> dict[str, int]:
        logger.info("Starting staging enrichment from Kaggle dataset")
        self._ensure_staging_tables()

        self.run_id = start_pipeline_run(
            self.db,
            logger,
            "movie_mart_staging",
            trigger_type="manual",
        )

        kaggle = self.enricher.load()
        raw_movies_main = self._read_table("movies_main", schema="raw")
        raw_movie_extended = self._read_table("movie_extended", schema="raw")
        raw_ratings = self._read_table("ratings", schema="raw")

        results: dict[str, int] = {}
        stats_summary: dict[str, dict[str, int]] = {}
        tasks = [
            ("movies_main", "raw.movies_main", "staging.movies_main"),
            ("movie_extended", "raw.movie_extended", "staging.movie_extended"),
            ("ratings", "raw.ratings", "staging.ratings"),
        ]

        try:
            for table_name, source_relation, target_relation in tasks:
                task_id = start_task_log(
                    self.db,
                    logger,
                    self.run_id,
                    stage_name="staging",
                    task_name=f"build_{table_name}",
                    source_relation=source_relation,
                    target_relation=target_relation,
                )
                try:
                    if table_name == "movies_main":
                        rows_out, rows_rejected, stats = self.stage_movies_main(
                            kaggle, raw_movies_main)
                    elif table_name == "movie_extended":
                        rows_out, rows_rejected, stats = self.stage_movie_extended(
                            kaggle, raw_movie_extended)
                    else:
                        rows_out, rows_rejected, stats = self.stage_ratings(
                            kaggle, raw_ratings, raw_movies_main)

                    results[table_name] = rows_out
                    stats_summary[table_name] = stats
                    complete_task_log(
                        self.db,
                        logger,
                        task_id,
                        status="completed",
                        # total rows before rejection
                        rows_in=stats.get("rows_touched", 0) + rows_rejected,
                        rows_out=rows_out,
                        rows_rejected=rows_rejected,
                        error_message=(
                            "dedup_summary: "
                            f"duplicate_groups={stats.get('duplicate_groups', 0)}, "
                            f"conflicting_groups={stats.get('conflicting_duplicate_groups', 0)}, "
                            f"potential_loss_keys={stats.get('keys_with_potential_value_loss', 0)}, "
                            f"recovered_keys={stats.get('keys_recovered_from_duplicates', 0)}, "
                            f"recovered_cells={stats.get('cells_recovered_from_duplicates', 0)}"
                        ),
                    )
                except Exception as exc:
                    complete_task_log(
                        self.db,
                        logger,
                        task_id,
                        status="failed",
                        error_message=str(exc),
                    )
                    raise

            complete_pipeline_run(
                self.db,
                logger,
                self.run_id,
                run_status="completed",
            )

            # Log summary stats
            logger.info("=" * 70)
            logger.info("STAGING SUMMARY:")
            logger.info("=" * 70)
            for table_name in ["movies_main", "movie_extended", "ratings"]:
                if table_name in results:
                    rows_out = results[table_name]
                    stats = stats_summary.get(table_name, {})
                    rows_touched = stats.get("rows_touched", 0)
                    values_filled = stats.get("values_filled", 0)
                    duplicate_groups = stats.get("duplicate_groups", 0)
                    conflicting_groups = stats.get(
                        "conflicting_duplicate_groups", 0)
                    potential_loss_keys = stats.get(
                        "keys_with_potential_value_loss", 0)

                    if potential_loss_keys > 0:
                        dedup_status = "REVIEW NEEDED"
                    elif conflicting_groups > 0:
                        dedup_status = "CHECK CONFLICTS"
                    elif duplicate_groups > 0:
                        dedup_status = "SAFE DEDUP"
                    else:
                        dedup_status = "NO DUPLICATES"

                    logger.info(f"  {table_name}:")
                    logger.info(
                        f"    - Enriched rows: {rows_touched}")
                    logger.info(
                        f"    - Missing values filled from Kaggle: {values_filled}")
                    logger.info(
                        f"    - Duplicate movie IDs found: {duplicate_groups}")
                    logger.info(
                        f"    - Duplicate IDs with conflicting values: {conflicting_groups}")
                    logger.info(
                        f"    - Duplicate IDs where dropped row may have useful data: {potential_loss_keys}")
                    logger.info(
                        f"    - Duplicate IDs recovered using non-blank fallback: {stats.get('keys_recovered_from_duplicates', 0)}")
                    logger.info(
                        f"    - Recovered cells from duplicates: {stats.get('cells_recovered_from_duplicates', 0)}")
                    logger.info(
                        f"    - Dedup status: {dedup_status}")
                    logger.info(f"    - Rows written to staging: {rows_out}")
            logger.info("=" * 70)
            logger.info("Staging completed successfully")
            return results
        except Exception as exc:
            complete_pipeline_run(
                self.db,
                logger,
                self.run_id,
                run_status="failed",
                error_message=str(exc),
            )
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build staging schema from raw tables using Kaggle TMDB enrichment"
    )
    parser.add_argument(
        "--kaggle-csv-path",
        default=None,
        help="Optional explicit path to TMDB_movie_dataset_v11.csv. If omitted, kagglehub downloads dataset.",
    )
    parser.add_argument(
        "--kaggle-dataset-slug",
        default="asaniczka/tmdb-movies-dataset-2023-930k-movies",
        help="Kaggle dataset slug used by kagglehub when --kaggle-csv-path is not provided.",
    )
    parser.add_argument(
        "--min-vote-count",
        type=int,
        default=20,
        help="Minimum vote_count for Kaggle rating fallback values.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate input sources and Kaggle file resolution without writing to the database.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    staging = MovieDataStaging(
        kaggle_csv_path=args.kaggle_csv_path,
        kaggle_dataset_slug=args.kaggle_dataset_slug,
        min_vote_count=args.min_vote_count,
    )

    try:
        if args.dry_run:
            _ = staging.enricher.load()
            logger.info(
                "Dry-run successful: Kaggle enrichment source is accessible")
            return 0

        staging.stage_all()
        return 0
    except Exception:
        logger.exception("Staging failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
