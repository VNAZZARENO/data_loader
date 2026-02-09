#!/usr/bin/env python3
"""
ATLAS Bloomberg Data Loader

Reads a YAML config (field mappings + universe list) and per-universe ticker
CSV files from tickers/, then uses xbbg to pull BDH data from Bloomberg,
writing a clean static xlsx per universe.

Usage:
    source .venv/bin/activate && python3 bloomberg_loader.py
    source .venv/bin/activate && python3 bloomberg_loader.py --dry-run
    source .venv/bin/activate && python3 bloomberg_loader.py --universe nky --dry-run
    source .venv/bin/activate && python3 bloomberg_loader.py --universe spx --today
"""

import argparse
import csv
import datetime as dt
import logging
import os
import sys
import traceback

import pandas as pd
import yaml
from tqdm import tqdm
from xbbg import blp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ATLASBloombergLoader:
    def __init__(
        self,
        config_path: str,
        start_date_override: str | None = None,
        end_date_override: str | None = None,
        dry_run: bool = False,
        universe: str | None = None,
        test: bool = False,
    ):
        self.dry_run = dry_run
        self.test = test
        self.config = self._load_config(config_path)

        # Resolve universe: CLI override -> config default -> "sxxr"
        available = self.config["universes"]["available"]
        self.universe = universe or self.config["universes"].get("default", "sxxr")
        if self.universe not in available:
            raise ValueError(
                f"Unknown universe '{self.universe}'. "
                f"Available: {', '.join(available)}"
            )

        if start_date_override:
            self.config["parameters"]["start_date"] = start_date_override
        if end_date_override:
            self.config["parameters"]["end_date"] = end_date_override

        self.start_date = self.config["parameters"]["start_date"]
        self.end_date = self.config["parameters"].get("end_date") or dt.date.today().isoformat()
        self.batch_size = self.config["bloomberg"]["batch_size"]
        self.ticker_suffix = self.config["bloomberg"]["ticker_suffix"]
        self.bdh_options = self.config["bloomberg"].get("bdh_options", {})
        self.fields = self.config["fields"]  # e.g. {"price": "PX_LAST", ...}
        self.tickers = self._load_tickers(self.universe)
        self.output_path = self.config["paths"]["output_xlsx"].format(
            universe=self.universe
        )

        # Test mode: 5 tickers, batch_size=2 (3 batches), separate output
        if self.test:
            self.tickers = self.tickers[:5]
            self.batch_size = 2
            base, ext = os.path.splitext(self.output_path)
            self.output_path = f"{base}_test{ext}"
            logger.info(
                f"TEST MODE: {len(self.tickers)} tickers, "
                f"batch_size={self.batch_size}, output={self.output_path}"
            )

        # Benchmark (optional, per-universe)
        benchmarks = self.config.get("benchmarks", {})
        self.benchmark = benchmarks.get(self.universe)

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------
    @staticmethod
    def _load_config(path: str) -> dict:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Config not found: {path}")
        with open(path) as f:
            cfg = yaml.safe_load(f)
        # Validate required keys
        for key in ("parameters", "paths", "bloomberg", "fields", "universes"):
            if key not in cfg:
                raise KeyError(f"Missing required config key: {key}")
        return cfg

    @staticmethod
    def _load_tickers(universe: str) -> list[str]:
        """Load ticker list from tickers/<universe>.csv."""
        project_root = os.path.dirname(os.path.abspath(__file__))
        ticker_file = os.path.join(project_root, "tickers", f"{universe}.csv")
        if not os.path.isfile(ticker_file):
            raise FileNotFoundError(f"Ticker file not found: {ticker_file}")
        with open(ticker_file, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if "Ticker" not in (reader.fieldnames or []):
                raise ValueError(f"Ticker file missing 'Ticker' column: {ticker_file}")
            tickers = [row["Ticker"].strip() for row in reader if row["Ticker"].strip()]
        if not tickers:
            raise ValueError(f"Ticker file is empty: {ticker_file}")
        return tickers

    # ------------------------------------------------------------------
    # Bloomberg extraction (3-tier error handling)
    # ------------------------------------------------------------------
    def _extract_field(self, bbg_field: str) -> pd.DataFrame:
        """Pull a single Bloomberg field for the full ticker universe.

        Returns a DataFrame with DatetimeIndex rows and raw-ticker columns.
        """
        # Build Bloomberg tickers (append suffix)
        bbg_tickers = [t + self.ticker_suffix for t in self.tickers]

        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would extract field {bbg_field} for {len(bbg_tickers)} tickers "
                f"({self.start_date} -> {self.end_date})"
            )
            for t in bbg_tickers[:10]:
                logger.info(f"  - {t}")
            if len(bbg_tickers) > 10:
                logger.info(f"  ... and {len(bbg_tickers) - 10} more")
            return pd.DataFrame()

        all_results: list[pd.DataFrame] = []
        failed_tickers: list[str] = []
        n_batches = (len(bbg_tickers) - 1) // self.batch_size + 1

        for i in range(0, len(bbg_tickers), self.batch_size):
            batch = bbg_tickers[i : i + self.batch_size]
            batch_num = i // self.batch_size + 1
            logger.info(f"  Batch {batch_num}/{n_batches} ({len(batch)} tickers)")

            try:
                df = blp.bdh(
                    tickers=batch,
                    flds=[bbg_field],
                    start_date=self.start_date,
                    end_date=self.end_date,
                    **self.bdh_options,
                )
                if not df.empty:
                    all_results.append(df)
            except Exception as e:
                logger.error(f"  Batch {batch_num} failed: {e}")
                logger.info("  Falling back to per-ticker extraction for this batch")

                for ticker in tqdm(batch, desc=f"  Batch {batch_num} fallback"):
                    try:
                        single = blp.bdh(
                            tickers=[ticker],
                            flds=[bbg_field],
                            start_date=self.start_date,
                            end_date=self.end_date,
                            **self.bdh_options,
                        )
                        if not single.empty:
                            all_results.append(single)
                        else:
                            logger.warning(f"    No data for {ticker}")
                            failed_tickers.append(ticker)
                    except Exception as te:
                        logger.warning(f"    Failed {ticker}: {te}")
                        failed_tickers.append(ticker)

        if not all_results:
            logger.error(f"  No data extracted for field {bbg_field}")
            return pd.DataFrame()

        combined = pd.concat(all_results, axis=1).sort_index()

        # xbbg returns MultiIndex columns: (ticker, field).
        # Flatten to just ticker names.
        if isinstance(combined.columns, pd.MultiIndex):
            combined = combined.droplevel(1, axis=1)

        # Strip the " Equity" suffix so columns match the original xlsx headers.
        combined.columns = [c.replace(self.ticker_suffix, "") for c in combined.columns]

        logger.info(
            f"  {bbg_field}: {combined.shape[1]} tickers, {combined.shape[0]} dates"
        )
        if failed_tickers:
            logger.warning(
                f"  {len(failed_tickers)} tickers failed for {bbg_field}: "
                + ", ".join(failed_tickers[:20])
                + ("..." if len(failed_tickers) > 20 else "")
            )

        return combined

    # ------------------------------------------------------------------
    # Benchmark extraction
    # ------------------------------------------------------------------
    def _extract_benchmark(self) -> pd.DataFrame:
        """Pull all fields for the benchmark ticker.

        Returns a DataFrame with DatetimeIndex and one column per field
        (using the sheet name as column name).
        """
        if not self.benchmark:
            return pd.DataFrame()

        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would extract benchmark {self.benchmark} "
                f"({self.start_date} -> {self.end_date})"
            )
            return pd.DataFrame()

        series: dict[str, pd.Series] = {}
        for sheet_name, bbg_field in self.fields.items():
            logger.info(f"  Benchmark {self.benchmark} — {bbg_field}")
            try:
                df = blp.bdh(
                    tickers=[self.benchmark],
                    flds=[bbg_field],
                    start_date=self.start_date,
                    end_date=self.end_date,
                    **self.bdh_options,
                )
                if not df.empty:
                    # Flatten MultiIndex columns and take the single series
                    if isinstance(df.columns, pd.MultiIndex):
                        df = df.droplevel(0, axis=1)
                    series[sheet_name] = df.iloc[:, 0]
                else:
                    logger.warning(f"  No benchmark data for {bbg_field}")
            except Exception as e:
                logger.warning(f"  Benchmark failed for {bbg_field}: {e}")

        if series:
            return pd.DataFrame(series)
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Excel output
    # ------------------------------------------------------------------
    def _write_xlsx(self, results: dict[str, pd.DataFrame], benchmark: pd.DataFrame | None = None) -> None:
        """Write all results to a multi-sheet xlsx file."""
        logger.info(f"Writing output to {self.output_path}")

        os.makedirs(os.path.dirname(self.output_path) or ".", exist_ok=True)

        with pd.ExcelWriter(self.output_path, engine="openpyxl") as writer:
            # parameters sheet
            params_df = pd.DataFrame(
                list(self.config["parameters"].items()),
                columns=["Parameter", "Value"],
            )
            params_df.to_excel(writer, sheet_name="parameters", index=False)

            # data sheets
            for sheet_name, df in results.items():
                if df.empty:
                    logger.warning(f"  Skipping empty sheet: {sheet_name}")
                    continue
                df.index.name = "Ticker"
                df.to_excel(writer, sheet_name=sheet_name)
                logger.info(f"  Sheet '{sheet_name}': {df.shape[0]} rows x {df.shape[1]} cols")

            # benchmark sheet
            if benchmark is not None and not benchmark.empty:
                benchmark.index.name = "Date"
                benchmark.to_excel(writer, sheet_name="benchmark")
                logger.info(
                    f"  Sheet 'benchmark' ({self.benchmark}): "
                    f"{benchmark.shape[0]} rows x {benchmark.shape[1]} cols"
                )

        logger.info(f"Output written: {self.output_path}")

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------
    def run(self) -> None:
        logger.info(
            f"ATLAS Bloomberg Loader — universe={self.universe}, "
            f"{len(self.tickers)} tickers, {len(self.fields)} fields"
        )
        logger.info(f"Date range: {self.start_date} -> {self.end_date}")
        logger.info(f"Batch size: {self.batch_size}")
        if self.benchmark:
            logger.info(f"Benchmark: {self.benchmark}")
        logger.info(f"Output: {self.output_path}")

        if self.dry_run:
            logger.info("=== DRY RUN — no Bloomberg API calls will be made ===")

        results: dict[str, pd.DataFrame] = {}

        for sheet_name, bbg_field in self.fields.items():
            logger.info(f"Extracting field: {bbg_field} -> sheet '{sheet_name}'")
            try:
                df = self._extract_field(bbg_field)
                results[sheet_name] = df
            except Exception as e:
                logger.error(f"Field-level failure for {bbg_field}: {e}")
                logger.error(traceback.format_exc())
                results[sheet_name] = pd.DataFrame()

        # Align all sheets to the price date index (forward-fill sparse fields like EPS)
        if "price" in results and not results["price"].empty:
            master_index = results["price"].index
            for sheet_name, df in results.items():
                if sheet_name == "price" or df.empty:
                    continue
                if len(df) < len(master_index):
                    logger.info(
                        f"  Reindexing '{sheet_name}' from {len(df)} to "
                        f"{len(master_index)} rows (forward-fill)"
                    )
                    results[sheet_name] = df.reindex(master_index).ffill()

        # Extract benchmark if configured
        benchmark_df = pd.DataFrame()
        if self.benchmark:
            logger.info(f"Extracting benchmark: {self.benchmark}")
            benchmark_df = self._extract_benchmark()

        if self.dry_run:
            logger.info("[DRY RUN] Skipping xlsx write")
            return

        # Only write if we got at least some data
        has_data = any(not df.empty for df in results.values())
        if has_data:
            self._write_xlsx(results, benchmark=benchmark_df)
        else:
            logger.error("No data extracted for any field — output file not written")

        # Summary
        logger.info("--- Summary ---")
        for name, df in results.items():
            if df.empty:
                logger.info(f"  {name}: EMPTY")
            else:
                logger.info(f"  {name}: {df.shape[0]} rows x {df.shape[1]} cols")


def main():
    parser = argparse.ArgumentParser(description="ATLAS Bloomberg Data Loader")
    parser.add_argument(
        "--config",
        default=os.path.join(os.path.dirname(__file__), "config", "atlas_config.yaml"),
        help="Path to YAML config (default: config/atlas_config.yaml)",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Override start date (e.g. 2013-01-01)",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Override end date (e.g. 2026-02-04)",
    )
    parser.add_argument(
        "--today",
        action="store_true",
        help="Set end date to today",
    )
    parser.add_argument(
        "--universe",
        default=None,
        help="Ticker universe (sxxr, nky, spx, pbh, sx5e, splpeqty). Default: sxxr",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and print plan without making API calls",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: 5 tickers, batch_size=2, writes to *_test.xlsx",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity",
    )
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log_level))

    end_date = args.end_date
    if args.today:
        end_date = dt.date.today().isoformat()

    loader = ATLASBloombergLoader(
        config_path=args.config,
        start_date_override=args.start_date,
        end_date_override=end_date,
        dry_run=args.dry_run,
        universe=args.universe,
        test=args.test,
    )
    loader.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
