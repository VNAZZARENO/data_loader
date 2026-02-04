#!/usr/bin/env python3
"""
ATLAS Bloomberg Data Loader

Reads a YAML config (ticker universe + field mappings) and uses xbbg to pull
BDH data from Bloomberg, writing a clean static xlsx.

Usage:
    source .venv/bin/activate && python3 bloomberg_loader.py
    source .venv/bin/activate && python3 bloomberg_loader.py --dry-run
    source .venv/bin/activate && python3 bloomberg_loader.py --end-date 2026-02-04
"""

import argparse
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
    ):
        self.dry_run = dry_run
        self.config = self._load_config(config_path)

        if start_date_override:
            self.config["parameters"]["start_date"] = start_date_override
        if end_date_override:
            self.config["parameters"]["end_date"] = end_date_override

        self.start_date = self.config["parameters"]["start_date"]
        self.end_date = self.config["parameters"]["end_date"]
        self.batch_size = self.config["bloomberg"]["batch_size"]
        self.ticker_suffix = self.config["bloomberg"]["ticker_suffix"]
        self.fields = self.config["fields"]  # e.g. {"price": "PX_LAST", ...}
        self.tickers = self.config["tickers"]  # raw tickers without suffix
        self.output_path = self.config["paths"]["output_xlsx"]

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
        for key in ("parameters", "paths", "bloomberg", "fields", "tickers"):
            if key not in cfg:
                raise KeyError(f"Missing required config key: {key}")
        if not cfg["tickers"]:
            raise ValueError("Ticker list is empty — run extract_tickers.py first")
        return cfg

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

        combined = pd.concat(all_results, axis=1)

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
    # Excel output
    # ------------------------------------------------------------------
    def _write_xlsx(self, results: dict[str, pd.DataFrame]) -> None:
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

        logger.info(f"Output written: {self.output_path}")

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------
    def run(self) -> None:
        logger.info(f"ATLAS Bloomberg Loader — {len(self.tickers)} tickers, {len(self.fields)} fields")
        logger.info(f"Date range: {self.start_date} -> {self.end_date}")
        logger.info(f"Batch size: {self.batch_size}")
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

        if self.dry_run:
            logger.info("[DRY RUN] Skipping xlsx write")
            return

        # Only write if we got at least some data
        has_data = any(not df.empty for df in results.values())
        if has_data:
            self._write_xlsx(results)
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
        "--dry-run",
        action="store_true",
        help="Validate config and print plan without making API calls",
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
    )
    loader.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
