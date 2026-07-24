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
    source .venv/bin/activate && python3 bloomberg_loader.py --universe jp --daily
    source .venv/bin/activate && python3 bloomberg_loader.py --agents squeeze --universe sxxr
    source .venv/bin/activate && python3 bloomberg_loader.py --agents all --universe sxxr --dry-run
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

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import option_universe  # noqa: E402  (local module, needs the path insert above)
import index_members  # noqa: E402  (local module, needs the path insert above)

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
        daily: bool = False,
        agents: str | None = None,
        mode: str = "static",
        fund: str | None = None,
        api_base_url: str | None = None,
        refresh_universe: bool = False,
    ):
        self.dry_run = dry_run
        self.test = test
        self.daily = daily
        self.agents = agents
        self.mode = mode
        self.refresh_universe = refresh_universe
        self.config = self._load_config(config_path)

        opt_cfg = self.config.get("option_modes", {})
        self.fund = fund or opt_cfg.get("fund", option_universe.DEFAULT_FUND)
        self.api_base_url = api_base_url or opt_cfg.get(
            "api_base_url", option_universe.DEFAULT_API_BASE_URL
        )

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
        overrides = self.config.get("universe_overrides", {}).get(self.universe, {})
        self.ticker_suffix = overrides.get("ticker_suffix", self.config["bloomberg"]["ticker_suffix"])
        self.bdh_options = overrides.get("bdh_options", self.config["bloomberg"].get("bdh_options", {}))
        self.fields = self._resolve_fields(overrides)
        self.tickers = self._resolve_universe_tickers()
        self.output_path = self._resolve_output_path()

        # Screening pulls only what is held today, so it does not need full
        # history: default to a rolling window unless the caller overrode dates.
        if self.mode == "screening" and not start_date_override:
            months = self.config.get("option_modes", {}).get("screening_months", 12)
            self.start_date = option_universe.screening_start_date(months)
            logger.info(f"Screening mode: rolling {months}-month window from {self.start_date}")

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

        # Daily incremental mode: load existing data and override date range
        self._existing_data: dict[str, pd.DataFrame] = {}
        self._existing_benchmark: pd.DataFrame | None = None
        if self.daily:
            self._existing_data, self._existing_benchmark = self._load_existing_xlsx()
            if "price" in self._existing_data and not self._existing_data["price"].empty:
                last_date = self._existing_data["price"].index.max()
                self.start_date = last_date.strftime("%Y-%m-%d")
                self.end_date = dt.date.today().isoformat()
                logger.info(
                    f"Daily mode: existing data up to {last_date.date()}, "
                    f"fetching {self.start_date} -> {self.end_date}"
                )
            else:
                raise ValueError(
                    f"Daily mode: existing file has no price data. "
                    f"Run a full extraction first."
                )

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

    def _resolve_fields(self, universe_overrides: dict) -> dict:
        """Resolve field set based on --agents flag.

        Priority: universe_overrides > agent profile > default fields.
        --agents all: union of all agent profiles.
        --agents <name>: that agent's fields.
        No flag: existing behavior (universe override or default fields).
        """
        if not self.agents:
            return universe_overrides.get("fields", self.config["fields"])

        agent_profiles = self.config.get("agents", {})
        available_agents = list(agent_profiles.keys())

        if self.agents == "all":
            # Union of all agent field profiles
            merged = {}
            for profile in agent_profiles.values():
                merged.update(profile.get("fields", {}))
            logger.info(
                f"Agent 'all': merged {len(merged)} fields from "
                f"{', '.join(available_agents)}"
            )
            return merged

        if self.agents not in agent_profiles:
            raise ValueError(
                f"Unknown agent '{self.agents}'. "
                f"Available: {', '.join(available_agents + ['all'])}"
            )

        fields = agent_profiles[self.agents].get("fields", {})
        logger.info(f"Agent '{self.agents}': {len(fields)} fields")
        return fields

    def _resolve_output_path(self) -> str:
        """Build output path, appending agent suffix when applicable.

        Option modes get their OWN file. A screening run holds a handful of
        names over one year: writing it to the bt/static path would silently
        destroy the long history the backtests depend on.
        """
        universe_token = self.universe
        if self.mode != "static":
            token_map = self.config.get("option_modes", {}).get("output_universe", {})
            universe_token = token_map.get(self.mode, f"{self.universe}_{self.mode}")

        base_path = self.config["paths"]["output_xlsx"].format(universe=universe_token)
        if self.agents and self.agents != "default":
            root, ext = os.path.splitext(base_path)
            return f"{root}_{self.agents}{ext}"
        return base_path

    def _resolve_universe_tickers(self) -> list[str]:
        """Ticker list for this universe, honouring the option mode."""
        loader_dir = os.path.dirname(os.path.abspath(__file__))
        base_csv = os.path.join(loader_dir, "tickers", f"{self.universe}.csv")

        if self.mode == "static":
            return self._load_tickers(self.universe)

        if not self.universe.startswith("option"):
            raise ValueError(
                f"--mode {self.mode} only applies to an option universe, got '{self.universe}'"
            )

        return option_universe.resolve_option_tickers(
            mode=self.mode,
            base_csv_path=base_csv,
            cache_csv_path=os.path.join(loader_dir, "tickers", f"{self.universe}_bt.csv"),
            fund=self.fund,
            base_url=self.api_base_url,
            since=self.config.get("option_modes", {}).get("bt_since"),
            refresh=self.refresh_universe,
        )

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
    # Load existing xlsx for daily incremental mode
    # ------------------------------------------------------------------
    def _load_existing_xlsx(self) -> tuple[dict[str, pd.DataFrame], pd.DataFrame | None]:
        """Read existing output xlsx for daily incremental merging."""
        if not os.path.isfile(self.output_path):
            raise FileNotFoundError(
                f"No existing file at {self.output_path}. "
                f"Run a full extraction first."
            )

        logger.info(f"Daily mode: reading existing data from {self.output_path}")
        existing_data: dict[str, pd.DataFrame] = {}

        for sheet_name in self.fields:
            try:
                df = pd.read_excel(
                    self.output_path, sheet_name=sheet_name, index_col=0
                )
                df.index = pd.to_datetime(df.index)
                existing_data[sheet_name] = df
                logger.info(
                    f"  Loaded sheet '{sheet_name}': "
                    f"{df.shape[0]} rows x {df.shape[1]} cols"
                )
            except Exception as e:
                logger.warning(f"  Could not read sheet '{sheet_name}': {e}")
                existing_data[sheet_name] = pd.DataFrame()

        existing_benchmark = None
        try:
            bm = pd.read_excel(
                self.output_path, sheet_name="benchmark", index_col=0
            )
            bm.index = pd.to_datetime(bm.index)
            existing_benchmark = bm
            logger.info(
                f"  Loaded sheet 'benchmark': "
                f"{bm.shape[0]} rows x {bm.shape[1]} cols"
            )
        except Exception:
            logger.info("  No existing benchmark sheet found (OK)")

        return existing_data, existing_benchmark

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
        agent_str = f", agents={self.agents}" if self.agents else ""
        logger.info(
            f"ATLAS Bloomberg Loader — universe={self.universe}{agent_str}, "
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

        # Daily mode: merge new data with existing data
        if self.daily and self._existing_data:
            for sheet_name, new_df in results.items():
                old_df = self._existing_data.get(sheet_name, pd.DataFrame())
                if old_df.empty:
                    continue
                if new_df.empty:
                    # No new data fetched — keep existing as-is
                    results[sheet_name] = old_df
                    logger.info(f"  '{sheet_name}': no new rows, keeping existing data")
                    continue
                merged = pd.concat([old_df, new_df])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                new_rows = len(merged) - len(old_df)
                logger.info(
                    f"  Merged '{sheet_name}': {len(old_df)} existing + "
                    f"{new_rows} new rows = {len(merged)} total"
                )
                results[sheet_name] = merged

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

        # Daily mode: merge benchmark
        if self.daily and self._existing_benchmark is not None and not self._existing_benchmark.empty:
            if benchmark_df.empty:
                benchmark_df = self._existing_benchmark
                logger.info("  Benchmark: no new rows, keeping existing data")
            else:
                merged_bm = pd.concat([self._existing_benchmark, benchmark_df])
                merged_bm = merged_bm[~merged_bm.index.duplicated(keep="last")].sort_index()
                new_bm_rows = len(merged_bm) - len(self._existing_benchmark)
                logger.info(
                    f"  Merged benchmark: {len(self._existing_benchmark)} existing + "
                    f"{new_bm_rows} new rows = {len(merged_bm)} total"
                )
                benchmark_df = merged_bm

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
        "--agents",
        default=None,
        help="Agent field profile (default, squeeze, all). "
             "Selects which Bloomberg fields to extract.",
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
        "--daily",
        action="store_true",
        help="Incremental update: read existing xlsx, fetch from last date to today, merge",
    )
    parser.add_argument(
        "--mode",
        choices=["static", "bt", "screening"],
        default="static",
        help="Option universe mode (option_* universes only). "
             "static: frozen tickers/<universe>.csv (default, legacy behaviour). "
             "bt: indices + every name the fund has ever held, cached to "
             "tickers/<universe>_bt.csv. "
             "screening: indices + current holdings only, rolling window.",
    )
    parser.add_argument(
        "--fund",
        default=None,
        help="Fund whose positions drive --mode bt/screening (default: PEQ)",
    )
    parser.add_argument(
        "--api-base-url",
        default=None,
        help="GetFundPortfolios base URL "
             "(default: https://pergam-tools/getfundportfolios-api)",
    )
    parser.add_argument(
        "--refresh-universe",
        action="store_true",
        help="--mode bt: rescan every position date instead of reusing the cached CSV",
    )
    parser.add_argument(
        "--update-universe",
        action="store_true",
        help="Refresh tickers/<universe>.csv from the live Bloomberg index "
             "membership (BDS INDX_MEMBERS on the universe's benchmark index), "
             "then exit without extracting. Prompts for confirmation before "
             "writing. Combine with --dry-run to preview the joiners/leavers "
             "diff without writing.",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Skip the --update-universe confirmation prompt (for cron / "
             "unattended runs).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity",
    )
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log_level))

    # --update-universe: refresh the ticker CSV from live index membership,
    # then exit. Handled before building the loader so it never loads the
    # stale list it is about to replace.
    if args.update_universe:
        cfg = ATLASBloombergLoader._load_config(args.config)
        universe = args.universe or cfg["universes"].get("default", "sxxr")
        im_cfg = cfg.get("index_members", {})
        index = im_cfg.get("index_override", {}).get(universe) or cfg.get(
            "benchmarks", {}
        ).get(universe)
        if not index:
            parser.error(
                f"--update-universe: no index known for universe '{universe}'. "
                f"Add it under 'benchmarks:' or 'index_members.index_override:' "
                f"in the config."
            )
        csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "tickers", f"{universe}.csv"
        )
        index_members.refresh_universe_csv(
            universe=universe,
            index=index,
            csv_path=csv_path,
            blp_module=blp,
            override_map=im_cfg.get("exchange_code_map", {}),
            dry_run=args.dry_run,
            assume_yes=args.yes,
        )
        return

    if args.daily and args.start_date:
        parser.error("--daily and --start-date are mutually exclusive")

    end_date = args.end_date
    if args.today:
        end_date = dt.date.today().isoformat()
    if args.daily:
        end_date = dt.date.today().isoformat()

    loader = ATLASBloombergLoader(
        config_path=args.config,
        start_date_override=args.start_date,
        end_date_override=end_date,
        dry_run=args.dry_run,
        universe=args.universe,
        test=args.test,
        daily=args.daily,
        agents=args.agents,
        mode=args.mode,
        fund=args.fund,
        api_base_url=args.api_base_url,
        refresh_universe=args.refresh_universe,
    )
    loader.run()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}")
        logger.critical(traceback.format_exc())
        sys.exit(1)
