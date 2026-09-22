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
import time
import traceback

import pandas as pd
import yaml
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import option_universe  # noqa: E402  (local module, needs the path insert above)
import index_members  # noqa: E402  (local module, needs the path insert above)
from dl import manifests, registry, requests_worker, smbio  # noqa: E402
from dl.store import derive as store_derive  # noqa: E402
from dl.store import fx as store_fx  # noqa: E402
from dl.store import layout as store_layout, legacy_xlsx, reader as store_reader, writer as store_writer  # noqa: E402


def _default_blp():
    """xbbg is only installed on the Bloomberg box; import it on first real use."""
    from xbbg import blp

    return blp

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
        blp_module=None,
    ):
        self._blp = blp_module
        self._field_reports: dict[str, manifests.FieldReport] = {}
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

        # Resolve universe: CLI override -> config default -> "sxxr".
        # A universe created in the dashboard lives only in the share registry:
        # it needs no YAML edit to be extractable.
        available = list(self.config["universes"]["available"])
        try:
            available += [u for u in registry.list_universes(self.config) if u not in available]
        except OSError as e:
            logger.warning(f"Universe registry unreachable ({e}); using config list only")
        self.universe = universe or self.config["universes"].get("default", "sxxr")
        if self.universe not in available:
            raise ValueError(
                f"Unknown universe '{self.universe}'. "
                f"Available: {', '.join(available)}"
            )
        self._registry_entry = self._load_registry_entry()

        if start_date_override:
            self.config["parameters"]["start_date"] = start_date_override
        if end_date_override:
            self.config["parameters"]["end_date"] = end_date_override

        self.start_date = self.config["parameters"]["start_date"]
        self.end_date = self.config["parameters"].get("end_date") or dt.date.today().isoformat()
        self.batch_size = self.config["bloomberg"]["batch_size"]
        overrides = self.config.get("universe_overrides", {}).get(self.universe, {})
        reg = self._registry_entry
        in_config = self.universe in self.config["universes"]["available"]
        default_suffix = self.config["bloomberg"]["ticker_suffix"]
        if reg is not None and not in_config:
            default_suffix = reg.ticker_suffix
            if not self.agents and reg.fields_profile:
                self.agents = reg.fields_profile
        self.ticker_suffix = overrides.get("ticker_suffix", default_suffix)
        self.bdh_options = overrides.get("bdh_options", self.config["bloomberg"].get("bdh_options", {}))
        # Extraction requires pandas with (ticker, field) columns, regardless of
        # xbbg defaults or session-wide output settings.
        self.bdh_options = {**self.bdh_options, "backend": "pandas", "format": "wide"}
        self.fields = self._resolve_fields(overrides)
        self.no_ffill_fields = set(
            overrides.get("no_ffill_fields", self.config["bloomberg"].get("no_ffill_fields", []))
        )
        self.tickers = self._resolve_universe_tickers()
        self.output_path = self._resolve_output_path()
        store_cfg = self.config.get("store", {})
        self.store_enabled = bool(store_cfg.get("enabled", False))
        self.xlsx_from_store = self.store_enabled and bool(store_cfg.get("xlsx_from_store", False))
        self.full_start_date = self.start_date

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
        if self.benchmark is None and self._registry_entry is not None:
            self.benchmark = self._registry_entry.benchmark

        self._manifest = manifests.RunManifest(
            universe=self.universe,
            profile=self.agents or "default",
            mode=self.mode,
            daily=self.daily,
            n_requested=len(self.tickers),
            registry_rev=getattr(self._registry_entry, "rev", None),
            ticker_source="registry" if self._tickers_from_registry else "csv",
        )

        # Daily incremental mode: load existing data and override date range
        self._existing_data: dict[str, pd.DataFrame] = {}
        self._existing_benchmark: pd.DataFrame | None = None
        if self.daily and self.xlsx_from_store:
            # The store holds per-ticker watermarks: no need to read the workbook back.
            wm = self._watermarks().get("price", {})
            if not wm:
                raise ValueError(
                    f"Daily mode: store for '{self.store_universe}' has no price data. "
                    f"Run a full extraction or 'python -m dl.migrate import-xlsx' first."
                )
            self.start_date = max(wm.values())
            self.end_date = dt.date.today().isoformat()
            logger.info(f"Daily mode (store): fetching {self.start_date} -> {self.end_date}")
        elif self.daily:
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

    @property
    def blp(self):
        if self._blp is None:
            self._blp = _default_blp()
        return self._blp

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
        self.store_universe = universe_token  # one store per output file family

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
            reg = self._registry_entry
            if reg is not None:
                if not reg.fetch_enabled:
                    raise ValueError(
                        f"Bloomberg fetch is disabled for universe '{self.universe}' "
                        f"in the registry (rev {reg.rev}). Re-enable it in the dashboard."
                    )
                tickers = reg.fetch_tickers()
                if not tickers:
                    raise ValueError(f"Registry universe '{self.universe}' has no ticker to fetch")
                n_dep = len(set(tickers) & set(reg.deprecated_tickers()))
                logger.info(
                    f"Universe from registry rev {reg.rev}: {len(tickers)} tickers "
                    f"({n_dep} deprecated still fetched, "
                    f"{len(reg.members) - len(tickers)} with fetch disabled)"
                )
                self._tickers_from_registry = True
                # The legacy xlsx is the tradable universe of the ATLAS strategies:
                # deprecated names go to the store only, never to the workbook.
                self._xlsx_exclude = set(reg.deprecated_tickers())
                return tickers
            logger.warning(
                f"Universe '{self.universe}' not in the share registry; "
                f"falling back to tickers/{self.universe}.csv"
            )
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

    def _load_registry_entry(self):
        """Registry on the share is re-read at every run (source of truth)."""
        self._tickers_from_registry = False
        self._xlsx_exclude: set[str] = set()
        try:
            if registry.exists(self.universe, self.config):
                return registry.load(self.universe, self.config)
        except (OSError, ValueError) as e:
            logger.warning(f"Could not read registry for '{self.universe}': {e}")
        return None

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
    def _watermarks(self) -> dict:
        return store_writer.load_state(self.store_universe, self.config, self.test).get("watermarks", {})

    def _fetch_groups(self, alias: str) -> list[tuple[list[str], str]]:
        """[(tickers, start_date)]. With the store, a ticker never seen for this field
        (index joiner, new field) is backfilled from the full start date instead of
        only from today -- the legacy --daily gave joiners no history."""
        if not self.store_enabled or self.dry_run:
            return [(self.tickers, self.start_date)]
        wm = self._watermarks().get(alias, {})
        new = [t for t in self.tickers if t not in wm]
        known = [t for t in self.tickers if t in wm]
        if self.start_date <= self.full_start_date:
            return [(self.tickers, self.start_date)]
        if not wm:  # field added to the profile after the first runs: full history, not just today
            logger.info(f"  New field '{alias}' for this store: backfill from {self.full_start_date}")
            return [(self.tickers, self.full_start_date)]
        groups = []
        if known:
            groups.append((known, self.start_date))
        if new:
            logger.info(f"  {len(new)} ticker(s) without history for '{alias}': backfill from {self.full_start_date}")
            groups.append((new, self.full_start_date))
        return groups

    def _extract_alias(self, alias: str, bbg_field: str) -> pd.DataFrame:
        frames = [self._extract_field(bbg_field, tickers, start) for tickers, start in self._fetch_groups(alias)]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1).sort_index() if len(frames) > 1 else frames[0]

    def _extract_field(self, bbg_field: str, tickers: list[str] | None = None,
                       start_date: str | None = None) -> pd.DataFrame:
        """Pull a single Bloomberg field for a ticker group (default: the full universe).

        Returns a DataFrame with DatetimeIndex rows and raw-ticker columns.
        """
        start_date = start_date or self.start_date
        # Build Bloomberg tickers (append suffix)
        bbg_tickers = [t + self.ticker_suffix for t in (self.tickers if tickers is None else tickers)]

        if self.dry_run:
            logger.info(
                f"[DRY RUN] Would extract field {bbg_field} for {len(bbg_tickers)} tickers "
                f"({start_date} -> {self.end_date})"
            )
            for t in bbg_tickers[:10]:
                logger.info(f"  - {t}")
            if len(bbg_tickers) > 10:
                logger.info(f"  ... and {len(bbg_tickers) - 10} more")
            return pd.DataFrame()

        all_results: list[pd.DataFrame] = []
        failed_tickers: list[str] = []
        n_batches = (len(bbg_tickers) - 1) // self.batch_size + 1
        fallback_batches = 0
        t0 = time.monotonic()

        for i in range(0, len(bbg_tickers), self.batch_size):
            batch = bbg_tickers[i : i + self.batch_size]
            batch_num = i // self.batch_size + 1
            logger.info(f"  Batch {batch_num}/{n_batches} ({len(batch)} tickers)")

            try:
                df = self.blp.bdh(
                    tickers=batch,
                    flds=[bbg_field],
                    start_date=start_date,
                    end_date=self.end_date,
                    **self.bdh_options,
                )
                if not df.empty:
                    all_results.append(df)
            except Exception as e:
                logger.error(f"  Batch {batch_num} failed: {e}")
                logger.info("  Falling back to per-ticker extraction for this batch")
                fallback_batches += 1

                for ticker in tqdm(batch, desc=f"  Batch {batch_num} fallback"):
                    try:
                        single = self.blp.bdh(
                            tickers=[ticker],
                            flds=[bbg_field],
                            start_date=start_date,
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

        report = manifests.FieldReport(
            bbg_field=bbg_field,
            failed=sorted(failed_tickers),
            batches=n_batches,
            fallback_batches=fallback_batches,
        )
        previous = self._field_reports.get(bbg_field)  # another fetch group of the same field
        self._field_reports[bbg_field] = report

        if not all_results:
            logger.error(f"  No data extracted for field {bbg_field}")
            report.missing = sorted(set(bbg_tickers) - set(failed_tickers))
            report.seconds = round(time.monotonic() - t0, 2)
            return pd.DataFrame()

        combined = pd.concat(all_results, axis=1).sort_index()

        # xbbg returns MultiIndex columns: (ticker, field).
        # Flatten to just ticker names.
        if isinstance(combined.columns, pd.MultiIndex):
            combined = combined.droplevel(1, axis=1)

        # A batch can silently return fewer columns than requested: persist the gap.
        # A column with no value at all (field not applicable to the security) is not
        # data: the store drops it, so the manifest must not count it as returned.
        returned = set(combined.columns[combined.notna().any()])
        report.n_returned = len(returned)
        report.missing = sorted(set(bbg_tickers) - returned - set(failed_tickers))
        report.seconds = round(time.monotonic() - t0, 2)
        if previous is not None:
            report.n_returned += previous.n_returned
            report.failed = sorted(set(report.failed) | set(previous.failed))
            report.missing = sorted(set(report.missing) | set(previous.missing))
            report.seconds = round(report.seconds + previous.seconds, 2)
            report.batches += previous.batches
            report.fallback_batches += previous.fallback_batches
        if report.missing:
            logger.warning(
                f"  {len(report.missing)} tickers requested but absent from the "
                f"response for {bbg_field}: " + ", ".join(report.missing[:20])
            )

        # Strip the " Equity" suffix so columns match the original xlsx headers.
        combined.columns = [c.replace(self.ticker_suffix, "") for c in combined.columns]

        logger.info(
            f"  {bbg_field}: {len(returned)}/{len(bbg_tickers)} tickers with data, "
            f"{combined.shape[0]} dates ({combined.shape[1]} columns)"
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
                df = self.blp.bdh(
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
    # Store: refdata, FX, derived layers
    # ------------------------------------------------------------------
    def _update_store_layers(self, benchmark: pd.DataFrame) -> None:
        """Benchmark, reference data, FX rates then fx_eur/clean. A failure here must not
        lose the raw data already written: it is reported in the manifest instead."""
        u, cfg, test = self.store_universe, self.config, self.test
        try:
            for col in (benchmark.columns if benchmark is not None else []):
                store_writer.upsert_long(u, store_layout.BENCHMARK, col,
                                         benchmark[[col]].rename(columns={col: "benchmark"}),
                                         config=cfg, test=test)
            self._update_refdata()
            self._update_fx()
            since = None if not self.daily else int(str(self.start_date)[:4])
            rep = store_derive.derive(u, cfg, test, since_year=since, registry_universe=self.universe)
            self._manifest.fx_missing = rep.get("fx_missing", [])
            self._manifest.clean_version = rep.get("clean_version")
            self._manifest.provisional_date = str(self.end_date)
        except Exception as e:
            logger.error(f"Store layers update failed: {e}")
            logger.error(traceback.format_exc())
            self._manifest.error = f"store layers: {type(e).__name__}: {e}"

    def _store_raw(self, alias: str, df: pd.DataFrame) -> None:
        """A store failure must never cost the legacy xlsx its data."""
        if not self.store_enabled or self.dry_run or df.empty:
            return
        try:
            store_writer.upsert_long(self.store_universe, "raw", alias, df, config=self.config, test=self.test)
            store_writer.update_state(self.store_universe, alias, df, self.config, self.test)
        except Exception as e:
            if self.xlsx_from_store:
                raise
            logger.error(f"Store write failed for '{alias}': {e}")
            self._manifest.error = f"store raw '{alias}': {type(e).__name__}: {e}"

    def _update_refdata(self) -> None:
        known = set(store_reader.read_refdata(self.store_universe, self.config, self.test).index)
        todo = [t for t in self.tickers if t not in known]
        if not todo:
            return
        try:
            raw = self.blp.bdp([t + self.ticker_suffix for t in todo],
                               ["CRNCY", "NAME", "GICS_SECTOR_NAME"],
                               backend="pandas", format="wide")
        except Exception as e:
            logger.warning(f"Reference data (BDP) failed, FX falls back to the exchange map: {e}")
            return
        if raw is None or raw.empty:
            return
        raw.columns = [str(c).lower() for c in raw.columns]
        df = pd.DataFrame({
            "currency": raw.get("crncy"), "name": raw.get("name"), "sector": raw.get("gics_sector_name"),
        })
        df.index = [str(i).replace(self.ticker_suffix, "") if self.ticker_suffix else str(i) for i in raw.index]
        store_writer.write_refdata(self.store_universe, df, self.config, self.test)

    def _update_fx(self) -> None:
        overrides = self.config.get("universe_overrides", {}).get(self.universe, {})
        if not overrides.get("fx_layer", True):
            return
        from dl import paths as dl_paths

        refdata = store_reader.read_refdata(self.store_universe, self.config, self.test)
        ccys = store_fx.required_currencies(self.tickers, refdata)
        if not ccys:
            return
        have = set(store_reader.read_fx(config=self.config, test=self.test).columns)
        for group, start in (([c for c in ccys if c in have], self.start_date),
                             ([c for c in ccys if c not in have], self.full_start_date)):
            if not group:
                continue
            logger.info(f"FX: {', '.join(group)} from {start}")
            df = self.blp.bdh(tickers=[store_fx.fx_ticker(c) for c in group], flds=["PX_LAST"],
                              start_date=start, end_date=self.end_date, **self.bdh_options)
            if df.empty:
                continue
            if isinstance(df.columns, pd.MultiIndex):
                df = df.droplevel(1, axis=1)
            df.columns = [str(c)[3:6] for c in df.columns]   # 'EURUSD Curncy' -> 'USD'
            store_writer.upsert_dir(dl_paths.fx_dir(self.config, self.test), df)

    # ------------------------------------------------------------------
    # Excel output
    # ------------------------------------------------------------------
    def _write_xlsx(self, results: dict[str, pd.DataFrame], benchmark: pd.DataFrame | None = None) -> None:
        """Write all results to a multi-sheet xlsx file."""
        logger.info(f"Writing output to {self.output_path}")

        # Write to a temp file then rename: the 18:15 ATLAS cron must never
        # read a half-written workbook.
        t0 = time.monotonic()
        smbio.atomic_write_via(
            self.output_path, lambda tmp: self._write_xlsx_to(tmp, results, benchmark)
        )
        self._manifest.xlsx = {
            "path": str(self.output_path),
            "bytes": os.path.getsize(self.output_path),
            "seconds": round(time.monotonic() - t0, 2),
        }
        logger.info(f"Output written: {self.output_path}")

    def _write_xlsx_to(self, path, results: dict[str, pd.DataFrame], benchmark: pd.DataFrame | None) -> None:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
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

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------
    def run(self) -> None:
        """Run the extraction and always persist a run manifest (unless dry-run)."""
        error = None
        try:
            self._run()
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            raise
        finally:
            if not self.dry_run:
                self._write_manifest(error)

    def _write_manifest(self, error: str | None) -> None:
        m = self._manifest
        m.date_range = [str(self.start_date), str(self.end_date)]
        m.per_field = {
            alias: self._field_reports[f]
            for alias, f in self.fields.items()
            if f in self._field_reports
        }
        m.finish(error or m.error)
        try:
            path = manifests.write(m, self.config)
            logger.info(f"Run manifest ({m.status}): {path}")
        except OSError as e:  # the share being down must not mask the run result
            logger.warning(f"Could not write run manifest: {e}")

    def _run(self) -> None:
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
        else:
            # Requests queued by the dashboard (index refresh, new index universe...)
            self._manifest.requests_processed = requests_worker.process_pending(self.blp, self.config)

        results: dict[str, pd.DataFrame] = {}

        for sheet_name, bbg_field in self.fields.items():
            logger.info(f"Extracting field: {bbg_field} -> sheet '{sheet_name}'")
            try:
                df = self._extract_alias(sheet_name, bbg_field)
                results[sheet_name] = df
                self._store_raw(sheet_name, df)
                if self._xlsx_exclude and not df.empty:
                    results[sheet_name] = df.drop(columns=[c for c in df.columns if c in self._xlsx_exclude])
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
                    aligned = df.reindex(master_index)
                    # a daily return is not a level: forward-filling it would invent returns
                    results[sheet_name] = aligned if sheet_name in self.no_ffill_fields else aligned.ffill()

        # Extract benchmark if configured
        benchmark_df = pd.DataFrame()
        if self.benchmark:
            logger.info(f"Extracting benchmark: {self.benchmark}")
            benchmark_df = self._extract_benchmark()
        new_benchmark = benchmark_df

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

        has_data = any(not df.empty for df in results.values())
        if self.store_enabled and has_data:
            self._update_store_layers(new_benchmark)

        # Only write if we got at least some data
        if has_data and self.xlsx_from_store:
            t0 = time.monotonic()
            info = legacy_xlsx.export(self.store_universe, self.output_path, list(self.fields),
                                      self.config["parameters"],
                                      tickers=[t for t in self.tickers if t not in self._xlsx_exclude],
                                      only_listed=bool(self._xlsx_exclude),
                                      no_ffill=self.no_ffill_fields,
                                      config=self.config, test=self.test)
            self._manifest.xlsx = {**info, "seconds": round(time.monotonic() - t0, 2), "source": "store"}
            logger.info(f"Output written from store: {self.output_path}")
        elif has_data:
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
        "--process-requests",
        action="store_true",
        help="Only process the dashboard request queue on the share (index "
             "membership refreshes, new index universes), then exit.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity",
    )
    args = parser.parse_args()

    logger.setLevel(getattr(logging, args.log_level))

    if args.process_requests:
        cfg = ATLASBloombergLoader._load_config(args.config)
        done = requests_worker.process_pending(_default_blp(), cfg)
        logger.info(f"{len(done)} request(s) processed: {', '.join(done) or '-'}")
        return

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
            blp_module=_default_blp(),
            override_map=im_cfg.get("exchange_code_map", {}),
            dry_run=args.dry_run,
            assume_yes=args.yes,
        )
        # The registry is the source of truth and is only written by the dashboard:
        # hand it the same membership as a finished request, to be applied there.
        if not args.dry_run and registry.exists(universe, cfg):
            from dl import requests_queue

            req = requests_queue.submit("index_members", universe, {"index": index}, "cli", cfg)
            requests_queue.claim(req["id"], cfg)
            requests_queue.complete(
                req["id"], requests_worker.run_index_members(universe, index, _default_blp(), cfg), cfg
            )
            logger.info(f"Registry diff queued for the dashboard: request {req['id']}")
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
