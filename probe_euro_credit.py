#!/usr/bin/env python3
"""Print raw Bloomberg replies for the empty euro_credit fields.

Run on the Bloomberg Terminal machine:
    python probe_euro_credit.py > probe_euro_credit.txt
    python probe_euro_credit.py --tickers "EUSA2 Curncy" --fields PX_LAST

Uses blpapi directly so securityError, fieldExceptions and responseError are
preserved. Reads the configured field list, but never runs the loader or writes
its workbook/store. BDH uses a short daily window without fill/calendar overrides.
A BDP value alone does not establish historical availability.
"""

import argparse
import datetime as dt
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import time

import yaml


def dump_response(session, request, api, timeout=30):
    """Print all partial/final replies, with a bounded wait and cancellation."""
    queue = api.EventQueue()
    print(request, flush=True)
    try:
        session.sendRequest(request, eventQueue=queue)
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"No complete Bloomberg response after {timeout}s")
            event = queue.nextEvent(max(1, min(1000, int(remaining * 1000))))
            for message in event:
                print(message, flush=True)
            if event.eventType() == api.Event.RESPONSE:
                return
            if event.eventType() == api.Event.REQUEST_STATUS:
                raise RuntimeError("Bloomberg request failed; see raw response above")
    finally:
        queue.purge()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path,
                        default=Path(__file__).parent / "config" / "atlas_config.yaml")
    parser.add_argument("--tickers", nargs="+", default=["LECPTREU Index", "HE00 Index"])
    parser.add_argument("--fields", nargs="+", help="Override configured euro_credit fields")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--end-date", type=dt.date.fromisoformat, default=dt.date.today())
    parser.add_argument("--timeout", type=int, default=30, help="Seconds per request")
    args = parser.parse_args()
    if args.days <= 0 or args.timeout <= 0:
        parser.error("--days and --timeout must be positive")
    if args.fields:
        fields = args.fields
    else:
        with args.config.open(encoding="utf-8") as source:
            fields = list(yaml.safe_load(source)["universe_overrides"]["euro_credit"]["fields"].values())

    # Lazy import lets --help work away from the Bloomberg machine.
    import blpapi

    for package in ("xbbg", "blpapi"):
        try:
            print(f"{package}={version(package)}", flush=True)
        except PackageNotFoundError:
            print(f"{package}: package metadata unavailable", flush=True)
    start = args.end_date - dt.timedelta(days=args.days)
    print(f"Window: {start} -> {args.end_date}; raw daily BDH, no fill overrides", flush=True)
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)
    session = blpapi.Session(options)
    failures = 0
    try:
        if not session.start():
            raise RuntimeError("Cannot start Bloomberg session; open the Terminal first")
        if not session.openService("//blp/refdata"):
            raise RuntimeError("Cannot open //blp/refdata")
        service = session.getService("//blp/refdata")
        for kind in ("ReferenceDataRequest", "HistoricalDataRequest"):
            request = service.createRequest(kind)
            for ticker in args.tickers:
                request.getElement("securities").appendValue(ticker)
            requested_fields = (["NAME"] if kind == "ReferenceDataRequest" else []) + fields
            for field in dict.fromkeys(requested_fields):
                request.getElement("fields").appendValue(field)
            if kind == "HistoricalDataRequest":
                request.set("startDate", start.strftime("%Y%m%d"))
                request.set("endDate", args.end_date.strftime("%Y%m%d"))
                request.set("periodicitySelection", "DAILY")
            print(f"\n=== {kind} ===", flush=True)
            try:
                dump_response(session, request, blpapi, args.timeout)
            except (RuntimeError, TimeoutError) as exc:
                failures += 1
                print(f"ERROR: {exc}", flush=True)
    finally:
        session.stop()
    print("\nInspect securityError, fieldExceptions and responseError in the raw replies.")
    return int(failures > 0)


if __name__ == "__main__":
    raise SystemExit(main())
