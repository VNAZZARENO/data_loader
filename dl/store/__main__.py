"""python -m dl.store {derive,compact,info} --universe u"""

import argparse
import logging

import pandas as pd

from .. import migrate, paths, smbio
from . import derive as derive_mod
from . import layout, reader


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("cmd", choices=["derive", "compact", "info"])
    ap.add_argument("--universe", required=True)
    ap.add_argument("--config", default=None)
    ap.add_argument("--out", default=None, help="compact : fichier parquet unique de sortie")
    ap.add_argument("--layers", default="raw,fx_eur,clean")
    ap.add_argument("--since-year", type=int, default=None)
    a = ap.parse_args(argv)
    config = migrate.load_config(a.config)
    print(f"Partage : {paths.share_root(config)}")

    if a.cmd == "derive":
        print(derive_mod.derive(a.universe, config, since_year=a.since_year))
    elif a.cmd == "info":
        for layer in layout.LAYERS:
            for f in reader.fields(a.universe, layer, config):
                print(f"  {layer:7s} {f:24s} {reader.years(a.universe, layer, f, config)}")
    else:
        out = a.out or str(paths.store_dir(a.universe, config) / f"{a.universe}_compact.parquet")
        parts = [reader.read(a.universe, layer=l, wide=False, splice_renames=False, config=config)
                 for l in a.layers.split(",")]
        table = pd.concat(parts, ignore_index=True)
        smbio.atomic_write_via(out, lambda tmp: table.to_parquet(tmp, compression="zstd"))
        print(f"{len(table):,} lignes -> {out}")


if __name__ == "__main__":
    main()
