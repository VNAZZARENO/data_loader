from dl import tickers


def test_split_ticker():
    assert tickers.split_ticker("ROP SE") == ("ROP", "SE")
    assert tickers.split_ticker("AV/ LN") == ("AV/", "LN")
    assert tickers.split_ticker("SPX") == ("SPX", "")


def test_parse_list_mixed_separators():
    assert tickers.parse_list("Ticker\r\nROG SW, san sm;\tMC FP\n\n") == ["ROG SW", "san sm", "MC FP"]


def test_normalize_equity_universe():
    r = tickers.normalize(
        ["rog sw equity", "SAN  SM", "MC FP", "MC FP Equity", "AAPL UQ"],
        ticker_suffix=" Equity",
        exchange_map={"SW": "SE", "SM": "SQ"},
        known_exchanges={"SE", "SQ", "FP"},
    )
    assert r["tickers"] == ["ROG SE", "SAN SQ", "MC FP", "AAPL UQ"]
    assert r["duplicates"] == ["MC FP"]
    assert r["unknown_exchanges"] == ["UQ"]
    assert r["changed"]["rog sw equity"] == "ROG SE"


def test_normalize_mixed_universe_keeps_yellow_key():
    r = tickers.normalize(["sx5e index", "MC FP", "EURUSD Curncy"], ticker_suffix="")
    assert r["tickers"] == ["SX5E Index", "MC FP Equity", "EURUSD Curncy"]
