import os
import time

import pytest

from dl import registry, requests_apply, requests_queue as rq, requests_worker
from dl.registry import ops

CFG = {"index_members": {"exchange_code_map": {"SW": "SE"}}}


def test_queue_lifecycle(share):
    r = rq.submit("index_members", "demo", {"index": "DEMO Index"})
    assert [x["id"] for x in rq.list_("pending")] == [r["id"]]
    assert rq.claim(r["id"])["status"] == "claimed"
    assert rq.claim(r["id"]) is None                          # deja pris
    rq.complete(r["id"], {"new_tickers": ["A FP"]})
    assert rq.get(r["id"])["status"] == "done" and rq.list_("claimed") == []
    rq.close(r["id"], "rejected", "non")
    assert rq.find(r["id"])[0] == "rejected"
    r2 = rq.submit("index_members", "demo", {"index": "X"})
    assert rq.cancel(r2["id"]) and not rq.cancel(r2["id"])
    with pytest.raises(ValueError):
        rq.submit("nope", "demo", {})


def test_stale_claim_is_requeued(share):
    r = rq.submit("index_members", "demo", {"index": "X"})
    rq.claim(r["id"])
    p = rq.find(r["id"])[1]
    os.utime(p, (time.time() - 7200,) * 2)
    assert rq.requeue_stale() == [r["id"]] and rq.get(r["id"])["status"] == "pending"


def test_worker_then_apply_index_refresh(share, fake_blp):
    registry.save(ops.create("demo", ["AAA FP", "BBB GY", "ROG SE"], date="2025-01-02"), None)
    fake_blp.members["DEMO Index"] = ["AAA FP", "ROG SW", "NEW NA"]      # Bloomberg dit SW, le projet SE
    r = rq.submit("index_members", "demo", {"index": "DEMO Index"})
    assert requests_worker.process_pending(fake_blp, CFG) == [r["id"]]
    assert registry.load("demo").rev == 1                                 # le worker n'ecrit pas le registre
    req = rq.get(r["id"])
    assert req["status"] == "done" and req["result"]["new_tickers"] == ["AAA FP", "NEW NA", "ROG SE"]
    pv = requests_apply.preview(req)
    assert pv["joiners"] == ["NEW NA"] and pv["leavers"] == ["BBB GY"] and not pv["guardrail"]
    assert requests_apply.apply(r["id"]) == {"rev": 2, "ops": 2}
    u = registry.load("demo")
    assert u.deprecated_tickers() == ["BBB GY"] and "BBB GY" in u.fetch_tickers()
    assert rq.get(r["id"])["status"] == "applied"


def test_new_index_universe_stub_learns_convention_from_other_universes(share, fake_blp):
    registry.save(ops.create("ref", ["NESN SE", "AAA FP"]), None)
    registry.save(ops.create("neuf", [], kind="index", source={"type": "bbg_index", "index": "N Index"}), None)
    fake_blp.members["N Index"] = ["NESN SW", "ZZZ SW"]
    r = rq.submit("index_members", "neuf", {"index": "N Index"})
    requests_worker.process_pending(fake_blp, {})
    requests_apply.apply(r["id"])
    u = registry.load("neuf")
    assert u.active_tickers() == ["NESN SE", "ZZZ SE"]
    assert all(m.periods[0].entry is None for m in u.members)             # composition initiale : date inconnue


def test_worker_failure_and_auto_apply(share, fake_blp):
    u = ops.create("demo", ["AAA FP", "BBB GY"], source={"type": "bbg_index", "index": "D Index", "auto_apply": True})
    registry.save(u, None)
    bad = rq.submit("index_members", "demo", {"index": "EMPTY Index"})
    requests_worker.process_pending(fake_blp, {})
    assert rq.get(bad["id"])["status"] == "failed" and rq.get(bad["id"])["error"]
    fake_blp.members["D Index"] = ["AAA FP", "BBB GY", "CCC FP"]
    ok = rq.submit("index_members", "demo", {"index": "D Index"})
    requests_worker.process_pending(fake_blp, {})
    assert requests_apply.auto_apply_pending() == [ok["id"]]
    assert registry.load("demo").active_tickers() == ["AAA FP", "BBB GY", "CCC FP"]
    # garde-fou : une composition effondree n'est jamais appliquee automatiquement
    fake_blp.members["D Index"] = ["AAA FP"]
    g = rq.submit("index_members", "demo", {"index": "D Index"})
    requests_worker.process_pending(fake_blp, {})
    assert requests_apply.auto_apply_pending() == [] and rq.get(g["id"])["status"] == "done"


def test_apply_history_builds_periods():
    u = ops.create("demo", ["AAA FP", "BBB GY"], date=None, entry_source="seed")
    u, _ = ops.deprecate(u, "BBB GY", "2025-08-01", "index")
    snaps = {"2024-03-29": ["AAA FP", "OLD FP"], "2024-06-28": ["OLD FP", "BBB GY"],
             "2024-09-30": ["AAA FP", "BBB GY"]}
    new, o = ops.apply_history(u, snaps)
    per = lambda t: [(p.entry, p.exit) for p in new.member(t).periods]
    assert per("AAA FP") == [("2024-03-29", "2024-06-28"), ("2024-09-30", None)]
    assert per("BBB GY") == [("2024-06-28", "2025-08-01")]               # sortie connue conservee
    assert per("OLD FP") == [("2024-03-29", "2024-09-30")] and not new.member("OLD FP").active
    assert len(o) == 3
