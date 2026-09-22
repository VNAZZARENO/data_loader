from types import SimpleNamespace

import pytest

import probe_euro_credit as probe


class Event(list):
    RESPONSE, PARTIAL_RESPONSE, REQUEST_STATUS, TIMEOUT = range(4)

    def __init__(self, kind, *messages):
        super().__init__(messages)
        self.kind = kind

    def eventType(self):
        return self.kind


def setup_api(events):
    pending = iter(events)
    state = {"purged": False, "requests": []}

    def purge():
        state["purged"] = True

    def next_event(timeout):
        assert 0 < timeout <= 1000
        return next(pending)

    queue = SimpleNamespace(nextEvent=next_event, purge=purge)
    api = SimpleNamespace(Event=Event, EventQueue=lambda: queue)
    session = SimpleNamespace(sendRequest=lambda request, **kw: state["requests"].append(request))
    return api, session, state


def test_probe_preserves_partial_replies_and_bloomberg_errors(capsys):
    api, session, state = setup_api([
        Event(Event.PARTIAL_RESPONSE, 'securityError = { category = BAD_SEC }'),
        Event(Event.RESPONSE, 'fieldExceptions = { fieldId = OAS_SPREAD_BID }'),
    ])
    probe.dump_response(session, "request", api)
    output = capsys.readouterr().out
    assert "BAD_SEC" in output and "OAS_SPREAD_BID" in output
    assert state["purged"] and state["requests"] == ["request"]


def test_probe_request_failure_keeps_message_and_cleans_up(capsys):
    api, session, state = setup_api([Event(Event.REQUEST_STATUS, "RequestFailure: connection lost")])
    with pytest.raises(RuntimeError, match="request failed"):
        probe.dump_response(session, "request", api)
    assert "connection lost" in capsys.readouterr().out
    assert state["purged"]


def test_probe_cancels_after_timeout(monkeypatch):
    api, session, state = setup_api([Event(Event.TIMEOUT)])
    clock = iter([0, 0, 31])
    monkeypatch.setattr(probe.time, "monotonic", lambda: next(clock))
    with pytest.raises(TimeoutError, match="30s"):
        probe.dump_response(session, "request", api, timeout=30)
    assert state["purged"]
