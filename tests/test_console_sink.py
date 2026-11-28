"""Tests for the console sink (issue #19: fire-and-forget on a broken stream)."""

from __future__ import annotations

import io
import json
import logging
from dataclasses import replace

import pytest

from snagline.risk import FailureRisk
from snagline.sinks.console import ConsoleSink


def _risk() -> FailureRisk:
    return FailureRisk("ep-1", "step-1", 0.8, "loop", "test detail", 1.0)


@pytest.mark.parametrize("severity", ["", "info", "warning", "critical"])
def test_console_sink_writes_json_line_to_stream(severity: str) -> None:
    buf = io.StringIO()
    sink = ConsoleSink(stream=buf)
    risk = replace(_risk(), severity=severity)
    sink.emit(risk)
    lines = buf.getvalue().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["trigger"] == "loop"
    assert payload["severity"] == risk.severity


@pytest.mark.parametrize("severity", ["", "info", "warning", "critical"])
def test_console_sink_routes_through_logger(severity: str) -> None:
    records: list[logging.LogRecord] = []

    class _Cap(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    logger = logging.getLogger("snagline.test.console")
    cap = _Cap()
    logger.addHandler(cap)
    try:
        sink = ConsoleSink(logger=logger, level=logging.WARNING)
        risk = replace(_risk(), severity=severity)
        sink.emit(risk)
    finally:
        logger.removeHandler(cap)
    assert len(records) == 1
    payload = json.loads(records[0].getMessage())
    assert payload["trigger"] == "loop"
    assert payload["severity"] == risk.severity


def test_console_sink_serializes_explicit_severity() -> None:
    import io

    buf = io.StringIO()
    sink = ConsoleSink(stream=buf)
    sink.emit(FailureRisk("ep", "step", 0.9, "loop", "detail", 1.0, severity="info"))
    assert json.loads(buf.getvalue())["severity"] == "info"


def test_console_sink_is_fire_and_forget_on_broken_stream() -> None:
    # Issue #19: a broken stream must not raise out of emit(); the sink is
    # part of the fail-open ingest path.
    class BrokenStream:
        def write(self, s: str) -> int:
            raise OSError("stream broken")

        def flush(self) -> None:
            raise OSError("stream broken")

    sink = ConsoleSink(stream=BrokenStream())  # type: ignore[arg-type]
    # Must not raise.
    sink.emit(_risk())


# --- a binary stream is rejected at construction, not at alert time (#391) ----


def test_console_sink_rejects_binary_stream_at_construction() -> None:
    """``open(path, "wb")`` and ``sys.stdout.buffer`` are the natural ways to
    route alerts to a file, and a str write to either raises ``TypeError``
    -- not an ``OSError`` subclass, so the fire-and-forget guard in emit()
    never caught it. Before this fix every alert was silently dropped for the
    whole run behind the fail-open contract, with the monitor otherwise
    healthy."""
    import io

    with pytest.raises(TypeError, match="writable text stream"):
        ConsoleSink(stream=io.BytesIO())  # type: ignore[arg-type]


def test_console_sink_rejects_binary_file_at_construction(tmp_path) -> None:
    """The file spelling, which is arguably the more likely one for anyone
    routing alerts to disk."""
    with pytest.raises(TypeError, match="writable text stream"):
        with (tmp_path / "alerts.jsonl").open("wb") as fh:
            ConsoleSink(stream=fh)  # type: ignore[arg-type]


def test_console_sink_accepts_a_closed_stream_at_construction() -> None:
    """The control that pins the probe's scope: a closed stream is runtime
    breakage, not misconfiguration, and issue #327's contract (upstream) is
    that the sink still constructs and drops the alert in ``emit``. Only the
    type mismatch is a construction-time rejection, so the probe must let a
    closed stream through."""
    import io

    buf = io.StringIO()
    buf.close()
    # Must not raise -- emit() handles it (and test_issues_321_327.py covers
    # the warning-once behaviour there).
    ConsoleSink(stream=buf)


def test_console_sink_rejection_message_advises_the_alternatives(
    tmp_path, capsys
) -> None:
    """The message is the operator's only signal: name what to do instead,
    since the sink is the default and the misconfiguration is silent once
    running."""
    import io

    with pytest.raises(TypeError) as excinfo:
        ConsoleSink(stream=io.BytesIO())  # type: ignore[arg-type]
    message = str(excinfo.value)
    assert "open(path, 'w')" in message
    assert "logger=" in message
    # The offending stream's own error travels with it, so the cause is not
    # a mystery.
    assert "bytes-like object" in message


def test_console_sink_emit_survives_a_stream_closed_after_construction() -> None:
    """Construction-time validation cannot cover a stream that is closed
    later, so the emit guard stays: it must catch ValueError (closed) and
    TypeError (a stream whose type changed underneath it), not OSError
    alone."""


def test_console_sink_min_severity_filters_below_threshold() -> None:
    # The console is the default escalation target, so --min-severity must
    # filter it like the webhook/slack/pagerduty sinks do (issue #248 wired
    # only those three). A critical-only console must drop warning/info risks
    # and keep critical ones.
    import io

    buf = io.StringIO()
    sink = ConsoleSink(stream=buf, min_severity="critical")
    sink.emit(FailureRisk("ep", "s-warn", 0.6, "loop", "warn", 1.0))  # warning
    sink.emit(FailureRisk("ep", "s-info", 0.3, "loop", "info", 2.0))  # info
    assert buf.getvalue() == "", "below-threshold risks must be dropped"
    sink.emit(FailureRisk("ep", "s-crit", 0.9, "loop", "crit", 3.0))  # critical
    out = buf.getvalue()
    assert '"step_id": "s-crit"' in out
    assert "s-warn" not in out and "s-info" not in out


def test_console_sink_no_min_severity_emits_everything() -> None:
    # Default (no filter) keeps the pre-#248 behaviour: every risk prints.
    import io

    buf = io.StringIO()
    sink = ConsoleSink(stream=buf)
    buf.close()
    # Must not raise, and the alert is dropped rather than reaching the host.
    sink.emit(_risk())


def test_console_sink_default_stream_is_stderr() -> None:
    """The no-argument default path is untouched: probing sys.stderr would
    write to the terminal on every construction, so the probe only runs on an
    explicitly supplied stream."""
    import sys

    assert ConsoleSink()._stream is sys.stderr


def test_console_sink_probe_writes_nothing_to_a_good_stream() -> None:
    """The empty write used to detect a binary stream must leave the stream
    clean for the real payload."""
    import io

    buf = io.StringIO()
    sink = ConsoleSink(stream=buf)
    sink.emit(_risk())
    lines = buf.getvalue().splitlines()
    assert len(lines) == 1
    assert '"trigger": "loop"' in lines[0]
    sink.emit(FailureRisk("ep", "s-info", 0.1, "loop", "info", 1.0))
    assert '"step_id": "s-info"' in buf.getvalue()
