"""Sink thresholds reject invalid levels before alerts can be filtered."""

from __future__ import annotations

import io
import json
from collections.abc import Callable

import pytest

from snagline.risk import FailureRisk
from snagline.sinks.base import AlertSink
from snagline.sinks.console import ConsoleSink
from snagline.sinks.logging_sink import LoggingSink
from snagline.sinks.pagerduty import PagerDutySink
from snagline.sinks.slack import SlackSink
from snagline.sinks.webhook import WebhookSink


@pytest.fixture(params=["console", "webhook", "slack", "logging", "pagerduty"])
def make_sink(request: pytest.FixtureRequest) -> Callable[[str | None], AlertSink]:
    factories: dict[str, Callable[[str | None], AlertSink]] = {
        "console": lambda level: ConsoleSink(min_severity=level),
        "webhook": lambda level: WebhookSink("http://localhost", min_severity=level),
        "slack": lambda level: SlackSink("http://localhost", min_severity=level),
        "logging": lambda level: LoggingSink(min_severity=level),
        "pagerduty": lambda level: PagerDutySink("test-key", min_severity=level),
    }
    return factories[request.param]


@pytest.mark.parametrize("level", ["fatal", "crit", "WARN", "high", "", " warning"])
def test_invalid_min_severity(
    make_sink: Callable[[str | None], AlertSink], level: str
) -> None:
    with pytest.raises(ValueError, match="invalid min_severity") as exc:
        make_sink(level)
    for valid in ("info", "warning", "critical"):
        assert repr(valid) in str(exc.value)


@pytest.mark.parametrize("level", [None, "info", "warning", "critical"])
def test_valid_min_severity(
    make_sink: Callable[[str | None], AlertSink], level: str | None
) -> None:
    make_sink(level)


@pytest.mark.parametrize(
    ("level", "expected"),
    [
        (None, ["info", "warning", "critical"]),
        ("info", ["info", "warning", "critical"]),
        ("warning", ["warning", "critical"]),
        ("critical", ["critical"]),
    ],
)
def test_console_threshold_filtering(level: str | None, expected: list[str]) -> None:
    output = io.StringIO()
    sink = ConsoleSink(stream=output, min_severity=level)
    for severity in ("info", "warning", "critical"):
        sink.emit(
            FailureRisk("ep", "step", 0.9, "loop", "test", 1.0, severity=severity)
        )
    assert [
        json.loads(line)["severity"] for line in output.getvalue().splitlines()
    ] == expected
