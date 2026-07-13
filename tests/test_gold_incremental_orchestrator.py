import argparse

import pytest

from scripts.gold.refresh_gold_incremental import (
    MODE_ENV,
    MODE_FAST,
    MODE_STANDARD,
    facts_output_has_changes,
    resolve_mode,
)


def test_facts_output_has_changes_returns_false_for_explicit_no_change_marker():
    """Orchestrator phải skip downstream khi facts step báo không có thay đổi."""
    output_lines = [
        "gold_facts_incremental_refresh",
        "gold_facts_new_rows_to_append_total: 0",
        "gold_facts_has_changes: false",
    ]

    assert facts_output_has_changes(output_lines) is False


def test_facts_output_has_changes_returns_true_for_explicit_change_marker():
    """Orchestrator phải chạy downstream khi facts step append rows mới."""
    output_lines = [
        "gold_facts_incremental_refresh",
        "gold_facts_new_rows_to_append_total: 42",
        "gold_facts_has_changes: true",
    ]

    assert facts_output_has_changes(output_lines) is True


def test_facts_output_has_changes_defaults_to_true_when_marker_is_missing():
    """Thiếu marker thì giữ hành vi an toàn là chạy downstream."""
    output_lines = [
        "gold_facts_incremental_refresh",
        "legacy output without change marker",
    ]

    assert facts_output_has_changes(output_lines) is True


def test_resolve_mode_defaults_to_fast_for_incremental_runs(monkeypatch):
    """Incremental refresh thường xuyên phải ưu tiên fast mode."""
    monkeypatch.delenv(MODE_ENV, raising=False)
    args = argparse.Namespace(mode=None, ignore_state=False)

    assert resolve_mode(args) == MODE_FAST


def test_resolve_mode_defaults_to_standard_for_bootstrap_runs(monkeypatch):
    """Bootstrap cần validation kỹ hơn nên mặc định là standard mode."""
    monkeypatch.delenv(MODE_ENV, raising=False)
    args = argparse.Namespace(mode=None, ignore_state=True)

    assert resolve_mode(args) == MODE_STANDARD


def test_resolve_mode_prefers_cli_over_environment(monkeypatch):
    """CLI mode phải thắng env để debug từng lần chạy dễ kiểm soát."""
    monkeypatch.setenv(MODE_ENV, MODE_STANDARD)
    args = argparse.Namespace(mode=MODE_FAST, ignore_state=True)

    assert resolve_mode(args) == MODE_FAST


def test_resolve_mode_rejects_invalid_environment_value(monkeypatch):
    """Env sai phải fail sớm thay vì chạy sai mode âm thầm."""
    monkeypatch.setenv(MODE_ENV, "surprise")
    args = argparse.Namespace(mode=None, ignore_state=False)

    with pytest.raises(ValueError):
        resolve_mode(args)
