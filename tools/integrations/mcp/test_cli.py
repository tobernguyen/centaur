"""Tests for CLI argument parsing."""

from __future__ import annotations

import io

import pytest
import typer
from typer.testing import CliRunner

from cli import _parse_args, app


def test_args_dash_with_empty_stdin_teaches_the_inline_fix(monkeypatch) -> None:
    # The prod failure mode: `--args -` from an exec interface with no stdin
    # attached must say how to fix it, not "Expecting value: line 1 column 1".
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    with pytest.raises(typer.BadParameter, match="inline"):
        _parse_args("-", [])


def test_args_dash_reads_piped_stdin(monkeypatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO('{"verdict": "safe"}'))
    assert _parse_args("-", []) == {"verdict": "safe"}


def test_param_pairs_parse_json_values_with_string_fallback() -> None:
    assert _parse_args(None, ["n=1", "s=text", 'j={"x": 2}']) == {
        "n": 1,
        "s": "text",
        "j": {"x": 2},
    }


def test_args_and_param_are_mutually_exclusive() -> None:
    with pytest.raises(typer.BadParameter, match="not both"):
        _parse_args("{}", ["a=1"])


def test_invalid_inline_json_still_reports_the_parse_error() -> None:
    with pytest.raises(typer.BadParameter, match="valid JSON"):
        _parse_args("{nope", [])


def test_call_accepts_the_natural_positional_json_guess(monkeypatch) -> None:
    # Agents habitually try `mcp call <server> <tool> '{json}'` first; accept it.
    monkeypatch.delenv("CENTAUR_MCP_SERVERS", raising=False)
    result = CliRunner().invoke(app, ["call", "srv", "tool", '{"a": 1}'])
    # Argument parsing succeeded; the failure is the deliberately unset registry.
    assert "no servers configured" in result.output


def test_call_rejects_positional_json_and_args_flag_together(monkeypatch) -> None:
    monkeypatch.delenv("CENTAUR_MCP_SERVERS", raising=False)
    result = CliRunner().invoke(app, ["call", "srv", "tool", "{}", "--args", "{}"])
    assert result.exit_code == 2
    assert "no servers configured" not in result.output
