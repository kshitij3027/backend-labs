"""Tests for :class:`src.query.synonyms.SynonymExpander`.

Covers the three axes that matter for expansion:

1. **Correctness on the default dictionary** — the seed JSON shipped
   with the module produces the expected synonyms for its head words.
2. **Caps** — per-token and overall expansion limits are enforced
   even when the dict offers more alternates than we accept.
3. **Robustness** — missing tokens, malformed JSON, and the WordNet
   fallback all degrade gracefully to a passthrough.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.query import synonyms as synonyms_module
from src.query.synonyms import SynonymExpander


# ---------------------------------------------------------------------------
# Default dictionary
# ---------------------------------------------------------------------------


def test_default_dict_expands_error_with_synonyms():
    """``error`` has three canonical synonyms baked into the default JSON."""
    expander = SynonymExpander()
    out = expander.expand(["error"])
    assert "error" in out
    # The default JSON ships failure/exception/fault as synonyms.
    assert "failure" in out
    assert "exception" in out
    assert "fault" in out


def test_originals_preserved():
    """Every input token must survive into the output."""
    expander = SynonymExpander()
    out = expander.expand(["error", "xyzzy", "slow"])
    assert "error" in out
    assert "xyzzy" in out
    assert "slow" in out


def test_dedup_input_tokens():
    """Duplicate input tokens collapse — no repeats in the output."""
    expander = SynonymExpander()
    out = expander.expand(["error", "error"])
    assert out.count("error") == 1


def test_lowercasing_normalizes_input():
    """Uppercase inputs map to lowercase outputs."""
    expander = SynonymExpander()
    out = expander.expand(["Error"])
    assert "error" in out
    assert "Error" not in out


# ---------------------------------------------------------------------------
# Missing-token passthrough
# ---------------------------------------------------------------------------


def test_unknown_token_returns_only_itself():
    """Tokens absent from the dict pass through untouched (no crash)."""
    expander = SynonymExpander()
    assert expander.expand(["xyzzy"]) == ["xyzzy"]


# ---------------------------------------------------------------------------
# Caps
# ---------------------------------------------------------------------------


def test_per_token_expansion_cap(tmp_path: Path):
    """Even if the dict offers more than the cap, only the first N expand.

    Build a custom JSON that maps one head word to six synonyms and
    assert only the first ``_MAX_EXPANSIONS_PER_TOKEN`` come through.
    """
    cap = synonyms_module._MAX_EXPANSIONS_PER_TOKEN
    many = [f"syn{i}" for i in range(cap + 3)]
    cfg = tmp_path / "syn.json"
    cfg.write_text(json.dumps({"head": many}), encoding="utf-8")

    expander = SynonymExpander(path=str(cfg))
    out = expander.expand(["head"])
    # Original plus exactly ``cap`` synonyms — nothing more.
    assert out[0] == "head"
    synonyms_out = [t for t in out if t != "head"]
    assert len(synonyms_out) == cap
    # The first ``cap`` of our generated names came through.
    for i in range(cap):
        assert f"syn{i}" in out
    # Over-the-cap entries did not.
    for i in range(cap, cap + 3):
        assert f"syn{i}" not in out


# ---------------------------------------------------------------------------
# Custom path + bad input
# ---------------------------------------------------------------------------


def test_custom_path_override(tmp_path: Path):
    """A caller-supplied path fully replaces the default dict."""
    cfg = tmp_path / "syn.json"
    cfg.write_text(json.dumps({"foo": ["bar"]}), encoding="utf-8")
    expander = SynonymExpander(path=str(cfg))
    out = expander.expand(["foo"])
    assert "foo" in out
    assert "bar" in out


def test_invalid_json_falls_back_to_empty_dict(tmp_path: Path):
    """A garbled JSON file must not crash construction or expansion."""
    cfg = tmp_path / "bad.json"
    cfg.write_text("this is not json { [", encoding="utf-8")
    expander = SynonymExpander(path=str(cfg))
    # No synonyms loaded → input passes through as-is.
    assert expander.expand(["foo"]) == ["foo"]


def test_nonexistent_path_falls_back_to_empty_dict(tmp_path: Path):
    """A path that doesn't exist also degrades to passthrough."""
    missing = tmp_path / "nope.json"
    expander = SynonymExpander(path=str(missing))
    assert expander.expand(["foo"]) == ["foo"]


# ---------------------------------------------------------------------------
# WordNet fallback (optional path, corpus baked in the image)
# ---------------------------------------------------------------------------


def test_wordnet_fallback_keeps_original(tmp_path: Path):
    """With WordNet enabled, the original token still comes through.

    The assertion stays tolerant: WordNet's synset list is not part of
    our contract, but the passthrough behaviour is. The test uses an
    empty dict so every lookup falls through to the WordNet path.
    """
    # Empty dict → every expansion goes through WordNet.
    cfg = tmp_path / "empty.json"
    cfg.write_text("{}", encoding="utf-8")
    expander = SynonymExpander(path=str(cfg), use_wordnet=True)
    out = expander.expand(["car"])
    assert "car" in out
    assert len(out) >= 1


# ---------------------------------------------------------------------------
# Ordering (originals come before their synonyms)
# ---------------------------------------------------------------------------


def test_original_appears_before_synonyms():
    """The head word is emitted first, then its synonyms."""
    expander = SynonymExpander()
    out = expander.expand(["error"])
    # ``error`` sits at position 0 — synonyms follow.
    assert out[0] == "error"


# ---------------------------------------------------------------------------
# Total cap (belt-and-braces — not required by spec but worth pinning)
# ---------------------------------------------------------------------------


def test_total_expansion_cap_respected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Across many tokens, the overall expansion stays capped.

    Patch ``_MAX_TOTAL_EXPANSIONS`` to a small value and verify the
    total count of *added* synonyms (output minus input) doesn't exceed
    it. Every input token must still appear in the output.
    """
    # Build a dict that gives each input two synonyms.
    cfg = tmp_path / "syn.json"
    cfg.write_text(
        json.dumps(
            {
                "a": ["a1", "a2"],
                "b": ["b1", "b2"],
                "c": ["c1", "c2"],
                "d": ["d1", "d2"],
            }
        ),
        encoding="utf-8",
    )
    # Cap at 3 total synonyms — below the 8 the dict could produce.
    monkeypatch.setattr(synonyms_module, "_MAX_TOTAL_EXPANSIONS", 3)

    expander = SynonymExpander(path=str(cfg))
    inputs = ["a", "b", "c", "d"]
    out = expander.expand(inputs)
    # All inputs survive.
    for tok in inputs:
        assert tok in out
    added = [t for t in out if t not in inputs]
    assert len(added) <= 3
