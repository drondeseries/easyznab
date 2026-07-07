"""
query_replace.py — per-title query rewriting for the Easynews indexer.

Sits in front of the generic Norwegian transliteration: maps a specific incoming
search TITLE to the exact form a release is posted under, for cases the generic
ASCII fold can't cover or where upstream (AIOStreams) has already mangled the
title. Applied to the title only; season/episode/year are appended by the caller
afterwards (or protected here if a client embedded them), so they always carry
over.

Configure with EASYNEWS_QUERY_REPLACE. Two accepted formats:

  Simple (easiest in .env) — rules separated by ';;' or newlines, 'match => to':
    EASYNEWS_QUERY_REPLACE="ikke lov a le pa hytta => Ikke lov aa le paa hytta ;; norsemen => Vikingane"

  JSON (for awkward characters / explicit ordering):
    EASYNEWS_QUERY_REPLACE=[{"match":"norsemen","to":"Vikingane"}]

Match syntax. Matching is CASE-INSENSITIVE. Write keys in the folded/lowercased
form the query actually arrives in (AIOStreams lowercases and ASCII-folds, so
"å"->"a", German "ü"->"ue", etc.):

  norsemen                  substring  -> that phrase is replaced by `to`
  temptation island*        starts-with-> whole title becomes `to`, or the
                                          literal "temptation island" if `to` empty
  *verfuehrung im paradies  ends-with  -> whole title becomes `to`, or the matched
                                          suffix literal if `to` empty
  *paradies*                contains   -> same, for a phrase anywhere in the title

Rules are tried in order; the FIRST match wins. Put more specific rules first.
"""
from __future__ import annotations

import json
import logging
import re
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# A rule is (core, to, anchor, pattern). anchor in {"substr","prefix","suffix",
# "contains"}; pattern is the precompiled word-bounded regex for "substr" rules
# (compiled once at parse time — apply_rules runs on every search request).
Rule = Tuple[str, str, str, Optional[re.Pattern]]

# Trailing release metadata to protect from truncation even if a client embeds it
# in q (e.g. "title s06e03"). Season/episode/quality only — NOT a bare year, so we
# never mistake titles like "1923" or "Blade Runner 2049" for metadata.
_TAIL_RE = re.compile(
    r"\s+(?=(?:s\d{1,2}(?:e\d{1,3})?|e\d{1,3}|\d{3,4}p)(?:\b|$))",
    re.IGNORECASE,
)
_WS_RE = re.compile(r"\s+")


def _split_title_tail(text: str) -> Tuple[str, str]:
    """Split 'Title S06E03' into ('Title', ' S06E03'). Tail keeps its leading
    space so callers can just concatenate without losing separator."""
    m = _TAIL_RE.search(text)
    if m:
        return text[: m.start()], text[m.start() :]
    return text, ""


def _parse_simple(raw: str) -> List[Rule]:
    """Parse the ';;'-separated simple format."""
    rules: List[Rule] = []
    for part in re.split(r";;|\n", raw):
        part = part.strip()
        if not part or part.startswith("#"):
            continue
        if "=>" not in part:
            logger.debug("query_replace: skipping unparseable rule %r", part)
            continue
        match_str, _, to_str = part.partition("=>")
        match_str = match_str.strip()
        to_str = to_str.strip()
        if not match_str:
            continue
        rules.append(_compile_rule(match_str, to_str))
    return rules


def _compile_rule(match_str: str, to_str: str) -> Rule:
    core = match_str.strip("*").strip()
    starts = match_str.endswith("*") and not match_str.startswith("*")
    ends   = match_str.startswith("*") and not match_str.endswith("*")
    contains = match_str.startswith("*") and match_str.endswith("*")

    if starts:
        return (core, to_str, "prefix", None)
    if ends:
        return (core, to_str, "suffix", None)
    if contains:
        return (core, to_str, "contains", None)
    # Substring rule: word-bounded search
    pat = re.compile(r"(?<!\w)" + re.escape(core) + r"(?!\w)", re.IGNORECASE)
    return (core, to_str, "substr", pat)


def parse_rules(raw: str) -> List[Rule]:
    """Parse EASYNEWS_QUERY_REPLACE value into a list of compiled rules.
    Returns [] for empty/invalid input (never raises)."""
    if not raw or not raw.strip():
        return []
    raw = raw.strip()
    # Try JSON first
    if raw.startswith("["):
        try:
            items = json.loads(raw)
            rules: List[Rule] = []
            for item in items:
                match_str = str(item.get("match", "")).strip()
                to_str = str(item.get("to", "")).strip()
                if match_str:
                    rules.append(_compile_rule(match_str, to_str))
            return rules
        except json.JSONDecodeError as exc:
            logger.warning("query_replace: JSON parse failed (%s), falling back to simple format", exc)
    return _parse_simple(raw)


def apply_rules(title: str, rules: List[Rule]) -> str:
    """Apply the first matching rule to *title* and return the rewritten title.
    Returns *title* unchanged if no rule matches."""
    if not rules or not title:
        return title

    head, tail = _split_title_tail(title)
    low = head.lower()

    for core, to_str, anchor, pattern in rules:
        core_low = core.lower()
        matched = False
        replacement = to_str  # default: use the explicit `to` string

        if anchor == "prefix":
            if low.startswith(core_low):
                matched = True
                replacement = to_str or core  # empty `to` → keep matched literal
        elif anchor == "suffix":
            if low.endswith(core_low):
                matched = True
                replacement = to_str or core
        elif anchor == "contains":
            if core_low in low:
                matched = True
                replacement = to_str or core
        else:  # "substr"
            assert pattern is not None
            m = pattern.search(head)
            if m:
                matched = True
                if to_str:
                    # Replace only the matched span, preserving surrounding text
                    replacement = pattern.sub(to_str, head, count=1)
                    return _WS_RE.sub(" ", replacement + tail).strip()
                # No `to` → replace matched span with the core literal (normalises case)
                replacement = pattern.sub(core, head, count=1)
                return _WS_RE.sub(" ", replacement + tail).strip()

        if matched:
            result = (replacement + tail) if replacement else title
            return _WS_RE.sub(" ", result).strip()

    return title
