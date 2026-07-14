"""Synthetic **labeled log corpus** generator — the project's ground-truth data foundation.

The NLP engine is trained and measured against data with *known* labels, but no real
labeled ops-log dataset exists here. This module manufactures one deterministically: a
balanced, realistic corpus of log lines, each carrying its exact ground-truth **intent**,
**sentiment/severity**, and **named entities**. It is imported by:

* **C4** (intent training) — :func:`produce_corpus` is the balanced training/eval set for
  the TF-IDF + LogisticRegression intent classifier.
* **C10** (E2E accuracy gates) — :func:`sample_messages` yields held-out labeled lines the
  verifier pushes through the live API to score intent accuracy and NER recall.
* **unit tests** across the NLP layer that need inputs with known answers.

Because it *is* the ground truth, two properties are non-negotiable:

1. **Determinism.** Every producer takes a ``seed`` and uses a private
   ``random.Random(seed)`` — never the global :mod:`random` module and never the wall
   clock — so the same arguments always yield a byte-identical corpus (reproducible
   training, stable test assertions). It is kept dependency-free (standard library only)
   so it imports everywhere, including the E2E scripts.
2. **Entity spans are sacred.** Each message is assembled by filling ``{slot}``
   placeholders in a template; the *exact* filled surface string and its label are
   recorded as ground truth, and every recorded surface is guaranteed to be an exact
   substring of the final message (this is what C10 scores NER recall against). The
   realism **noise** — timestamp/level prefixes, casing jitter, synonym swaps, suffixes —
   is applied only to the *connective* skeleton (before any slot is filled, while the
   entities are still ``{placeholder}`` tokens) or as a pure prepend/append. So **noise
   never mutates an entity span**: slot surfaces stay verbatim and findable.

Sentiment is a property of the **template, not the intent**: ``authentication`` carries
both a positive "login succeeded" line and a negative "invalid password" line;
``error_report`` / ``resource_warning`` skew negative/critical; ``health_check`` and a
successful ``deployment`` skew positive/neutral; ``config_change`` is mostly neutral. This
teaches the downstream models that phrasing — not the coarse intent — drives severity.
"""

from __future__ import annotations

import random
import re
from dataclasses import dataclass

#: The closed set of intent labels the classifier learns (C4). Order is stable and used
#: verbatim as the per-intent generation loop in :func:`produce_corpus`.
INTENTS: list[str] = [
    "authentication",
    "deployment",
    "error_report",
    "health_check",
    "resource_warning",
    "network",
    "database",
    "config_change",
]

#: The custom log-entity labels the NER layer (C3) targets. General spaCy entities (ORG,
#: DATE, ...) are additive at runtime and are not part of this synthetic ground truth.
ENTITY_LABELS: list[str] = [
    "SERVICE",
    "HOST",
    "IP",
    "USER_ID",
    "ERROR_CODE",
    "PATH",
    "URL",
    "PORT",
]

#: The severity/sentiment classes (C5). Assigned per-template (see the module docstring),
#: never per-intent.
SENTIMENTS: list[str] = ["positive", "neutral", "negative", "critical"]


@dataclass(frozen=True)
class LogSample:
    """One synthetic log line plus its complete ground-truth labels.

    Immutable (``frozen=True``) so a sample is hashable and cannot be accidentally mutated
    after generation.

    Attributes:
        message: The rendered log line (slots filled, optional realism noise applied).
        intent: The line's intent — one of :data:`INTENTS`.
        entities: The ground-truth named entities as ``(surface_text, label)`` pairs, where
            ``label`` is one of :data:`ENTITY_LABELS` and ``surface_text`` is guaranteed to
            be an exact substring of ``message``. Every sample carries at least one entity.
        sentiment: The line's severity/sentiment — one of :data:`SENTIMENTS`.
    """

    message: str
    intent: str
    entities: tuple[tuple[str, str], ...]
    sentiment: str


@dataclass(frozen=True)
class _Template:
    """A generation template: intent + expected sentiment + a slotted text skeleton.

    Attributes:
        intent: The intent every line rendered from this template carries.
        sentiment: The sentiment/severity this specific phrasing implies (per-template).
        text: The skeleton with ``{slot}`` placeholders, e.g.
            ``"invalid password for user {user} on {service}"``.
        slots: Ordered ``(slot_name, entity_label)`` pairs. Each ``slot_name`` must appear
            exactly once as ``{slot_name}`` in ``text`` and each ``entity_label`` must be a
            member of :data:`ENTITY_LABELS` (both enforced by :func:`_validate_templates`).
    """

    intent: str
    sentiment: str
    text: str
    slots: tuple[tuple[str, str], ...]


# ---------------------------------------------------------------------------------------
# Slot vocabularies — realistic surface strings per entity label. ``IP`` is intentionally
# absent: dotted-quads are generated per-draw by :func:`_random_ip` from the seeded RNG
# (per the corpus spec) rather than chosen from a fixed pool.
# ---------------------------------------------------------------------------------------
_VOCAB: dict[str, tuple[str, ...]] = {
    "SERVICE": (
        "auth-svc", "payments-api", "gateway", "inventory-svc", "notifications",
        "user-svc", "billing-svc", "search-svc", "cache-svc", "order-svc",
    ),
    "HOST": (
        "web-01", "web-02", "db-03", "cache-02", "worker-07", "app-11", "lb-01",
        "queue-04", "edge-09", "db-01",
    ),
    "USER_ID": (
        "4821", "u_1002", "user-88", "9930", "u_4471", "user-207", "1123", "u_5560",
    ),
    "ERROR_CODE": (
        "E4012", "E503", "ERR_TIMEOUT", "E1001", "E404", "ERR_CONN_REFUSED", "E500",
        "ERR_DB_LOCK", "E429", "E409",
    ),
    "PATH": (
        "/var/log/app.log", "/etc/nginx/nginx.conf", "/data/db/wal",
        "/var/lib/postgres/data", "/opt/app/config.yaml", "/etc/hosts",
        "/tmp/upload.tmp", "/var/log/syslog",
    ),
    "URL": (
        "https://api.example.com/v1/pay", "http://gateway.internal/health",
        "https://auth.example.com/token", "http://web-01.internal/status",
        "https://cdn.example.com/assets", "http://metrics.internal/scrape",
    ),
    "PORT": ("8080", "5432", "6379", "443", "80", "9092", "27017", "3000", "8000"),
}

#: RFC-1918 private prefixes used to synthesize IPs; the remaining octets are drawn 0-255.
_IP_PREFIXES: tuple[str, ...] = ("10", "192.168", "172.16")


# ---------------------------------------------------------------------------------------
# Realism noise — see the module docstring's "entity spans are sacred" invariant. Every
# perturbation here is applied to the *connective* skeleton (pre-fill) or as a pure
# prepend/append, never to a filled entity surface.
# ---------------------------------------------------------------------------------------
_CASING_PROB = 0.30   #: chance a connective word is upper/capitalized
_SYNONYM_PROB = 0.30  #: chance one connective word is swapped for a synonym
_PREFIX_PROB = 0.50   #: chance a "date time LEVEL" prefix is prepended
_SUFFIX_PROB = 0.25   #: chance a short connective suffix is appended

#: Log levels used in the synthetic timestamp prefix (noise, not an entity/label).
_LEVELS: tuple[str, ...] = ("INFO", "WARN", "ERROR", "DEBUG")

#: Short connective tails appended as noise. None contains ``{`` / ``}`` (they are added
#: after slot filling, so they can never interfere with placeholder substitution).
_SUFFIXES: tuple[str, ...] = (
    "-- see runbook",
    "(retrying)",
    "; oncall notified",
    "-- no action needed",
    "(auto-remediated)",
    "[handled]",
)

#: Sentiment-preserving synonyms for *connective* words only. Swapping these never changes
#: the ground-truth sentiment; multi-word replacements are fine (still connective text).
_SYNONYMS: dict[str, tuple[str, ...]] = {
    "failed": ("errored", "did not complete"),
    "completed": ("finished", "succeeded"),
    "successfully": ("cleanly", "without errors"),
    "detected": ("observed", "flagged"),
    "high": ("elevated", "excessive"),
    "slow": ("sluggish", "degraded"),
    "invalid": ("bad", "malformed"),
    "returned": ("responded with", "produced"),
    "warning": ("alert", "caution"),
    "lost": ("dropped", "severed"),
    "reloaded": ("refreshed", "re-read"),
    "updated": ("modified", "revised"),
    "passed": ("succeeded", "cleared"),
    "killed": ("terminated", "oom-killed"),
    "unreachable": ("not reachable", "unavailable"),
}


# ---------------------------------------------------------------------------------------
# The template table. Each intent gets 6-7 templates spanning realistic phrasings, with
# sentiment attached per-template (never per-intent). Every template declares >= 1 slot,
# so every generated sample carries >= 1 ground-truth entity.
# ---------------------------------------------------------------------------------------
_TEMPLATES: tuple[_Template, ...] = (
    # --- authentication (positive / negative / neutral / critical all present) ---
    _Template("authentication", "positive",
              "user {user} authenticated successfully from {ip}",
              (("user", "USER_ID"), ("ip", "IP"))),
    _Template("authentication", "negative",
              "invalid password for user {user} on {service}",
              (("user", "USER_ID"), ("service", "SERVICE"))),
    _Template("authentication", "negative",
              "failed login attempt for {user} from {ip} returned {code}",
              (("user", "USER_ID"), ("ip", "IP"), ("code", "ERROR_CODE"))),
    _Template("authentication", "neutral",
              "user {user} logged out of {service}",
              (("user", "USER_ID"), ("service", "SERVICE"))),
    _Template("authentication", "positive",
              "token issued for {user} by {service} at {url}",
              (("user", "USER_ID"), ("service", "SERVICE"), ("url", "URL"))),
    _Template("authentication", "critical",
              "brute-force attack on {service} from {ip} detected",
              (("service", "SERVICE"), ("ip", "IP"))),

    # --- deployment (skews positive/neutral, one negative for realism) ---
    _Template("deployment", "positive",
              "deployment of {service} to {host} completed successfully",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("deployment", "neutral",
              "rolling out {service} version to {host}",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("deployment", "positive",
              "{service} deployed and healthy on {host} port {port}",
              (("service", "SERVICE"), ("host", "HOST"), ("port", "PORT"))),
    _Template("deployment", "neutral",
              "deployment pipeline triggered for {service} by {user}",
              (("service", "SERVICE"), ("user", "USER_ID"))),
    _Template("deployment", "negative",
              "deployment of {service} to {host} failed with {code}",
              (("service", "SERVICE"), ("host", "HOST"), ("code", "ERROR_CODE"))),
    _Template("deployment", "neutral",
              "rollback of {service} on {host} initiated",
              (("service", "SERVICE"), ("host", "HOST"))),

    # --- error_report (skews negative/critical) ---
    _Template("error_report", "negative",
              "{service} on {host} returned {code} for request to {url}",
              (("service", "SERVICE"), ("host", "HOST"), ("code", "ERROR_CODE"),
               ("url", "URL"))),
    _Template("error_report", "critical",
              "fatal error {code} in {service} — process crashed on {host}",
              (("code", "ERROR_CODE"), ("service", "SERVICE"), ("host", "HOST"))),
    _Template("error_report", "negative",
              "unhandled exception {code} while writing to {path}",
              (("code", "ERROR_CODE"), ("path", "PATH"))),
    _Template("error_report", "critical",
              "panic: {service} segfault on {host}, core dumped to {path}",
              (("service", "SERVICE"), ("host", "HOST"), ("path", "PATH"))),
    _Template("error_report", "negative",
              "request to {url} failed with {code} on {service}",
              (("url", "URL"), ("code", "ERROR_CODE"), ("service", "SERVICE"))),
    _Template("error_report", "critical",
              "critical failure {code}: {service} unreachable at {ip}",
              (("code", "ERROR_CODE"), ("service", "SERVICE"), ("ip", "IP"))),

    # --- health_check (skews positive/neutral) ---
    _Template("health_check", "positive",
              "health check passed for {service} on {host}",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("health_check", "neutral",
              "probing {url} for liveness",
              (("url", "URL"),)),
    _Template("health_check", "positive",
              "{service} healthy — all checks green on port {port}",
              (("service", "SERVICE"), ("port", "PORT"))),
    _Template("health_check", "neutral",
              "readiness probe for {service} on {host} returned status",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("health_check", "negative",
              "health check failed for {service} at {url}",
              (("service", "SERVICE"), ("url", "URL"))),
    _Template("health_check", "positive",
              "heartbeat received from {host} for {service}",
              (("host", "HOST"), ("service", "SERVICE"))),

    # --- resource_warning (skews negative/critical) ---
    _Template("resource_warning", "negative",
              "high memory usage on {host} for {service}",
              (("host", "HOST"), ("service", "SERVICE"))),
    _Template("resource_warning", "critical",
              "disk full on {host}: {path} at capacity",
              (("host", "HOST"), ("path", "PATH"))),
    _Template("resource_warning", "negative",
              "cpu throttling detected on {host} running {service}",
              (("host", "HOST"), ("service", "SERVICE"))),
    _Template("resource_warning", "critical",
              "out of memory: {service} killed on {host}",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("resource_warning", "negative",
              "connection pool for {service} near limit on port {port}",
              (("service", "SERVICE"), ("port", "PORT"))),
    _Template("resource_warning", "negative",
              "disk usage warning for {path} on {host}",
              (("path", "PATH"), ("host", "HOST"))),

    # --- network (skews neutral/negative, one critical) ---
    _Template("network", "negative",
              "connection to {ip} port {port} timed out from {service}",
              (("ip", "IP"), ("port", "PORT"), ("service", "SERVICE"))),
    _Template("network", "neutral",
              "routing traffic for {service} through {host}",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("network", "negative",
              "packet loss detected between {host} and {ip}",
              (("host", "HOST"), ("ip", "IP"))),
    _Template("network", "neutral",
              "established connection to {url} from {service}",
              (("url", "URL"), ("service", "SERVICE"))),
    _Template("network", "critical",
              "network partition: {host} cannot reach {ip}",
              (("host", "HOST"), ("ip", "IP"))),
    _Template("network", "negative",
              "DNS resolution failed for {url} on {host}",
              (("url", "URL"), ("host", "HOST"))),

    # --- database (skews neutral/negative, one critical, one positive) ---
    _Template("database", "neutral",
              "query executed on {service} against {host}",
              (("service", "SERVICE"), ("host", "HOST"))),
    _Template("database", "negative",
              "slow query on {service}: {code} while reading {path}",
              (("service", "SERVICE"), ("code", "ERROR_CODE"), ("path", "PATH"))),
    _Template("database", "critical",
              "database {service} connection lost on {host} port {port}",
              (("service", "SERVICE"), ("host", "HOST"), ("port", "PORT"))),
    _Template("database", "negative",
              "replication lag rising on {host} for {service}",
              (("host", "HOST"), ("service", "SERVICE"))),
    _Template("database", "negative",
              "deadlock detected {code} on {service} at {host}",
              (("code", "ERROR_CODE"), ("service", "SERVICE"), ("host", "HOST"))),
    _Template("database", "positive",
              "backup of {service} completed to {path}",
              (("service", "SERVICE"), ("path", "PATH"))),
    _Template("database", "neutral",
              "migration applied to {service} on {host}",
              (("service", "SERVICE"), ("host", "HOST"))),

    # --- config_change (mostly neutral) ---
    _Template("config_change", "neutral",
              "configuration reloaded for {service} from {path}",
              (("service", "SERVICE"), ("path", "PATH"))),
    _Template("config_change", "neutral",
              "updated {path} on {host} for {service}",
              (("path", "PATH"), ("host", "HOST"), ("service", "SERVICE"))),
    _Template("config_change", "neutral",
              "feature flag changed for {service} by {user}",
              (("service", "SERVICE"), ("user", "USER_ID"))),
    _Template("config_change", "neutral",
              "port for {service} changed to {port} on {host}",
              (("service", "SERVICE"), ("port", "PORT"), ("host", "HOST"))),
    _Template("config_change", "positive",
              "config validation passed for {path} on {service}",
              (("path", "PATH"), ("service", "SERVICE"))),
    _Template("config_change", "negative",
              "invalid config {code} in {path} for {service}",
              (("code", "ERROR_CODE"), ("path", "PATH"), ("service", "SERVICE"))),
)

#: Templates grouped by intent for the balanced per-intent loop in :func:`produce_corpus`.
_TEMPLATES_BY_INTENT: dict[str, list[_Template]] = {
    intent: [t for t in _TEMPLATES if t.intent == intent] for intent in INTENTS
}

#: Flat view used by :func:`sample_messages` for mixed-intent random sampling.
_ALL_TEMPLATES: tuple[_Template, ...] = _TEMPLATES

#: Matches ``{slot_name}`` placeholders (word characters only) inside a template's text.
_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


def _placeholders(text: str) -> list[str]:
    """Return the slot names referenced as ``{name}`` in ``text``."""
    return _PLACEHOLDER_RE.findall(text)


def _validate_templates() -> None:
    """Fail fast at import if the template table violates a ground-truth invariant.

    Guards the properties every downstream consumer relies on: labels come from the
    published vocabularies, every intent is represented, each template has a fillable
    vocabulary, and each template's declared slots line up *exactly* with the
    ``{placeholders}`` in its text (so no slot is left unfilled and no placeholder ever
    leaks into a rendered message).
    """
    if len(set(INTENTS)) != len(INTENTS):
        raise ValueError("INTENTS contains duplicates")

    for tpl in _TEMPLATES:
        if tpl.intent not in INTENTS:
            raise ValueError(f"template intent {tpl.intent!r} not in INTENTS")
        if tpl.sentiment not in SENTIMENTS:
            raise ValueError(f"template sentiment {tpl.sentiment!r} not in SENTIMENTS")
        if not tpl.slots:
            raise ValueError(f"template has no slots: {tpl.text!r}")

        slot_names = [name for name, _ in tpl.slots]
        if len(set(slot_names)) != len(slot_names):
            raise ValueError(f"duplicate slot name in template: {tpl.text!r}")

        for _, label in tpl.slots:
            if label not in ENTITY_LABELS:
                raise ValueError(f"slot label {label!r} not in ENTITY_LABELS")
            if label != "IP" and label not in _VOCAB:
                raise ValueError(f"no vocabulary pool for label {label!r}")

        # Placeholders in the text and declared slots must be the same set (both ways):
        # a mismatch would either leave "{slot}" in the output or record a phantom entity.
        if set(_placeholders(tpl.text)) != set(slot_names):
            raise ValueError(
                f"placeholders {sorted(set(_placeholders(tpl.text)))} != "
                f"slots {sorted(set(slot_names))} in template: {tpl.text!r}"
            )

    covered = {t.intent for t in _TEMPLATES}
    missing = [i for i in INTENTS if i not in covered]
    if missing:
        raise ValueError(f"intents with no templates: {missing}")


_validate_templates()


def _random_ip(rng: random.Random) -> str:
    """Synthesize a private dotted-quad IP from the seeded RNG (e.g. ``10.12.34.56``)."""
    parts = _random_ip_prefix(rng).split(".")
    while len(parts) < 4:
        parts.append(str(rng.randint(0, 255)))
    return ".".join(parts)


def _random_ip_prefix(rng: random.Random) -> str:
    """Pick one RFC-1918 prefix. Split out so IP octet count varies by prefix family."""
    return rng.choice(_IP_PREFIXES)


def _draw_value(label: str, rng: random.Random) -> str:
    """Draw one realistic surface string for ``label`` from the seeded RNG.

    ``IP`` is generated per-draw; every other label is chosen from its :data:`_VOCAB` pool.
    """
    if label == "IP":
        return _random_ip(rng)
    return rng.choice(_VOCAB[label])


def _vary_casing(text: str, rng: random.Random) -> str:
    """Upper/capitalize one *connective* word, leaving ``{placeholder}`` tokens untouched.

    Candidates are alphabetic tokens only, so ``{service}`` (has braces), ``—`` and
    ``{host},`` (have punctuation) can never be selected — the invariant holds by
    construction because entities are still placeholders when this runs.
    """
    words = text.split(" ")
    candidates = [i for i, w in enumerate(words) if w.isalpha()]
    if not candidates:
        return text
    i = rng.choice(candidates)
    transform = rng.choice((str.upper, str.capitalize))
    words[i] = transform(words[i])
    return " ".join(words)


def _swap_synonym(text: str, rng: random.Random) -> str:
    """Swap one connective word for a sentiment-preserving synonym (slots untouched).

    Only exact whole-word matches against :data:`_SYNONYMS` are eligible; ``{placeholder}``
    tokens never match a key, so entity slots are never rewritten.
    """
    words = text.split(" ")
    candidates = [i for i, w in enumerate(words) if w in _SYNONYMS]
    if not candidates:
        return text
    i = rng.choice(candidates)
    words[i] = rng.choice(_SYNONYMS[words[i]])
    return " ".join(words)


def _timestamp_prefix(rng: random.Random) -> str:
    """A realistic ``2026-07-1x HH:MM:SS LEVEL`` log prefix (pure prepend noise)."""
    day = rng.randint(10, 19)
    hh, mm, ss = rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59)
    return f"2026-07-{day:02d} {hh:02d}:{mm:02d}:{ss:02d} {rng.choice(_LEVELS)}"


def _fill_and_perturb(tpl: _Template, rng: random.Random) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Render one message from ``tpl`` and return it with its ground-truth entities.

    Order of operations enforces the "noise never touches entity spans" invariant:

    1. Perturb the *skeleton* (casing, synonym) while entities are still ``{slot}`` tokens.
    2. Fill each slot from the seeded RNG, recording the exact surface + label.
    3. Optionally prepend a timestamp/level prefix and/or append a suffix — pure
       prepend/append that cannot alter an already-filled surface.
    """
    # (1) connective-only perturbations on the skeleton (placeholders still intact).
    skeleton = tpl.text
    if rng.random() < _CASING_PROB:
        skeleton = _vary_casing(skeleton, rng)
    if rng.random() < _SYNONYM_PROB:
        skeleton = _swap_synonym(skeleton, rng)

    # (2) fill slots, recording each exact surface string as ground truth.
    entities: list[tuple[str, str]] = []
    message = skeleton
    for name, label in tpl.slots:
        value = _draw_value(label, rng)
        message = message.replace("{" + name + "}", value)
        entities.append((value, label))

    # (3) prefix / suffix noise — prepend/append only, entity surfaces stay verbatim.
    if rng.random() < _PREFIX_PROB:
        message = f"{_timestamp_prefix(rng)} {message}"
    if rng.random() < _SUFFIX_PROB:
        message = f"{message} {rng.choice(_SUFFIXES)}"

    return message, tuple(entities)


def _build_sample(tpl: _Template, rng: random.Random) -> LogSample:
    """Assemble a fully-labeled :class:`LogSample` from a template and the seeded RNG."""
    message, entities = _fill_and_perturb(tpl, rng)
    return LogSample(
        message=message,
        intent=tpl.intent,
        entities=entities,
        sentiment=tpl.sentiment,
    )


def produce_corpus(n_per_intent: int = 150, seed: int = 42) -> list[LogSample]:
    """Build a balanced labeled corpus — exactly ``n_per_intent`` samples per intent.

    Iterates :data:`INTENTS` in order and, within each intent, selects templates
    round-robin (``i % len(templates)``). Round-robin guarantees two things at once: the
    count is exactly ``n_per_intent`` per intent, and — once ``n_per_intent`` reaches the
    template count — *every* template (hence every per-template sentiment) is represented.
    The intra-template variety comes from the seeded slot fills and realism noise, so lines
    sharing a template are still rarely byte-identical.

    Fully deterministic: a single private ``random.Random(seed)`` drives all slot draws and
    noise, so identical arguments yield an identical corpus.

    Args:
        n_per_intent: Samples to emit for each intent (``>= 0``). Total size is
            ``n_per_intent * len(INTENTS)``.
        seed: Seeds the private RNG; the same seed reproduces the same corpus exactly.

    Returns:
        The corpus grouped by intent in :data:`INTENTS` order.
    """
    if n_per_intent < 0:
        raise ValueError("n_per_intent must be >= 0")

    rng = random.Random(seed)
    corpus: list[LogSample] = []
    for intent in INTENTS:
        templates = _TEMPLATES_BY_INTENT[intent]
        for i in range(n_per_intent):
            corpus.append(_build_sample(templates[i % len(templates)], rng))
    return corpus


def sample_messages(n: int, seed: int = 0) -> list[LogSample]:
    """Return ``n`` random labeled samples with *mixed* intents (for E2E ground truth).

    Each sample draws a template uniformly from the full table (so intents are mixed), then
    fills and perturbs it exactly as :func:`produce_corpus` does. Deterministic for a given
    ``seed``.

    Args:
        n: Number of samples to produce (``>= 0``).
        seed: Seeds the private RNG; the same seed reproduces the same samples exactly.

    Returns:
        Exactly ``n`` fully-labeled :class:`LogSample` objects.
    """
    if n < 0:
        raise ValueError("n must be >= 0")

    rng = random.Random(seed)
    return [_build_sample(rng.choice(_ALL_TEMPLATES), rng) for _ in range(n)]
