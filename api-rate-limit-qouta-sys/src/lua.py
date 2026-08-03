"""The decision script, and the KEYS/ARGV contract that lets Python build a call to it.

.. rubric:: This module is PURE

A module-level string constant and a handful of integers. No ``redis`` import, no I/O, nothing to
construct. That is the same rule ``src.keys`` follows and it exists for the same reason: the script
is the highest-risk code in the project, and everything *about* it that can be inspected without a
server should be inspectable without a server. The script's **behaviour** is proved against a real
``redis:7-alpine`` in ``tests/integration/``; its **call shape** is proved here and in
``tests/unit/test_limiter.py``.

The script-as-a-Python-string form is the repo's precedent (see
``active-passive-failover-log-processor/src/redis_client.py``). A separate ``.lua`` file would have
to be found at runtime relative to a package that is COPYed into two different images, and a
missing file would surface as a startup crash in production rather than as an import error in CI.

.. rubric:: One script, four gates, all read before any mutation

Two scripts — "check the rate, then check the quota" — cannot be made correct. Whichever runs first
has already charged the caller by the time the second one refuses, and undoing it needs a
compensating write that can itself fail. So all four gates read their state, all four decide, and
only then does anything get written:

1. **token bucket** per ``(user, endpoint)`` — burst
2. **sliding window** per user, account-wide — sustained rate (Cloudflare weighted counter)
3. **daily quota**
4. **monthly quota**

.. rubric:: Micro-tokens

Lua 5.1 numbers are IEEE doubles and Lua->RESP conversion **truncates** decimals. A bucket holding
``3.9`` tokens would be reported as ``3`` and, worse, would drift: every partial refill would lose
its fraction to the next truncation. So the stored token count is scaled by :data:`MICRO_TOKENS`
(1e6) and every arithmetic step is an exact integer. 1e6 micro-tokens per token keeps a
one-millisecond refill exact for any rate up to 60 000 requests per minute, and the largest value
this puts on the wire (``capacity * 1e6``) is nowhere near the 2^53 boundary where doubles stop
representing integers exactly.

.. rubric:: What is deliberately NOT here

* ``redis.replicate_commands()`` — deprecated on Redis 7, where effect replication is already the
  default and the call is a no-op that only serves to make the script fail on a future server that
  removed it.
* A ``#!lua`` shebang — a shebang makes the body a *function library* registered with
  ``FUNCTION LOAD`` rather than an ``EVAL`` script, with different flags and different replica
  semantics. This is an ``EVAL`` script executed through redis-py's ``register_script`` handle,
  which is what gives us transparent ``NOSCRIPT`` recovery after a ``SCRIPT FLUSH`` or a failover.
* Analytics. C9 records usage in a *separate* script, because analytics keys are global and
  time-bucketed and therefore cannot share the ``{user}`` hash tag — folding them in here would be
  a guaranteed ``CROSSSLOT`` the day this shards, and an analytics error would abort the script
  that decides whether a request is allowed.
"""

from __future__ import annotations

from src.keys import MS_PER_MINUTE

__all__ = [
    "ARGV_BUCKET_TTL_MS",
    "ARGV_COST",
    "ARGV_DAILY_EXPIRE_AT",
    "ARGV_DEFAULT_TIER",
    "ARGV_HEAD_ARITY",
    "ARGV_MONTHLY_EXPIRE_AT",
    "ARGV_NOW_MS_OVERRIDE",
    "ARGV_SW_ENABLED",
    "ARGV_SW_PREFIX",
    "ARGV_SW_WINDOW_MS",
    "ARGV_TIER_COUNT",
    "ARGV_TIER_TABLE",
    "KEYS_ARITY",
    "KEY_BUCKET",
    "KEY_QUOTA_DAILY",
    "KEY_QUOTA_MONTHLY",
    "KEY_USER",
    "MICRO_TOKENS",
    "NO_CLOCK_OVERRIDE",
    "RLQ_CHECK_AND_CONSUME",
    "RLQ_CHECK_AND_CONSUME_NAME",
    "SW_DISABLED",
    "SW_ENABLED",
    "TIER_ARGV_SLOTS",
    "UNENFORCED_PERIOD",
]

# ---------------------------------------------------------------------------------------------
# KEYS contract
#
# All four keys carry the SAME `{user_id}` hash tag (see `src.keys.hash_tag`), so they provably
# hash to one Redis Cluster slot and one EVALSHA may touch all of them. The `sw:{user}:<index>`
# keys the script derives internally carry that tag too — which is exactly why the script is given
# a PREFIX rather than a finished key.
#
# These are 1-based because Lua's KEYS table is. `src.limiter` builds its list from them so a
# reordering here is one edit rather than four silent off-by-ones.
# ---------------------------------------------------------------------------------------------

#: ``rate_limit:{uid}:<METHOD>:<template>`` — HASH of ``t`` (micro-tokens) and ``ts`` (epoch ms).
KEY_BUCKET = 1

#: ``quota:daily:{uid}:YYYY-MM-DD`` — STRING counter, ``EXPIREAT`` next UTC midnight.
KEY_QUOTA_DAILY = 2

#: ``quota:monthly:{uid}:YYYY-MM`` — STRING counter, ``EXPIREAT`` the 1st of next UTC month.
KEY_QUOTA_MONTHLY = 3

#: ``user:{uid}`` — HASH; only the ``tier`` field is read. See the script's tier-resolution block
#: for why that read happens *inside* the script rather than in a Python cache.
KEY_USER = 4

#: How many KEYS the script expects. Asserted by ``src.limiter`` at build time.
KEYS_ARITY = 4

# ---------------------------------------------------------------------------------------------
# ARGV contract
#
# ARGV[1..9] are built per request by `src.limiter`; ARGV[10..] is the tier table, spliced in
# verbatim from `TierRegistry.snapshot().argv_tail`, which is pre-rendered once per snapshot and
# therefore costs the hot path nothing. That split is the reason ARGV_HEAD_ARITY is 9 and the tier
# COUNT lives at 10: the count is the first element of the pre-rendered tail, not of the head.
# ---------------------------------------------------------------------------------------------

ARGV_COST = 1
ARGV_BUCKET_TTL_MS = 2
ARGV_SW_PREFIX = 3
ARGV_SW_WINDOW_MS = 4
ARGV_SW_ENABLED = 5
ARGV_DAILY_EXPIRE_AT = 6
ARGV_MONTHLY_EXPIRE_AT = 7
ARGV_DEFAULT_TIER = 8
ARGV_NOW_MS_OVERRIDE = 9
ARGV_TIER_COUNT = 10
ARGV_TIER_TABLE = 11

#: Number of ARGV elements ``src.limiter`` builds itself, before the pre-rendered tier tail.
ARGV_HEAD_ARITY = 9

#: Slots per tier in the flat tail: ``name, rpm, burst, daily, monthly``.
#:
#: The Lua side indexes with ``ARGV_TIER_TABLE + (i - 1) * 5``, so this number is baked into the
#: script's arithmetic and cannot be read from ``src.tiers`` at runtime. It MUST equal
#: :data:`src.tiers.ARGV_SLOTS_PER_TIER`; ``tests/unit/test_limiter.py`` asserts exactly that,
#: because the two are a producer/consumer pair whose disagreement would silently re-price every
#: tier rather than raising.
TIER_ARGV_SLOTS = 5

#: ``ARGV[5]`` values for the account-wide sliding-window gate.
SW_ENABLED = "1"
SW_DISABLED = "0"

#: ``ARGV[9]`` in production. Any non-positive value means "read ``redis.call('TIME')``".
NO_CLOCK_OVERRIDE = "0"

#: ``ARGV[6]`` / ``ARGV[7]`` when a quota period is switched off (``QUOTA_DAILY_ENABLED=false``).
#:
#: A period with no boundary is not a period: the script neither reads nor increments the counter
#: and reports ``limit = 0``, which :class:`~src.models.LimitDecision` already renders as
#: :data:`~src.models.UNLIMITED` and which already suppresses the ``X-Quota-*`` headers. Encoding
#: the switch in the expiry rather than in a tenth ARGV slot keeps the head arity fixed and keeps
#: the sentinel meaningful in a ``MONITOR`` trace: an ``EXPIREAT`` of 0 is visibly "no period".
UNENFORCED_PERIOD = 0

#: Scale factor for stored tokens. See the module docstring.
MICRO_TOKENS = 1_000_000

#: The name the script is registered under on :class:`~src.redis_client.RedisGateway`.
RLQ_CHECK_AND_CONSUME_NAME = "rlq_check_and_consume"


# ---------------------------------------------------------------------------------------------
# The script
#
# `MS_PER_MINUTE` is interpolated from `src.keys` rather than written twice: the same constant
# governs Python's period arithmetic and Lua's refill arithmetic, and two copies that disagree
# would make the limiter refill at a rate nothing in the test suite compares against the tier's
# advertised rpm.
# ---------------------------------------------------------------------------------------------

RLQ_CHECK_AND_CONSUME = f"""
-- rlq_check_and_consume: four gates, all READ and EVALUATED before ANY mutation.
--
-- KEYS[1] rate_limit:{{uid}}:<METHOD>:<template>   HASH   t = micro-tokens, ts = epoch ms
-- KEYS[2] quota:daily:{{uid}}:YYYY-MM-DD           STRING counter
-- KEYS[3] quota:monthly:{{uid}}:YYYY-MM            STRING counter
-- KEYS[4] user:{{uid}}                             HASH   tier, status, created_at
--
-- ARGV[1] cost              ARGV[6] daily_expire_at     ARGV[10] tier_count
-- ARGV[2] bucket_ttl_ms     ARGV[7] monthly_expire_at   ARGV[11+] name,rpm,burst,daily,monthly
-- ARGV[3] sw_prefix         ARGV[8] default_tier
-- ARGV[4] sw_window_ms      ARGV[9] now_ms_override
-- ARGV[5] sw_enabled

local MICRO         = {MICRO_TOKENS}
local MS_PER_MINUTE = {MS_PER_MINUTE}
local TIER_SLOTS    = {TIER_ARGV_SLOTS}
local TIER_AT       = {ARGV_TIER_TABLE}

local REASON_OK      = 'ok'
local REASON_RATE    = 'rate_limit'
local REASON_WINDOW  = 'sliding_window'
local REASON_DAILY   = 'quota_daily'
local REASON_MONTHLY = 'quota_monthly'

local STATE_RESET      = 'reset'
local STATE_ACTIVE     = 'active'
local STATE_EXHAUSTED  = 'exhausted'
local STATE_UNENFORCED = 'unenforced'

local bucket_key  = KEYS[1]
local daily_key   = KEYS[2]
local monthly_key = KEYS[3]
local user_key    = KEYS[4]

local cost              = tonumber(ARGV[1]) or 1
local bucket_ttl_ms     = tonumber(ARGV[2]) or 0
local sw_prefix         = ARGV[3]
local window_ms         = tonumber(ARGV[4]) or 0
local sw_enabled        = ARGV[5] == '1'
local daily_expire_at   = tonumber(ARGV[6]) or 0
local monthly_expire_at = tonumber(ARGV[7]) or 0
local default_tier      = ARGV[8]
local now_override      = tonumber(ARGV[9]) or 0
local tier_count        = tonumber(ARGV[10]) or 0

-- Every number handed BACK to redis.call goes through here first. That is an invariant, not a
-- style: `cost` included, even though Limiter.check and parse_endpoint_costs both already refuse
-- anything below 1, because a rule that holds "everywhere except three INCRBYs" is a rule nobody
-- can rely on when reading the script.
--
-- Why it is worth having, stated accurately: Redis 7 already renders an INTEGRAL Lua number as a
-- plain integer -- script_lua.c takes a `num == (long long)num` fast path and only falls through
-- to fpconv_dtoa (shortest round-trip, which renders 1750000000000 as '1.75e+12') for genuinely
-- fractional values. So on the server this project ships against, this helper changes nothing.
-- It is kept because that fast path is a property of the SERVER BUILD: Redis <= 6.2 had only the
-- fpconv path, and this script is the one piece of the system that must behave identically on a
-- laptop, in CI's redis:7-alpine, and on whatever managed Redis a deployment points at. Formatting
-- the argument ourselves removes the version dependency instead of documenting it.
--
-- NOTE: '%d' TRUNCATES -- string.format('%d', 2.7) is '2', not an error. This is a formatter, not
-- a validator, and nothing may lean on it to reject a fractional input. Every value passed through
-- it is already integral by construction (micro-token arithmetic floors, the clock is floored
-- milliseconds, and the expiries arrive as integer strings from Python).
local function int_arg(value)
  return string.format('%d', value)
end

--------------------------------------------------------------------------------------------
-- The clock
--
-- redis.call('TIME') is the ONE clock every replica shares, and that is the entire reason it is
-- read here rather than passed in. A client-supplied `now` is *input*: a pod whose system clock
-- runs 40 seconds fast would compute a 40-second refill on its very first request and keep
-- refilling its own bucket ahead of everyone else's, permanently, with nothing in the system able
-- to notice. The account-wide window would split into one window per replica for the same reason.
--
-- TIME returns two RESP *strings* (seconds, microseconds) even though they are numbers -- hence
-- tonumber() on both. Milliseconds, floored, so the whole decision path stays integral.
--
-- ARGV[9] is the test-only override and is inert unless it is strictly positive. `src.limiter`
-- refuses to send anything else unless Settings.allow_clock_override is on, so outside the tests
-- this branch is unreachable by construction rather than by convention.
--------------------------------------------------------------------------------------------
local now_ms
if now_override > 0 then
  now_ms = now_override
else
  local t = redis.call('TIME')
  now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

--------------------------------------------------------------------------------------------
-- Tier resolution -- deliberately INSIDE the script
--
-- It costs one HGET against a key that is already in this slot, and in exchange there is **no
-- per-user tier cache anywhere in the system to invalidate**. An admin moving a principal between
-- tiers is one HSET and takes effect on the very next request, on every replica, with no restart,
-- no pub/sub message to miss and no TTL to wait out.
--
-- Only what a tier *means* is cached (it arrives as ARGV from the registry's 5-second snapshot).
-- Caching *who is on which tier* in Python would put a stale answer on the one thing an operator
-- changes during an incident, and would need an invalidation protocol across replicas to fix.
--
-- A tier name that is not in the table falls back to default_tier: an operator can delete a tier
-- from config:tiers while principals still reference it, and the safe reading of "this tier no
-- longer exists" is the most restrictive tier, never "no limits found".
--------------------------------------------------------------------------------------------
local function find_tier(name)
  if name == nil or name == false then
    return nil
  end
  for i = 1, tier_count do
    local base = TIER_AT + (i - 1) * TIER_SLOTS
    if ARGV[base] == name then
      return {{
        name    = ARGV[base],
        rpm     = tonumber(ARGV[base + 1]) or 0,
        burst   = tonumber(ARGV[base + 2]) or 0,
        daily   = tonumber(ARGV[base + 3]) or 0,
        monthly = tonumber(ARGV[base + 4]) or 0,
      }}
    end
  end
  return nil
end

local stored_tier = redis.call('HGET', user_key, 'tier')
local tier = find_tier(stored_tier)
if tier == nil then
  tier = find_tier(default_tier)
end
if tier == nil then
  -- Not survivable, and specifically not survivable *quietly*. With no tier there are no limits,
  -- and "no limits found" is indistinguishable from "unlimited" at the point the decision is made
  -- -- i.e. the whole enforcement layer silently switched off. Settings._default_tier_must_exist
  -- and TierRegistry._parse_tiers both make this unreachable; if it is ever reached, an error
  -- reply becomes a ResponseError, which the gateway classifies as a bug in this service rather
  -- than as an outage, so it surfaces as a 500 instead of as a fail-open.
  return redis.error_reply(
    'rlq_check_and_consume: no tier configuration for default tier ' .. tostring(default_tier)
  )
end

local tier_name = tier.name
local rpm       = tier.rpm
local capacity  = tier.burst

--------------------------------------------------------------------------------------------
-- GATE 1 -- token bucket per (user, endpoint). READ ONLY in this block.
--
-- Lazy refill: tokens = min(capacity, tokens + elapsed * rate). Nothing is written on a timer;
-- the bucket is brought up to date at the instant it is read, which is why a bucket nobody has
-- touched for a day costs nothing to maintain.
--
-- A key that has never been seen starts FULL. It is the only defensible starting state: starting
-- empty would refuse a first-time caller for a minute, and any other value is a number nobody
-- can explain to them.
--------------------------------------------------------------------------------------------
local capacity_micro = capacity * MICRO
local cost_micro     = cost * MICRO

-- Milliseconds to refill an empty bucket to capacity. Used three ways: to bound `elapsed` (which
-- keeps the refill product exactly representable), to detect a stored timestamp that is absurdly
-- far in the future, and to cap a retry that can never actually be satisfied.
local full_refill_ms = 0
if rpm > 0 and capacity > 0 then
  full_refill_ms = math.ceil(capacity * MS_PER_MINUTE / rpm)
end

local stored       = redis.call('HMGET', bucket_key, 't', 'ts')
local tokens_micro = tonumber(stored[1])
local last_ms      = tonumber(stored[2])
if tokens_micro == nil or last_ms == nil then
  tokens_micro = capacity_micro
  last_ms      = now_ms
end

local elapsed_ms = now_ms - last_ms
if elapsed_ms < 0 then
  -- The clock went BACKWARDS (an NTP step, or a failover to a node whose clock trails). Clamp to
  -- zero: no refill, and no penalty either -- the caller did nothing wrong and must not be
  -- charged for the operator's clock.
  if -elapsed_ms > full_refill_ms then
    -- The stored stamp is more than a whole refill period in the FUTURE, so honouring it would
    -- freeze this principal out for the entire duration of the skew -- potentially hours, on a
    -- bucket that should recover in a minute. A bucket that cannot be reasoned about is reset to
    -- the state we can defend: full.
    tokens_micro = capacity_micro
  end
  elapsed_ms = 0
elseif elapsed_ms > full_refill_ms then
  -- Refill is clamped at capacity anyway, so anything past a full refill period is the same
  -- answer. Clamping here keeps `elapsed_ms * rpm * MICRO` far below 2^53, where doubles stop
  -- representing integers exactly -- the one place this script could silently start lying.
  elapsed_ms = full_refill_ms
end

if rpm > 0 and elapsed_ms > 0 then
  -- Integer-first ordering, deliberately. Folding `rpm * MICRO / MS_PER_MINUTE` into a per-ms
  -- rate first gives 16666.666... for rpm=1000, and multiplying that by elapsed then flooring is
  -- off by up to a micro-token per call -- a drift that only shows up as a bucket that is
  -- mysteriously one token short after a few thousand requests.
  tokens_micro = tokens_micro + math.floor(elapsed_ms * rpm * MICRO / MS_PER_MINUTE)
end
if tokens_micro > capacity_micro then
  tokens_micro = capacity_micro
end
if tokens_micro < 0 then
  tokens_micro = 0
end

-- capacity <= 0 means this gate is not enforcing anything, the same reading `limit <= 0` gets in
-- the window and quota gates. One convention across all four, so an operator zeroing a number
-- gets one behaviour rather than three.
local bucket_enforced = capacity > 0
local bucket_ok       = true
local bucket_retry_ms = 0
if bucket_enforced and tokens_micro < cost_micro then
  bucket_ok = false
  if rpm > 0 then
    bucket_retry_ms = math.ceil((cost_micro - tokens_micro) * MS_PER_MINUTE / (rpm * MICRO))
    if bucket_retry_ms > full_refill_ms then
      -- Reachable only when cost > capacity, i.e. a request that can NEVER be admitted by this
      -- bucket. There is no honest retry interval for that, so report the time to a full bucket:
      -- bounded, and it does not promise a wait that would work.
      bucket_retry_ms = full_refill_ms
    end
  else
    bucket_retry_ms = bucket_ttl_ms
  end
  if bucket_retry_ms < 1 then
    bucket_retry_ms = 1
  end
end

--------------------------------------------------------------------------------------------
-- GATE 2 -- account-wide sliding window, Cloudflare weighted counter. READ ONLY in this block.
--
--     used = ceil(prev * ((W - into) / W) + curr)
--
-- Two counters and two GETs, versus a ZSET log's O(n) trim on a single-threaded server. The
-- weighting is what stops the classic fixed-window boundary burst: `limit` requests at the end of
-- window k plus `limit` at the start of k+1 would admit 2x limit in a couple of seconds, and this
-- formula still counts almost all of window k a second into window k+1.
--
-- The window INDEX is computed here, from the clock above, and the two keys are built from the
-- prefix. Computing the index in Python would reintroduce exactly the second clock this design
-- removed: a replica running fast would write into the next window before the others are reading
-- it, and the account-wide gate would quietly become a per-replica gate. All `sw:{{uid}}:*` keys
-- carry the same hash tag as KEYS[1], so deriving them here is provably still one slot.
--------------------------------------------------------------------------------------------
local window_limit    = rpm
local window_used     = 0
local window_reset_ms = 0
local window_ok       = true
local window_retry_ms = 0
local cur_key         = nil
local window_active   = sw_enabled and window_ms > 0

if window_active then
  local window_index = math.floor(now_ms / window_ms)
  local into         = now_ms - window_index * window_ms
  cur_key            = sw_prefix .. ':' .. int_arg(window_index)
  local prev_key     = sw_prefix .. ':' .. int_arg(window_index - 1)

  local curr = tonumber(redis.call('GET', cur_key)) or 0
  local prev = tonumber(redis.call('GET', prev_key)) or 0

  window_reset_ms = window_ms - into
  window_used     = math.ceil(prev * ((window_ms - into) / window_ms) + curr)

  if window_limit > 0 and window_used + cost > window_limit then
    window_ok = false
    -- Earliest instant this gate would admit `cost`. Within the current window only `prev`
    -- decays, so solve prev * (W - into') / W <= limit - curr - cost for into'.
    local slack = window_limit - curr - cost
    if slack >= 0 and prev > 0 then
      local into_needed = math.ceil(window_ms * (prev - slack) / prev)
      if into_needed > window_ms then
        into_needed = window_ms
      end
      window_retry_ms = into_needed - into
    else
      -- `curr` alone already leaves no room, so no amount of `prev` decaying helps. Relief starts
      -- only once THIS window becomes the previous one and begins decaying in its turn.
      window_retry_ms = window_reset_ms
      if curr > 0 then
        local room = window_limit - cost
        if room < 0 then
          room = 0
        end
        local decay = math.ceil(window_ms * (curr - room) / curr)
        if decay < 0 then
          decay = 0
        end
        if decay > window_ms then
          decay = window_ms
        end
        window_retry_ms = window_retry_ms + decay
      end
    end
    if window_retry_ms < 1 then
      window_retry_ms = 1
    end
  end
end

--------------------------------------------------------------------------------------------
-- GATES 3 and 4 -- daily and monthly quota. READ ONLY in this block.
--
-- `limit <= 0` means UNLIMITED -- the escape hatch an enterprise tier needs, and already the
-- encoding `src.models` uses when it reports -1 remaining. An expire_at of 0 means the period
-- itself is switched off (QUOTA_*_ENABLED=false): not read, not counted, reported as limit 0.
--------------------------------------------------------------------------------------------
local daily_counts = daily_expire_at > 0
local daily_limit  = 0
local daily_before = 0
if daily_counts then
  daily_limit  = tier.daily
  daily_before = tonumber(redis.call('GET', daily_key)) or 0
end

local monthly_counts = monthly_expire_at > 0
local monthly_limit  = 0
local monthly_before = 0
if monthly_counts then
  monthly_limit  = tier.monthly
  monthly_before = tonumber(redis.call('GET', monthly_key)) or 0
end

local daily_ok       = true
local daily_retry_ms = 0
if daily_limit > 0 and daily_before + cost > daily_limit then
  daily_ok = false
  daily_retry_ms = daily_expire_at * 1000 - now_ms
  if daily_retry_ms < 1 then
    daily_retry_ms = 1
  end
end

local monthly_ok       = true
local monthly_retry_ms = 0
if monthly_limit > 0 and monthly_before + cost > monthly_limit then
  monthly_ok = false
  monthly_retry_ms = monthly_expire_at * 1000 - now_ms
  if monthly_retry_ms < 1 then
    monthly_retry_ms = 1
  end
end

--------------------------------------------------------------------------------------------
-- The verdict, and the retry interval
--
-- retry_after is the MAXIMUM across the failing gates, and `reason` names THAT gate -- never the
-- nearest wall. Telling a caller who is both rate limited and out of daily quota to come back in
-- 3 seconds hands them a 429 for the next eight hours, one retry at a time, and every one of
-- those retries is load this service refused and still had to serve. Reporting the furthest wall
-- is the only advice that is true when they act on it.
--
-- Strict `>` on the comparison, so ties resolve to the earlier gate in this fixed order and the
-- answer is deterministic rather than dependent on which limit happened to be configured larger.
--------------------------------------------------------------------------------------------
local allowed  = bucket_ok and window_ok and daily_ok and monthly_ok
local reason   = REASON_OK
local retry_ms = 0

if not bucket_ok and bucket_retry_ms > retry_ms then
  retry_ms = bucket_retry_ms
  reason   = REASON_RATE
end
if not window_ok and window_retry_ms > retry_ms then
  retry_ms = window_retry_ms
  reason   = REASON_WINDOW
end
if not daily_ok and daily_retry_ms > retry_ms then
  retry_ms = daily_retry_ms
  reason   = REASON_DAILY
end
if not monthly_ok and monthly_retry_ms > retry_ms then
  retry_ms = monthly_retry_ms
  reason   = REASON_MONTHLY
end

--------------------------------------------------------------------------------------------
-- Mutation -- ALL of it, or NONE of it
--
-- A DENIAL WRITES NOTHING. Not the spent token, not the quota counter, not even the refilled
-- bucket. The refill is linear and clamped, so recomputing it from the older `ts` on the next
-- request yields the identical number -- persisting it buys nothing and costs a write.
--
-- That matters most under exactly the traffic you least want to amplify: a client in a retry loop
-- against a limit it has already hit produces a flood of *denied* requests, and every one of them
-- performs ZERO writes. A limiter that persisted its refill on refusal would turn a caller's bad
-- backoff into write load on the single-threaded server every other caller depends on.
--------------------------------------------------------------------------------------------
local tokens_after  = tokens_micro
local window_after  = window_used
local daily_after   = daily_before
local monthly_after = monthly_before

if allowed then
  tokens_after = tokens_micro - cost_micro
  if tokens_after < 0 then
    tokens_after = 0
  end
end

-- How long until the bucket is FULL again from its post-decision level. One quantity, two uses:
-- it is `bucket_reset_ms` on the reply, and it is the floor under the key's TTL below.
local bucket_reset_ms = 0
if rpm > 0 and capacity_micro > tokens_after then
  bucket_reset_ms = math.ceil((capacity_micro - tokens_after) * MS_PER_MINUTE / (rpm * MICRO))
end

if allowed then
  -- Level-aware TTL: max(bucket_ttl_ms, time-to-refill). An expiry must never silently gift a
  -- caller a full bucket. With a flat 1-hour TTL, a slow tier whose bucket takes longer than an
  -- hour to refill would have its drained bucket deleted and recreated FULL by the next request
  -- -- an unlimited allowance available to anyone patient enough to pause for the TTL.
  local ttl_ms = bucket_ttl_ms
  if bucket_reset_ms > ttl_ms then
    ttl_ms = bucket_reset_ms
  end
  if ttl_ms < 1 then
    ttl_ms = 1
  end
  redis.call('HSET', bucket_key, 't', int_arg(tokens_after), 'ts', int_arg(now_ms))
  redis.call('PEXPIRE', bucket_key, int_arg(ttl_ms))

  if cur_key ~= nil then
    -- 2 x W, not W: the PREVIOUS window has to outlive its own window, or the current window has
    -- nothing to weight and the boundary burst this algorithm exists to stop comes straight back.
    redis.call('INCRBY', cur_key, int_arg(cost))
    redis.call('PEXPIRE', cur_key, int_arg(2 * window_ms))
    -- ceil(x + n) == ceil(x) + n for integer n, so the post-increment weighted count is exactly
    -- this. No second GET.
    window_after = window_used + cost
  end

  -- EXPIREAT with an ABSOLUTE instant, not EXPIRE with a duration. Absolute is idempotent: it can
  -- be reissued on every request with no bookkeeping about whether it was already set, and there
  -- is no INCR/EXPIRE race in which a counter created by one replica never gets its TTL from
  -- another. A relative TTL applied on first write would keep a counter created at 18:00 alive
  -- until 18:00 the next day, so it would still be there, half-spent, when the new day began.
  if daily_counts then
    daily_after = redis.call('INCRBY', daily_key, int_arg(cost))
    redis.call('EXPIREAT', daily_key, int_arg(daily_expire_at))
  end
  if monthly_counts then
    monthly_after = redis.call('INCRBY', monthly_key, int_arg(cost))
    redis.call('EXPIREAT', monthly_key, int_arg(monthly_expire_at))
  end
end

--------------------------------------------------------------------------------------------
-- Period state: unenforced / exhausted / reset / active, checked in that order
--
-- `unenforced` FIRST, because a period with no ceiling has no other state that is true. That is
-- both the "unlimited" tier (limit <= 0, the enterprise escape hatch) and a period switched off
-- entirely (expire_at 0, which already forced limit to 0 above). Reporting `reset` there -- which
-- an earlier version did -- is a false CLAIM rather than a harmless default: `reset` says a period
-- boundary has just rolled over, so a client could reasonably render "your quota just refreshed"
-- for a quota nobody is counting. This is also exactly the condition under which the decision
-- reports -1 remaining and suppresses the X-Quota-* headers, so all three now agree.
--
-- `exhausted` next, because "this was the period's first request AND it consumed the whole
-- allowance" is exhausted, not reset. `reset` needs the PRE-request counter: it means the period
-- rolled over, which is a different fact from "you have 1000 left" and is what lets a client tell
-- a fresh day from an unused month.
--------------------------------------------------------------------------------------------
local function period_state(limit, before, after)
  if limit <= 0 then
    return STATE_UNENFORCED
  end
  if after >= limit then
    return STATE_EXHAUSTED
  end
  if before == 0 then
    return STATE_RESET
  end
  return STATE_ACTIVE
end

--------------------------------------------------------------------------------------------
-- The reply: exactly 19 positional elements, in LUA_REPLY_FIELDS order.
--
-- Every element is an integer or a string, and none may be nil. Lua->RESP truncates numbers AND
-- STOPS AT THE FIRST NIL -- a nullable slot does not arrive as a null, it silently shortens the
-- list, and every field after it shifts one place left into a decoder that would happily read a
-- quota counter as `allowed`. `allowed` is an explicit 1/0 for the same reason: Lua `false`
-- converts to RESP Nil, which would truncate the reply to nothing at all.
--------------------------------------------------------------------------------------------
local allowed_int = 0
if allowed then
  allowed_int = 1
end

return {{
  allowed_int,                                              --  1 allowed
  reason,                                                   --  2 reason
  tier_name,                                                --  3 tier
  capacity,                                                 --  4 bucket_limit
  math.floor(tokens_after / MICRO),                         --  5 bucket_remaining
  bucket_reset_ms,                                          --  6 bucket_reset_ms
  window_limit,                                             --  7 window_limit
  window_after,                                             --  8 window_used
  window_reset_ms,                                          --  9 window_reset_ms
  daily_limit,                                              -- 10 daily_limit
  daily_after,                                              -- 11 daily_used
  daily_expire_at,                                          -- 12 daily_expire_at
  period_state(daily_limit, daily_before, daily_after),     -- 13 daily_state
  monthly_limit,                                            -- 14 monthly_limit
  monthly_after,                                            -- 15 monthly_used
  monthly_expire_at,                                        -- 16 monthly_expire_at
  period_state(monthly_limit, monthly_before, monthly_after),-- 17 monthly_state
  retry_ms,                                                 -- 18 retry_ms
  now_ms,                                                   -- 19 now_ms
}}
"""
