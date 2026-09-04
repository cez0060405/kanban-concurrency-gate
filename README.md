# kanban-concurrency-gate

**AIMD dynamic concurrency gate for multi-agent kanban boards.**

A small, dependency-free Python script that automatically adjusts how many kanban tasks run in parallel, based on real failure rates. Uses **AIMD** (Additive Increase / Multiplicative Decrease) — the same congestion-control family TCP uses — to keep the board at a healthy concurrency level without manual tuning.

> 中文：多智能体看板动态并发闸门。根据真实失败率自动调节并发水位，AIMD 算法（加法增/乘法减），零依赖单文件。

## Why

Multi-agent kanban systems have a concurrency problem:

- **Too many parallel workers** → provider rate limits, cascading failures, token waste (real case: 21 crashes in one evening during peak hours).
- **Too few** → backlog grows, tasks idle.

Static limits (`max_in_progress=2`) are a blunt instrument: they don't adapt when the provider is flaky at 22:40 or rock-solid at 03:00. This gate **measures the board's own failure rate** and adjusts the concurrency limit accordingly.

## How it works

```
                    ┌─────────────────────────────┐
                    │  task_runs (outcomes)        │
                    │  completed / crashed /        │
                    │  rate_limited / timed_out ... │
                    └──────────────┬──────────────┘
                                   │ window of last N runs
                                   ▼
                    ┌─────────────────────────────┐
                    │  AIMD adjust (every 1 min)  │
                    │  fail rate > 0.2  → limit-1 │  multiplicative decrease
                    │  no fails + backlog → limit+1 │ additive increase
                    └──────────────┬──────────────┘
                                   │
        ┌──────────────────────────┼──────────────────────────┐
        ▼                          ▼                          ▼
┌───────────────┐        ┌─────────────────┐        ┌─────────────────┐
│  ready tasks  │ ─────▶ │  scheduled queue │ ─────▶ │  running (≤limit)│
│  (gate:queue) │        │  (gate:release)  │        │  workers         │
└───────────────┘        └─────────────────┘        └─────────────────┘
```

### The loop (every minute)

1. **Measure**: read the last N task run outcomes from the board DB.
2. **Adjust** (AIMD):
   - failure rate > threshold (default 0.2) → `limit -= 1` (multiplicative decrease, floor at 1)
   - zero failures + enough samples + backlog exists → `limit += 1` (additive increase, cap at 8)
   - otherwise → hold
3. **Queue**: all `ready` tasks → `scheduled` (so the dispatcher can't over-run).
4. **Release**: `limit - running` tasks from the scheduled queue, oldest first.
5. **Converge**: when no backlog, pin the limit near the actual running count (`running + 2`) — it drifts down when idle, up when busy.

### Self-healing

If the gate itself crashes (no run for > 5 min), the next run **releases all scheduled tasks** — the board never stays stuck behind a dead gate.

## Usage

```bash
# one pass (cron no_agent, every 1 min; empty output = no action, no delivery)
python concurrency-gate.py --once

# current state
python concurrency-gate.py --status

# daemon loop (60s per pass)
python concurrency-gate.py --daemon
```

### Cron wiring (no_agent)

```
schedule: every 1 min
script:   concurrency-gate.py --once
no_agent: true
```

Empty output → nothing delivered. Only actual adjustments (downgrade/upgrade/queue/release) produce output.

## Configuration

Defaults are sane; override via env or edit the `DEFAULTS` dict:

| Key | Default | Meaning |
|---|---|---|
| `limit` | 2 | initial concurrency limit |
| `max_limit` | 8 | ceiling for additive increase |
| `min_limit` | 1 | floor for multiplicative decrease |
| `fail_threshold` | 0.2 | failure rate that triggers decrease |
| `min_samples` | 10 | samples needed before increase is allowed |
| `window_size` | 20 | outcome window for rate measurement |

`HERMES_HOME` env var overrides the default `~/AppData/Local/hermes` base path (board DB + state/log files live under it).

## Failure classification

Only **process-level** failures count as "fail" for the gate:

- `crashed`, `spawn_failed`, `rate_limited` → **fail** (concurrency-related; lowering the limit helps)
- `timed_out` → **excluded** (usually "iteration budget exhausted" = task too big, should be split — lowering concurrency won't help)
- `completed`, `blocked`, `gave_up`, `scheduled` → ok

## Requirements

- A kanban board with SQLite `tasks` + `task_runs` tables (statuses: `ready`/`scheduled`/`running`)
- A `hermes kanban schedule/unblock` CLI (or equivalent) for queue/release
- Python 3.8+ (stdlib only)

## Related

- [kanban-comment-delegation](https://github.com/cez0060405/kanban-comment-delegation) — comment-driven task handoff between worker identities (the other half of the orchestration toolkit)

## License

MIT
