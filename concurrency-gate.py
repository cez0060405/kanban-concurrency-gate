#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""concurrency-gate.py — 动态并发闸门（AIMD 调节）
监控所有看板任务失败情况，用 scheduled 状态排队，实时调节并发水位。
cron no_agent 每 1 分钟跑 --once；平时输出空（不投递），有调节动作才输出。
用法: python concurrency-gate.py --once | --status | --daemon
"""
import argparse, json, os, re, sqlite3, subprocess, sys, time, datetime

HERMES = os.environ.get("HERMES_HOME", os.path.expanduser("~/AppData/Local/hermes"))
STATE_FILE = os.path.join(HERMES, "logs", "concurrency-gate-state.json")
LOG_FILE = os.path.join(HERMES, "logs", "concurrency-gate.log")
BOARD_DBS = [os.path.join(HERMES, "kanban.db")]   # Task 0 实测：default board 主 DB
# 失败信号 = 并发相关失败（进程级）。timed_out 排除：实测多为
# "Iteration budget exhausted (50/50)"（任务太大该拆，降并发无用）。
FAIL_OUTCOMES = {"crashed", "spawn_failed", "rate_limited"}
DEFAULTS = {"limit": 2, "max_limit": 8, "min_limit": 1,
            "fail_threshold": 0.2, "min_samples": 10, "window_size": 20}

def classify_outcome(outcome):
    return "fail" if outcome in FAIL_OUTCOMES else "ok"

def adjust(state, fail_threshold=DEFAULTS["fail_threshold"],
           min_samples=DEFAULTS["min_samples"], max_limit=DEFAULTS["max_limit"],
           has_backlog=False):
    """AIMD 调节（纯函数）。返回 (action, new_state)；action: 'up'|'down'|'hold'。
    回升条件 = 无失败 + 样本够 + 有积压（scheduled 非空）——平时任务少停在低位，
    任务积压了才慢慢升。"""
    s = dict(state)
    window = list(s.get("window", []))
    if not window:
        return ("hold", s)
    fails = sum(1 for o in window if classify_outcome(o[0] if isinstance(o, tuple) else o) == "fail")
    rate = fails / len(window)
    if rate > fail_threshold:
        s["limit"] = max(DEFAULTS["min_limit"], s["limit"] - 1)
        return ("down", s)
    if fails == 0 and len(window) >= min_samples and has_backlog:
        s["limit"] = min(max_limit, s["limit"] + 1)
        return ("up", s)
    return ("hold", s)

def release_plan(limit, running, ready_tasks):
    """放行数 = limit - running，按传入顺序（调用方按创建时间排序）。"""
    n = max(0, limit - running)
    return ready_tasks[:n]

def db_connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn

def board_snapshot():
    """返回 {board_slug: {"running": int, "ready": [tid,...], "scheduled": [tid,...]}}，扫描 BOARD_DBS。"""
    out = {}
    for path in BOARD_DBS:
        if not os.path.exists(path):
            continue
        conn = db_connect(path)
        try:
            slug = os.path.basename(os.path.dirname(path)) or "default"
            running = conn.execute(
                "SELECT COUNT(*) c FROM tasks WHERE status='running'").fetchone()["c"]
            ready = [r["id"] for r in conn.execute(
                "SELECT id FROM tasks WHERE status='ready' ORDER BY created_at ASC")]
            scheduled = [r["id"] for r in conn.execute(
                "SELECT id FROM tasks WHERE status='scheduled' ORDER BY created_at ASC")]
            out[slug] = {"running": running, "ready": ready, "scheduled": scheduled}
        finally:
            conn.close()
    return out

def recent_outcomes(limit=DEFAULTS["window_size"]):
    """跨所有 board 取最近 N 个 run 的 outcome（按 ended_at 倒序）。"""
    rows = []
    for path in BOARD_DBS:
        if not os.path.exists(path):
            continue
        conn = db_connect(path)
        try:
            rows += [r["outcome"] for r in conn.execute(
                "SELECT outcome FROM task_runs WHERE outcome IS NOT NULL "
                "ORDER BY ended_at DESC LIMIT ?", (limit,))]
        finally:
            conn.close()
    return rows[:limit]

def sh(cmd, timeout=60):
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout)
    return r

def gate_queue(tid):
    """ready/todo → scheduled（排队）。已 running 会失败，忽略即可。"""
    r = sh(["hermes", "kanban", "schedule", tid, "gate:queue"])
    return r.returncode == 0

def gate_release(tid):
    """scheduled → ready（放行）。"""
    r = sh(["hermes", "kanban", "unblock", tid, "--reason", "gate:release"])
    return r.returncode == 0

def log(msg):
    line = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def run_once():
    """一轮闸门。返回输出字符串（空 = 无动作，cron 不投递）。"""
    st = load_state()
    now = time.time()
    # 自愈：上次运行超过 5 分钟（异常退出）→ 先全量放行一轮
    last_run = st.get("last_run", 0)
    if last_run and now - last_run > 300:
        log("自愈: 上次运行超过 5 分钟，全量放行 scheduled 任务")
        snap0 = board_snapshot()
        for b in snap0.values():
            for tid in b.get("scheduled", []):
                gate_release(tid)
    st["last_run"] = now

    snap = board_snapshot()
    total_running = sum(b["running"] for b in snap.values())
    all_ready = [tid for b in snap.values() for tid in b["ready"]]
    all_scheduled = [tid for b in snap.values() for tid in b["scheduled"]]

    # 1. 调节（用窗口内 run 统计；回升需有积压）
    outcomes = recent_outcomes(st.get("window_size", DEFAULTS["window_size"]))
    st["window"] = outcomes
    old_limit = st.get("limit", DEFAULTS["limit"])
    action, st = adjust(st, has_backlog=bool(all_scheduled))
    limit = st["limit"]

    # 2. 排队：所有 ready 先 schedule（抢在 dispatcher 前），跳过 recently_released
    recently = st.get("recently_released", {})
    queued = 0
    for tid in all_ready:
        if tid in recently and now - recently[tid] < 120:
            continue
        if gate_queue(tid):
            queued += 1

    # 3. 放行：limit - running 个，从 scheduled 队列挑（含手动 scheduled 的）
    #    最旧优先（快照顺序已按 created_at）
    plan = release_plan(limit, total_running, all_scheduled)
    released = 0
    for tid in plan:
        if gate_release(tid):
            released += 1
            recently[tid] = now
    st["recently_released"] = {k: v for k, v in recently.items() if now - v < 120}

    # 4. 收敛：无积压时 limit 贴到 running+2（双向：高了降、低了升）
    converged = False
    if not all_scheduled:
        new_limit = max(DEFAULTS["min_limit"], min(DEFAULTS["max_limit"], total_running + 2))
        if new_limit != limit:
            st["limit"] = new_limit
            limit = new_limit
            converged = True

    save_state(st)
    msgs = []
    if action == "down" and limit < old_limit:
        msgs.append(f"⚠️ 并发降级: {old_limit}→{limit}（窗口失败率超阈值）")
    elif action == "up" and limit > old_limit:
        msgs.append(f"✅ 并发回升: {old_limit}→{limit}（窗口稳定）")
    if queued:
        msgs.append(f"排队 {queued} 个任务")
    if released:
        msgs.append(f"放行 {released} 个任务（水位 {total_running + released}/{limit}）")
    if converged:
        msgs.append(f"收敛: 并发上限 {old_limit}→{limit}（无积压，贴近实际水位）")
    if msgs:
        log(" | ".join(msgs))
        return " | ".join(msgs)
    return ""   # 无动作 → 空输出 → cron 不投递

def main():
    ap = argparse.ArgumentParser(description="动态并发闸门")
    ap.add_argument("--once", action="store_true", help="跑一轮就退出（cron 用）")
    ap.add_argument("--status", action="store_true", help="打印当前状态")
    ap.add_argument("--daemon", action="store_true", help="常驻循环（默认 60s 一轮）")
    args = ap.parse_args()
    if args.status:
        print(json.dumps(load_state(), ensure_ascii=False, indent=2))
        return 0
    if args.once:
        out = run_once()
        if out:
            print(out)
        return 0
    if args.daemon:
        while True:
            try:
                out = run_once()
                if out:
                    print(out, flush=True)
            except Exception as e:
                log(f"ERROR {e}")
            time.sleep(60)
        return 0
    # 无参数默认 = --once（cron no_agent 直接引用脚本名）
    out = run_once()
    if out:
        print(out)
    return 0

def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return dict(DEFAULTS, window=[])

def save_state(st):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    sys.exit(main())
