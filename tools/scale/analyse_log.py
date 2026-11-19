#!/usr/bin/env python3
"""Protocol checks and counts over the nginx announce log (announces.jsonl).

    analyse_log.py --log RUN/announces.jsonl [--timeline RUN/timeline.json]
                   [--manifest W/catalogue/manifest.json --seed 1]
                   [--json out.json] [--interval 1800 --min-interval 900]

One JSON line per request, written by the nginx in front of the tracker (see
trackers.py for the format). The timeline, written by run.py, gives what the
log cannot: the interval the tracker advertised when, the windows in which the
runner forced announces itself (not counted as early), graceful stops, kills,
ramp steps and outages. Times in the timeline are this host's clock; probe
requests in the log give the offset to the containers' clock.

Checked per (host, info_hash, peer_id), in request order (request start =
log time - request time):

  before_min_interval    a regular announce sooner than the min interval the
                         last successful answer advertised (2 s tolerance)
  before_retry_after     any announce but `stopped` sooner than the
                         Retry-After of a 429/503 just before it
  retry_no_backoff       3+ failures in a row with no Retry-After (444
                         reset, 5xx, failure reason) whose gaps never grow
  first_without_started  the first announce of a peer is not event=started
  restart_without_started  an announce after `stopped` that is not `started`
  missing_stopped        still active (last event not `stopped`) when a
                         graceful stop ended
  counters_backwards     uploaded or downloaded lower than earlier in the
                         same session
  counters_jump          uploaded or downloaded up by more than 1 TiB at once
  absurd_counters        missing, not a number, negative or above 1 PiB
  nonzero_transfer       uploaded/downloaded above 0 (no real peer exists)
  left_above_size, unknown_torrent, wrong_host, bad_passkey  (with --manifest)

Also: per-host status counts and request times, announces/s per phase, the
gap clients actually left after each advertised interval, rates per ramp step,
and what each outage window looked like and how soon the host was back.

Python standard library only. Prints a table; --json writes everything.
"""
import argparse
import bisect
import collections
import json
import os
import statistics
import sys
from urllib.parse import unquote_to_bytes

PIB = 1 << 50
TIB = 1 << 40
TOL = 2.0

VIOLATIONS = ["before_min_interval", "before_retry_after", "retry_no_backoff", "first_without_started",
              "restart_without_started", "missing_stopped", "counters_backwards", "counters_jump",
              "absurd_counters", "nonzero_transfer", "left_above_size", "unknown_torrent", "wrong_host",
              "bad_passkey"]


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(p / 100.0 * (len(xs) - 1)))))
    return xs[k]


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def ok(q):
    return 200 <= q["st"] < 300 and q.get("fault") != "failure"


def status_class(q):
    st = q["st"]
    if q.get("fault") == "failure":
        return "failure_reason"
    if 200 <= st < 300:
        return "2xx"
    if st == 429:
        return "429"
    if st in (444, 499):
        return "reset_%d" % st
    if st >= 500:
        return "5xx"
    return "other_%d" % st


class Timeline:
    def __init__(self, data, interval, min_interval, offset):
        ev = (data or {}).get("events", [])
        self.offset = offset
        sh = lambda t: None if t is None else t + offset  # noqa: E731
        iv = sorted((sh(e["t"]), e["interval"], e["min_interval"]) for e in ev if e["kind"] == "interval")
        if not iv:
            iv = [(float("-inf"), interval, min_interval)]
        self.iv_t = [x[0] for x in iv]
        self.iv = iv
        self.quiet = [(sh(e["start"]), sh(e["end"])) for e in ev if e["kind"] == "quiet"]
        self.checkpoints = sorted(({"kind": e["kind"], "start": sh(e["start"]), "end": sh(e["end"])}
                                   for e in ev if e["kind"] in ("graceful_stop", "kill")), key=lambda c: c["end"])
        self.steps = [dict(e, start=sh(e["start"]), mid=sh(e["mid"]), end=sh(e["end"]))
                      for e in ev if e["kind"] == "ramp_step"]
        self.outages = [dict(e, start=sh(e["start"]), end=sh(e["end"])) for e in ev if e["kind"] == "outage_end"]
        self.phases = sorted((sh(e["t"]), e["name"]) for e in ev if e["kind"] == "phase")

    def advertised(self, t):
        i = bisect.bisect_right(self.iv_t, t) - 1
        x = self.iv[max(0, i)]
        return x[1], x[2]

    def is_quiet(self, t):
        return any(a <= t <= b for a, b in self.quiet)

    def phase(self, t):
        i = bisect.bisect_right([p[0] for p in self.phases], t) - 1
        return self.phases[i][1] if i >= 0 else "before"


def read_log(path):
    ann, scrapes, probes, bad = [], collections.Counter(), [], 0
    with open(path, "rb") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                x = json.loads(line)
            except ValueError:
                bad += 1
                continue
            p = x.get("path", "")
            if x.get("probe"):
                probes.append(x)
            elif p.endswith("/announce"):
                x["t"] = x["ts"] - (x.get("rt") or 0)
                ann.append(x)
            elif p.endswith("/scrape"):
                scrapes[x.get("host")] += 1
    ann.sort(key=lambda q: q["t"])
    return ann, scrapes, probes, bad


def clock_offset(probes):
    """Log clock minus host clock, from probe=<tag>-<host time> requests."""
    d = []
    for p in probes:
        try:
            d.append(p["ts"] - p.get("rt", 0) - float(p["probe"].rsplit("-", 1)[1]))
        except (ValueError, IndexError):
            pass
    return round(statistics.median(d), 3) if d else 0.0


def analyse(log_path, timeline=None, manifest=None, seed=None, interval=1800, min_interval=900):
    ann, scrapes, probes, bad = read_log(log_path)
    off = clock_offset(probes)
    tl = Timeline(timeline, interval, min_interval, off)
    sizes, home, passkeys = {}, {}, {}
    if manifest:
        size_of = {p["pid"]: p["length"] for p in manifest["payloads"]}
        for t in manifest["torrents"]:
            sizes[t["info_hash"]] = size_of[t["pid"]]
            home[t["info_hash"]] = t["host"]
        if seed is not None:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from catalogue import passkey
            passkeys = {h: passkey(seed, h) for h in set(home.values())}

    hosts = collections.defaultdict(lambda: {"requests": 0, "status": collections.Counter(),
                                             "events": collections.Counter(), "url_variants": collections.Counter(),
                                             "rt": [], "violations": collections.Counter(),
                                             "per_phase": collections.Counter()})
    by_key = collections.defaultdict(list)
    for q in ann:
        h = hosts[q["host"]]
        h["requests"] += 1
        h["status"][status_class(q)] += 1
        h["events"][q.get("ev") or "none"] += 1
        h["rt"].append(q.get("rt") or 0)
        h["per_phase"][tl.phase(q["t"])] += 1
        segs = [s for s in q["path"].split("/") if s]
        variant = "path" if len(segs) >= 2 else ("query" if q.get("pk") else "plain")
        h["url_variants"][variant] += 1
        ih = unquote_to_bytes(q.get("ih") or "").hex()
        q["ihx"] = ih
        q["variant"] = variant
        q["pkey"] = segs[-2] if variant == "path" else q.get("pk", "")
        by_key[(q["host"], ih, q.get("pid"))].append(q)

    gaps_by_iv = collections.defaultdict(list)
    retry_gaps = []
    stop_checks = [{"kind": c["kind"], "start": c["start"], "end": c["end"],
                    "missing": collections.Counter(), "stopped_not_accepted": collections.Counter()}
                   for c in tl.checkpoints]

    for (host, ih, pid), qs in by_key.items():
        V = hosts[host]["violations"]
        sess_up = sess_down = None
        fail_gaps = []
        prev = None
        for q in qs:
            ev = q.get("ev") or ""
            up, down, left = as_int(q.get("up")), as_int(q.get("down")), as_int(q.get("left"))
            vals = (up, down, left)
            if any(v is None or v < 0 or v > PIB for v in vals):
                V["absurd_counters"] += 1
            else:
                if up > 0 or down > 0:
                    V["nonzero_transfer"] += 1
                if ih in sizes and left > sizes[ih]:
                    V["left_above_size"] += 1
            if home:
                if ih not in home:
                    V["unknown_torrent"] += 1
                elif home[ih] != host:
                    V["wrong_host"] += 1
                if passkeys and q["variant"] != "plain" and q["pkey"] != passkeys.get(host):
                    V["bad_passkey"] += 1
            if prev is None:
                if ev != "started":
                    V["first_without_started"] += 1
            elif (prev.get("ev") == "stopped") and ev != "started":
                V["restart_without_started"] += 1
            if ev == "started":
                sess_up = sess_down = None
                fail_gaps = []
            if up is not None and down is not None and up >= 0 and down >= 0:
                if sess_up is not None and (up < sess_up or down < sess_down):
                    V["counters_backwards"] += 1
                if sess_up is not None and (up - sess_up > TIB or down - sess_down > TIB):
                    V["counters_jump"] += 1
                sess_up, sess_down = up, down
            if prev is not None and ev != "stopped" and not tl.is_quiet(q["t"]):
                gap = q["t"] - prev["t"]
                if ok(prev):
                    fail_gaps = []
                    if prev.get("ev") != "stopped":
                        iv, mi = tl.advertised(prev["t"])
                        if ev in ("", "empty"):
                            gaps_by_iv[iv].append(gap)
                            if gap < mi - TOL:
                                V["before_min_interval"] += 1
                elif as_int(prev.get("ra")):
                    ra = as_int(prev["ra"])
                    if prev["st"] == 429:
                        retry_gaps.append(gap)
                    if gap < ra - TOL:
                        V["before_retry_after"] += 1
                    fail_gaps = []
                else:
                    fail_gaps.append(gap)
                    if len(fail_gaps) == 3 and all(b <= a * 1.05 for a, b in zip(fail_gaps, fail_gaps[1:])):
                        V["retry_no_backoff"] += 1
            prev = q
        # graceful stops: who was still active when the stop ended?
        for i, c in enumerate(stop_checks):
            lo = stop_checks[i - 1]["end"] + TOL if i else float("-inf")
            hi = c["end"] + TOL
            last = None
            for q in qs:
                if lo < q["t"] <= hi:
                    last = q
            if last is None:
                continue
            if last.get("ev") != "stopped":
                c["missing"][host] += 1
                if c["kind"] == "graceful_stop":
                    V["missing_stopped"] += 1
            elif not ok(last):
                c["stopped_not_accepted"][host] += 1

    span = (ann[-1]["t"] - ann[0]["t"]) if len(ann) > 1 else 0
    out_hosts = {}
    for name in sorted(hosts):
        h = hosts[name]
        rts = h["rt"]
        out_hosts[name] = {
            "requests": h["requests"],
            "per_s_whole_log": round(h["requests"] / span, 2) if span else None,
            "status": dict(h["status"]),
            "events": dict(h["events"]),
            "url_variants": dict(h["url_variants"]),
            "rt_median_ms": round(1000 * statistics.median(rts), 1) if rts else None,
            "rt_p95_ms": round(1000 * pct(rts, 95), 1) if rts else None,
            "scrapes": scrapes.get(name, 0),
            "per_phase": dict(h["per_phase"]),
            "violations": {k: v for k, v in h["violations"].items() if v},
        }

    starts = [q["t"] for q in ann]

    def count_in(a, b, host=None):
        if host is None:
            return bisect.bisect_left(starts, b) - bisect.bisect_left(starts, a)
        return sum(1 for q in ann[bisect.bisect_left(starts, a):bisect.bisect_left(starts, b)] if q["host"] == host)

    steps = []
    for s in tl.steps:
        st = collections.Counter(status_class(q) for q in ann[bisect.bisect_left(starts, s["mid"]):
                                                             bisect.bisect_left(starts, s["end"])])
        g = gaps_by_iv.get(s["interval"], [])
        steps.append({"interval": s["interval"],
                      "rate_whole_step": round(count_in(s["start"], s["end"]) / (s["end"] - s["start"]), 2),
                      "rate_second_half": round(count_in(s["mid"], s["end"]) / (s["end"] - s["mid"]), 2),
                      "status_second_half": dict(st),
                      "observed_gap_median_s": round(statistics.median(g), 1) if g else None})
    outages = []
    for o in tl.outages:
        win = [q for q in ann[bisect.bisect_left(starts, o["start"]):bisect.bisect_left(starts, o["end"])]
               if q["host"] == o["host"]]
        after = [q for q in ann[bisect.bisect_left(starts, o["end"]):] if q["host"] == o["host"] and ok(q)]
        outages.append({"host": o["host"], "mode": o["mode"], "seconds": round(o["end"] - o["start"], 1),
                        "requests": len(win), "status": dict(collections.Counter(status_class(q) for q in win)),
                        "first_ok_after_s": round(after[0]["t"] - o["end"], 1) if after else None,
                        "ok_in_60s_after": sum(1 for q in after if q["t"] < o["end"] + 60)})
    tot = collections.Counter()
    for h in out_hosts.values():
        tot.update(h["violations"])
    return {
        "lines": len(ann), "bad_lines": bad, "scrapes": sum(scrapes.values()),
        "span_s": round(span, 1), "clock_offset_s": off, "keys": len(by_key),
        "hosts": out_hosts,
        "violations_total": dict(tot),
        "gaps_by_interval": {str(k): {"n": len(v), "median_s": round(statistics.median(v), 1),
                                      "p10_s": round(pct(v, 10), 1), "p90_s": round(pct(v, 90), 1),
                                      "min_s": round(min(v), 1)}
                             for k, v in sorted(gaps_by_iv.items())},
        "retry_after_429": {"n": len(retry_gaps), "median_gap_s": round(statistics.median(retry_gaps), 1)
                            if retry_gaps else None,
                            "min_gap_s": round(min(retry_gaps), 1) if retry_gaps else None},
        "checkpoints": [{"kind": c["kind"], "missing_stopped": dict(c["missing"]),
                         "missing_total": sum(c["missing"].values()),
                         "stopped_not_accepted": dict(c["stopped_not_accepted"])} for c in stop_checks],
        "ramp_steps": steps,
        "outages": outages,
    }


def table(r):
    cls = ["2xx", "failure_reason", "429", "5xx", "reset_444", "reset_499"]
    L = ["| host | announces | " + " | ".join(cls) + " | rt median ms | rt p95 ms |",
         "|---" * (len(cls) + 4) + "|"]
    for h, s in r["hosts"].items():
        L.append("| %s | %d | %s | %s | %s |" % (h, s["requests"], " | ".join(str(s["status"].get(c, 0)) for c in cls),
                                                s["rt_median_ms"], s["rt_p95_ms"]))
    used = [v for v in VIOLATIONS if r["violations_total"].get(v)]
    L += ["", "| violation | " + " | ".join(r["hosts"]) + " | total |", "|---" * (len(r["hosts"]) + 2) + "|"]
    for v in used:
        L.append("| %s | %s | %d |" % (v, " | ".join(str(s["violations"].get(v, 0)) for s in r["hosts"].values()),
                                       r["violations_total"][v]))
    if not used:
        L.append("| (none) |" + " |" * (len(r["hosts"]) + 1))
    if r["gaps_by_interval"]:
        L += ["", "| advertised interval s | gaps | median gap s | p10 | p90 | min |", "|---|---|---|---|---|---|"]
        for iv, g in r["gaps_by_interval"].items():
            L.append("| %s | %d | %s | %s | %s | %s |" % (iv, g["n"], g["median_s"], g["p10_s"], g["p90_s"], g["min_s"]))
    for o in r["outages"]:
        L.append("\nOutage %s (%s, %ss): %d announces %s; first 2xx %ss after it ended." % (
            o["host"], o["mode"], o["seconds"], o["requests"], o["status"], o["first_ok_after_s"]))
    for c in r["checkpoints"]:
        L.append("Checkpoint %s: %d peers without event=stopped %s." % (c["kind"], c["missing_total"],
                                                                         c["missing_stopped"]))
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", required=True)
    ap.add_argument("--timeline")
    ap.add_argument("--manifest")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--interval", type=int, default=1800, help="when there is no timeline")
    ap.add_argument("--min-interval", type=int, default=900)
    ap.add_argument("--json")
    a = ap.parse_args()
    tl = man = None
    if a.timeline:
        with open(a.timeline, encoding="utf-8") as fh:
            tl = json.load(fh)
    if a.manifest:
        with open(a.manifest, encoding="utf-8") as fh:
            man = json.load(fh)
    r = analyse(a.log, tl, man, a.seed, a.interval, a.min_interval)
    if a.json:
        with open(a.json, "w") as fh:
            json.dump(r, fh, indent=1)
    print(table(r))


if __name__ == "__main__":
    main()
