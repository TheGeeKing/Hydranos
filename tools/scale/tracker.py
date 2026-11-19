#!/usr/bin/env python3
"""A misbehaving BEP 3 HTTP tracker that answers as several hosts at once.

One process, one port. The host a client meant is read from its Host header
(t1..t6 in the benchmark, where they are Docker network aliases of this one
container), and every host has its own behaviour and its own counters:

  latency      log-normal added delay, [median_ms, sigma]
  rate_cap     announces/s the host accepts; past it 429 + Retry-After
  p_503        share answered 503 + Retry-After
  p_reset      share whose connection is reset (RST, no answer)
  p_failure    share answered 200 with a bencoded "failure reason"
  outage       {"start": s, "duration": s, "mode": "reset"|"timeout"|"503"}:
               the host is fully down for that window (seconds after start
               or after a /control outage call), then recovers

It also counts what a well-behaved client should not do: announcing again
before the advertised min interval, retrying before Retry-After, a first
announce without event=started, leaving without event=stopped (counted when
the runner says the client stopped gracefully), and counters that cannot be
true (negative, non-numeric, over 1 PiB, or `left` above the torrent's size).

  GET  /announce, /<passkey>/announce, /scrape
  GET  /stats       live counters (JSON)
  POST /control     {"interval", "min_interval", "hosts", "outage",
                     "ramp", "checkpoint", "reset"} (see handle_control)

    tracker.py [--port 6969] [--profile default|clean] [--config FILE]
               [--manifest manifest.json] [--csv series.csv] [--stats-out FILE]

Python standard library only. Randomness is seeded per host; answers race on
threads, so two runs draw the same sequence but not in the same order.
"""
import argparse
import collections
import csv
import hashlib
import json
import math
import os
import random
import signal
import socket
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote_to_bytes

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from catalogue import bencode, passkey  # noqa: E402

HOSTS = ["t1", "t2", "t3", "t4", "t5", "t6"]
PIB = 1 << 50
EARLY_TOLERANCE = 2.0

PROFILES = {
    # Nothing on purpose: the tracker answers at once, every time.
    "clean": {h: {} for h in HOSTS},
    # A mixed day: one quick host, one slow, two rate-capped, one flaky. The
    # outage is not in the profile: run.py schedules it on t6 once the steady
    # phase starts (an outage in a profile counts from the tracker's start).
    "default": {
        "t1": {"latency": [15, 0.4]},
        "t2": {"latency": [60, 0.8], "rate_cap": 40, "retry_after": 60},
        "t3": {"latency": [120, 1.0], "p_503": 0.02, "p_reset": 0.01, "retry_after": 120},
        "t4": {"latency": [25, 0.5], "p_failure": 0.01},
        "t5": {"latency": [250, 0.6], "rate_cap": 15, "retry_after": 30},
        "t6": {"latency": [40, 0.5]},
    },
}


class Host:
    def __init__(self, name, seed, cfg):
        self.name = name
        self.rng = random.Random("%s|%s" % (seed, name))
        self.lock = threading.Lock()
        self.configure(cfg)
        self.reset_counters()

    def configure(self, cfg):
        self.cfg = dict(cfg)
        self.tokens = float(self.cfg.get("rate_cap") or 0)
        self.tok_at = time.monotonic()
        o = self.cfg.get("outage")
        self.outage = None
        if o:
            self.set_outage(o["start"], o["duration"], o.get("mode", "reset"))

    def set_outage(self, start_in, duration, mode):
        t0 = time.monotonic() + start_in
        self.outage = (t0, t0 + duration, mode)

    def reset_counters(self):
        self.c = collections.Counter()
        self.events = collections.Counter()
        self.variants = collections.Counter()
        self.viol = collections.Counter()
        self.torrents = set()
        self.keys = {}  # (info_hash, peer_id) -> [last_ok, min_interval, retry_until, active]
        self.latency_sum = 0.0

    def draw(self):
        with self.lock:
            return self.rng.random()

    def latency(self):
        lat = self.cfg.get("latency")
        if not lat:
            return 0.0
        with self.lock:
            ms = self.rng.lognormvariate(math.log(lat[0]), lat[1])
        return min(ms, 30000.0) / 1000.0

    def take_token(self):
        cap = self.cfg.get("rate_cap")
        if not cap:
            return True
        with self.lock:
            now = time.monotonic()
            self.tokens = min(float(cap), self.tokens + (now - self.tok_at) * cap)
            self.tok_at = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

    def down(self):
        if not self.outage:
            return None
        t0, t1, mode = self.outage
        now = time.monotonic()
        return mode if t0 <= now < t1 else None

    def snapshot(self):
        with self.lock:
            active = sum(1 for k in self.keys.values() if k[3])
            served = self.c["ok"] + self.c["failure"]
            return {
                "requests": self.c["requests"],
                "ok": self.c["ok"],
                "failure_reason": self.c["failure"],
                "http_429": self.c["r429"],
                "http_5xx": self.c["r5xx"],
                "resets": self.c["reset"],
                "timeouts": self.c["timeout"],
                "outage_hits": self.c["outage"],
                "scrapes": self.c["scrape"],
                "bad_requests": self.c["bad"],
                "events": dict(self.events),
                "url_variants": dict(self.variants),
                "violations": dict(self.viol),
                "unique_torrents": len(self.torrents),
                "active_peers": active,
                "avg_added_latency_ms": round(1000 * self.latency_sum / served, 1) if served else 0,
                "down_now": self.down(),
                "config": self.cfg,
            }


class State:
    def __init__(self, args, profile):
        self.started = time.monotonic()
        self.started_unix = time.time()
        self.seed = args.seed
        self.interval = args.interval
        self.min_interval = args.min_interval
        self.hosts = {h: Host(h, args.seed, profile.get(h, {})) for h in HOSTS}
        self.other = Host("other", args.seed, {})
        self.sizes = {}
        self.home = {}
        if args.manifest:
            with open(args.manifest, encoding="utf-8") as fh:
                m = json.load(fh)
            size_of = {p["pid"]: p["length"] for p in m["payloads"]}
            for t in m["torrents"]:
                ih = bytes.fromhex(t["info_hash"])
                self.sizes[ih] = size_of[t["pid"]]
                self.home[ih] = t["host"]
        self.passkeys = {h: passkey(args.seed, h) for h in HOSTS}
        self.ramp = None
        self.quiet_until = 0.0  # no early-announce violations before this (monotonic)
        self.lock = threading.Lock()
        self.checkpoints = []

    def host(self, name):
        return self.hosts.get(name, self.other)

    def advertised(self):
        with self.lock:
            return self.interval, self.min_interval

    def stats(self):
        hosts = {h: o.snapshot() for h, o in self.hosts.items()}
        if self.other.c["requests"]:
            hosts["other"] = self.other.snapshot()
        totals = collections.Counter()
        for s in hosts.values():
            for k in ("requests", "ok", "failure_reason", "http_429", "http_5xx", "resets", "timeouts"):
                totals[k] += s[k]
        iv, mi = self.advertised()
        return {
            "uptime": round(time.monotonic() - self.started, 1),
            "started_unix": self.started_unix,
            "interval": iv,
            "min_interval": mi,
            "totals": dict(totals),
            "hosts": hosts,
            "ramp": self.ramp_report(),
            "checkpoints": self.checkpoints,
        }

    # ---- ramp: lower the advertised interval step by step

    def start_ramp(self, steps, step_seconds):
        if not isinstance(step_seconds, list):
            step_seconds = [float(step_seconds)] * len(steps)
        with self.lock:
            self.ramp = {"steps": list(steps), "step_seconds": step_seconds,
                         "t0": time.monotonic(), "marks": [], "done": False}
        threading.Thread(target=self._ramp_loop, daemon=True).start()

    def _ramp_loop(self):
        r = self.ramp
        for iv, secs in zip(r["steps"], r["step_seconds"]):
            with self.lock:
                self.interval = iv
                self.min_interval = iv
            t_start = time.monotonic()
            req0 = self.total_ok()
            time.sleep(secs / 2)
            req_mid = self.total_ok()
            t_mid = time.monotonic()
            time.sleep(secs / 2)
            req_end = self.total_ok()
            t_end = time.monotonic()
            r["marks"].append({
                "interval": iv,
                "started_s": round(t_start - self.started, 1),
                "rate_whole_step": round((req_end - req0) / (t_end - t_start), 2),
                "rate_second_half": round((req_end - req_mid) / (t_end - t_mid), 2),
            })
        r["done"] = True

    def total_ok(self):
        return sum(h.c["requests"] for h in self.hosts.values())

    def ramp_report(self):
        r = self.ramp
        if not r:
            return None
        return {"steps": r["steps"], "step_seconds": r["step_seconds"],
                "results": list(r["marks"]), "done": r["done"]}

    def checkpoint(self, kind):
        """The runner says the client is gone. `graceful_stop`: every peer still
        active never sent event=stopped. `kill`: just forget them."""
        out = {"kind": kind, "at_s": round(time.monotonic() - self.started, 1), "missing_stopped": {}}
        for h in self.hosts.values():
            with h.lock:
                n = 0
                for k in h.keys.values():
                    if k[3]:
                        n += 1
                        k[3] = False
                if kind == "graceful_stop" and n:
                    h.viol["missing_stopped"] += n
                out["missing_stopped"][h.name] = n
        self.checkpoints.append(out)
        return out


def parse_query(qs):
    out = {}
    for part in qs.split("&"):
        if not part:
            continue
        k, _, v = part.partition("=")
        out[unquote_to_bytes(k).decode("latin-1")] = unquote_to_bytes(v)
    return out


def num(params, key):
    v = params.get(key)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        return "bad"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "scale-tracker/1"
    S = None  # State

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="text/plain", headers=()):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def reset(self):
        try:
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.connection.close()
        except OSError:
            pass
        self.close_connection = True

    def host_name(self):
        h = (self.headers.get("Host") or "").strip().lower()
        if h.startswith("["):
            return h
        return h.split(":")[0]

    def do_GET(self):
        path, _, qs = self.path.partition("?")
        if path == "/stats":
            return self.send(200, json.dumps(self.S.stats(), indent=1).encode(), "application/json")
        segs = [s for s in path.split("/") if s]
        if not segs or segs[-1] not in ("announce", "scrape"):
            return self.send(404, b"not found")
        h = self.S.host(self.host_name())
        params = parse_query(qs)
        if segs[-1] == "scrape":
            return self.scrape(h, params)
        self.announce(h, segs, params)

    def scrape(self, h, params):
        with h.lock:
            h.c["scrape"] += 1
        ih = params.get("info_hash")
        files = {}
        if ih and len(ih) == 20:
            files[ih] = {"complete": 1 + ih[0] % 20, "downloaded": ih[2] % 50, "incomplete": ih[1] % 5}
        self.send(200, bencode({"files": files}))

    def announce(self, h, segs, params):
        now = time.monotonic()
        with h.lock:
            h.c["requests"] += 1
        mode = h.down()
        if mode:
            with h.lock:
                h.c["outage"] += 1
            return self.fail(h, mode)

        ih = params.get("info_hash")
        pid = params.get("peer_id")
        if not ih or len(ih) != 20 or not pid or len(pid) != 20 or num(params, "port") in (None, "bad"):
            with h.lock:
                h.c["bad"] += 1
            return self.send(400, bencode({"failure reason": "bad announce"}))

        variant = "path" if len(segs) >= 2 else ("query" if "passkey" in params else "plain")
        event = params.get("event", b"").decode("latin-1")
        key = (ih, pid)
        S = self.S
        with h.lock:
            h.variants[variant] += 1
            h.events[event or "none"] += 1
            h.torrents.add(ih)
            pk = segs[-2] if variant == "path" else params.get("passkey", b"").decode("latin-1")
            if variant != "plain" and h.name in S.passkeys and pk != S.passkeys[h.name]:
                h.viol["bad_passkey"] += 1
            if S.home and ih not in S.home:
                h.viol["unknown_torrent"] += 1
            elif S.home and S.home[ih] != h.name:
                h.viol["wrong_host"] += 1
            for f in ("uploaded", "downloaded", "left"):
                v = num(params, f)
                if v is None or v == "bad" or v < 0 or v > PIB:
                    h.viol["absurd_counters"] += 1
                    break
            else:
                if ih in S.sizes and num(params, "left") > S.sizes[ih]:
                    h.viol["left_above_size"] += 1
                if num(params, "uploaded") > 0 or num(params, "downloaded") > 0:
                    # No real peer exists here, so nothing can have moved.
                    h.viol["nonzero_transfer"] += 1
            k = h.keys.get(key)
            if k is None:
                if event != "started":
                    h.viol["first_without_started"] += 1
                k = h.keys[key] = [None, 0, 0.0, False]
            elif event in ("", "empty") and now >= S.quiet_until:
                if k[0] is not None and now - k[0] < k[1] - EARLY_TOLERANCE:
                    h.viol["before_min_interval"] += 1
                if now < k[2] - EARLY_TOLERANCE:
                    h.viol["before_retry_after"] += 1
            k[3] = event != "stopped"

        delay = h.latency()
        r = h.draw()
        p503 = h.cfg.get("p_503", 0)
        preset = h.cfg.get("p_reset", 0)
        pfail = h.cfg.get("p_failure", 0)
        retry = int(h.cfg.get("retry_after", 60))
        if not h.take_token():
            with h.lock:
                h.c["r429"] += 1
                k[2] = now + retry
            return self.send(429, b"slow down", headers=[("Retry-After", str(retry))])
        if delay:
            time.sleep(delay)
        if r < p503:
            with h.lock:
                h.c["r5xx"] += 1
                k[2] = now + retry
            return self.send(503, b"overloaded", headers=[("Retry-After", str(retry))])
        if r < p503 + preset:
            with h.lock:
                h.c["reset"] += 1
            return self.reset()
        if r < p503 + preset + pfail:
            with h.lock:
                h.c["failure"] += 1
                h.latency_sum += delay
            return self.send(200, bencode({"failure reason": "torrent not registered with this tracker"}))
        iv, mi = S.advertised()
        with h.lock:
            h.c["ok"] += 1
            h.latency_sum += delay
            if event != "stopped":
                k[0] = time.monotonic()
                k[1] = mi
        body = {
            "interval": iv,
            "min interval": mi,
            "complete": 1 + ih[0] % 20,
            "incomplete": ih[1] % 5,
            "peers": b"",
        }
        self.send(200, bencode(body))

    def fail(self, h, mode):
        if mode == "503":
            with h.lock:
                h.c["r5xx"] += 1
            return self.send(503, b"down for maintenance", headers=[("Retry-After", "60")])
        if mode == "timeout":
            with h.lock:
                h.c["timeout"] += 1
            time.sleep(90)
        else:
            with h.lock:
                h.c["reset"] += 1
        self.reset()

    def do_POST(self):
        if self.path != "/control":
            return self.send(404, b"not found")
        n = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(n) or b"{}")
            out = handle_control(self.S, body)
        except (ValueError, KeyError, TypeError) as e:
            return self.send(400, json.dumps({"error": str(e)}).encode(), "application/json")
        self.send(200, json.dumps(out).encode(), "application/json")


def handle_control(S, body):
    """interval/min_interval: advertise these from now on.
    hosts: {"t2": {...}} replace a host's behaviour; "profile": name for all.
    outage: {"host", "start_in", "duration", "mode"}.
    ramp: {"steps": [..intervals..], "step_seconds": n or [n per step]}: each
          step advertises its interval (and the same min interval) and
          reports the announce rate over its second half.
    checkpoint: "graceful_stop" | "kill".  reset: true zeroes every counter.
    quiet_for: seconds during which early announces are not counted."""
    out = {}
    with S.lock:
        if "interval" in body:
            S.interval = int(body["interval"])
        if "min_interval" in body:
            S.min_interval = int(body["min_interval"])
    if "profile" in body:
        prof = PROFILES[body["profile"]]
        for h, o in S.hosts.items():
            o.configure(prof.get(h, {}))
    for h, cfg in (body.get("hosts") or {}).items():
        S.hosts[h].configure(cfg)
    if "outage" in body:
        o = body["outage"]
        S.hosts[o["host"]].set_outage(float(o.get("start_in", 0)), float(o["duration"]), o.get("mode", "reset"))
    if "reset" in body and body["reset"]:
        for o in list(S.hosts.values()) + [S.other]:
            with o.lock:
                o.reset_counters()
        S.checkpoints = []
    if "quiet_for" in body:
        # The runner is about to force announces (a manual reannounce of
        # everything): those are not the client's doing.
        S.quiet_until = time.monotonic() + float(body["quiet_for"])
    if "checkpoint" in body:
        out["checkpoint"] = S.checkpoint(body["checkpoint"])
    if "ramp" in body:
        r = body["ramp"]
        secs = r.get("step_seconds", 180)
        secs = [float(x) for x in secs] if isinstance(secs, list) else float(secs)
        S.start_ramp([int(x) for x in r["steps"]], secs)
    out["interval"], out["min_interval"] = S.advertised()
    return out


def series_writer(S, path, every):
    fields = ["ts", "elapsed_s", "host", "interval", "requests", "ok", "failure_reason",
              "http_429", "http_5xx", "resets", "timeouts", "announces_per_s"]
    new = not os.path.exists(path)
    fh = open(path, "a", newline="")
    w = csv.writer(fh)
    if new:
        w.writerow(fields)
    last = {}
    while True:
        time.sleep(every)
        iv, _ = S.advertised()
        el = round(time.monotonic() - S.started, 1)
        for name, h in S.hosts.items():
            with h.lock:
                cur = (h.c["requests"], h.c["ok"], h.c["failure"], h.c["r429"], h.c["r5xx"],
                       h.c["reset"], h.c["timeout"])
            prev = last.get(name, (0,) * len(cur))
            d = [max(0, a - b) for a, b in zip(cur, prev)]
            last[name] = cur
            w.writerow([round(time.time(), 1), el, name, iv] + d + [round(d[0] / every, 2)])
        fh.flush()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024  # read at listen(), so a class attribute

    def server_bind(self):
        # HTTPServer.server_bind resolves its own name (getfqdn), which can
        # take seconds on a machine without reverse DNS. Nothing here uses it.
        import socketserver
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=6969)
    ap.add_argument("--seed", type=int, default=1, help="the catalogue seed (passkeys, randomness)")
    ap.add_argument("--profile", default="default", choices=sorted(PROFILES))
    ap.add_argument("--config", help="JSON {host: {...}} merged over the profile")
    ap.add_argument("--interval", type=int, default=1800)
    ap.add_argument("--min-interval", type=int, default=900)
    ap.add_argument("--manifest", help="catalogue manifest: enables unknown/wrong-host/left checks")
    ap.add_argument("--csv", help="append a per-host time series here")
    ap.add_argument("--csv-every", type=float, default=5.0)
    ap.add_argument("--stats-out", help="write the final /stats here on exit")
    a = ap.parse_args()
    profile = json.loads(json.dumps(PROFILES[a.profile]))
    if a.config:
        with open(a.config, encoding="utf-8") as fh:
            for h, cfg in json.load(fh).items():
                profile.setdefault(h, {}).update(cfg)
    S = State(a, profile)
    Handler.S = S
    srv = Server((a.bind, a.port), Handler)
    if a.csv:
        threading.Thread(target=series_writer, args=(S, a.csv, a.csv_every), daemon=True).start()

    def stop(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    print("tracker on %s:%d, profile %s, interval %d" % (a.bind, a.port, a.profile, a.interval), flush=True)
    try:
        srv.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        if a.stats_out:
            with open(a.stats_out, "w") as fh:
                json.dump(S.stats(), fh, indent=1)


if __name__ == "__main__":
    main()
