#!/usr/bin/env python3
"""Tracker backends for run.py.

  torrust      Torrust Tracker (public mode) behind nginx. The default.
  opentracker  opentracker behind the same nginx, no faults: a cross-check.
               Its interval is compiled in (~1800 s, randomised), so no ramp.
  homemade     tools/scale/tracker.py, the legacy all-in-one tracker.

For the first two, one nginx container carries the network aliases t1..t6
and has one server block per host. nginx does the misbehaviour (the same
per-host profiles as tracker.py: latency and random 503/reset/failure in
nginx/faults.js, rate caps with limit_req + 429 + Retry-After, an outage
window as an include that run.py flips with `nginx -s reload`) and writes one
JSON line per request to WORKRUN/announces.jsonl. Everything the tracker
itself does not report (per-host counts, statuses, events, counters) comes
from that log: LogTail follows it live for the sampler and tracker.csv, and
analyse_log.py checks the protocol over it afterwards.

Python standard library only.
"""
import collections
import csv
import json
import os
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import unquote_to_bytes

HERE = os.path.dirname(os.path.abspath(__file__))
from tracker import HOSTS, PROFILES  # noqa: E402  (one source for the host profiles)

TORRUST_TOKEN = "scale-admin-token"  # matches nginx/torrust.toml
BACKEND_ADDR = {"torrust": "torrust:7070", "opentracker": "opentracker:6969"}
FAIL_MSG = "torrent not registered with this tracker"
# opentracker advertises a randomised interval around 1800 s and min
# interval = interval / 2 (seen: 1632/816, 1855/927, 1969/984). Fixed at build
# time. The min interval here is the floor the log checks hold clients to.
OPENTRACKER_INTERVAL = (1800, 800)


def merged_profile(name, overrides=None):
    prof = json.loads(json.dumps(PROFILES[name]))
    for h, cfg in (overrides or {}).items():
        prof.setdefault(h, {}).update(cfg)
    return prof


def load_overrides(arg):
    """--tracker-config: a JSON string or a file holding {host: {...}}."""
    if not arg:
        return {}
    if os.path.exists(arg):
        with open(arg, encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(arg)


# ---------------------------------------------------------------- nginx config


def _rate(cap):
    cap = float(cap)
    if cap >= 1:
        return "%dr/s" % round(cap)
    return "%dr/m" % max(1, round(cap * 60))


def render_nginx(profile, backend, log_path, conf_dir, faults=True):
    """nginx.conf for six hosts in front of `backend`. `conf_dir` is where the
    outage includes live, as nginx sees it."""
    up = BACKEND_ADDR[backend]
    L = []
    w = L.append
    w("# Written by tools/scale/trackers.py; regenerated on every profile change.")
    w("load_module modules/ngx_http_js_module.so;")
    w("user nginx;")
    w("worker_processes 2;")
    w("error_log /dev/stderr notice;")
    w("pid /tmp/nginx.pid;")
    w("events { worker_connections 16384; }")
    w("http {")
    w("  default_type text/plain;")
    w("  server_tokens off;")
    w("  keepalive_timeout 75s;")
    w("  keepalive_requests 1000000;")
    w("  resolver 127.0.0.11 valid=5s ipv6=off;")
    w("  js_path /scale/nginx;")
    w("  js_import faults from faults.js;")
    w("  js_var $up_status;")
    w("  js_var $added_ms;")
    w("  map $request_uri $req_path { \"~^(?<p>[^?]*)\" $p; default \"\"; }")
    w("  upstream backend {")
    w("    zone backend 64k;")
    w("    server %s resolve;" % up)
    w("    keepalive 64;")
    w("  }")
    w("  limit_req_status 429;")
    w("  limit_req_log_level info;")
    w("  log_format scale escape=json '{\"ts\":$msec,\"host\":\"$server_name\",\"path\":\"$req_path\","
      "\"st\":$status,\"rt\":$request_time,\"ust\":\"$up_status$upstream_status\",\"lim\":\"$limit_req_status\","
      "\"ra\":\"$sent_http_retry_after\",\"fault\":\"$fault\",\"out\":\"$outage\",\"add\":\"$added_ms\","
      "\"ev\":\"$arg_event\",\"ih\":\"$arg_info_hash\",\"pid\":\"$arg_peer_id\",\"port\":\"$arg_port\","
      "\"up\":\"$arg_uploaded\",\"down\":\"$arg_downloaded\",\"left\":\"$arg_left\",\"pk\":\"$arg_passkey\","
      "\"probe\":\"$arg_probe\",\"ip\":\"$remote_addr\",\"conn\":$connection,\"creq\":$connection_requests}';")
    w("  access_log %s scale;" % log_path)
    w("  proxy_http_version 1.1;")
    w("  proxy_set_header Connection \"\";")
    w("  proxy_set_header Host $host;")
    w("  proxy_set_header X-Forwarded-For $remote_addr;")
    w("  proxy_connect_timeout 5s;")
    w("  proxy_read_timeout 30s;")
    for h in HOSTS:
        cfg = profile.get(h, {}) if faults else {}
        ra = int(cfg.get("retry_after", 60))
        if cfg.get("rate_cap"):
            w("  limit_req_zone $server_name zone=rl_%s:1m rate=%s;" % (h, _rate(cfg["rate_cap"])))
        w("  map $status $ra_%s { 429 \"%d\"; 503 \"%d\"; default \"\"; }" % (h, ra, ra))
    for h in HOSTS + ["other"]:
        cfg = profile.get(h, {}) if (faults and h != "other") else {}
        lat = cfg.get("latency") or [0, 0]
        w("  server {")
        if h == "other":
            w("    listen 6969 default_server;")
            w("    server_name other;")
        else:
            w("    listen 6969;")
            w("    server_name %s;" % h)
            w("    add_header Retry-After $ra_%s always;" % h)
        w("    set $fault \"\";")
        w("    set $outage \"\";")
        if h != "other":
            w("    include %s/outage-%s.conf;" % (conf_dir, h))
        w("    set $lat_median %s;" % lat[0])
        w("    set $lat_sigma %s;" % lat[1])
        w("    set $p_503 %s;" % cfg.get("p_503", 0))
        w("    set $p_reset %s;" % cfg.get("p_reset", 0))
        w("    set $p_failure %s;" % cfg.get("p_failure", 0))
        w("    location ~ ^/(?:[^/]+/)?announce$ {")
        if cfg.get("rate_cap"):
            w("      limit_req zone=rl_%s burst=%d nodelay;" % (h, max(1, int(float(cfg["rate_cap"])))))
        w("      js_content faults.announce;")
        w("    }")
        w("    location ~ ^/(?:[^/]+/)?scrape$ { proxy_pass http://backend/scrape$is_args$args; }")
        w("    location = /_up { internal; proxy_pass http://backend/announce$is_args$args; }")
        w("    location = /_f503 { internal; set $fault 503; return 503 \"overloaded\"; }")
        w("    location = /_freset { internal; set $fault reset; return 444; }")
        w("    location = /_ffail { internal; set $fault failure; return 200 \"d14:failure reason%d:%se\"; }"
          % (len(FAIL_MSG), FAIL_MSG))
        w("    location / { return 404 \"not found\"; }")
        w("  }")
    w("}")
    return "\n".join(L) + "\n"


def outage_conf(mode):
    if mode == "reset":
        return "set $outage reset;\nreturn 444;\n"
    if mode == "503":
        return "set $outage 503;\nreturn 503 \"down for maintenance\";\n"
    return ""


def render_torrust(interval, interval_min):
    with open(os.path.join(HERE, "nginx", "torrust.toml"), encoding="utf-8") as fh:
        s = fh.read()
    return s.replace("@INTERVAL@", str(int(interval))).replace("@INTERVAL_MIN@", str(int(interval_min)))


def write_lf(path, text):
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


# ---------------------------------------------------------------- live log tail

COUNTERS = ("requests", "ok", "failure_reason", "http_429", "http_5xx", "resets", "timeouts", "outage_hits")


class LogTail:
    """Follows announces.jsonl and keeps tracker.py-shaped counters per host,
    plus who is still active (last event not `stopped`) for the graceful-stop
    checkpoint. Writes tracker.csv (same columns as tracker.py's) as it goes."""

    def __init__(self, path, csv_path=None, csv_every=5.0, interval_fn=None):
        self.path = path
        self.csv_path = csv_path
        self.csv_every = csv_every
        self.interval_fn = interval_fn or (lambda: None)
        self.lock = threading.Lock()
        self.c = {h: collections.Counter() for h in HOSTS + ["other"]}
        self.active = {}  # (host, ih, pid) -> bool
        self.pos = 0
        self.buf = b""
        self.bad_lines = 0
        self.started = time.time()
        self._stop = threading.Event()
        self._thread = None

    def poll(self):
        try:
            with open(self.path, "rb") as fh:
                fh.seek(self.pos)
                data = fh.read()
        except OSError:
            return
        if not data:
            return
        self.pos += len(data)
        data = self.buf + data
        lines = data.split(b"\n")
        self.buf = lines.pop()
        with self.lock:
            for line in lines:
                if line.strip():
                    self._count(line)

    def _count(self, line):
        try:
            x = json.loads(line)
        except ValueError:
            self.bad_lines += 1
            return
        if not x.get("path", "").endswith("/announce"):
            return
        h = x.get("host") if x.get("host") in self.c else "other"
        c = self.c[h]
        st = x.get("st")
        c["requests"] += 1
        if x.get("out"):
            c["outage_hits"] += 1
        if x.get("fault") == "failure":
            c["failure_reason"] += 1
        elif 200 <= st < 300:
            c["ok"] += 1
        elif st == 429:
            c["http_429"] += 1
        elif st == 504:
            c["timeouts"] += 1
            c["http_5xx"] += 1
        elif st >= 500:
            c["http_5xx"] += 1
        elif st in (444, 499):
            c["resets"] += 1
        ev = x.get("ev") or ""
        c["ev_" + (ev or "none")] += 1
        self.active[(h, x.get("ih"), x.get("pid"))] = ev != "stopped"

    def stats(self):
        self.poll()
        with self.lock:
            hosts = {}
            for h, c in self.c.items():
                if h == "other" and not c["requests"]:
                    continue
                d = {k: c[k] for k in COUNTERS}
                d["events"] = {k[3:]: v for k, v in c.items() if k.startswith("ev_")}
                d["active_peers"] = sum(1 for k, v in self.active.items() if v and k[0] == h)
                hosts[h] = d
            totals = collections.Counter()
            for d in hosts.values():
                for k in COUNTERS:
                    totals[k] += d[k]
        return {"hosts": hosts, "totals": dict(totals)}

    def total_requests(self):
        self.poll()
        with self.lock:
            return sum(c["requests"] for c in self.c.values())

    def checkpoint(self, kind):
        """Every key still active was never stopped. Then forget them."""
        self.poll()
        out = {"kind": kind, "at": time.time(), "missing_stopped": {}}
        with self.lock:
            for k, v in self.active.items():
                if v:
                    out["missing_stopped"][k[0]] = out["missing_stopped"].get(k[0], 0) + 1
            self.active = {k: False for k in self.active}
        return out

    def _csv_loop(self):
        fields = ["ts", "elapsed_s", "host", "interval", "requests", "ok", "failure_reason",
                  "http_429", "http_5xx", "resets", "timeouts", "announces_per_s"]
        new = not os.path.exists(self.csv_path)
        with open(self.csv_path, "a", newline="") as fh:
            wr = csv.writer(fh)
            if new:
                wr.writerow(fields)
            last = {}
            while not self._stop.wait(self.csv_every):
                self.poll()
                with self.lock:
                    snap = {h: tuple(c[k] for k in COUNTERS[:7]) for h, c in self.c.items() if h != "other"}
                iv = self.interval_fn()
                for h, cur in snap.items():
                    prev = last.get(h, (0,) * len(cur))
                    d = [max(0, a - b) for a, b in zip(cur, prev)]
                    last[h] = cur
                    wr.writerow([round(time.time(), 1), round(time.time() - self.started, 1), h, iv] + d
                                + [round(d[0] / self.csv_every, 2)])
                fh.flush()

    def start(self):
        if self.csv_path:
            self._thread = threading.Thread(target=self._csv_loop, daemon=True)
            self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        self.poll()


# ---------------------------------------------------------------- backends


def _http(method, url, body=None, timeout=10, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    hdr = dict(headers or {})
    if data:
        hdr["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=hdr)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except (urllib.error.URLError, OSError) as e:
        return None, str(e).encode()


class Homemade:
    """tracker.py, driven through its /stats and /control."""
    name = "homemade"
    services = ["tracker"]
    can_set_interval = True
    has_faults = True

    def __init__(self, run):
        self.run = run
        self.url = "http://127.0.0.1:%d" % run.a.tracker_port
        self.interval = (run.a.interval, run.a.interval // 2)

    def _json(self, method, path, body=None):
        st, raw = _http(method, self.url + path, body, timeout=30)
        try:
            return json.loads(raw) if st == 200 else None
        except ValueError:
            return None

    def up(self):
        self.run.dc("up", "-d", "tracker", capture=True)
        deadline = time.time() + 60
        while time.time() < deadline and not self.stats():
            time.sleep(0.5)
        if not self.stats():
            raise SystemExit("tracker /stats did not answer")

    def stats(self):
        return self._json("GET", "/stats")

    def total_requests(self):
        return ((self.stats() or {}).get("totals") or {}).get("requests")

    def set_profile(self, name):
        self._json("POST", "/control", {"profile": name})

    def set_interval(self, iv, mi):
        self.interval = (iv, mi)
        self._json("POST", "/control", {"interval": iv, "min_interval": mi})
        return 0.0

    def quiet(self, seconds):
        self._json("POST", "/control", {"quiet_for": seconds})

    def schedule_outage(self, host, at, dur, mode):
        self._json("POST", "/control", {"outage": {"host": host, "start_in": at, "duration": dur, "mode": mode}})

    def checkpoint(self, kind):
        return (self._json("POST", "/control", {"checkpoint": kind}) or {}).get("checkpoint")

    def backend_stats(self):
        return None

    def finish(self, out_dir):
        st = self.stats() or {}
        self.run.dc("stop", "tracker", capture=True, check=False)  # writes tracker-final.json
        hosts = {h: {k: v for k, v in s.items() if k != "config"} for h, s in st.get("hosts", {}).items()}
        return hosts, st.get("checkpoints"), None


class Proxied:
    """Torrust or opentracker behind nginx; counts come from the nginx log."""

    def __init__(self, run, backend):
        self.run = run
        self.a = run.a
        self.name = backend
        self.services = [backend, "nginx"]
        self.can_set_interval = backend == "torrust"
        self.conf_host = os.path.join(run.dir, "tracker-conf")
        self.conf_ctr = "/work/%s/tracker-conf" % run.rel
        self.log_host = os.path.join(run.dir, "announces.jsonl")
        self.log_ctr = "/work/%s/announces.jsonl" % run.rel
        self.overrides = load_overrides(self.a.tracker_config)
        self.profile_name = self.a.tracker_profile
        # opentracker is the plain cross-check: no faults, no outage.
        self.faults = self.has_faults = backend == "torrust"
        if backend == "torrust":
            self.interval = (self.a.interval, self.a.interval // 2)
        else:
            self.interval = OPENTRACKER_INTERVAL
        self.api = "http://127.0.0.1:%d" % self.a.tracker_api_port
        self.tail = None
        self.checkpoints = []

    # -- files

    def write_nginx(self):
        write_lf(os.path.join(self.conf_host, "nginx.conf"),
                 render_nginx(merged_profile(self.profile_name, self.overrides), self.name, self.log_ctr,
                              self.conf_ctr, faults=self.faults))

    def write_outage(self, host, mode):
        write_lf(os.path.join(self.conf_host, "outage-%s.conf" % host), outage_conf(mode))

    def reload(self):
        r = self.run.dc("exec", "-T", "nginx", "nginx", "-c", self.conf_ctr + "/nginx.conf", "-s", "reload",
                        capture=True, check=False)
        if r.returncode != 0:
            raise SystemExit("nginx reload failed: %s" % (r.stderr or r.stdout)[-1000:])

    # -- lifecycle

    def up(self):
        os.makedirs(self.conf_host, exist_ok=True)
        for h in HOSTS:
            self.write_outage(h, None)
        self.write_nginx()
        if self.name == "torrust":
            write_lf(os.path.join(self.conf_host, "torrust.toml"), render_torrust(*self.interval))
        open(self.log_host, "ab").close()
        self.run.dc("up", "-d", self.name, capture=True)
        self.wait_backend(60)
        self.run.dc("up", "-d", "nginx", capture=True)
        deadline = time.time() + 30
        while time.time() < deadline:
            st, _ = _http("GET", "http://127.0.0.1:%d/" % self.a.tracker_port)
            if st == 404:
                break
            time.sleep(0.5)
        else:
            raise SystemExit("nginx did not answer on %d" % self.a.tracker_port)
        self.tail = LogTail(self.log_host, os.path.join(self.run.dir, "tracker.csv"),
                            interval_fn=lambda: self.interval[0]).start()
        self.probe("up")

    def probe(self, tag):
        """One request the log can be matched with, to measure the clock
        offset between this host and the containers' log timestamps."""
        t = time.time()
        _http("GET", "http://127.0.0.1:%d/probe?probe=%s-%.3f" % (self.a.tracker_port, tag, t))
        return t

    def wait_backend(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.backend_stats() is not None:
                return
            time.sleep(0.3)
        raise SystemExit("%s did not come up" % self.name)

    def backend_stats(self):
        if self.name == "torrust":
            st, raw = _http("GET", self.api + "/api/v1/stats?token=" + TORRUST_TOKEN, timeout=5)
            try:
                return json.loads(raw) if st == 200 else None
            except ValueError:
                return None
        st, raw = _http("GET", self.api + "/stats?mode=everything", timeout=5)
        if st != 200:
            return None
        txt = raw.decode("utf-8", "replace")

        def grab(pat):
            m = re.search(pat, txt, re.S)
            return int(m.group(1)) if m else None
        return {"torrents": grab(r"<torrents>.*?<count_mutex>(\d+)"), "peers": grab(r"<peers>\s*<count>(\d+)"),
                "seeds": grab(r"<seeds>\s*<count>(\d+)"),
                "tcp_announces": grab(r"<tcp>.*?<announce>(\d+)"), "tcp_scrapes": grab(r"<tcp>.*?<scrape>(\d+)")}

    def stats(self):
        return self.tail.stats() if self.tail else None

    def total_requests(self):
        return self.tail.total_requests()

    def set_profile(self, name):
        self.profile_name = name
        self.write_nginx()
        self.reload()

    def set_interval(self, iv, mi):
        """Torrust only: rewrite its config and restart it (it keeps no state
        the benchmark needs). Returns how long the restart took."""
        if not self.can_set_interval:
            raise SystemExit("the %s backend has a fixed interval" % self.name)
        self.interval = (iv, mi)
        write_lf(os.path.join(self.conf_host, "torrust.toml"), render_torrust(iv, mi))
        t = time.time()
        self.run.dc("restart", "-t", "5", "torrust", capture=True)
        self.wait_backend(60)
        return round(time.time() - t, 2)

    def quiet(self, seconds):
        pass  # analyse_log.py reads the quiet window from the timeline

    def schedule_outage(self, host, at, dur, mode):
        if mode not in ("reset", "503"):
            raise SystemExit("outage mode %r is homemade-only; use reset or 503" % mode)

        def go():
            time.sleep(at)
            self.write_outage(host, mode)
            self.reload()
            t0 = time.time()
            self.run.mark("outage", host=host, mode=mode, start=t0)
            time.sleep(dur)
            self.write_outage(host, None)
            self.reload()
            self.run.mark("outage_end", host=host, mode=mode, start=t0, end=time.time())
        threading.Thread(target=go, daemon=True).start()

    def checkpoint(self, kind):
        cp = self.tail.checkpoint(kind)
        cp["backend"] = self.backend_stats()
        self.checkpoints.append(cp)
        return cp

    def finish(self, out_dir):
        self.probe("end")
        time.sleep(1.5)
        self.tail.stop()
        st = self.tail.stats()
        final = {"hosts": st["hosts"], "totals": st["totals"], "checkpoints": self.checkpoints,
                 "backend": self.backend_stats(), "bad_log_lines": self.tail.bad_lines}
        with open(os.path.join(out_dir, "tracker-final.json"), "w") as fh:
            json.dump(final, fh, indent=1)
        r = self.run.dc("logs", "--no-color", "nginx", self.name, capture=True, check=False)
        with open(os.path.join(out_dir, "tracker-containers.log"), "w", encoding="utf-8") as fh:
            fh.write(r.stdout[-5_000_000:])
        return st["hosts"], self.checkpoints, final["backend"]


def make(run):
    if run.a.tracker == "homemade":
        return Homemade(run)
    return Proxied(run, run.a.tracker)


def unquote_hex(s):
    """A percent-encoded info_hash or peer_id from the log, as hex."""
    return unquote_to_bytes(s or "").hex()


if __name__ == "__main__":
    # Print the generated nginx.conf for a profile, for reading.
    import sys
    print(render_nginx(merged_profile(sys.argv[1] if len(sys.argv) > 1 else "default"), "torrust",
                       "/work/announces.jsonl", "/work/tracker-conf"))
