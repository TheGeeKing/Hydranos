#!/usr/bin/env python3
"""Tier 1 scale benchmark runner: one Hydranos, ~20,000 generated torrents,
six local misbehaving tracker hosts.

    python tools/scale/run.py --work /abs/workdir [--count 20000] [--seed 1]
                              [--runs 1] [--steady 1800] [--ramp] [--quick]
                              [--tracker torrust|opentracker|homemade]

The tracker is Torrust Tracker behind nginx by default (trackers.py); nginx
carries the six host names and the faults and logs every announce, and
analyse_log.py checks the protocol over that log. --tracker opentracker runs
the same flow against opentracker with no faults; --tracker homemade is the
legacy tools/scale/tracker.py.

Per run, on fresh volumes:
  generate catalogue (once) -> tracker up -> payload into the data volume ->
  Hydranos first start -> setup -> add everything (timed) -> wait for checks ->
  steady state (tracker outage on t6 in the middle) -> optional announce ramp
  -> graceful restart (timed) -> kill -9 restart (timed) -> results.

The ramp runs BEFORE the restarts on purpose: Hydranos admits a catalogue
into its announce schedule over one default interval (30 min) after each
start, so a ramp after a restart would measure the admission, not the limit.

Output: WORK/results/<stamp>/run-N/{samples.csv,tracker.csv,tracker-final.json,
result.json,hydranos.log,timeline.json} (+ announces.jsonl, log-analysis.*,
tracker-conf/ with nginx/Torrust) and WORK/results/<stamp>/summary.{json,md}.
Python standard library and the docker CLI (with compose v2) only.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
import platform
import re
import secrets
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import analyse_log  # noqa: E402
import sample  # noqa: E402
import trackers  # noqa: E402

COMPOSE = os.path.join(HERE, "compose.yml")
PROJECT = "hydranos-scale"
ALL_PROFILES = ["torrust", "opentracker", "homemade", "tools"]
SAVE_PATH = "/data/payload"


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------- docker


class Compose:
    def __init__(self, env):
        self.env = dict(os.environ, **env)

    def __call__(self, *args, check=True, capture=False, timeout=None):
        cmd = ["docker", "compose", "-f", COMPOSE, "-p", PROJECT] + list(args)
        r = subprocess.run(cmd, env=self.env, capture_output=capture, text=True,
                           encoding="utf-8", errors="replace", timeout=timeout)
        if check and r.returncode != 0:
            raise SystemExit("failed: %s\n%s" % (" ".join(cmd), (r.stderr or "")[-2000:] if capture else ""))
        return r

    def container(self, service):
        r = self("ps", "-a", "-q", service, capture=True)
        ids = r.stdout.split()
        if not ids:
            raise SystemExit("no container for " + service)
        return ids[0]


def docker_out(*args):
    r = subprocess.run(["docker"] + list(args), capture_output=True, text=True, encoding="utf-8", errors="replace")
    return r.stdout.strip() if r.returncode == 0 else None


# ---------------------------------------------------------------- HTTP


def http(method, url, key=None, body=None, ctype="application/json", timeout=30):
    headers = {}
    if key:
        headers["X-Api-Key"] = key
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        headers["Content-Type"] = ctype
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw[:1] in (b"{", b"[") else raw)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw
    except (urllib.error.URLError, OSError) as e:
        return None, str(e)


def multipart(fields, files):
    boundary = "scale" + uuid.uuid4().hex
    out = []
    for k, v in fields.items():
        out.append(("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n" % (boundary, k, v)).encode())
    for name, blob in files:
        out.append(("--%s\r\nContent-Disposition: form-data; name=\"torrents\"; filename=\"%s\"\r\n"
                    "Content-Type: application/x-bittorrent\r\n\r\n" % (boundary, name)).encode())
        out.append(blob)
        out.append(b"\r\n")
    out.append(("--%s--\r\n" % boundary).encode())
    return b"".join(out), "multipart/form-data; boundary=" + boundary


def wait_for(pred, timeout, every=1.0, what="condition"):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(every)
    raise SystemExit("timed out after %ds waiting for %s" % (timeout, what))


# ---------------------------------------------------------------- one run


class Run:
    def __init__(self, a, n, out_dir, manifest):
        self.a = a
        self.n = n
        self.dir = out_dir
        self.manifest = manifest
        self.api = "http://127.0.0.1:%d" % a.hydranos_port
        rel = self.rel = os.path.relpath(out_dir, a.work).replace(os.sep, "/")
        env = {
            "COMPOSE_PROFILES": a.tracker,
            "SCALE_WORK": a.work,
            "SCALE_RUN_DIR": rel,
            "SCALE_SEED": str(a.seed),
            "TRACKER_PROFILE": a.tracker_profile,
            "TRACKER_INTERVAL": str(a.interval),
            "TRACKER_MIN_INTERVAL": str(a.interval // 2),
            "TRACKER_PORT": str(a.tracker_port),
            "TRACKER_API_PORT": str(a.tracker_api_port),
            "HYDRANOS_PORT": str(a.hydranos_port),
            "HYDRANOS_CPUS": str(a.cpus),
            "HYDRANOS_MEM": a.mem,
        }
        if a.image:
            env["HYDRANOS_IMAGE"] = a.image
        if hasattr(os, "getuid"):
            env["SCALE_UID"], env["SCALE_GID"] = str(os.getuid()), str(os.getgid())
        self.dc = Compose(env)
        self.r = {"run": n, "tracker": a.tracker}
        self.key = None
        self.timeline = []
        os.makedirs(self.dir, exist_ok=True)
        self.trk = trackers.make(self)

    # -- helpers

    def mark(self, kind, **kw):
        """A timeline event, host clock. analyse_log.py reads them."""
        kw.setdefault("t", time.time())
        self.timeline.append(dict(kind=kind, **kw))

    def set_phase(self, name):
        self.sampler.phase = name
        self.mark("phase", name=name)

    def down(self):
        args = []
        for p in ALL_PROFILES:
            args += ["--profile", p]
        self.dc(*args, "down", "-v", "--remove-orphans", check=False, capture=True)

    def startup(self):
        st, body = http("GET", self.api + "/api/startup", timeout=5)
        return body if st == 200 and isinstance(body, dict) else None

    def wait_ready(self, timeout):
        t0 = time.time()
        wait_for(lambda: (self.startup() or {}).get("ready"), timeout, 0.5, "Hydranos /api/startup ready")
        return round(time.time() - t0, 2)

    def engines(self):
        st, body = http("GET", self.api + "/api/engines", self.key)
        if st != 200:
            return None
        return body if isinstance(body, list) else body.get("engines", [])

    def states(self):
        tot = {}
        for e in self.engines() or []:
            for k, v in ((e.get("stats") or {}).get("states") or {}).items():
                tot[k] = tot.get(k, 0) + (v or 0)
        return tot

    def checking(self):
        st, body = http("GET", self.api + "/api/hoard/page?limit=1&state=checking_files", self.key)
        page = body.get("filtered") if st == 200 and isinstance(body, dict) else None
        return max(page or 0, self.states().get("checking", 0))

    def wait_checks(self, timeout, expect_total=None):
        """Until nothing is checking (three polls in a row) and, when given,
        every torrent is in the catalogue. Returns the peak seen."""
        peak = 0
        quiet = 0
        end = time.time() + timeout
        while time.time() < end:
            c = self.checking()
            peak = max(peak, c)
            total = sum((e.get("stats") or {}).get("torrents") or e.get("torrents") or 0
                        for e in self.engines() or [])
            if c == 0 and (expect_total is None or total >= expect_total):
                quiet += 1
                if quiet >= 3:
                    return peak
            else:
                quiet = 0
            time.sleep(2)
        raise SystemExit("checks still running after %ds" % timeout)

    # -- phases

    def bring_up(self):
        self.down()
        log("run %d: tracker up (%s)" % (self.n, self.a.tracker))
        self.trk.up()
        self.mark("interval", interval=self.trk.interval[0], min_interval=self.trk.interval[1])
        log("run %d: payload into the data volume (and a verify)" % self.n)
        t = time.time()
        self.dc("run", "--rm", "loader", capture=True)
        self.r["payload_load_s"] = round(time.time() - t, 1)
        log("run %d: Hydranos first start" % self.n)
        t = time.time()
        self.dc("up", "-d", "hydranos", capture=True)
        self.cid = self.dc.container("hydranos")
        self.sampler = sample.Sampler(self.cid, os.path.join(self.dir, "samples.csv"), self.api, self.trk.stats,
                                      self.a.sample_every)
        self.set_phase("first_start")
        self.sampler.start()
        wait_for(self.startup, 120, 0.5, "Hydranos to answer")
        self.wait_ready(300)
        self.r["first_start_ready_s"] = round(time.time() - t, 2)

    def setup(self):
        code = wait_for(self.setup_code, 60, 1, "the setup code in the container log")
        password = secrets.token_urlsafe(18)
        st, body = http("POST", self.api + "/api/setup",
                        body={"username": "bench", "password": password, "setup_code": code})
        if st != 200 or not isinstance(body, dict) or not body.get("api_key"):
            st, body = http("POST", self.api + "/api/login", body={"username": "bench", "password": password})
        if st != 200 or not body.get("api_key"):
            raise SystemExit("setup failed: %s %s" % (st, body))
        self.key = body["api_key"]
        st, status = http("GET", self.api + "/api/status", self.key)
        self.limit = int((status or {}).get("upload_limit_bytes") or 64_000_000)
        self.r["hydranos_version"] = (status or {}).get("version")
        self.r["upload_limit_bytes"] = self.limit

    def setup_code(self):
        r = self.dc("logs", "--no-color", "hydranos", capture=True, check=False)
        m = re.findall(r"setup code ([A-Z0-9]{5}-[A-Z0-9]{5})", r.stdout + r.stderr)
        return m[-1] if m else None

    def add_all(self):
        """Add the whole catalogue through POST /api/torrents/upload.

        Builds with the batch upload (unreleased after v4.9.2, see CHANGELOG)
        take many files per request; releases up to v4.9.2 keep ONE file per
        request and silently ignore the others. The first torrent goes alone
        with batch=true: a batch-shaped answer ("results") says the build
        batches, a single-torrent answer says it does not, and the rest are
        then sent one per request on --add-concurrency connections."""
        self.set_phase("add")
        cat = os.path.join(self.a.work, "catalogue")
        ts = self.manifest["torrents"]
        blobs = []
        for t in ts:
            with open(os.path.join(cat, t["file"]), "rb") as fh:
                blobs.append((os.path.basename(t["file"]), fh.read()))
        fields = {"mode": "hoard", "savepath": SAVE_PATH, "create_subfolder": "false", "batch": "true"}
        tally = {"added": 0, "present": 0, "refused": 0}
        refused = []
        url = self.api + "/api/torrents/upload"

        def send(group):
            body, ctype = multipart(fields, group)
            return http("POST", url, self.key, body, ctype, timeout=900)

        def count(st, res):
            if st == 200 and isinstance(res, dict) and "results" in res:
                for k in tally:
                    v = res.get(k, 0)
                    tally[k] += v if isinstance(v, int) else len(v)
                refused.extend(x for x in res["results"] if x.get("status") == "refused")
            elif st == 200 and isinstance(res, dict) and res.get("info_hash"):
                tally["added"] += 1
            elif st == 409:
                tally["present"] += 1
            else:
                tally["refused"] += 1
                refused.append({"status": st, "error": str(res)[:300]})

        t0 = time.time()
        st, res = send(blobs[:1])
        batched = st == 200 and isinstance(res, dict) and "results" in res
        count(st, res)
        rest = blobs[1:]
        if batched:
            groups, cur, size = [], [], 0
            cap = min(self.limit // 2, 48 << 20)
            for b in rest:
                if cur and (len(cur) >= self.a.batch or size + len(b[1]) > cap):
                    groups.append(cur)
                    cur, size = [], 0
                cur.append(b)
                size += len(b[1]) + 300
            if cur:
                groups.append(cur)
            log("run %d: batch upload, %d torrents in %d requests" % (self.n, len(ts), len(groups) + 1))
            for g in groups:
                st, res = send(g)
                if st != 200 or not isinstance(res, dict):
                    raise SystemExit("upload refused: %s %s" % (st, str(res)[:500]))
                count(st, res)
            nreq = len(groups) + 1
        else:
            log("run %d: this build adds one .torrent per request; %d requests on %d connections" % (
                self.n, len(ts), self.a.add_concurrency))
            with concurrent.futures.ThreadPoolExecutor(self.a.add_concurrency) as ex:
                for st, res in ex.map(lambda b: send([b]), rest):
                    count(st, res)
            nreq = len(ts)
        self.r["add_s"] = round(time.time() - t0, 2)
        self.r["add_per_s"] = round(len(ts) / max(self.r["add_s"], 0.001), 1)
        self.r["add_mode"] = "batch" if batched else "single, %d connections" % self.a.add_concurrency
        self.r["add_requests"] = nreq
        self.r["add_results"] = tally
        self.r["add_refused_examples"] = refused[:5]
        self.t_add = t0
        if tally["added"] + tally["present"] < len(ts):
            log("run %d: WARNING only %d of %d torrents went in" % (self.n, tally["added"] + tally["present"], len(ts)))

    def check_phase(self):
        self.set_phase("check")
        tl = self.r["add_results"]
        peak = self.wait_checks(self.a.check_timeout, tl["added"] + tl["present"])
        self.r["checks_done_after_add_s"] = round(time.time() - self.t_add, 1)
        self.r["checking_peak"] = peak
        self.r["states_after_check"] = self.states()
        self.r["by_kind"] = self.states_by_kind()
        self.r["mem_after_check"] = self.mem_now()

    def states_by_kind(self):
        st, rows = http("GET", self.api + "/api/engines/hoard/torrents", self.key, timeout=120)
        if st != 200 or not isinstance(rows, list):
            return {"error": "could not list torrents: %s" % st}
        kind = {}
        pids = {p["pid"]: p for p in self.manifest["payloads"]}
        for t in self.manifest["torrents"]:
            f = pids[t["pid"]]["fault"]
            kind[t["info_hash"]] = "cross_seed" if t["cross"] else ("fault_" + f if f else "clean")
        out = {}
        for row in rows:
            h = (row.get("info_hash") or row.get("hash") or "").lower()
            k = kind.get(h, "unknown")
            s = row.get("state") or "?"
            out.setdefault(k, {}).setdefault(s, 0)
            out[k][s] += 1
        return out

    def mem_now(self):
        cg = sample.read_cgroup(self.cid) or {}
        cur = cg.get("current", 0)
        return {"working_set": cur - cg.get("inactive_file", 0), "current": cur, "anon": cg.get("anon")}

    def steady(self):
        self.set_phase("steady")
        a = self.a
        s0 = self.trk.stats()
        t0 = time.time()
        outage = a.outage_for > 0 and self.trk.has_faults
        if outage:
            self.trk.schedule_outage(a.outage_host, a.outage_at, a.outage_for, a.outage_mode)
        log("run %d: steady state for %ds" % (self.n, a.steady))
        time.sleep(a.steady)
        s1 = self.trk.stats()
        dur = time.time() - t0
        rows = self.sampler.rows_in("steady")
        tail = [r for r in rows if r["ts"] >= rows[-1]["ts"] - min(300, a.steady / 2)] if rows else []
        self.r["steady"] = {
            "seconds": round(dur),
            "mem_working_set_median": med([r.get("mem_working_set") for r in rows]),
            "mem_working_set_max": mx([r.get("mem_working_set") for r in rows]),
            "mem_working_set_end": rows[-1].get("mem_working_set") if rows else None,
            "jemalloc_resident_end": rows[-1].get("jemalloc_resident") if rows else None,
            "cpu_cores_mean": mean([r.get("cpu_cores") for r in rows]),
            "cpu_cores_mean_last_5min": mean([r.get("cpu_cores") for r in tail]),
            "announce_needed_per_s": rows[-1].get("announce_needed_per_s") if rows else None,
            "announce_late_end": rows[-1].get("announce_late") if rows else None,
            "tracker": tracker_delta(s0, s1, dur),
            "tracker_rate_last_5min": rate_tail(tail),
            "outage": {"host": a.outage_host, "at_s": a.outage_at, "for_s": a.outage_for,
                       "mode": a.outage_mode} if outage else None,
        }

    def ramp(self):
        """Lower the advertised interval step by step and measure the announce
        rate the tracker receives. Driven from here, on this host's clock: the
        tracker's clock can run slow inside a Docker Desktop VM, and the
        homemade tracker's in-process ramp lost its last step to that."""
        a = self.a
        if not self.trk.can_set_interval:
            log("run %d: no ramp, the %s backend has a fixed interval" % (self.n, self.trk.name))
            self.r["ramp"] = {"skipped": "the %s backend advertises a fixed interval" % self.trk.name}
            return
        self.set_phase("ramp")
        steps = [int(x) for x in a.ramp_steps.split(",")]
        if a.ramp_profile:
            self.trk.set_profile(a.ramp_profile)
        n = len(self.manifest["torrents"])
        restart_s = [self.trk.set_interval(steps[0], steps[0])]
        self.mark("interval", interval=steps[0], min_interval=steps[0])
        # Every torrent announces now, so each picks up the first step's
        # interval instead of waiting out the 30-minute one. Those announces
        # are the runner's doing, not early ones.
        q0 = time.time()
        self.trk.quiet(2 * steps[0])
        # Starts a little early: the log clock and this one differ by up to
        # a second or so on Docker Desktop.
        self.mark("quiet", start=q0 - 5, end=q0 + 2 * steps[0], why="ramp reannounce")
        st, body = http("POST", self.api + "/api/selection/reannounce", self.key,
                        {"selection": {"filter": "", "view": "hoard", "expect": n}, "params": {}})
        bump = (st, body if isinstance(body, dict) else str(body)[:200])
        # A step lasts twice the larger of its interval and the one before:
        # one to let every torrent come back and pick up the new interval,
        # one measured.
        durs, prev = [], steps[0]
        for iv in steps:
            durs.append(2 * max(prev, iv))
            prev = iv
        log("run %d: announce ramp %s, %d s" % (self.n, steps, sum(durs)))
        res = []
        for i, (iv, d) in enumerate(zip(steps, durs)):
            if i:
                restart_s.append(self.trk.set_interval(iv, iv))
                self.mark("interval", interval=iv, min_interval=iv)
            t0, c0 = time.time(), self.trk.total_requests()
            time.sleep(d / 2)
            tm, cm = time.time(), self.trk.total_requests()
            time.sleep(d / 2)
            te, ce = time.time(), self.trk.total_requests()
            self.mark("ramp_step", interval=iv, start=t0, mid=tm, end=te)
            res.append({"interval": iv, "started_s": round(t0 - self.sampler.t0, 1),
                        "rate_whole_step": round((ce - c0) / (te - t0), 2),
                        "rate_second_half": round((ce - cm) / (te - tm), 2),
                        "needed_per_s": round(n / iv, 1), "tracker_restart_s": restart_s[i]})
            log("run %d: ramp step %ds: %.1f announces/s (needed %.1f)" % (
                self.n, iv, res[-1]["rate_second_half"], n / iv))
        self.r["ramp"] = {
            "bump": bump,
            "profile": a.ramp_profile or a.tracker_profile,
            "steps": res,
            "max_sustained_per_s": max((x["rate_second_half"] for x in res), default=None),
        }
        self.trk.set_interval(a.interval, a.interval // 2)
        self.mark("interval", interval=a.interval, min_interval=a.interval // 2)
        self.trk.set_profile(a.tracker_profile)

    def restart(self, kind):
        self.set_phase(kind + "_stop")
        t = time.time()
        if kind == "graceful":
            self.dc("stop", "-t", "150", "hydranos", capture=True)
        else:
            self.dc("kill", "-s", "SIGKILL", "hydranos", capture=True)
        stop_s = round(time.time() - t, 2)
        self.mark("graceful_stop" if kind == "graceful" else "kill", start=t, end=time.time())
        cp = self.trk.checkpoint("graceful_stop" if kind == "graceful" else "kill") or {}
        self.set_phase(kind + "_start")
        t = time.time()
        self.dc("start", "hydranos", capture=True)
        self.wait_ready(1800)
        ready = round(time.time() - t, 2)  # from the start command, container start included
        st = self.startup() or {}
        peak = self.wait_checks(self.a.check_timeout)
        checks_done = round(time.time() - t, 1)
        # Let the first announces after the restart reach the tracker, so it
        # can tell whether they carry event=started.
        time.sleep(self.a.settle)
        self.r[kind + "_restart"] = {
            "stop_s": stop_s,
            "ready_s": ready,
            "restored": st.get("restored"),
            "total": st.get("total"),
            "checks_done_s": checks_done,
            "checking_peak": peak,
            "missing_stopped": sum(cp.get("missing_stopped", {}).values()) if kind == "graceful" else None,
            "tracker_backend_at_stop": cp.get("backend"),
            "states": self.states(),
            "mem_after": self.mem_now(),
        }

    def finish(self):
        self.sampler.stop()
        r = self.dc("logs", "--no-color", "hydranos", capture=True, check=False)
        with open(os.path.join(self.dir, "hydranos.log"), "w", encoding="utf-8") as fh:
            fh.write(r.stdout)
        peak = [x.get("mem_peak") for x in self.sampler.rows if x.get("mem_peak")]
        self.r["mem_peak_cgroup"] = max(peak) if peak else None
        img = docker_out("inspect", "-f", "{{.Image}}", self.cid)
        self.r["image_id"] = img
        hosts, cps, backend = self.trk.finish(self.dir)
        self.r["tracker_hosts"] = hosts
        self.r["tracker_checkpoints"] = cps
        self.r["tracker_backend_final"] = backend
        with open(os.path.join(self.dir, "timeline.json"), "w") as fh:
            json.dump({"clock": "host", "events": self.timeline}, fh, indent=1)
        self.analyse()
        with open(os.path.join(self.dir, "result.json"), "w") as fh:
            json.dump(self.r, fh, indent=1)
        if not self.a.keep:
            self.down()

    def analyse(self):
        """Protocol checks over the nginx log, folded into the result."""
        path = os.path.join(self.dir, "announces.jsonl")
        if self.a.tracker == "homemade" or not os.path.exists(path):
            return
        t = time.time()
        rep = analyse_log.analyse(path, {"events": self.timeline}, self.manifest, self.a.seed,
                                  *self.trk.interval)
        with open(os.path.join(self.dir, "log-analysis.json"), "w") as fh:
            json.dump(rep, fh, indent=1)
        with open(os.path.join(self.dir, "log-analysis.md"), "w", encoding="utf-8") as fh:
            fh.write(analyse_log.table(rep) + "\n")
        for h, s in rep["hosts"].items():
            d = self.r["tracker_hosts"].setdefault(h, {})
            d["violations"] = s["violations"]
            d["status"] = s["status"]
            d["rt_median_ms"], d["rt_p95_ms"] = s["rt_median_ms"], s["rt_p95_ms"]
        self.r["log_analysis"] = {k: rep[k] for k in ("lines", "keys", "span_s", "clock_offset_s",
                                                      "violations_total", "gaps_by_interval", "retry_after_429",
                                                      "checkpoints", "ramp_steps", "outages")}
        self.r["log_analysis_table"] = analyse_log.table(rep)
        for x, y in zip((self.r.get("ramp") or {}).get("steps", []), rep["ramp_steps"]):
            x["observed_gap_median_s"] = y["observed_gap_median_s"]
            x["status_second_half"] = y["status_second_half"]
        log("run %d: log analysis (%d announces) in %.0fs" % (self.n, rep["lines"], time.time() - t))

    def go(self):
        try:
            self.bring_up()
            self.setup()
            self.add_all()
            self.check_phase()
            log("run %d: added in %.1fs, checks done %.1fs after the add started; %s" % (
                self.n, self.r["add_s"], self.r["checks_done_after_add_s"], self.r["states_after_check"]))
            self.steady()
            if self.a.ramp:
                self.ramp()
            self.restart("graceful")
            log("run %d: graceful restart ready in %ss" % (self.n, self.r["graceful_restart"]["ready_s"]))
            self.restart("kill")
            log("run %d: kill -9 restart ready in %ss" % (self.n, self.r["kill_restart"]["ready_s"]))
        finally:
            if hasattr(self, "sampler"):
                self.finish()
            elif not self.a.keep:
                self.down()
        return self.r


# ---------------------------------------------------------------- numbers


def clean(xs):
    return [x for x in xs if isinstance(x, (int, float))]


def med(xs):
    xs = clean(xs)
    return statistics.median(xs) if xs else None


def mean(xs):
    xs = clean(xs)
    return round(statistics.fmean(xs), 3) if xs else None


def mx(xs):
    xs = clean(xs)
    return max(xs) if xs else None


def tracker_delta(s0, s1, dur):
    if not s0 or not s1:
        return None
    out = {}
    for h, b in s1["hosts"].items():
        a = s0["hosts"].get(h, {})
        d = {k: b[k] - a.get(k, 0) for k in ("requests", "ok", "failure_reason", "http_429", "http_5xx",
                                               "resets", "timeouts", "outage_hits")}
        d["per_s"] = round(d["requests"] / dur, 2)
        out[h] = d
    out["all_per_s"] = round(sum(v["requests"] for v in out.values()) / dur, 2)
    return out


def rate_tail(rows):
    rows = [r for r in rows if isinstance(r.get("tracker_requests"), int)]
    if len(rows) < 2 or rows[-1]["ts"] <= rows[0]["ts"]:
        return None
    return round((rows[-1]["tracker_requests"] - rows[0]["tracker_requests"]) / (rows[-1]["ts"] - rows[0]["ts"]), 2)


HEADLINE = [
    ("add_s", "add all torrents, s"),
    ("add_per_s", "adds per second"),
    ("checks_done_after_add_s", "add + hash checks done, s"),
    ("mem_after_check.working_set", "working set after checks, bytes"),
    ("steady.mem_working_set_median", "steady working set median, bytes"),
    ("steady.mem_working_set_max", "steady working set max, bytes"),
    ("steady.jemalloc_resident_end", "jemalloc resident at end of steady, bytes"),
    ("steady.cpu_cores_mean", "steady CPU, cores"),
    ("steady.tracker.all_per_s", "steady announces/s (tracker side)"),
    ("steady.tracker_rate_last_5min", "announces/s, last 5 min of steady"),
    ("ramp.max_sustained_per_s", "ramp: max sustained announces/s"),
    ("graceful_restart.stop_s", "graceful stop, s"),
    ("graceful_restart.missing_stopped", "peers left without event=stopped"),
    ("graceful_restart.ready_s", "graceful restart to ready, s"),
    ("graceful_restart.checking_peak", "torrents rechecked after graceful restart"),
    ("kill_restart.ready_s", "kill -9 restart to ready, s"),
    ("kill_restart.checking_peak", "torrents rechecked after kill -9"),
    ("mem_peak_cgroup", "cgroup memory.peak (includes page cache), bytes"),
]


def dig(d, path):
    for p in path.split("."):
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    return d


def summarize(results):
    out = {}
    for path, label in HEADLINE:
        xs = clean([dig(r, path) for r in results])
        if xs:
            out[path] = {"label": label, "median": statistics.median(xs), "min": min(xs), "max": max(xs),
                         "runs": len(xs)}
    return out


def fmt(v, path):
    if v is None:
        return "-"
    if "bytes" in dict(HEADLINE)[path]:
        return "%.2f GiB" % (v / (1 << 30)) if v > (1 << 29) else "%.0f MiB" % (v / (1 << 20))
    return ("%.2f" % v).rstrip("0").rstrip(".") if isinstance(v, float) else str(v)


def write_md(path, meta, summ, results):
    lines = ["# Hydranos tier 1 scale run", "",
             "%d torrents, seed %d, %d run(s), image `%s`, tracker `%s`." % (
                 meta["torrents"], meta["seed"], len(results), meta["image"], meta.get("tracker")),
             "", "| | median | min | max |", "|---|---|---|---|"]
    for p, s in summ.items():
        lines.append("| %s | %s | %s | %s |" % (s["label"], fmt(s["median"], p), fmt(s["min"], p), fmt(s["max"], p)))
    lines += ["", "Host: %s; Docker %s; kernel %s; envelope %s CPUs, %s memory." % (
        meta["host"]["platform"], meta["docker"]["server_version"], meta["docker"]["kernel"],
        meta["envelope"]["cpus"], meta["envelope"]["mem"]), ""]
    last = results[-1]
    if last.get("by_kind"):
        lines += ["States after the add, by kind of torrent (last run):", "", "```",
                  json.dumps(last["by_kind"], indent=1), "```", ""]
    viol = {h: s.get("violations") for h, s in (last.get("tracker_hosts") or {}).items() if s.get("violations")}
    lines += ["Tracker-side protocol violations (last run, whole run):", "", "```", json.dumps(viol, indent=1), "```"]
    if last.get("log_analysis_table"):
        lines += ["", "From the nginx announce log (last run; analyse_log.py):", "", last["log_analysis_table"]]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------- metadata


def file_hash(*paths):
    h = hashlib.sha256()
    for p in paths:
        with open(p, "rb") as fh:
            h.update(fh.read().replace(b"\r\n", b"\n"))
    return h.hexdigest()


def compose_default(var, override=None):
    if override:
        return override
    with open(COMPOSE, encoding="utf-8") as fh:
        return re.search(r"\$\{%s:-([^}]+)\}" % var, fh.read()).group(1)


TRACKER_IMAGES = {"torrust": ["SCALE_NGINX_IMAGE", "SCALE_TORRUST_IMAGE"],
                  "opentracker": ["SCALE_NGINX_IMAGE", "SCALE_OPENTRACKER_IMAGE"], "homemade": []}


def pull_images(a):
    imgs = [compose_default("HYDRANOS_IMAGE", a.image), compose_default("SCALE_PYTHON_IMAGE")]
    imgs += [compose_default(v) for v in TRACKER_IMAGES[a.tracker]]
    for img in imgs:
        if docker_out("image", "inspect", "-f", "{{.Id}}", img) is None:
            log("pulling " + img)
            subprocess.run(["docker", "pull", "-q", img], check=True)


def metadata(a, manifest):
    info = docker_out("info", "--format", "{{json .}}")
    info = json.loads(info) if info else {}
    image = compose_default("HYDRANOS_IMAGE", a.image)
    digests = docker_out("image", "inspect", "-f", "{{json .RepoDigests}}", image)
    args = {k: v for k, v in vars(a).items() if k not in ("work",)}
    return {
        "date_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "seed": a.seed,
        "torrents": len(manifest["torrents"]),
        "catalogue": {k: manifest[k] for k in ("payload_bytes", "payload_files")},
        "image": image,
        "image_repo_digests": json.loads(digests) if digests else None,
        "tracker": a.tracker,
        "tracker_images": [compose_default(v) for v in TRACKER_IMAGES[a.tracker]],
        "envelope": {"cpus": a.cpus, "mem": a.mem},
        "host": {"platform": platform.platform(), "machine": platform.machine(), "cpus": os.cpu_count(),
                 "python": platform.python_version()},
        "docker": {"server_version": info.get("ServerVersion"), "kernel": info.get("KernelVersion"),
                   "os": info.get("OperatingSystem"), "cgroup": info.get("CgroupVersion"),
                   "ncpu": info.get("NCPU"), "mem_total": info.get("MemTotal"),
                   "storage_driver": info.get("Driver")},
        "config_hash": hashlib.sha256((file_hash(COMPOSE, *[os.path.join(HERE, f) for f in (
                                           "tracker.py", "catalogue.py", "run.py", "trackers.py", "analyse_log.py",
                                           "nginx/faults.js", "nginx/torrust.toml")])
                                       + json.dumps(args, sort_keys=True)
                                       + json.dumps(manifest["params"], sort_keys=True)).encode()).hexdigest(),
        "args": args,
        "catalogue_params": manifest["params"],
    }


# ---------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", required=True, help="work directory (catalogue + results), outside the repo")
    ap.add_argument("--count", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--avg-kib", type=int, default=100)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--steady", type=int, default=1800, help="steady-state seconds (default one interval)")
    ap.add_argument("--interval", type=int, default=1800, help="interval the trackers advertise")
    ap.add_argument("--tracker", default="torrust", choices=["torrust", "opentracker", "homemade"],
                    help="torrust (nginx + Torrust, default), opentracker (nginx + opentracker, no faults, "
                         "fixed interval) or homemade (legacy tracker.py)")
    ap.add_argument("--tracker-profile", default="default", choices=["default", "clean"])
    ap.add_argument("--tracker-config", help="JSON {host: {...}} (or a file) merged over the profile")
    ap.add_argument("--outage-host", default="t6")
    ap.add_argument("--outage-at", type=int, default=300, help="seconds into steady state")
    ap.add_argument("--outage-for", type=int, default=300, help="0 = no outage")
    ap.add_argument("--outage-mode", default="reset", choices=["reset", "timeout", "503"],
                    help="reset = connection closed with no answer; timeout is homemade-only")
    ap.add_argument("--ramp", action="store_true", help="find the max sustained announce rate")
    ap.add_argument("--ramp-steps", help="intervals, s (Hydranos floors at 60; default 180,90,60, --quick 60)")
    ap.add_argument("--ramp-profile", default="clean", help="tracker profile during the ramp ('' = keep)")
    ap.add_argument("--settle", type=int, default=60, help="seconds after each restart before the next step")
    ap.add_argument("--batch", type=int, default=500, help=".torrent files per upload request")
    ap.add_argument("--add-concurrency", type=int, default=4,
                    help="parallel requests when the build adds one .torrent per request")
    ap.add_argument("--check-timeout", type=int, default=3600)
    ap.add_argument("--sample-every", type=float, default=2.0)
    ap.add_argument("--cpus", default=os.environ.get("HYDRANOS_CPUS", "4"))
    ap.add_argument("--mem", default=os.environ.get("HYDRANOS_MEM", "4g"))
    ap.add_argument("--image", default=os.environ.get("HYDRANOS_IMAGE"))
    ap.add_argument("--hydranos-port", type=int, default=18199)
    ap.add_argument("--tracker-port", type=int, default=16969)
    ap.add_argument("--tracker-api-port", type=int, default=16970, help="Torrust REST API / opentracker /stats")
    ap.add_argument("--keep", action="store_true", help="leave the last run's containers and volumes")
    ap.add_argument("--quick", action="store_true",
                    help="smoke test: 300 torrents, 60 s interval, 150 s steady, short outage")
    a = ap.parse_args()
    if a.quick:
        if a.count == 20000:
            a.count = 300
        # A 60 s interval, so the outage and the steady phase see re-announces.
        a.steady, a.outage_at, a.outage_for, a.interval, a.settle = 150, 30, 60, 60, 20
        a.ramp_steps = a.ramp_steps or "60"
    a.ramp_steps = a.ramp_steps or "180,90,60"
    if a.tracker != "homemade" and a.outage_mode == "timeout":
        raise SystemExit("--outage-mode timeout needs --tracker homemade (nginx does reset or 503)")
    if a.tracker == "homemade" and a.tracker_config:
        raise SystemExit("--tracker-config is for the nginx backends; tracker.py takes --config by hand")
    a.work = os.path.abspath(a.work)
    if os.path.commonpath([a.work, HERE]) == HERE:
        raise SystemExit("put --work outside tools/scale (the tracker mounts this folder read-only)")

    cat = os.path.join(a.work, "catalogue")
    mpath = os.path.join(cat, "manifest.json")
    manifest = None
    if os.path.exists(mpath):
        with open(mpath, encoding="utf-8") as fh:
            manifest = json.load(fh)
        p = manifest["params"]
        if p["count"] != a.count or p["seed"] != a.seed or p["total_bytes_target"] != a.count * a.avg_kib * 1024:
            log("catalogue in %s does not match --count/--seed/--avg-kib, regenerating" % cat)
            manifest = None
    if manifest is None:
        log("generating %d torrents (seed %d)" % (a.count, a.seed))
        t = time.time()
        subprocess.run([sys.executable, os.path.join(HERE, "catalogue.py"), "generate", "--out", cat,
                        "--count", str(a.count), "--seed", str(a.seed), "--avg-kib", str(a.avg_kib),
                        "--no-payload"], check=True, stdout=subprocess.DEVNULL)
        log("catalogue generated in %.0fs" % (time.time() - t))
        with open(mpath, encoding="utf-8") as fh:
            manifest = json.load(fh)

    stamp = time.strftime("%Y%m%d-%H%M%S")
    root = os.path.join(a.work, "results", stamp)
    os.makedirs(root, exist_ok=True)
    pull_images(a)
    meta = metadata(a, manifest)
    with open(os.path.join(root, "meta.json"), "w") as fh:
        json.dump(meta, fh, indent=1)
    results = []
    for n in range(1, a.runs + 1):
        results.append(Run(a, n, os.path.join(root, "run-%d" % n), manifest).go())
        summ = summarize(results)
        with open(os.path.join(root, "summary.json"), "w") as fh:
            json.dump({"meta": meta, "summary": summ, "runs": results}, fh, indent=1)
        write_md(os.path.join(root, "summary.md"), meta, summ, results)
    log("done: %s" % os.path.join(root, "summary.md"))
    with open(os.path.join(root, "summary.md"), encoding="utf-8") as fh:
        print(fh.read())


if __name__ == "__main__":
    main()
