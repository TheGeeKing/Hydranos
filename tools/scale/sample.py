#!/usr/bin/env python3
"""Sample one container's real resource use, Hydranos's own figures and the
tracker's counters into a CSV time series.

Memory and CPU come from the container's cgroup v2 files, read through
`docker exec`, never from `docker stats`: `docker stats` counts the page cache,
which at this size is mostly the payload the hash check just read and says
nothing about the client. Working set = memory.current - inactive_file, the
figure the kernel would not give back without swapping. CPU cores = the delta
of cpu.stat usage_usec over the wall-clock delta.

    sample.py --container hydranos-scale-hydranos-1 --out samples.csv \
              [--api http://127.0.0.1:8199] [--tracker http://127.0.0.1:6969] \
              [--every 2] [--duration 600]

run.py imports Sampler and sets `phase` as the scenario moves on.
"""
import argparse
import csv
import json
import subprocess
import threading
import time
import urllib.request

FIELDS = [
    "ts", "elapsed_s", "phase",
    "mem_current", "mem_working_set", "mem_anon", "mem_file", "mem_inactive_file", "mem_peak",
    "cpu_cores", "cpu_throttled_ms",
    "ready", "startup_phase", "restored", "total",
    "torrents", "seeding", "checking", "downloading", "stopped", "error", "queued",
    "announces_ok", "announces_failed", "announce_in_flight", "announce_late",
    "announce_needed_per_s", "jemalloc_allocated", "jemalloc_resident",
    "tracker_requests", "tracker_ok", "tracker_429", "tracker_5xx", "tracker_resets",
]

CGROUP_SH = ("cd /sys/fs/cgroup && echo current $(cat memory.current) && "
             "echo peak $(cat memory.peak 2>/dev/null || echo 0) && cat memory.stat cpu.stat")


def read_cgroup(container):
    """memory.current, memory.peak, memory.stat and cpu.stat as one dict."""
    try:
        out = subprocess.run(["docker", "exec", container, "sh", "-c", CGROUP_SH],
                             capture_output=True, text=True, timeout=20)
    except (subprocess.TimeoutExpired, OSError):
        return None  # the container is stopping or restarting
    if out.returncode != 0:
        return None
    return parse_cgroup(out.stdout.splitlines())


def parse_cgroup(lines):
    d = {}
    for line in lines:
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            d[parts[0]] = int(parts[1])
    return d


class CgroupStream:
    """One long-lived `docker exec` that prints the cgroup files every
    `every` seconds, so each sample does not pay for a new exec (one exec
    costs 1-6 s on Docker Desktop). The loop's own `cat`s run inside the
    container's cgroup; at one round every few seconds that is noise.
    Reconnects when the container restarts."""

    def __init__(self, container, every):
        self.container = container
        self.every = every
        self.latest = None  # (host time, dict)
        self._stop = threading.Event()
        self._proc = None
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        script = ("while :; do echo now $(date +%%s%%N); %s; echo ---; sleep %s; done"
                  % (CGROUP_SH, self.every))
        while not self._stop.is_set():
            try:
                self._proc = subprocess.Popen(["docker", "exec", self.container, "sh", "-c", script],
                                              stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                buf = []
                for line in self._proc.stdout:
                    if line.startswith("---"):
                        self.latest = (time.time(), parse_cgroup(buf))
                        buf = []
                    else:
                        buf.append(line)
                self._proc.wait()
            except OSError:
                pass
            self._stop.wait(1.0)

    def fresh(self, max_age):
        if self.latest and time.time() - self.latest[0] <= max_age:
            return self.latest[1]
        return None

    def close(self):
        self._stop.set()
        if self._proc and self._proc.poll() is None:
            self._proc.kill()


def get(url, key=None, timeout=10):
    req = urllib.request.Request(url, headers={"X-Api-Key": key} if key else {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def get_json(url, key=None, timeout=10):
    try:
        return json.loads(get(url, key, timeout))
    except (OSError, ValueError):
        return None


def parse_metrics(text):
    """Prometheus text -> {name: [(labels dict, value)]}."""
    out = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        try:
            head, value = line.rsplit(" ", 1)
            v = float(value)
        except ValueError:
            continue
        labels = {}
        name = head
        if "{" in head:
            name, rest = head.split("{", 1)
            for part in rest.rstrip("}").split(","):
                if "=" in part:
                    k, _, val = part.partition("=")
                    labels[k.strip()] = val.strip().strip('"')
        out.setdefault(name, []).append((labels, v))
    return out


def msum(m, name, **match):
    return sum(v for labels, v in m.get(name, []) if all(labels.get(k) == x for k, x in match.items()))


class Sampler:
    def __init__(self, container, out, api, tracker=None, every=2.0):
        self.container = container
        self.api = api.rstrip("/")
        self.tracker = tracker.rstrip("/") if isinstance(tracker, str) else tracker
        self.every = every
        self.phase = "start"
        self.out = out
        self.rows = []
        self._stop = threading.Event()
        self._prev = None
        self.t0 = time.time()
        self.cg = CgroupStream(container, max(1.0, every))

    def sample(self):
        now = time.time()
        row = {"ts": round(now, 2), "elapsed_s": round(now - self.t0, 1), "phase": self.phase}
        cg = self.cg.fresh(3 * self.every + 2)
        if cg:
            cur = cg.get("current", 0)
            inact = cg.get("inactive_file", 0)
            row.update(mem_current=cur, mem_working_set=max(0, cur - inact), mem_anon=cg.get("anon"),
                       mem_file=cg.get("file"), mem_inactive_file=inact, mem_peak=cg.get("peak"))
            usage = cg.get("usage_usec")
            thr = cg.get("throttled_usec", 0)
            when = cg.get("now", 0) / 1e9  # the container's clock, taken with the files
            if self._prev and usage is not None and usage >= self._prev[1] and when > self._prev[0]:
                dt = when - self._prev[0]
                row["cpu_cores"] = round((usage - self._prev[1]) / 1e6 / dt, 3)
                row["cpu_throttled_ms"] = round((thr - self._prev[2]) / 1000, 1)
            if not self._prev or when > self._prev[0]:
                self._prev = (when, usage, thr) if usage is not None else None
        else:
            self._prev = None
        st = get_json(self.api + "/api/startup", timeout=5)
        if st:
            row.update(ready=int(bool(st.get("ready"))), startup_phase=st.get("phase"),
                       restored=st.get("restored"), total=st.get("total"))
        try:
            m = parse_metrics(get(self.api + "/metrics", timeout=10).decode("utf-8", "replace"))
        except OSError:
            m = None
        if m:
            row.update(
                torrents=msum(m, "hydra_torrents"),
                seeding=msum(m, "hydra_torrents_by_state", state="seeding"),
                checking=msum(m, "hydra_torrents_by_state", state="checking"),
                downloading=msum(m, "hydra_torrents_by_state", state="downloading"),
                stopped=msum(m, "hydra_torrents_by_state", state="stopped"),
                error=msum(m, "hydra_torrents_by_state", state="error"),
                queued=msum(m, "hydra_torrents_by_state", state="queued"),
                announces_ok=msum(m, "hydra_announces_total", result="ok"),
                announces_failed=msum(m, "hydra_announces_total", result="failed"),
                announce_in_flight=msum(m, "hydra_announce_in_flight"),
                announce_late=msum(m, "hydra_announce_late"),
                announce_needed_per_s=round(msum(m, "hydra_announce_needed_per_second"), 2),
                jemalloc_allocated=msum(m, "hydra_memory_allocated_bytes"),
                jemalloc_resident=msum(m, "hydra_memory_resident_bytes"),
            )
        if self.tracker:
            # A URL serving tracker.py's /stats, or a callable returning the
            # same shape (run.py's nginx log tail).
            ts = self.tracker() if callable(self.tracker) else get_json(self.tracker + "/stats", timeout=5)
            if ts:
                t = ts["totals"]
                row.update(tracker_requests=t.get("requests"), tracker_ok=t.get("ok"),
                           tracker_429=t.get("http_429"), tracker_5xx=t.get("http_5xx"),
                           tracker_resets=t.get("resets"))
        return row

    def run(self, duration=None):
        with open(self.out, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
            if fh.tell() == 0:
                w.writeheader()
            end = time.time() + duration if duration else None
            while not self._stop.is_set() and (end is None or time.time() < end):
                t = time.time()
                try:
                    row = self.sample()
                except Exception as e:  # one bad sample must not end the series
                    row = {"ts": round(t, 2), "elapsed_s": round(t - self.t0, 1), "phase": self.phase,
                           "startup_phase": "sample error: %s" % type(e).__name__}
                self.rows.append(row)
                w.writerow(row)
                fh.flush()
                self._stop.wait(max(0.0, self.every - (time.time() - t)))

    def start(self):
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self.cg.close()
        self._thread.join(timeout=30)

    def rows_in(self, phase):
        return [r for r in self.rows if r.get("phase") == phase]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--container", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--api", default="http://127.0.0.1:8199")
    ap.add_argument("--tracker", default="http://127.0.0.1:6969")
    ap.add_argument("--every", type=float, default=2.0)
    ap.add_argument("--duration", type=float, help="seconds (default: until Ctrl-C)")
    ap.add_argument("--phase", default="manual")
    a = ap.parse_args()
    s = Sampler(a.container, a.out, a.api, a.tracker, a.every)
    s.phase = a.phase
    try:
        s.run(a.duration)
    except KeyboardInterrupt:
        pass
    finally:
        s.cg.close()


if __name__ == "__main__":
    main()
