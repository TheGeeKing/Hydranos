# Scale benchmark, tier 1

This harness runs a Hydranos scale test that you can reproduce yourself, on an ordinary machine, in about an hour. It loads **~20,000 generated torrents** into one Hydranos, points them at **six local tracker hosts that misbehave on purpose** (Torrust Tracker behind nginx), and measures how long the add and the hash checks take, memory and CPU, the announce rate, graceful and `kill -9` restarts, and what the trackers saw.

It is not the 290,000-torrent comparison from the main README and [`docs/scale-290k.md`](../../docs/scale-290k.md). That run is described at the end of this page.

## What is measured, and what is not

Measured, per run:

| Figure | Source |
|---|---|
| Time to add the whole catalogue, adds/s | wall clock around `POST /api/torrents/upload` |
| Time until every hash check is done | `/api/engines` states and `/api/hoard/page?state=checking_files` |
| State of every torrent by kind (clean, cross-seed, missing file, partial, corrupt piece) | `/api/engines/hoard/torrents` against the manifest |
| Working-set memory (`memory.current - inactive_file`), `memory.peak` | the container's cgroup v2 files, read with `docker exec` |
| CPU in cores | `cpu.stat usage_usec` delta over wall time |
| jemalloc allocated/resident, announces ok/failed, announce late/needed | Hydranos `/metrics` |
| Announces/s per tracker host over time; 2xx/429/5xx/resets/failure reasons | the nginx access log (`announces.jsonl`, followed live into `tracker.csv`) |
| Protocol violations: announce before min interval or Retry-After, retries without backoff, first announce without `started`, graceful stop without `stopped`, counters that go backwards or jump, wrong passkey, wrong host | `analyse_log.py` over the nginx log |
| Max sustained announce rate (optional ramp) | the nginx log, as run.py lowers Torrust's advertised interval |
| Gap clients actually leave after each advertised interval | `analyse_log.py` |
| Graceful stop time, restart to ready, rechecks after the restart; the same after `kill -9` | `docker compose stop/kill/start` and `/api/startup` |
| Peers the tracker itself still holds after a graceful stop (cross-check) | Torrust `/api/v1/stats` or opentracker `/stats` |

`docker stats` is not used. It counts the page cache, and after a hash check most of that cache is payload the kernel read on the client's behalf.

Not measured:

- **Transfer.** No real peer exists. Nothing is uploaded or downloaded, so peer-wire code, choking and disk reads for peers are not exercised. The tracker returns an empty peer list.
- **Real tracker latency.** The trackers run on the same Docker network as Hydranos, so the delays are the ones nginx adds (the `default` profile adds 15-250 ms medians). Live trackers on the internet behave differently. **Do not put these numbers next to the main README's live-tracker table.**
- **A tracker's own limits.** Torrust and opentracker answer in microseconds here. The caps and faults are nginx's, not the tracker software's.
- **Other clients.** Only Hydranos is measured for now. See "Adding another client" below.
- **Admission is part of steady state.** Hydranos spreads a catalogue's first announces over one default interval (30 min, by design: `admit_quota` in `typhon-engine/src/hydra/announce/scheduler.rs`). A 30-minute steady phase therefore shows the climb toward `N / interval` announces/s, not a flat line.

## Machine envelope

A "standard machine" here means **4 cores, 16 GiB of RAM, an SSD and Docker** (Linux, or Docker Desktop on Windows/macOS). The Hydranos container is capped at **4 CPUs and 4 GiB of memory, with no swap**, whatever the host has, so two hosts give comparable numbers as long as each can provide that much. Change the caps with `--cpus` / `--mem` (or `HYDRANOS_CPUS` / `HYDRANOS_MEM`). Every result records the caps it ran under.

Disk throttling (`blkio_config` read/write bps and iops) is in `compose.yml`, commented out. It is per device, so name the block device behind Docker's volumes on your host. It needs the cgroup v2 io controller over a real device, which Docker Desktop's VM may not expose.

The default catalogue is ~1.9 GiB of payload (`--avg-kib 100`). The Docker data volume needs that much space, plus a few hundred MiB.

## Running it

You need Python 3.10+ on the host and Docker with compose v2. The harness uses no Python packages beyond the standard library.

```sh
# 1. smoke test: 300 torrents, 60 s interval, about 8 minutes
python tools/scale/run.py --work /path/outside/repo/scale-work --quick

# 2. tier 1: 20,000 torrents, 30 min steady state, announce ramp (~75 min)
python tools/scale/run.py --work /path/outside/repo/scale-work --ramp

# 3. three runs, median and range
python tools/scale/run.py --work /path/outside/repo/scale-work --runs 3

# 4. cross-check against opentracker (no faults, fixed interval, no ramp)
python tools/scale/run.py --work /path/outside/repo/scale-work --tracker opentracker
```

On Windows, give `--work` as a normal path (`C:\bench\scale-work`). The catalogue is generated once per `--count/--seed/--avg-kib` and reused. Each run starts from **fresh volumes** (`docker compose down -v`), and the harness removes its containers and volumes at the end unless you pass `--keep`.

Useful options: `--tracker torrust|opentracker|homemade` (default `torrust`), `--count`, `--seed`, `--steady SECONDS`, `--interval` (advertised to clients, default 1800; Torrust only), `--tracker-profile default|clean`, `--tracker-config '{"t2": {"rate_cap": 5}}'` (JSON or a file, merged over the profile), `--outage-host/--outage-at/--outage-for/--outage-mode reset|503`, `--ramp-steps 180,90,60`, `--settle 60` (seconds after each restart, so the first announces after it reach the tracker), `--batch 500`, `--add-concurrency 4`, `--image REF` (another build). The Hydranos API is published on `127.0.0.1:18199`, nginx (the six tracker hosts) on `127.0.0.1:16969`, and Torrust's REST API or opentracker's `/stats` on `127.0.0.1:16970`. The ports are shifted from the usual ones so they do not collide with a real install.

All images are pinned by digest in `compose.yml` (Torrust and opentracker with the same digests as `tools/interop/run.sh`). `run.py` pulls whatever is missing.

Each piece can also be used by hand:

```sh
python tools/scale/catalogue.py generate --out W/catalogue --count 20000 --seed 1   # add --no-payload to skip writing data
python tools/scale/catalogue.py verify --catalogue W/catalogue                        # re-hash a sample
python tools/scale/analyse_log.py --log RUN/announces.jsonl --timeline RUN/timeline.json \
    --manifest W/catalogue/manifest.json --seed 1 --json out.json                  # re-check a run's log
python tools/scale/trackers.py default                                               # print the nginx.conf a profile gives
python tools/scale/sample.py --container hydranos-scale-hydranos-1 --out s.csv --api http://127.0.0.1:18199
```

### The scenario

1. Generate the catalogue on the host (`.torrent` files and the manifest; no payload).
2. Start the tracker (Torrust, then nginx in front of it). Then run a one-shot `loader` container that regenerates the payload from the seed **directly into the data volume** (so nothing crosses a slow host bind mount), re-hashes a sample with `verify`, and chowns it to uid 1000.
3. Start Hydranos. Read the setup code from its log, `POST /api/setup`, and keep the returned API key.
4. Add everything with `mode=hoard`, `savepath=/data/payload`, over data that already exists, so the add runs a hash check. A build with the batch upload (after v4.9.2; see CHANGELOG "Unreleased") takes 500 files per request, under the 64 MB limit. v4.9.2 and earlier keep **one** file per request, so the runner detects this with its first request and sends one torrent per request on 4 connections. `add_mode` in the results says which path ran.
5. Wait until no torrent is checking. Record states by kind and memory.
6. Steady state (default 1800 s). `t6` goes fully down 300 s in, for 300 s: run.py swaps its outage include and reloads nginx, twice.
7. Optional ramp (Torrust and homemade only): switch nginx to the `clean` profile, set Torrust to the first step's interval (rewrite its config, restart it), reannounce everything once (early-announce violations are not counted for that burst), then advertise 180, 90 and 60 s intervals, one Torrust restart per step. Each step lasts twice the larger of its own interval and the previous one, and is measured over its second half. Steps are timed on the host's clock. The ramp runs before the restarts because admission after a restart would mask it. Hydranos floors intervals at 60 s.
8. Graceful restart (`docker compose stop -t 150`, then start). Time the stop and the restart to `ready`. Peers that never sent `event=stopped` are counted from the log.
9. `kill -9` restart, timed the same way.

## The catalogue

All of it is generated and deterministic: SHAKE-256 over (seed, payload, file, chunk) gives the bytes, so the same seed gives the same info hashes on any machine. There is no real or copyrighted data. The `.torrent` files are valid: canonical bencode with sorted keys, correct SHA-1 piece hashes, and every info hash unique. `verify` checks all of this.

- **Sizes** are log-normal (σ = 2), scaled to the requested total. Most payloads are a few KiB, a few reach tens of MiB, and one payload is capped at 256 MiB.
- **Piece lengths** run from 16 KiB to 4 MiB: mostly sized to the payload, with some doubled or quadrupled, and 5% at 1-4 MiB regardless of size.
- **Packs.** 15% are multi-file torrents with 2-60 files in nested folders. Some names are non-ASCII or ~100 characters long.
- **Private.** 85% are flagged private, with a `source` of `SCALE-Tn`. Their announce URLs come in three forms: plain `/announce`, a passkey path segment (`/<key>/announce`), or `?passkey=`.
- **Cross-seeds.** 10% of the torrents are cross-seeds: the same payload, files and save path, announced to another host. The info hash differs because the `source` field differs. (The announce URL sits outside the info dict, so it alone would not change the hash.) This exercises Hydranos's shared-file handling.
- **Faulty.** 3% are faulty, split evenly between a missing file, a file truncated to half, and one corrupted piece. Faults land only on payloads that have no cross-seed.
- Torrents are spread round-robin over `t1`..`t6` at `http://tN:6969`.

TODO: BitTorrent v2 and hybrid torrents are not generated. The catalogue is v1 only.

## The trackers

By default the tracker is **Torrust Tracker** (public mode; `nginx/torrust.toml`, based on `tools/interop/torrust.toml`) behind **nginx**. One nginx container carries the network aliases `t1`..`t6`, so Hydranos sees six tracker hosts, and has one `server` block per host. nginx strips the `/<passkey>/` path segment and passes everything else to Torrust; Torrust does not check keys. `trackers.py` writes `nginx.conf` and `torrust.toml` into each run's `tracker-conf/`, so a result keeps the exact configuration it ran with.

nginx does the misbehaviour. The `default` profile is the same as the legacy tracker's (`PROFILES` in `tracker.py`, read by `trackers.py`):

| host | behaviour |
|---|---|
| t1 | fast (15 ms median) |
| t2 | 60 ms, capped at 40 announces/s, then 429 + `Retry-After: 60` |
| t3 | 120 ms, 2% 503 + Retry-After, 1% connection closed with no answer |
| t4 | 25 ms, 1% `failure reason: torrent not registered` |
| t5 | 250 ms, capped at 15/s, 429 + `Retry-After: 30` |
| t6 | 40 ms, plus the outage run.py schedules (reset or 503 for the window) |

How, and what nginx cannot do:

- **Rate caps** are `limit_req` per host (`burst` = the cap, `nodelay`), `limit_req_status 429`, and a `Retry-After` header added on 429 and 503 only. The 429 body is nginx's stock HTML page.
- **Latency** comes from njs (`nginx/faults.js`, shipped in the official image): a log-normal delay per request, then a subrequest to the tracker. Plain nginx has no delay directive; `limit_req` without `nodelay` only queues over a rate, which is not a latency distribution. `Math.random` cannot be seeded in njs, so the faults repeat in rate but not in sequence.
- **Random faults** (503, close, failure reason) are drawn in the same njs handler, after the delay, as `tracker.py` did.
- **"Reset"** is `return 444`: nginx closes the connection without answering. The client sees an empty reply (EOF), not a TCP RST.
- **The outage** is an include per host, empty or `return 444;` / `return 503`, flipped by `nginx -s reload`. A reload is graceful, so requests already in flight finish. The legacy `timeout` mode (hold the request 90 s) is not offered.
- **Changing the interval** needs a Torrust restart (it keeps no state the benchmark needs). Announces that arrive during the 1-2 s restart get 502, and Torrust's peer table starts over, so its peer count after the ramp only covers peers that announced since. The log is unaffected.

Torrust's `/api/v1/stats` is read back at each stop (`tracker_backend_at_stop`) and at the end, as a cross-check of the log.

**opentracker** (`--tracker opentracker`) is the baseline: the same nginx and catalogue, with no faults and no outage. Its interval is compiled in (randomised around 1800 s, 1632-1969 s seen, min interval half of it; the log checks use 800 s), so `--interval` and the ramp do not apply and are skipped, and a `--quick` run sees few re-announces.

**homemade** (`--tracker homemade`) is the legacy option: `tracker.py`, one Python process that tells the hosts apart by `Host` header and counts everything itself (`/stats`, `POST /control`). It also has the `timeout` outage mode. The log analysis does not run for it.

### The announce log and its checks

nginx writes one JSON line per request to `announces.jsonl`: time (ms), host, path, status, request time, upstream status, `limit_req` result, the `Retry-After` sent, the injected fault, the outage flag, the added delay, and the announce's `event`, `info_hash`, `peer_id`, `port`, `uploaded`, `downloaded`, `left` and `passkey`, raw. run.py follows it live (the sampler's tracker columns, `tracker.csv`, the graceful-stop count) and `analyse_log.py` checks it after the run, per (host, info_hash, peer_id):

| check | meaning |
|---|---|
| `before_min_interval` | a regular announce sooner than the min interval of the last successful answer (2 s tolerance) |
| `before_retry_after` | an announce (other than `stopped`) sooner than the Retry-After of the 429/503 before it |
| `retry_no_backoff` | 3 failures in a row with no Retry-After (close, 5xx, failure reason) whose gaps do not grow |
| `first_without_started`, `restart_without_started` | the first announce, or the one after `stopped`, is not `started` |
| `missing_stopped` | still active when a graceful stop ended |
| `counters_backwards`, `counters_jump`, `absurd_counters`, `nonzero_transfer` | uploaded/downloaded lower than before in the session, up by over 1 TiB at once, not a number / negative / over 1 PiB, or above 0 with no peer |
| `left_above_size`, `unknown_torrent`, `wrong_host`, `bad_passkey` | against the manifest |

`timeline.json` tells it what the log cannot: which interval was advertised when, the reannounce window that is not counted, the stops, the ramp steps and the outages. A probe request at start and end gives the offset between the host's clock and the containers' log clock. It also reports the gap clients actually left after each advertised interval (`gaps_by_interval`), each outage window's statuses and how soon the host answered 2xx again, and the rate per ramp step.

## Reading the output

`WORK/results/<stamp>/`:

- `summary.md` holds the headline table (median, min, max over runs), the states by kind and the tracker violations. `summary.json` holds the same data plus every run's full result. `meta.json` records the seed, the image reference and its repo digests, host and Docker/kernel versions, the cgroup version, the envelope, the arguments, the catalogue parameters, and a `config_hash` over the harness files, the arguments and the catalogue parameters. Two results are comparable when their `config_hash` and image digest match.
- `run-N/samples.csv` is the time series every 2 s, with a `phase` column (`first_start`, `add`, `check`, `steady`, `ramp`, `graceful_stop`/`_start`, `kill_stop`/`_start`). Use `mem_working_set` and `cpu_cores`. `mem_current` and `mem_peak` include the page cache.
- `run-N/tracker.csv` gives per-host counts every 5 s (`announces_per_s` is that host's rate). `tracker-final.json` holds the final counts and the tracker's own stats.
- `run-N/announces.jsonl` is the nginx log, `log-analysis.json`/`.md` what `analyse_log.py` found in it, `timeline.json` the run's events, `tracker-conf/` the nginx and Torrust configuration, `tracker-containers.log` their output.
- `run-N/result.json` is everything for that run, with `tracker` naming the backend (also in `meta.json`). `hydranos.log` is the container log.

How to read some of the figures:

- **`by_kind`.** Clean torrents and cross-seeds should be `seeding`. Missing, partial and corrupt ones should not be.
- **`missing_stopped`** is the number of peers still active after a graceful stop: their last announce in the log was not `event=stopped`. Hydranos gives its `stopped` announces at most 5 s, so at 20k torrents across slow, rate-capped hosts some are expected.
- **`nonzero_transfer`** should be 0, since no peer exists.

## Adding another client

The client-specific code is all in `run.py`:

| Hydranos step | What another client needs |
|---|---|
| `compose.yml` service `hydranos` | a service with the same caps, `cgroup: private`, the `data` volume at its download path, and the `scale` network |
| `Run.setup` | log in and get credentials |
| `Run.add_all` | add `.torrent` files over existing data at `/data/payload`, with no subfolder for single-file torrents |
| `Run.wait_checks` / `states` / `states_by_kind` | count torrents per state |
| `Run.wait_ready` | "the API answers and the library is loaded" |
| `Sampler` `/metrics` fields | optional. The cgroup and tracker columns work for any client. |

The trackers, the log analysis, the catalogue, the cgroup sampling and the summary do not depend on the client. The cleanest way to add one is a subclass of `Run` plus a `--client` switch. That switch is not built yet. qBittorrent's WebUI API (`/api/v2/torrents/add`), Transmission's RPC (`torrent-add` with `metainfo`) and rtorrent's XML-RPC (`load.raw_start`) all cover the add step.

## Reproducing the 290k run

It is not automated, and this harness does not reproduce it. The published numbers come from one machine with ~32 GiB of RAM, ~293,000 real torrents on six live private trackers, and clients run one after another over several days (rtorrent alone took 15 hours to start). See [`docs/scale-290k.md`](../../docs/scale-290k.md) for the method and caveats.

To get closer with this harness, run `--count 290000 --avg-kib 20 --steady 7200 --mem 32g --cpus 8` on a machine that has the memory. Expect hours for a single client, and keep in mind that local trackers are not live ones.
