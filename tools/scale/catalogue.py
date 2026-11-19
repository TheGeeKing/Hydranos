#!/usr/bin/env python3
"""Seeded, deterministic, legal torrent catalogue for the scale benchmark.

Every payload byte comes from SHAKE-256 over (seed, payload id, file index,
chunk index): nothing real, nothing copyrighted, and the same seed always gives
the same bytes, the same .torrent files and the same info hashes.

    catalogue.py generate --out DIR [--count 20000] [--seed 1] [--no-payload]
    catalogue.py payload  --catalogue DIR --data DATA_DIR
    catalogue.py verify   --catalogue DIR [--data DATA_DIR] [--sample 200]

`generate` writes DIR/manifest.json, DIR/torrents/*.torrent and (unless
--no-payload) the payload files under DIR/data. `payload` writes the payload
files of an existing catalogue somewhere else (the benchmark runs it inside a
container, straight into the Docker volume Hydranos reads). `verify` checks
every .torrent (canonical bencode, info hash, uniqueness, cross-seed pairs) and
re-hashes a sample of payloads against their piece hashes; faulty torrents must
fail the way the manifest says they do.

Python standard library only.
"""
import argparse
import hashlib
import json
import math
import os
import random
import sys

CHUNK = 1 << 20
HOSTS = ["t1", "t2", "t3", "t4", "t5", "t6"]
TRACKER_PORT = 6969
FAULTS = ("missing", "partial", "corrupt")

# ---------------------------------------------------------------- bencode


def bencode(obj):
    out = []
    _enc(obj, out)
    return b"".join(out)


def _enc(obj, out):
    if isinstance(obj, bool):
        raise TypeError("bool is not bencodable")
    if isinstance(obj, int):
        out.append(b"i%de" % obj)
    elif isinstance(obj, (bytes, bytearray)):
        out.append(b"%d:" % len(obj))
        out.append(bytes(obj))
    elif isinstance(obj, str):
        _enc(obj.encode("utf-8"), out)
    elif isinstance(obj, list):
        out.append(b"l")
        for v in obj:
            _enc(v, out)
        out.append(b"e")
    elif isinstance(obj, dict):
        out.append(b"d")
        items = [(k.encode("utf-8") if isinstance(k, str) else k, v) for k, v in obj.items()]
        for k, v in sorted(items, key=lambda kv: kv[0]):
            _enc(k, out)
            _enc(v, out)
        out.append(b"e")
    else:
        raise TypeError("cannot bencode %r" % type(obj))


def bdecode(data):
    """Decode; returns (value, spans) where spans maps top-level dict keys to
    their raw (start, end) byte range, so the info dict is hashed as written."""
    spans = {}

    def dec(i, top):
        c = data[i:i + 1]
        if c == b"i":
            j = data.index(b"e", i)
            return int(data[i + 1:j]), j + 1
        if c == b"l":
            i += 1
            lst = []
            while data[i:i + 1] != b"e":
                v, i = dec(i, False)
                lst.append(v)
            return lst, i + 1
        if c == b"d":
            i += 1
            d = {}
            last = None
            while data[i:i + 1] != b"e":
                k, i = dec(i, False)
                if not isinstance(k, bytes):
                    raise ValueError("dict key is not a string")
                if last is not None and k <= last:
                    raise ValueError("dict keys not sorted/unique: %r" % k)
                last = k
                start = i
                v, i = dec(i, False)
                if top:
                    spans[k] = (start, i)
                d[k] = v
            return d, i + 1
        if c.isdigit():
            j = data.index(b":", i)
            n = int(data[i:j])
            return data[j + 1:j + 1 + n], j + 1 + n
        raise ValueError("bad bencode at %d" % i)

    val, end = dec(0, True)
    if end != len(data):
        raise ValueError("trailing bytes after bencode")
    return val, spans


# ---------------------------------------------------------------- payload bytes


def chunk_bytes(seed, pid, fidx, k, n):
    return hashlib.shake_256(b"hydranos-scale|%d|%d|%d|%d" % (seed, pid, fidx, k)).digest(n)


def file_stream(seed, pid, fidx, size):
    k = 0
    left = size
    while left > 0:
        n = min(CHUNK, left)
        yield chunk_bytes(seed, pid, fidx, k, n)
        left -= n
        k += 1


def piece_hashes(seed, payload):
    """SHA-1 of every piece over the concatenated files, as the torrent wants."""
    plen = payload["piece_length"]
    out = []
    buf = bytearray()
    for fidx, f in enumerate(payload["files"]):
        for b in file_stream(seed, payload["pid"], fidx, f["length"]):
            buf += b
            while len(buf) >= plen:
                out.append(hashlib.sha1(buf[:plen]).digest())
                del buf[:plen]
    if buf:
        out.append(hashlib.sha1(buf).digest())
    return b"".join(out)


# ---------------------------------------------------------------- catalogue shape

WORDS = ["alpha", "bravo", "cobalt", "delta", "ember", "falcon", "garnet", "harbor",
         "indigo", "juniper", "kestrel", "lumen", "meadow", "nimbus", "onyx", "pylon",
         "quartz", "raven", "sierra", "tundra", "umber", "vortex", "willow", "xenon",
         "yarrow", "zephyr"]
UNICODE = ["Été", "Ñandú", "日本語", "Ελληνικά", "Русский", "한국어", "café", "naïve",
           "Zürich", "Øresund", "İstanbul", "中文字幕"]
FOLDERS = ["Season 01", "Season 02", "extras", "Sub", "Disc 1", "Disc 2", "Featurettes",
           "サンプル", "Bonus Material", "scans"]
EXTS = [".bin", ".mkv", ".flac", ".epub", ".iso", ".dat", ".nfo", ".txt"]


def pick_name(rng, pid):
    words = [rng.choice(WORDS) for _ in range(rng.randint(2, 4))]
    r = rng.random()
    if r < 0.08:
        words.insert(1, rng.choice(UNICODE))
    elif r < 0.12:
        # A long name: about 100 characters, still far under 255 bytes.
        while len(".".join(words)) < 95:
            words.append(rng.choice(WORDS))
    return "scale-%06d.%s" % (pid, ".".join(words))


def piece_length_for(rng, size):
    target = max(1, size // 1000)
    plen = 1 << max(14, math.ceil(math.log2(target)) if target > 1 else 14)
    r = rng.random()
    if r < 0.05:
        plen = 1 << rng.randint(20, 22)          # 1-4 MiB whatever the size
    elif r < 0.25:
        plen <<= 1
    elif r < 0.35:
        plen <<= 2
    return max(1 << 14, min(plen, 1 << 22))


def split_pack(rng, name, total):
    nfiles = min(60, 2 + int(rng.lognormvariate(1.3, 0.9)))
    weights = [rng.lognormvariate(0, 1.0) for _ in range(nfiles)]
    s = sum(weights)
    files = []
    used = set()
    for i, w in enumerate(weights):
        length = max(1, int(total * w / s))
        depth = rng.choice([0, 0, 1, 1, 2, 3])
        folders = [rng.choice(FOLDERS) for _ in range(depth)]
        base = "%02d.%s" % (i + 1, rng.choice(WORDS))
        if rng.random() < 0.06:
            base += "." + rng.choice(UNICODE)
        path = folders + [base + rng.choice(EXTS)]
        key = "/".join(path).lower()
        if key in used:
            path[-1] = "%02d.dup%s" % (i + 1, path[-1])
        used.add("/".join(path).lower())
        files.append({"path": path, "length": length})
    return files


def build_shape(count, seed, total_bytes, private_share, cross_share, faulty_share,
                pack_share, max_size):
    rng = random.Random(seed)
    n_cross = int(round(count * cross_share))
    n_orig = count - n_cross
    raw = [min(rng.lognormvariate(0.0, 2.0), 3000.0) for _ in range(n_orig)]
    factor = total_bytes / sum(raw)
    sizes = [max(1024, min(max_size, int(r * factor))) for r in raw]

    payloads = []
    for pid in range(n_orig):
        name = pick_name(rng, pid)
        size = sizes[pid]
        pack = rng.random() < pack_share
        files = split_pack(rng, name, size) if pack else [{"path": [name], "length": size}]
        total = sum(f["length"] for f in files)
        payloads.append({
            "pid": pid,
            "name": name,
            "pack": pack,
            "files": files,
            "length": total,
            "piece_length": piece_length_for(rng, total),
            "fault": None,
        })

    # Faults land on originals that get no cross-seed: a clean pair stays clean.
    n_fault = int(round(count * faulty_share))
    order = list(range(n_orig))
    rng.shuffle(order)
    faulty = order[:n_fault]
    for k, pid in enumerate(faulty):
        payloads[pid]["fault"] = FAULTS[k % len(FAULTS)]
    clean = order[n_fault:]
    if n_cross > len(clean):
        raise SystemExit("too many cross-seeds for the clean payloads")
    cross_of = clean[:n_cross]

    torrents = []
    for pid in range(n_orig):
        host = HOSTS[pid % len(HOSTS)]
        torrents.append({"pid": pid, "host": host, "cross": False,
                         "private": rng.random() < private_share})
    for pid in sorted(cross_of):
        own = pid % len(HOSTS)
        host = HOSTS[(own + 1 + rng.randrange(len(HOSTS) - 1)) % len(HOSTS)]
        torrents.append({"pid": pid, "host": host, "cross": True, "private": True})
    for idx, t in enumerate(torrents):
        t["idx"] = idx
        # idx // 6, not idx: hosts go round-robin on idx, so every host gets all three forms.
        t["variant"] = ("plain", "path", "query")[(idx // 6) % 3] if t["private"] else "plain"
    return payloads, torrents


def passkey(seed, host):
    return hashlib.sha1(b"passkey|%d|%s" % (seed, host.encode())).hexdigest()[:32]


def announce_url(seed, t):
    base = "http://%s:%d" % (t["host"], TRACKER_PORT)
    if t["variant"] == "path":
        return "%s/%s/announce" % (base, passkey(seed, t["host"]))
    if t["variant"] == "query":
        return "%s/announce?passkey=%s" % (base, passkey(seed, t["host"]))
    return base + "/announce"


def info_dict(payload, pieces, t):
    info = {"name": payload["name"], "piece length": payload["piece_length"], "pieces": pieces}
    if payload["pack"]:
        info["files"] = [{"length": f["length"], "path": f["path"]} for f in payload["files"]]
    else:
        info["length"] = payload["files"][0]["length"]
    if t["private"]:
        info["private"] = 1
        # The `source` field is what cross-seed tooling relies on: the announce
        # URL sits outside the info dict, so without it a cross-seed would have
        # the SAME info hash as its original.
        info["source"] = "SCALE-" + t["host"].upper()
    return info


# ---------------------------------------------------------------- writing payloads


def long_path(p):
    p = os.path.abspath(p)
    if os.name == "nt" and not p.startswith("\\\\?\\"):
        return "\\\\?\\" + p
    return p


def payload_paths(data_root, payload):
    if payload["pack"]:
        return [os.path.join(data_root, payload["name"], *f["path"]) for f in payload["files"]]
    return [os.path.join(data_root, payload["name"])]


def write_payload(seed, data_root, payload):
    """Write one payload, faults included. Returns bytes written."""
    paths = payload_paths(data_root, payload)
    fault = payload["fault"]
    written = 0
    for fidx, (f, path) in enumerate(zip(payload["files"], paths)):
        if fault == "missing" and fidx == len(paths) - 1:
            continue
        size = f["length"]
        if fault == "partial" and fidx == 0:
            size = size // 2
        os.makedirs(long_path(os.path.dirname(path)), exist_ok=True)
        with open(long_path(path), "wb") as fh:
            left = size
            for b in file_stream(seed, payload["pid"], fidx, f["length"]):
                if left <= 0:
                    break
                part = b[:left]
                fh.write(part)
                left -= len(part)
                written += len(part)
    if fault == "corrupt":
        # Flip one byte in the middle piece of the concatenated stream.
        plen = payload["piece_length"]
        npieces = (payload["length"] + plen - 1) // plen
        target = (npieces // 2) * plen
        for f, path in zip(payload["files"], paths):
            if target < f["length"]:
                with open(long_path(path), "r+b") as fh:
                    fh.seek(target)
                    b = fh.read(1)
                    fh.seek(target)
                    fh.write(bytes([b[0] ^ 0xFF]))
                break
            target -= f["length"]
    return written


# ---------------------------------------------------------------- commands


def cmd_generate(a):
    total = int(a.total_gib * (1 << 30)) if a.total_gib else a.count * a.avg_kib * 1024
    payloads, torrents = build_shape(a.count, a.seed, total, a.private_share, a.cross_share,
                                     a.faulty_share, a.pack_share, a.max_mib << 20)
    os.makedirs(os.path.join(a.out, "torrents"), exist_ok=True)
    data_root = os.path.join(a.out, "data")
    by_pid = {p["pid"]: p for p in payloads}
    pieces_of = {}
    seen = {}
    for n, p in enumerate(payloads):
        pieces_of[p["pid"]] = piece_hashes(a.seed, p)
        if not a.no_payload:
            write_payload(a.seed, data_root, p)
        if (n + 1) % 2000 == 0:
            print("  %d/%d payloads hashed" % (n + 1, len(payloads)), file=sys.stderr)
    for t in torrents:
        p = by_pid[t["pid"]]
        info = info_dict(p, pieces_of[p["pid"]], t)
        meta = {
            "announce": announce_url(a.seed, t),
            "created by": "hydranos-scale catalogue.py",
            "creation date": 1735689600 + t["idx"],
            "comment": "synthetic benchmark payload, seed %d" % a.seed,
            "info": info,
        }
        raw = bencode(meta)
        ih = hashlib.sha1(bencode(info)).hexdigest()
        if ih in seen:
            raise SystemExit("info hash collision: %d and %d" % (seen[ih], t["idx"]))
        seen[ih] = t["idx"]
        t["info_hash"] = ih
        t["announce"] = meta["announce"]
        t["file"] = "torrents/%06d.torrent" % t["idx"]
        with open(os.path.join(a.out, t["file"]), "wb") as fh:
            fh.write(raw)
    params = {k: getattr(a, k) for k in ("count", "seed", "private_share", "cross_share",
                                         "faulty_share", "pack_share", "max_mib")}
    params["total_bytes_target"] = total
    manifest = {
        "version": 1,
        "params": params,
        "save_path": "/data/payload",
        "payload_bytes": sum(p["length"] for p in payloads),
        "payload_files": sum(len(p["files"]) for p in payloads),
        "payloads": payloads,
        "torrents": torrents,
    }
    with open(os.path.join(a.out, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, separators=(",", ":"))
    kinds = summarize(manifest)
    print(json.dumps(kinds, indent=1))


def summarize(m):
    ts = m["torrents"]
    by_pid = {p["pid"]: p for p in m["payloads"]}
    plens = sorted({p["piece_length"] for p in m["payloads"]})
    return {
        "torrents": len(ts),
        "payloads": len(m["payloads"]),
        "payload_bytes": m["payload_bytes"],
        "payload_files": m["payload_files"],
        "packs": sum(1 for p in m["payloads"] if p["pack"]),
        "private": sum(1 for t in ts if t["private"]),
        "cross_seeds": sum(1 for t in ts if t["cross"]),
        "faulty": {f: sum(1 for p in m["payloads"] if p["fault"] == f) for f in FAULTS},
        "per_host": {h: sum(1 for t in ts if t["host"] == h) for h in HOSTS},
        "piece_lengths": plens,
        "largest_payload": max(p["length"] for p in by_pid.values()),
    }


def cmd_payload(a):
    m = load_manifest(a.catalogue)
    seed = m["params"]["seed"]
    total = 0
    for n, p in enumerate(m["payloads"]):
        total += write_payload(seed, a.data, p)
        if (n + 1) % 2000 == 0:
            print("  %d/%d payloads written" % (n + 1, len(m["payloads"])), file=sys.stderr)
    print("wrote %d payloads, %d bytes, under %s" % (len(m["payloads"]), total, a.data))


def load_manifest(d):
    with open(os.path.join(d, "manifest.json"), encoding="utf-8") as fh:
        return json.load(fh)


def rehash_disk(data_root, payload):
    """Hash the payload as it is on disk. Returns (piece hashes, problem)."""
    plen = payload["piece_length"]
    out = []
    buf = bytearray()
    for f, path in zip(payload["files"], payload_paths(data_root, payload)):
        try:
            with open(long_path(path), "rb") as fh:
                got = 0
                while True:
                    b = fh.read(CHUNK)
                    if not b:
                        break
                    got += len(b)
                    buf += b
                    while len(buf) >= plen:
                        out.append(hashlib.sha1(buf[:plen]).digest())
                        del buf[:plen]
        except FileNotFoundError:
            return None, "missing"
        if got != f["length"]:
            return None, "partial"
    if buf:
        out.append(hashlib.sha1(buf).digest())
    return b"".join(out), None


def cmd_verify(a):
    m = load_manifest(a.catalogue)
    problems = []
    infos = {}
    hashes = {}
    for t in m["torrents"]:
        with open(os.path.join(a.catalogue, t["file"]), "rb") as fh:
            raw = fh.read()
        try:
            meta, spans = bdecode(raw)
        except ValueError as e:
            problems.append("%s: %s" % (t["file"], e))
            continue
        if bencode(meta) != raw:
            problems.append("%s: not canonical bencode" % t["file"])
        s, e = spans[b"info"]
        ih = hashlib.sha1(raw[s:e]).hexdigest()
        if ih != t["info_hash"]:
            problems.append("%s: info hash %s, manifest says %s" % (t["file"], ih, t["info_hash"]))
        if meta[b"announce"].decode() != t["announce"]:
            problems.append("%s: announce differs from manifest" % t["file"])
        if ih in hashes:
            problems.append("duplicate info hash %s (%s, %s)" % (ih, hashes[ih], t["file"]))
        hashes[ih] = t["file"]
        infos[t["idx"]] = meta[b"info"]

    # Cross-seeds: same payload as their original, and still a distinct torrent.
    by_pid = {}
    for t in m["torrents"]:
        by_pid.setdefault(t["pid"], []).append(t)
    pairs = 0
    for pid, ts in by_pid.items():
        if len(ts) < 2:
            continue
        # The payload fields must match; `source` and `private` may differ (a
        # public original cross-seeded to a private tracker).
        skip = (b"source", b"private")
        base = {k: v for k, v in infos[ts[0]["idx"]].items() if k not in skip}
        for t in ts[1:]:
            pairs += 1
            other = {k: v for k, v in infos[t["idx"]].items() if k not in skip}
            if other != base:
                problems.append("cross-seed %d: payload differs from torrent %d" % (t["idx"], ts[0]["idx"]))
            if t["info_hash"] == ts[0]["info_hash"]:
                problems.append("cross-seed %d shares its original's info hash" % t["idx"])
            if t["host"] == ts[0]["host"]:
                problems.append("cross-seed %d on its original's tracker" % t["idx"])

    checked = {"clean_ok": 0, "fault_as_expected": 0}
    if a.data:
        rng = random.Random(a.sample_seed)
        pool = m["payloads"]
        faulty = [p for p in pool if p["fault"]]
        sample = rng.sample(pool, min(a.sample, len(pool)))
        # Every fault kind gets looked at, whatever the sample drew.
        for f in FAULTS:
            sample += [p for p in faulty if p["fault"] == f][:2]
        torrent_of = {t["pid"]: t for t in m["torrents"] if not t["cross"]}
        seen = set()
        for p in sample:
            if p["pid"] in seen:
                continue
            seen.add(p["pid"])
            want = infos[torrent_of[p["pid"]]["idx"]][b"pieces"]
            got, why = rehash_disk(a.data, p)
            if p["fault"] is None:
                if why or got != want:
                    problems.append("payload %d (%s): %s" % (p["pid"], p["name"], why or "piece hash mismatch"))
                else:
                    checked["clean_ok"] += 1
            else:
                bad_pieces = None
                if got is not None:
                    bad_pieces = sum(1 for i in range(0, len(want), 20) if got[i:i + 20] != want[i:i + 20])
                ok = (p["fault"] == why) or (p["fault"] == "corrupt" and bad_pieces == 1)
                if ok:
                    checked["fault_as_expected"] += 1
                else:
                    problems.append("payload %d: fault %s, found %s / %s bad pieces" % (p["pid"], p["fault"], why, bad_pieces))
    report = {
        "torrents_checked": len(infos),
        "unique_info_hashes": len(hashes),
        "cross_seed_pairs": pairs,
        "payloads_rehashed": checked,
        "problems": problems[:50],
        "problem_count": len(problems),
    }
    print(json.dumps(report, indent=1, ensure_ascii=False))
    sys.exit(1 if problems else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="build a catalogue")
    g.add_argument("--out", required=True)
    g.add_argument("--count", type=int, default=20000)
    g.add_argument("--seed", type=int, default=1)
    g.add_argument("--avg-kib", type=int, default=100, help="mean payload size (default 100 KiB: ~1.9 GiB for 20k)")
    g.add_argument("--total-gib", type=float, default=0, help="total payload size; overrides --avg-kib")
    g.add_argument("--max-mib", type=int, default=256, help="cap on one payload")
    g.add_argument("--private-share", type=float, default=0.85)
    g.add_argument("--cross-share", type=float, default=0.10)
    g.add_argument("--faulty-share", type=float, default=0.03)
    g.add_argument("--pack-share", type=float, default=0.15)
    g.add_argument("--no-payload", action="store_true", help="write only .torrent files and the manifest")
    p = sub.add_parser("payload", help="write the payload files of a catalogue")
    p.add_argument("--catalogue", required=True)
    p.add_argument("--data", required=True)
    v = sub.add_parser("verify", help="check .torrent files and re-hash a sample of payloads")
    v.add_argument("--catalogue", required=True)
    v.add_argument("--data", help="payload root (default: CATALOGUE/data when it exists)")
    v.add_argument("--sample", type=int, default=200)
    v.add_argument("--sample-seed", type=int, default=7)
    a = ap.parse_args()
    if a.cmd == "verify" and not a.data and os.path.isdir(os.path.join(a.catalogue, "data")):
        a.data = os.path.join(a.catalogue, "data")
    {"generate": cmd_generate, "payload": cmd_payload, "verify": cmd_verify}[a.cmd](a)


if __name__ == "__main__":
    main()
