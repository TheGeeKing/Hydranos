// Per-host latency and random faults for the announce path (njs, shipped in
// the official nginx image). nginx.conf, written by trackers.py, sets these
// per server block:
//
//   $lat_median  added delay, median in ms (0 = none)
//   $lat_sigma   log-normal sigma of that delay
//   $p_503 $p_reset $p_failure   share of announces answered 503 (+ Retry-After),
//                closed with no answer (444), or 200 with a bencoded
//                "failure reason"
//
// The rate cap (limit_req, 429 + Retry-After) and the outage window run in
// nginx itself before this handler, so a capped or down host answers at once.
// The draw order matches tracker.py: delay first, then the fault. Math.random
// is not seedable in njs, so faults repeat in rate, not in sequence.

function gauss() {
    var u = 0;
    while (u === 0) {
        u = Math.random();
    }
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * Math.random());
}

function num(r, name) {
    var v = r.variables[name];
    return v ? parseFloat(v) : 0;
}

function announce(r) {
    var med = num(r, 'lat_median');
    var ms = med > 0 ? Math.min(med * Math.exp(num(r, 'lat_sigma') * gauss()), 30000) : 0;
    var x = Math.random();
    var p1 = num(r, 'p_503');
    var p2 = p1 + num(r, 'p_reset');
    var p3 = p2 + num(r, 'p_failure');

    // The query goes along with an internal redirect, so the log line of a
    // faulted announce still carries its info_hash, event and counters.
    var args = r.variables.args || '';
    var q = args ? '?' + args : '';

    function go() {
        if (x < p1) {
            return r.internalRedirect('/_f503' + q);
        }
        if (x < p2) {
            return r.internalRedirect('/_freset' + q);
        }
        if (x < p3) {
            return r.internalRedirect('/_ffail' + q);
        }
        r.subrequest('/_up', { args: args }, function (rep) {
            r.variables.up_status = String(rep.status);
            var ct = rep.headersOut['Content-Type'];
            if (ct) {
                r.headersOut['Content-Type'] = ct;
            }
            r.return(rep.status, rep.responseBuffer || '');
        });
    }

    r.variables.added_ms = String(Math.round(ms));
    if (ms >= 1) {
        setTimeout(go, ms);
    } else {
        go();
    }
}

export default { announce };
