#!/usr/bin/env python3
"""Cold-start statistics (PG2 Task 17, design §46 §7.3 and §16).

    coldstart_stats.py --sha SHA [--host HOST] SAMPLES.json [SAMPLES.json ...]
    coldstart_stats.py --self-test

Each SAMPLES.json is one variant's run, as `it_cold_start_samples` writes
it: {"runtime": "compose", "pool": "off", "samples_ms": [...]}, the time from
StartEndpoint on a suspended endpoint to the first SELECT 1 through its
address. Prints one JSON document: per variant, p50, p90 and p99 (nearest
rank, as the WAL benchmark's stats.py), min, max and mean in ms, with §46
§16's targets beside them. Nothing gates on them yet (PG2b exit).
"""
import argparse
import json
import math
import sys
import unittest

# §46 §7.3 and §16: through PgDog, from the first client connection; here,
# from StartEndpoint (PgDog and the waker come with PG2c).
TARGETS = {
    ("kubernetes", "on"): {"p50_ms": 2000, "p99_ms": 5000},
    ("kubernetes", "off"): {"p99_ms": 15000},
    ("compose", "off"): {"p99_ms": 20000},
}


def pct(sorted_ms, q):
    """The nearest-rank `q` quantile of sorted samples; None for none."""
    if not sorted_ms:
        return None
    rank = max(1, math.ceil(q * len(sorted_ms)))
    return round(sorted_ms[rank - 1], 1)


def stats_from_samples(samples_ms):
    """p50, p90 and p99 (nearest rank), min, max and mean, in ms."""
    s = sorted(float(x) for x in samples_ms)
    if not s:
        return {"n": 0}
    return {
        "n": len(s),
        "p50_ms": pct(s, 0.50),
        "p90_ms": pct(s, 0.90),
        "p99_ms": pct(s, 0.99),
        "min_ms": round(s[0], 1),
        "max_ms": round(s[-1], 1),
        "mean_ms": round(sum(s) / len(s), 1),
    }


def variant(doc):
    runtime, pool = doc["runtime"], doc["pool"]
    out = {"runtime": runtime, "pool": pool, **stats_from_samples(doc["samples_ms"])}
    target = TARGETS.get((runtime, pool))
    if target:
        out["target"] = target
        out["meets_target"] = all(
            out.get(k) is not None and out[k] <= v for k, v in target.items()
        )
    out["samples_ms"] = [round(float(x), 1) for x in doc["samples_ms"]]
    return out


class Tests(unittest.TestCase):
    def test_stats_from_samples(self):
        st = stats_from_samples(list(range(100, 0, -1)))
        self.assertEqual(st["n"], 100)
        self.assertEqual(st["p50_ms"], 50.0)
        self.assertEqual(st["p90_ms"], 90.0)
        self.assertEqual(st["p99_ms"], 99.0)
        self.assertEqual((st["min_ms"], st["max_ms"], st["mean_ms"]), (1.0, 100.0, 50.5))
        one = stats_from_samples([1234.56])
        self.assertEqual((one["p50_ms"], one["p99_ms"]), (1234.6, 1234.6))
        self.assertEqual(stats_from_samples([]), {"n": 0})
        # Nearest rank on 50 samples: p99 is the largest, p90 the 45th.
        st = stats_from_samples(range(1, 51))
        self.assertEqual((st["p90_ms"], st["p99_ms"]), (45.0, 50.0))

    def test_variant_checks_its_target(self):
        doc = {"runtime": "compose", "pool": "off", "samples_ms": [900, 1100, 25000]}
        v = variant(doc)
        self.assertEqual(v["target"], {"p99_ms": 20000})
        self.assertFalse(v["meets_target"])
        v = variant({**doc, "samples_ms": [900, 1100]})
        self.assertTrue(v["meets_target"])
        self.assertNotIn("target", variant({**doc, "runtime": "elsewhere"}))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--sha")
    p.add_argument("--host", default="")
    p.add_argument("--date", default="")
    p.add_argument("samples", nargs="*")
    a = p.parse_args()
    if a.self_test:
        r = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(Tests)
        )
        sys.exit(0 if r.wasSuccessful() else 1)
    if not a.sha or not a.samples:
        p.error("--sha and at least one SAMPLES.json")
    runs = []
    for path in a.samples:
        with open(path) as fh:
            runs.append(variant(json.load(fh)))
    print(json.dumps({"date": a.date, "sha": a.sha, "host": a.host, "runs": runs}, indent=2))


if __name__ == "__main__":
    main()
