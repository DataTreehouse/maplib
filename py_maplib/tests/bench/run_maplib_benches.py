"""Runs the maplib_benches benchmark suite against the maplib checked out here, and compares runs.

maplib_benches is a separate repository (about 4 GB with its test data). It is not copied into
maplib: this script runs it from its own checkout with the Python environment that runs this
script, so it measures the maplib installed there (``maturin develop --release``).

    python run_maplib_benches.py run --tier quick --label main
    python run_maplib_benches.py run --tier full --scale 4 --label rdf12-ids
    python run_maplib_benches.py compare main rdf12-ids
    python run_maplib_benches.py tiers

The checkout is found from --benches-dir, else the MAPLIB_BENCHES environment variable, else a
``maplib_benches`` folder next to this repository.

Tiers (times on an M4 Pro with 24 GB and an open-source build, which skips about two thirds of the
tests as they need maplib Enterprise Edition):

- quick (about 25 s): the workload benchmarks (memory per triple, scaling of loads, queries and
  updates, leaks, steady states; synthetic data) and the storage benchmarks. For every change.
- full (about 2 min): every test of the suite, also BSBM, the DIM queries, serialization and the
  real datasets (trainmarks, 1.2 GB of Turtle, and the ERA datasets).

--scale sets WORKLOADS_SCALE, which multiplies the sizes of the workload benchmarks (1: 350,000
to 1.4 M triples). Their README recommends 4 (up to 5.6 M triples) to judge the scaling of
queries that take only milliseconds at scale 1. Compare runs at the same scale.

Each run refuses to start while cargo, rustc or maturin run, and when the compiled maplib is older
than its Rust sources (see rdf12_benchmark.py). Features of maplib Enterprise Edition are skipped
on an open-source build (maplib_benches_plugin.py).

A run writes to benches_results/<label>/ (ignored by git):
- meta.json: the maplib and maplib_benches commits, the build, the tier and the settings
- bmf.json: every test's latency and the extra measures it records (MB, ratios, exponents), in
  Bencher Metric Format
- junit.xml and pytest.log: the outcome of each test and the output
- workloads/: the raw metrics of each workload case

compare lists the tests whose outcome differs and the measures that changed beyond the noise.
The defaults come from two quick runs of the same build: memory measured from RSS varied by
about 10 MB per step (so values under 25 MB are not judged), leak-check slopes by about 1 MB per
iteration, timings under 5 ms by large factors, and the marginal memory per triple by under 1%.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from rdf12_benchmark import REPO, build_info, builds_running, check_build, git_info  # noqa: E402

RESULTS = HERE / "benches_results"

# Test selections of each tier, as pytest arguments relative to the maplib_benches checkout.
TIERS = {
    "quick": ["tests/test_workloads.py", "tests/test_storage_benchmark.py"],
    "full": ["tests"],
}


def find_benches(arg) -> Path:
    candidates = [arg, os.environ.get("MAPLIB_BENCHES"), REPO.parent / "maplib_benches"]
    for c in candidates:
        if c and (Path(c) / "tests" / "conftest.py").exists():
            return Path(c).resolve()
    sys.exit(
        "maplib_benches not found. Pass --benches-dir, set MAPLIB_BENCHES, or check it out next "
        f"to this repository ({REPO.parent / 'maplib_benches'})."
    )


def benches_info(benches: Path) -> dict:
    def git(*a):
        r = subprocess.run(["git", "-C", str(benches), *a], capture_output=True, text=True)
        return r.stdout.strip()

    return {"path": str(benches), "commit": git("rev-parse", "--short", "HEAD")}


def outcomes(junit: Path) -> dict:
    """Test id -> passed, failed, error or skipped (with the reason)."""
    out = {}
    if not junit.exists():
        return out
    for tc in ET.parse(junit).iter("testcase"):
        tid = f"{tc.get('classname')}::{tc.get('name')}"
        status = "passed"
        for kind in ("failure", "error", "skipped"):
            el = tc.find(kind)
            if el is not None:
                status = kind if kind != "failure" else "failed"
                if kind == "skipped":
                    status += f" ({(el.get('message') or '')[:60]})"
                break
        out[tid] = status
    return out


def run(args):
    benches = find_benches(args.benches_dir)
    busy = builds_running()
    if busy:
        sys.exit("Builds are running, which disturbs the timings:\n" + "\n".join(busy))
    build = build_info()
    check_build(build, args.allow_stale)

    out = RESULTS / args.label
    if out.exists():
        if not args.overwrite:
            sys.exit(f"{out} exists. Pass --overwrite or choose another --label.")
        shutil.rmtree(out)
    out.mkdir(parents=True)

    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(HERE), env.get("PYTHONPATH")]))
    settings = {}
    if args.scale is not None:
        env["WORKLOADS_SCALE"] = str(args.scale)
        settings["WORKLOADS_SCALE"] = args.scale
    if args.bsbm_products is not None:
        env["BSBM_PRODUCTS"] = str(args.bsbm_products)
        settings["BSBM_PRODUCTS"] = args.bsbm_products

    selection = TIERS[args.tier]
    cmd = [
        sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
        "-p", "maplib_benches_plugin", "-rfE", f"--junitxml={out / 'junit.xml'}",
        *selection, *args.pytest_args,
    ]  # fmt: skip
    meta = {
        "label": args.label,
        "tier": args.tier,
        "selection": selection,
        "pytest_args": args.pytest_args,
        "settings": settings,
        "maplib": git_info(),
        "build": build,
        "benches": benches_info(benches),
        "started": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    print(f"maplib {meta['maplib']['branch']}@{meta['maplib']['commit']}"
          f"{' (dirty)' if meta['maplib']['dirty'] else ''}, "
          f"maplib_benches @{meta['benches']['commit']}, tier {args.tier}", flush=True)

    # The suite writes bmf.json into its working directory at the end of the session, so an old
    # one must not be mistaken for this run's.
    (benches / "bmf.json").unlink(missing_ok=True)
    tic = time.time()
    with open(out / "pytest.log", "w") as log:
        p = subprocess.Popen(cmd, cwd=benches, env=env, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)
        for line in p.stdout:
            sys.stdout.write(line)
            log.write(line)
        p.wait()
    meta["seconds"] = round(time.time() - tic, 1)
    meta["exit_code"] = p.returncode

    if (benches / "bmf.json").exists():
        shutil.copy(benches / "bmf.json", out / "bmf.json")
    raw = benches / "workloads" / "results"
    if raw.exists():
        shutil.copytree(raw, out / "workloads")
    (out / "meta.json").write_text(json.dumps(meta, indent=2))

    counts = {}
    for status in outcomes(out / "junit.xml").values():
        key = status.split(" ")[0]
        counts[key] = counts.get(key, 0) + 1
    print(f"\n{counts} in {meta['seconds']:.0f}s; results in {out}")
    sys.exit(p.returncode)


def load_run(path: Path):
    path = Path(path)
    if not path.is_dir():
        path = RESULTS / path
    meta = json.loads((path / "meta.json").read_text())
    bmf = json.loads((path / "bmf.json").read_text()) if (path / "bmf.json").exists() else {}
    return meta, bmf, outcomes(path / "junit.xml")


def compare(args):
    (ma, ba, oa), (mb, bb, ob) = load_run(args.a), load_run(args.b)

    def name(m):
        g = m["maplib"]
        return f"{m['label']} ({g['branch']}@{g['commit']}{'+dirty' if g['dirty'] else ''})"

    print(f"A: {name(ma)}\nB: {name(mb)}")
    if ma["build"].get("module_mtime") == mb["build"].get("module_mtime"):
        print("WARNING: both runs used the same compiled maplib, so they measure the same code.")
    if ma["benches"]["commit"] != mb["benches"]["commit"]:
        print(f"WARNING: different maplib_benches commits ({ma['benches']['commit']} and "
              f"{mb['benches']['commit']}), so tests and bounds may differ.")
    if ma.get("settings") != mb.get("settings"):
        print(f"WARNING: different settings: {ma.get('settings')} and {mb.get('settings')}")

    changed = [(t, oa.get(t, "absent"), ob.get(t, "absent"))
               for t in sorted(set(oa) | set(ob)) if oa.get(t) != ob.get(t)]
    if changed:
        print("\nOutcomes that differ:")
        for t, a, b in changed:
            print(f"  {t}: {a} -> {b}")

    rows = []
    for key in sorted(set(ba) & set(bb)):
        for measure in sorted(set(ba[key]) & set(bb[key])):
            va, vb = ba[key][measure]["value"], bb[key][measure]["value"]
            rows.append((key, measure, va, vb, significant(measure, va, vb, args)))
    flagged = [r for r in rows if r[4]]
    print(f"\n{len(rows)} measures in both runs, {len(flagged)} changed beyond the noise "
          f"(ratio over {args.threshold:.0%} above {args.min_mb} MB / {args.min_ms} ms, "
          f"exponents by more than {args.exponent_diff}):")
    for key, measure, va, vb, _ in sorted(flagged, key=lambda r: -change(r[1], r[2], r[3])):
        print("  " + format_row(key, measure, va, vb))
    if args.all:
        print("\nAll measures:")
        for key, measure, va, vb, _ in rows:
            print("  " + format_row(key, measure, va, vb))


def kind(measure: str) -> str:
    """How the suite's measures are judged. bmf.json gives no units, so they follow its names:
    "latency" is in nanoseconds, "exponent" a fitted scaling exponent, "slope" the growth of RSS
    in MB per iteration of a loop (leak checks), "bytes" bytes per triple, "mb" megabytes,
    "_over_" and "ratio" dimensionless ratios, and the rest (p50, p99, seconds) seconds."""
    m = measure.lower()
    if m == "latency":
        return "ns"
    if "exponent" in m:
        return "exponent"
    if "slope" in m:
        return "slope"
    if "bytes" in m:
        return "bytes"
    if "_over_" in m or "ratio" in m or "speedup" in m or "throughput" in m:
        return "ratio"
    if "mb" in m:
        return "mb"
    return "s"


def change(measure, va, vb) -> float:
    """The size of a change, comparable across measures for sorting."""
    if kind(measure) in ("exponent", "slope"):
        return abs(vb - va)
    if va > 0 and vb > 0:
        return abs(max(va, vb) / min(va, vb) - 1)
    return 0.0


def significant(measure, va, vb, args) -> bool:
    """Whether a change is beyond the noise. Tiny memory and time values vary by large factors
    between identical runs (0.05 MB of peak, 0.3 ms of latency), so values below a floor are
    not judged by their ratio, and exponents are judged by their difference."""
    k = kind(measure)
    if k == "exponent":
        return abs(vb - va) > args.exponent_diff
    if k == "slope":
        return abs(vb - va) > args.slope_diff
    if va <= 0 or vb <= 0:
        return False
    floor = {
        "mb": args.min_mb, "s": args.min_ms / 1e3, "ns": args.min_ms * 1e6, "ratio": 0, "bytes": 0
    }[k]  # fmt: skip
    if max(va, vb) < floor:
        return False
    return abs(vb / va - 1) > args.threshold


def format_row(key, measure, va, vb) -> str:
    shown = key.split("::", 1)[-1] if "::" in key else key
    if kind(measure) in ("exponent", "slope"):
        delta = f"{vb - va:+6.2f}  "
    else:
        delta = f"{vb / va:6.2f}x " if va > 0 and vb > 0 else "     -  "
    return f"{delta} {measure:<24} {va:>12.4g} -> {vb:<12.4g} {shown[:90]}"


def tiers(_args):
    for t, sel in TIERS.items():
        print(f"{t}: {' '.join(sel)}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run", help="run a tier of maplib_benches")
    p.add_argument("--tier", choices=TIERS, default="quick")
    p.add_argument("--label", required=True, help="name of the results folder")
    p.add_argument("--benches-dir", default=None)
    p.add_argument("--scale", type=float, default=None, help="WORKLOADS_SCALE (default 1)")
    p.add_argument("--bsbm-products", type=int, default=None, help="BSBM_PRODUCTS (default 300)")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--allow-stale", action="store_true",
                   help="measure even if the compiled maplib is older than its sources")
    p.add_argument("pytest_args", nargs=argparse.REMAINDER,
                   help="further pytest arguments after --, e.g. -- -k load_scaling")
    p.set_defaults(func=run)

    p = sub.add_parser("compare", help="compare two runs (folders or labels)")
    p.add_argument("a")
    p.add_argument("b")
    p.add_argument("--threshold", type=float, default=0.25,
                   help="relative change of MB and times to flag (default 0.25)")
    p.add_argument("--min-mb", type=float, default=25.0,
                   help="MB values below this are not judged (default 25)")
    p.add_argument("--slope-diff", type=float, default=2.0,
                   help="change of an RSS growth slope (MB per iteration) to flag (default 2)")
    p.add_argument("--min-ms", type=float, default=5.0,
                   help="times below this many ms are not judged (default 5)")
    p.add_argument("--exponent-diff", type=float, default=0.25,
                   help="change of a scaling exponent to flag (default 0.25)")
    p.add_argument("--all", action="store_true", help="list every measure")
    p.set_defaults(func=compare)

    sub.add_parser("tiers", help="list the tiers").set_defaults(func=tiers)

    args = ap.parse_args()
    if getattr(args, "pytest_args", None) and args.pytest_args[0] == "--":
        args.pytest_args = args.pytest_args[1:]
    args.func(args)


if __name__ == "__main__":
    main()
