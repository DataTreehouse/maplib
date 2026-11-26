"""Benchmark: storing and querying statements about statements, as a baseline for RDF 1.2.

Generates a synthetic graph, loads it in a fresh Python process per measurement, and records
load time, memory and query latencies, so that storage designs for RDF 1.2 triple terms can be
compared with each other and with what maplib does today.

    python rdf12_benchmark.py run --triples 1e5,1e6,1e7 --label baseline
    python rdf12_benchmark.py compare rdf12_results/baseline.json rdf12_results/other.json
    python rdf12_benchmark.py generate --triples 1e6 --encoding rdf12 --out /tmp/g.nt

The graph has `--triples` asserted triples over `--predicates` predicates. Each predicate has
one object type (IRI, string, integer or dateTime, in turn), and every subject has one triple
per predicate. Strings repeat from a vocabulary, so the string dictionaries see realistic
sharing. A fraction (`--reified`) of the triples are also described by a statement node
`ex:stmt/i` with an `ex:source` and an `ex:since` date. That description is written in one of
three encodings:

- plain: no statement nodes at all (the cost of the asserted triples alone)
- reification: classic RDF reification (rdf:type rdf:Statement, rdf:subject, rdf:predicate,
  rdf:object), which maplib can store today
- rdf12: RDF 1.2 `ex:stmt/i rdf:reifies <<( s p o )>>`, which needs triple term support

The same questions are asked of every encoding, and the number of rows each query returns is
recorded, so encodings that hold the same information must agree. A load that fails (e.g.
rdf12 before triple terms are supported) is recorded as an error, not a crash of the run.

Measurements, per size and encoding, each in its own process:
- load_s: Model.read of the N-Triples file (parsing, dictionary encoding, sorting, indexing)
- maplib_mb: memory allocated by maplib after loading (maplib.memory_usage()), i.e. the
  triple tables and the string dictionaries; bytes_per_triple divides it by all stored triples
- rss_load_mb: growth of the resident memory of the process while loading, a cross-check of
  maplib_mb that also counts memory the allocator holds on to
- rss_mb / peak_rss_mb: resident memory of the process after loading / at its highest
- queries: the first run (plans and caches are cold) and the median of --repeats runs with
  the query cache off

Build maplib in release mode (maturin develop --release) before measuring. The run refuses to
start while cargo, rustc or maturin run, as they disturb the timings, and when the compiled
maplib module is older than the Rust sources (e.g. after a pull without a rebuild), as it
would not measure the checked out code.
"""

import argparse
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_RESULTS = HERE / "rdf12_results"
DEFAULT_DATA = Path(tempfile.gettempdir()) / "maplib_rdf12_bench"

EX = "http://example.org/"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
XSD = "http://www.w3.org/2001/XMLSchema#"
ENCODINGS = ["plain", "reification", "rdf12"]
OBJECT_KINDS = ["iri", "string", "integer", "datetime"]

# Rows generated at a time, which bounds the memory of the generator.
CHUNK = 2_000_000
# Model.read parses this many triples before storing them. Where the batches end changes the
# memory of the stored triples by about 5% (segments and their statistics are made per batch),
# in different directions for different files, so the files are read in one batch to compare
# encodings fairly.
BATCH_SIZE = 2_000_000_000


# ---------------------------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------------------------


def parse_count(s: str) -> int:
    return int(float(s))


def mix32(i, seed: int, k: int):
    """A pseudo random 32 bit value of the UInt64 expression i, with a murmur3 style finaliser,
    so that low bits do not follow i (a plain multiplicative hash keeps i mod 2^n in its low
    bits, which would tie values to predicates). Products stay below 2^64."""
    m = 2**32
    x = ((i + seed) * k + 0x165667B1) % m
    x = x ^ (x // 2**16)
    x = (x * 0x85EBCA6B) % m
    x = x ^ (x // 2**13)
    x = (x * 0xC2B2AE35) % m
    return x ^ (x // 2**16)


def generate(
    path: Path,
    n: int,
    encoding: str,
    predicates: int,
    reified: float,
    seed: int,
    described_predicates: int | None = None,
):
    """Writes the graph as N-Triples (N-Triples 1.2 for rdf12) to `path`. With
    `described_predicates` k, only triples of the predicates p0 to p(k-1) are described (a
    fraction `reified` of them), so that the statements are about a few predicates."""
    import polars as pl

    n_subjects = max(1, n // predicates)
    vocab = max(1_000, n // 100)
    threshold = int(reified * 1_000_000)
    tmp = path.with_suffix(".partial")
    with open(tmp, "wb") as f:
        for start in range(0, n, CHUNK):
            end = min(n, start + CHUNK)
            i = pl.int_range(start, end, dtype=pl.UInt64, eager=True).alias("i")
            df = pl.DataFrame(i).with_columns(
                # Two independent pseudo random values per triple.
                h=mix32(pl.col("i"), seed, 0x9E3779B1),
                g=mix32(pl.col("i"), seed, 0x85EBCA6B),
            )
            df = df.with_columns(
                s=pl.format("<{}e/{}>", pl.lit(EX), pl.col("i") // predicates),
                pj=pl.col("i") % predicates,
            ).with_columns(p=pl.format("<{}p{}>", pl.lit(EX), pl.col("pj")))
            kind = pl.col("pj") % len(OBJECT_KINDS)
            month = (pl.col("h") % 12 + 1).cast(pl.String).str.zfill(2)
            day = (pl.col("h") % 28 + 1).cast(pl.String).str.zfill(2)
            year = (pl.col("h") % 25 + 2000).cast(pl.String)
            df = df.with_columns(
                o=pl.when(kind == 0)
                .then(pl.format("<{}e/{}>", pl.lit(EX), pl.col("h") % n_subjects))
                .when(kind == 1)
                .then(pl.format('"label {}"', pl.col("h") % vocab))
                .when(kind == 2)
                .then(pl.format('"{}"^^<{}integer>', pl.col("h") % 100_000, pl.lit(XSD)))
                .otherwise(
                    pl.format(
                        '"{}-{}-{}T12:00:00Z"^^<{}dateTime>', year, month, day, pl.lit(XSD)
                    )
                )
            )
            asserted = pl.format("{} {} {} .", "s", "p", "o")
            if encoding == "plain" or threshold == 0:
                lines = asserted
            else:
                r = pl.format("<{}stmt/{}>", pl.lit(EX), pl.col("i"))
                src = pl.format("<{}src/{}>", pl.lit(EX), pl.col("g") % 1000)
                since = pl.format(
                    '"{}-06-15"^^<{}date>', (pl.col("g") % 25 + 2000).cast(pl.String), pl.lit(XSD)
                )
                annotations = [
                    pl.format("{} <{}source> {} .", r, pl.lit(EX), src),
                    pl.format("{} <{}since> {} .", r, pl.lit(EX), since),
                ]
                if encoding == "reification":
                    statement = [
                        pl.format("{} <{}type> <{}Statement> .", r, pl.lit(RDF), pl.lit(RDF)),
                        pl.format("{} <{}subject> {} .", r, pl.lit(RDF), "s"),
                        pl.format("{} <{}predicate> {} .", r, pl.lit(RDF), "p"),
                        pl.format("{} <{}object> {} .", r, pl.lit(RDF), "o"),
                    ]
                else:
                    statement = [
                        pl.format("{} <{}reifies> <<( {} {} {} )>> .", r, pl.lit(RDF), "s", "p", "o")
                    ]
                described = pl.concat_str([asserted, *statement, *annotations], separator="\n")
                is_described = pl.col("g") % 1_000_000 < threshold
                if described_predicates is not None:
                    is_described = is_described & (pl.col("pj") < described_predicates)
                lines = pl.when(is_described).then(described).otherwise(asserted)
            df.select(lines.alias("line")).write_csv(f, include_header=False, quote_style="never")
    tmp.rename(path)


def data_path(
    data_dir: Path, n: int, encoding: str, predicates: int, reified: float, seed: int,
    described_predicates: int | None = None,
) -> Path:  # fmt: skip
    d = "" if described_predicates is None else f"_d{described_predicates}"
    return data_dir / f"g_{n}_{encoding}_p{predicates}_r{reified}{d}_s{seed}.nt"


def ensure_data(data_dir: Path, n, encoding, predicates, reified, seed, described_predicates=None) -> Path:
    path = data_path(data_dir, n, encoding, predicates, reified, seed, described_predicates)
    if not path.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
        t = time.perf_counter()
        generate(path, n, encoding, predicates, reified, seed, described_predicates)
        print(
            f"  generated {path.name} ({path.stat().st_size / 1e6:.0f} MB) "
            f"in {time.perf_counter() - t:.1f}s",
            flush=True,
        )
    return path


# ---------------------------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------------------------

PREFIXES = f"PREFIX ex: <{EX}> PREFIX rdf: <{RDF}> PREFIX xsd: <{XSD}>\n"


def statement_pattern(encoding: str, r: str, s: str, p: str, o: str) -> str:
    """The graph pattern matching statement node r describing the triple (s, p, o)."""
    if encoding == "reification":
        return f"{r} rdf:subject {s} ; rdf:predicate {p} ; rdf:object {o} ."
    return f"{r} rdf:reifies <<( {s} {p} {o} )>> ."


def queries(encoding: str, n: int, predicates: int) -> dict:
    """Query name -> SPARQL. The statement queries are left out for the plain encoding."""
    k = (n // predicates) // 2  # a subject in the middle
    q = {
        # Asserted triples only: what RDF 1.2 support must not slow down.
        "count_all": "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }",
        "scan_iri_pred": "SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p0 ?o }",
        "point_subject": f"SELECT ?p ?o WHERE {{ <{EX}e/{k}> ?p ?o }}",
        "join_two_preds": "SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p0 ?x . ?x ex:p1 ?l }",
        "filter_string": 'SELECT (COUNT(*) AS ?c) WHERE { ?s ex:p1 ?l FILTER(?l = "label 7") }',
        # Returning rows makes maplib produce and decode the values, which counts can skip.
        "rows_join_two_preds": "SELECT ?s ?l WHERE { ?s ex:p0 ?x . ?x ex:p1 ?l }",
        "rows_datetime_range": "SELECT ?s ?d WHERE { ?s ex:p3 ?d "
        'FILTER(?d >= "2010-01-01T00:00:00Z"^^xsd:dateTime '
        '&& ?d < "2011-01-01T00:00:00Z"^^xsd:dateTime) }',
    }
    # About 1.6 of the 16 triples of a subject are described, so a point lookup on one subject
    # finds too few statements to time; this looks up 50 neighbouring subjects.
    subjects = " ".join(f"<{EX}e/{j}>" for j in range(k, k + 50))
    if encoding == "plain":
        return q
    st = lambda r, s, p, o: statement_pattern(encoding, r, s, p, o)  # noqa: E731
    q.update(
        {
            # All statements about triples of one predicate, with their source.
            "stmt_by_pred": "SELECT (COUNT(*) AS ?c) WHERE { "
            + st("?r", "?s", "ex:p0", "?o")
            + " ?r ex:source ?src }",
            # Statements about the triples of given subjects.
            "stmt_point_subjects": f"SELECT ?s ?p ?o ?src WHERE {{ VALUES ?s {{ {subjects} }} "
            + st("?r", "?s", "?p", "?o")
            + " ?r ex:source ?src }",
            # All statements about one predicate with their values, as rows.
            "rows_stmt_by_pred": "SELECT ?s ?o ?src ?d WHERE { "
            + st("?r", "?s", "ex:p1", "?o")
            + " ?r ex:source ?src ; ex:since ?d }",
            # The triples a source vouches for.
            "stmt_by_source": f"SELECT ?s ?p ?o WHERE {{ ?r ex:source <{EX}src/7> . "
            + st("?r", "?s", "?p", "?o")
            + " }",
            # Annotation join: described triples that are asserted, with a filter on the date.
            "stmt_join_asserted": "SELECT (COUNT(*) AS ?c) WHERE { "
            + st("?r", "?s", "ex:p1", "?o")
            + ' ?s ex:p1 ?o . ?r ex:since ?d FILTER(?d >= "2015-01-01"^^xsd:date) }',
            # Statements whose object is a literal of a given type, ordered.
            "stmt_order_by": "SELECT ?s ?o WHERE { "
            + st("?r", "?s", "ex:p3", "?o")
            + " } ORDER BY ?o ?s LIMIT 100",
        }
    )
    return q


# ---------------------------------------------------------------------------------------------
# Measurement (child process)
# ---------------------------------------------------------------------------------------------


def peak_rss_mb() -> float:
    # ru_maxrss is in bytes on macOS and in kilobytes on Linux.
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e6 if sys.platform == "darwin" else r / 1e3


def rss_mb() -> float:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(os.getpid())], capture_output=True, text=True)
    return int(out.stdout.strip()) / 1e3


def result_rows(res) -> int:
    """The number of rows, or the count of a COUNT query."""
    if res.columns == ["c"] and res.height == 1:
        return int(res["c"][0])
    return res.height


def measure(
    path: Path, encoding: str, n: int, predicates: int, repeats: int, batch_size: int
) -> dict:
    import maplib

    out = {"rss_start_mb": rss_mb()}
    m = maplib.Model()
    t = time.perf_counter()
    try:
        m.read(str(path), format="ntriples", triples_batch_size=batch_size)
    except Exception as e:  # noqa: BLE001 - recorded, so other runs go on
        out["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        return out
    out["load_s"] = time.perf_counter() - t
    out["maplib_mb"] = maplib.memory_usage() / 1e6
    out["rss_mb"] = rss_mb()
    out["rss_load_mb"] = out["rss_mb"] - out["rss_start_mb"]
    out["peak_rss_mb_load"] = peak_rss_mb()

    qs = {}
    for name, q in queries(encoding, n, predicates).items():
        q = PREFIXES + q
        try:
            t = time.perf_counter()
            res = m.query(q)
            first = time.perf_counter() - t
            times = []
            for _ in range(repeats):
                t = time.perf_counter()
                m.query(q, cache=False)
                times.append(time.perf_counter() - t)
            qs[name] = {
                "rows": result_rows(res),
                "first_s": first,
                "median_s": statistics.median(times) if times else first,
            }
        except Exception as e:  # noqa: BLE001
            qs[name] = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
    out["queries"] = qs
    stored = qs.get("count_all", {}).get("rows")
    if stored:
        out["stored_triples"] = stored
        out["bytes_per_triple"] = out["maplib_mb"] * 1e6 / stored
    out["peak_rss_mb"] = peak_rss_mb()
    return out


def child(args):
    res = measure(
        Path(args.path), args.encoding, args.triples, args.predicates, args.repeats,
        args.batch_size,
    )  # fmt: skip
    print("RESULT " + json.dumps(res), flush=True)


# ---------------------------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------------------------


def builds_running() -> list:
    out = subprocess.run(["ps", "-axo", "stat=,comm="], capture_output=True, text=True).stdout
    busy = []
    for line in out.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2 and not parts[0].startswith("T"):
            if os.path.basename(parts[1]) in ("cargo", "rustc", "maturin"):
                busy.append(line.strip())
    return busy


def git_info() -> dict:
    def git(*a):
        r = subprocess.run(["git", *a], capture_output=True, text=True, cwd=HERE)
        return r.stdout.strip()

    return {
        "commit": git("rev-parse", "--short", "HEAD"),
        "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
    }


REPO = HERE.parents[2]


def build_info() -> dict:
    """The compiled maplib module and the newest of the Rust sources it is built from. A pull or
    an edit updates the modification times of the changed files, so a module older than its
    sources is a stale build that does not measure the checked out code."""
    import maplib

    modules = sorted(Path(maplib.__file__).parent.glob("maplib*.so")) + sorted(
        Path(maplib.__file__).parent.glob("maplib*.pyd")
    )
    newest, newest_file = 0.0, None
    roots = [REPO / "lib", REPO / "py_maplib" / "src"]
    files = [REPO / "Cargo.toml", REPO / "Cargo.lock", REPO / "py_maplib" / "Cargo.toml"]
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in ("target", "tests", "benches")]
            files.extend(Path(dirpath) / f for f in filenames if f.endswith((".rs", ".toml")))
    for f in files:
        try:
            m = f.stat().st_mtime
        except OSError:
            continue
        if m > newest:
            newest, newest_file = m, f
    module = modules[0] if modules else None
    return {
        "module": str(module) if module else None,
        "module_mtime": module.stat().st_mtime if module else None,
        "newest_source": str(newest_file.relative_to(REPO)) if newest_file else None,
        "newest_source_mtime": newest,
    }


def check_build(info: dict, allow_stale: bool):
    if info["module_mtime"] is None:
        print("WARNING: could not find the compiled maplib module, the build is not checked")
        return
    if info["module_mtime"] < info["newest_source_mtime"]:
        msg = (
            f"The compiled maplib module ({time.ctime(info['module_mtime'])}) is older than "
            f"{info['newest_source']} ({time.ctime(info['newest_source_mtime'])}), so it does not "
            "measure the checked out code. Rebuild with: maturin develop --release "
            "(or pass --allow-stale)."
        )
        if not allow_stale:
            sys.exit(msg)
        print("WARNING: " + msg)


def run_child(
    path: Path, encoding: str, n: int, predicates: int, repeats: int, batch_size: int
) -> dict:
    cmd = [
        sys.executable, __file__, "child", "--path", str(path), "--encoding", encoding,
        "--triples", str(n), "--predicates", str(predicates), "--repeats", str(repeats),
        "--batch-size", str(batch_size),
    ]  # fmt: skip
    env = dict(os.environ)
    # The allocator of maplib counts allocations per thread and adds them to the global count
    # every 4 MiB by default, so maplib.memory_usage() can be off by 4 MiB per thread. 64 KiB
    # makes it accurate to about a megabyte, at a negligible cost.
    env["POLARS_OOC_DRIFT_THRESHOLD"] = str(64 * 1024)
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    for line in p.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[len("RESULT "):])
    return {"error": f"child exited with {p.returncode}: {p.stderr[-1000:]}"}


def run(args):
    busy = builds_running()
    if busy:
        sys.exit("Builds are running, which disturbs the timings:\n" + "\n".join(busy))
    import maplib

    build = build_info()
    check_build(build, args.allow_stale)
    sizes = [parse_count(x) for x in args.triples.split(",")]
    encodings = args.encodings.split(",")
    results = {
        "label": args.label,
        "git": git_info(),
        "build": build,
        "maplib_version": getattr(maplib, "__version__", None),
        "machine": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
        },
        "params": {
            "predicates": args.predicates,
            "reified": args.reified,
            "seed": args.seed,
            "repeats": args.repeats,
            "batch_size": args.batch_size,
        },
        "runs": [],
    }
    for n in sizes:
        for enc in encodings:
            path = ensure_data(Path(args.data_dir), n, enc, args.predicates, args.reified, args.seed)
            print(f"{n:>12,} {enc:<12} ", end="", flush=True)
            res = run_child(path, enc, n, args.predicates, args.repeats, args.batch_size)
            res.update({"triples": n, "encoding": enc, "file_mb": path.stat().st_size / 1e6})
            results["runs"].append(res)
            if "error" in res:
                print("ERROR " + res["error"].splitlines()[0][:150], flush=True)
            else:
                print(
                    f"load {res['load_s']:7.2f}s  maplib {res['maplib_mb']:9.1f} MB  "
                    f"{res.get('bytes_per_triple', float('nan')):6.1f} B/triple  "
                    f"peak {res['peak_rss_mb']:9.1f} MB",
                    flush=True,
                )
    check_agreement(results)
    out = Path(args.out) if args.out else DEFAULT_RESULTS / f"{args.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2))
    print(f"results written to {out}")


def check_agreement(results: dict):
    """Warns when encodings holding the same information return different numbers of rows."""
    by_size = {}
    for r in results["runs"]:
        for name, q in r.get("queries", {}).items():
            if "rows" in q:
                by_size.setdefault((r["triples"], name), {})[r["encoding"]] = q["rows"]
    for (n, name), rows in sorted(by_size.items()):
        # count_all differs by design: the encodings store different numbers of triples.
        if name != "count_all" and len(set(rows.values())) > 1:
            print(f"WARNING: encodings disagree on {name} at {n:,} triples: {rows}")


def compare(args):
    files = [json.loads(Path(f).read_text()) for f in args.files]
    keys = {}
    for i, res in enumerate(files):
        for r in res["runs"]:
            key = (r["triples"], r["encoding"])
            metrics = {}
            if "error" in r:
                metrics["error"] = r["error"].splitlines()[0][:40]
            else:
                for m in ("load_s", "maplib_mb", "bytes_per_triple", "rss_load_mb", "peak_rss_mb"):
                    metrics[m] = r.get(m)
                for qn, q in r.get("queries", {}).items():
                    metrics[f"q:{qn}"] = q.get("median_s", q.get("error", "")[:30] or None)
            keys.setdefault(key, [None] * len(files))[i] = metrics
    labels = [f["label"] + "@" + f["git"]["commit"] for f in files]
    for (n, enc), per_file in sorted(keys.items()):
        print(f"\n== {n:,} triples, {enc}")
        names = []
        for m in per_file:
            for k in m or {}:
                if k not in names:
                    names.append(k)
        header = f"{'metric':<26}" + "".join(f"{lbl[:22]:>24}" for lbl in labels)
        if len(files) > 1:
            header += f"{'last/first':>12}"
        print(header)
        for name in names:
            vals = [(m or {}).get(name) for m in per_file]
            row = f"{name:<26}"
            for v in vals:
                row += f"{v:>24.4g}" if isinstance(v, (int, float)) else f"{str(v)[:22]:>24}"
            if len(files) > 1 and all(isinstance(v, (int, float)) and v for v in (vals[0], vals[-1])):
                row += f"{vals[-1] / vals[0]:>12.2f}"
            print(row)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--predicates", type=int, default=16)
        p.add_argument("--reified", type=float, default=0.1, help="fraction of triples described")
        p.add_argument("--seed", type=int, default=0)

    p = sub.add_parser("run", help="generate data as needed and measure")
    p.add_argument("--triples", default="1e5,1e6", help="comma separated sizes, e.g. 1e5,1e6,1e7")
    p.add_argument("--encodings", default=",".join(ENCODINGS))
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--label", default="run")
    p.add_argument("--out", default=None)
    p.add_argument("--data-dir", default=str(DEFAULT_DATA))
    p.add_argument(
        "--batch-size", type=parse_count, default=BATCH_SIZE,
        help="triples_batch_size of Model.read (default: the whole file in one batch)",
    )  # fmt: skip
    p.add_argument(
        "--allow-stale", action="store_true",
        help="measure even if the compiled maplib is older than its sources",
    )  # fmt: skip
    common(p)
    p.set_defaults(func=run)

    p = sub.add_parser("generate", help="only write a data file")
    p.add_argument("--triples", type=parse_count, required=True)
    p.add_argument("--encoding", choices=ENCODINGS, required=True)
    p.add_argument("--out", required=True)
    common(p)
    p.set_defaults(
        func=lambda a: generate(Path(a.out), a.triples, a.encoding, a.predicates, a.reified, a.seed)
    )

    p = sub.add_parser("compare", help="compare result files")
    p.add_argument("files", nargs="+")
    p.set_defaults(func=compare)

    p = sub.add_parser("child")
    p.add_argument("--path", required=True)
    p.add_argument("--encoding", required=True)
    p.add_argument("--triples", type=int, required=True)
    p.add_argument("--predicates", type=int, required=True)
    p.add_argument("--repeats", type=int, required=True)
    p.add_argument("--batch-size", type=int, required=True)
    p.set_defaults(func=child)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
