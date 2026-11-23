"""Benchmark: Model.map_rml over JSON, XML and CSV sources, against what it is built from.

Run with: python rml_benchmark.py [--records 200000] [--repeats 1] [--formats json,xml,csv]
    [--methods rml,facade,table] [--shapes flat,tags]

A plant with n pumps is generated as a JSON document, an XML document and a CSV file, and mapped
to the same triples in three ways:

- rml: Model.map_rml with an RML mapping. For JSON and XML this maps the document to a temporary
  Façade-X graph, evaluates the iterator and the references as one SPARQL query over it, and maps
  the resulting table with the R2RML engine. For CSV the file is read with polars.
- facade: Model.map_json or Model.map_xml of the same document alone. This is the first step of
  map_rml on its own, so the difference to rml is the cost of the SPARQL query, the conversion of
  its result and the mapping of the table. It gives other triples (the Façade-X graph), so its
  triple count differs.
- table: the same records as a polars DataFrame through Model.map_r2rml(tables=...). This is the
  last step of map_rml on its own: what a path evaluator that produced the table at no cost would
  leave.

Shapes: "flat" has an IRI template subject with a class, a string, a number and an IRI template
object per pump. "tags" adds a reference with two values per pump (a JSON array, a repeated XML
element), which doubles the rows of the table. CSV has no "tags" shape.

Every run is its own Python process, which reports its peak RSS. The stages of map_rml and
map_r2rml (sources = Façade-X mapping and SPARQL, evaluate = term maps, expand = insertion) are
read from maplib's debug log (target r2rml_timing). Generating the documents is not timed.
Refuses to start while cargo, rustc or maturin run (stopped builds are ignored).
"""

import argparse
import json
import os
import re
import resource
import subprocess
import sys
import tempfile
import time

PREFIXES = """@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix rml: <http://semweb.mmlab.be/ns/rml#> .
@prefix ql: <http://semweb.mmlab.be/ns/ql#> .
@prefix ex: <http://example.org/> .
"""

# The references of the id, the name, the power, the site code and the tags, per format.
REFERENCES = {
    "json": {"id": "id", "name": "name", "kw": "kw", "site": "site.code", "tag": "tags[*]"},
    "xml": {"id": "@id", "name": "name", "kw": "kw", "site": "site/@code", "tag": "tag"},
    "csv": {"id": "id", "name": "name", "kw": "kw", "site": "site", "tag": None},
    "table": {"id": "id", "name": "name", "kw": "kw", "site": "site", "tag": "tag"},
}
SOURCES = {
    "json": 'rml:logicalSource [ rml:source "plant" ; rml:referenceFormulation ql:JSONPath ; rml:iterator "$.pumps[*]" ]',
    "xml": 'rml:logicalSource [ rml:source "plant" ; rml:referenceFormulation ql:XPath ; rml:iterator "/plant/pump" ]',
    "csv": 'rml:logicalSource [ rml:source "plant" ; rml:referenceFormulation ql:CSV ]',
    "table": 'rr:logicalTable [ rr:tableName "plant" ]',
}

TIMING = re.compile(r"r2rml_timing: (\w+)=([0-9.]+)")


def mapping(fmt: str, shape: str) -> str:
    r = REFERENCES[fmt]
    tags = ""
    if shape == "tags":
        tags = f'  rr:predicateObjectMap [ rr:predicate ex:tag ; rr:objectMap [ rml:reference "{r["tag"]}" ] ] ;\n'
    return PREFIXES + f"""
<#Pump> {SOURCES[fmt]} ;
  rr:subjectMap [ rr:template "http://example.org/pump/{{{r["id"]}}}" ; rr:class ex:Pump ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rml:reference "{r["name"]}" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:kw ; rr:objectMap [ rml:reference "{r["kw"]}" ] ] ;
{tags}  rr:predicateObjectMap [ rr:predicate ex:site ;
    rr:objectMap [ rr:template "http://example.org/site/{{{r["site"]}}}" ] ] .
"""


def builds_running() -> list:
    out = subprocess.run(["ps", "-axo", "stat=,comm="], capture_output=True, text=True).stdout
    busy = []
    for line in out.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2 and not parts[0].startswith("T"):
            if os.path.basename(parts[1]) in ("cargo", "rustc", "maturin"):
                busy.append(line.strip())
    return busy


def peak_rss_gb() -> float:
    # ru_maxrss is in bytes on macOS and in kilobytes on Linux.
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e9 if sys.platform == "darwin" else r / 1e6


def pump(i: int) -> dict:
    return {
        "id": i,
        "name": f"Pump {i % 1000} A",
        "kw": (i % 400) * 0.5 + 0.25,
        "site": f"N{i % 500}",
        "tags": [f"t{i % 7}", f"u{i % 11}"],
    }


def make_json(n: int) -> str:
    parts = []
    for i in range(n):
        p = pump(i)
        parts.append(
            f'{{"id":{p["id"]},"name":"{p["name"]}","kw":{p["kw"]},"site":{{"code":"{p["site"]}"}},'
            f'"tags":["{p["tags"][0]}","{p["tags"][1]}"]}}'
        )
    return '{"plant":"North","pumps":[' + ",".join(parts) + "]}"


def make_xml(n: int) -> str:
    parts = ['<plant name="North">']
    for i in range(n):
        p = pump(i)
        parts.append(
            f'<pump id="{p["id"]}"><name>{p["name"]}</name><kw>{p["kw"]}</kw><site code="{p["site"]}"/>'
            f'<tag>{p["tags"][0]}</tag><tag>{p["tags"][1]}</tag></pump>'
        )
    parts.append("</plant>")
    return "".join(parts)


def make_table(n: int, shape: str, strings: bool):
    """The table that the SPARQL query of map_rml gives: a row per pump, or per pump and tag."""
    import polars as pl

    i = pl.int_range(0, n, eager=True).alias("id")
    df = pl.DataFrame({"id": i}).select(
        pl.col("id"),
        pl.format("Pump {} A", pl.col("id") % 1000).alias("name"),
        ((pl.col("id") % 400) * 0.5 + 0.25).alias("kw"),
        pl.format("N{}", pl.col("id") % 500).alias("site"),
        pl.format("t{}", pl.col("id") % 7).alias("t"),
        pl.format("u{}", pl.col("id") % 11).alias("u"),
    )
    if shape == "tags":
        df = pl.concat([
            df.select("id", "name", "kw", "site", pl.col("t").alias("tag")),
            df.select("id", "name", "kw", "site", pl.col("u").alias("tag")),
        ])
    else:
        df = df.drop("t", "u")
    if strings:
        # As the values of a CSV or XML source are.
        df = df.with_columns(pl.col("id").cast(pl.String), pl.col("kw").cast(pl.String))
    return df


def count_triples(m) -> int:
    return int(m.query("SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }")["c"][0])


def child(args):
    from maplib import Model

    n, fmt, method, shape = args.records, args.format, args.method, args.shape
    m = Model()
    if method == "table":
        # JSON numbers are typed, the values of XML and CSV are strings.
        df = make_table(n, shape, strings=fmt != "json")
        rss_before = peak_rss_gb()
        t0 = time.perf_counter()
        m.map_r2rml(mapping("table", shape), tables={"plant": df})
    elif fmt == "csv":
        folder = tempfile.mkdtemp()
        path = os.path.join(folder, "plant.csv")
        make_table(n, "flat", strings=False).write_csv(path)
        rss_before = peak_rss_gb()
        t0 = time.perf_counter()
        m.map_rml(mapping("csv", shape), sources={"plant": path})
    else:
        doc = make_json(n) if fmt == "json" else make_xml(n)
        rss_before = peak_rss_gb()
        t0 = time.perf_counter()
        if method == "facade":
            (m.map_json if fmt == "json" else m.map_xml)(doc)
        else:
            m.map_rml(mapping(fmt, shape), sources={"plant": doc})
    total = time.perf_counter() - t0
    res = {"total": total, "rss_before": rss_before, "triples": count_triples(m), "peak_rss": peak_rss_gb()}
    print("RESULT " + json.dumps(res), flush=True)


def run_child(fmt, method, shape, records):
    env = dict(os.environ)
    env["RUST_LOG"] = "warn,r2rml_timing=debug"
    cmd = [sys.executable, __file__, "--child", "--format", fmt, "--method", method, "--shape", shape,
           "--records", str(records)]
    with tempfile.TemporaryFile("w+") as err:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=err, text=True, env=env)
        err.seek(0)
        log = err.read()
    if p.returncode != 0:
        raise RuntimeError(f"{fmt}/{method}/{shape} failed:\n{log[-3000:]}")
    res = json.loads([line for line in p.stdout.splitlines() if line.startswith("RESULT ")][-1][7:])
    for k, v in TIMING.findall(log):
        res[k] = res.get(k, 0.0) + float(v)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=200_000)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--formats", default="json,xml,csv")
    ap.add_argument("--methods", default="rml,facade,table")
    ap.add_argument("--shapes", default="flat,tags")
    ap.add_argument("--rss-limit-gb", type=float, default=0.0)
    ap.add_argument("--json", help="write all results to this file")
    ap.add_argument("--child", action="store_true")
    ap.add_argument("--format")
    ap.add_argument("--method")
    ap.add_argument("--shape")
    args = ap.parse_args()
    if args.child:
        return child(args)
    while busy := builds_running():
        print(f"waiting for builds: {busy[:3]}", flush=True)
        time.sleep(30)
    results = []
    cols = ["sources", "evaluate", "expand", "total"]
    print(f"records={args.records:,}  (seconds; peak RSS and RSS before mapping in GB)")
    print(f"{'format':<8}{'shape':<7}{'method':<9}{'triples':>11}" + "".join(f"{c:>10}" for c in cols)
          + f"{'peak':>8}{'before':>8}")
    for fmt in args.formats.split(","):
        for shape in args.shapes.split(","):
            if fmt == "csv" and shape == "tags":
                continue
            for method in args.methods.split(","):
                if method == "facade" and (fmt == "csv" or shape == "tags"):
                    # The Façade-X mapping does not depend on the shape, and CSV has none.
                    continue
                best = None
                for _ in range(args.repeats):
                    r = run_child(fmt, method, shape, args.records)
                    if best is None or r["total"] < best["total"]:
                        best = r
                    if args.rss_limit_gb and r["peak_rss"] > args.rss_limit_gb:
                        sys.exit(f"{fmt}/{method}/{shape}: peak RSS {r['peak_rss']:.1f} GB over the limit")
                best.update(format=fmt, shape=shape, method=method, records=args.records)
                results.append(best)
                vals = "".join(f"{best[c]:>10.3f}" if c in best else f"{'':>10}" for c in cols)
                print(f"{fmt:<8}{shape:<7}{method:<9}{best['triples']:>11,}{vals}"
                      f"{best['peak_rss']:>8.2f}{best['rss_before']:>8.2f}", flush=True)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
