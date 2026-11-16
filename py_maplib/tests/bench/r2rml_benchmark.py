"""Benchmark: Model.map_r2rml against the equivalent hand-built Model.map_triples calls.

Run with: python r2rml_benchmark.py [--rows 1000000] [--repeats 1] [--scenarios a,b] [--sources duckdb,polars]

A table person (n rows) and a table dept (n / 100 rows) are generated in an in-memory DuckDB.
Each scenario maps them with one R2RML mapping, from DuckDB (database=con, so the SQL fetch and
the Arrow conversion are included) and from polars DataFrames (tables=...), and maps the same
triples with hand-built polars DataFrames through Model.map_triples (validate_iris=False), which
ends in the same triplestore insertion, so the overhead of R2RML is visible. Scenarios:

- literals: IRI template subject with one column, four column-valued literals (string, integer,
  double, boolean)
- iri_templates: subject and object IRI templates with several columns that need percent encoding
- class_lang: rr:class and an rr:language literal (the hand-built side maps a plain string)
- datatypes: rr:datatype xsd:dateTime with three time zone offsets, and xsd:date
- blank_nodes: a blank node subject template (the hand-built side uses IRIs)
- graphs: a column-valued graph map with four graphs
- join: a referencing object map with a join condition to the dept table

Every scenario runs in its own Python process, which reports its peak RSS. The stages of
map_r2rml (SQL fetch + Arrow conversion, term evaluation, expansion and insertion) are read from
maplib's debug log (target r2rml_timing); "expand" is the insertion into the triplestore. For
the hand-built side, "evaluate" is building the DataFrames with polars and "expand" is
Model.map_triples. Refuses to start while cargo, rustc or maturin run
(stopped builds are ignored).
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

EX = "http://example.org/"
PREFIXES = f"""@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
"""

MAPPINGS = {
    "literals": """
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:first ; rr:objectMap [ rr:column "first" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:age ; rr:objectMap [ rr:column "age" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:salary ; rr:objectMap [ rr:column "salary" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:active ; rr:objectMap [ rr:column "active" ] ] .
""",
    "iri_templates": """
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{last}/{first}/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:city ;
    rr:objectMap [ rr:template "http://example.org/city/{city}/{dept_id}" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:same ;
    rr:objectMap [ rr:template "http://example.org/person/{id}" ] ] .
""",
    "class_lang": """
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ; rr:class ex:Person ] ;
  rr:predicateObjectMap [ rr:predicate ex:name ;
    rr:objectMap [ rr:column "first" ; rr:language "en" ] ] .
""",
    "datatypes": """
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:born ;
    rr:objectMap [ rr:column "born" ; rr:datatype xsd:dateTime ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:day ;
    rr:objectMap [ rr:column "day" ; rr:datatype xsd:date ] ] .
""",
    "blank_nodes": """
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "{id}" ; rr:termType rr:BlankNode ] ;
  rr:predicateObjectMap [ rr:predicate ex:first ; rr:objectMap [ rr:column "first" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:age ; rr:objectMap [ rr:column "age" ] ] .
""",
    "graphs": """
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ;
    rr:graphMap [ rr:template "http://example.org/graph/{g}" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:first ; rr:objectMap [ rr:column "first" ] ] ;
  rr:predicateObjectMap [ rr:predicate ex:age ; rr:objectMap [ rr:column "age" ] ] .
""",
    "join": """
<#D> rr:logicalTable [ rr:tableName "dept" ] ;
  rr:subjectMap [ rr:template "http://example.org/dept/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:deptName ; rr:objectMap [ rr:column "name" ] ] .
<#P> rr:logicalTable [ rr:tableName "person" ] ;
  rr:subjectMap [ rr:template "http://example.org/person/{id}" ] ;
  rr:predicateObjectMap [ rr:predicate ex:dept ;
    rr:objectMap [ rr:parentTriplesMap <#D> ;
      rr:joinCondition [ rr:child "dept_id" ; rr:parent "id" ] ] ] .
""",
}

TIMING = re.compile(r"r2rml_timing: (\w+)=([0-9.]+)")


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


def make_database(n: int):
    import duckdb

    con = duckdb.connect()
    con.execute(
        f"""CREATE TABLE person AS SELECT
            i::INTEGER AS id,
            'Ann ' || (i % 1000)::VARCHAR AS first,
            'Bjørn-' || (i % 997)::VARCHAR AS last,
            (i % 90)::INTEGER AS age,
            ((i % 10000) * 1.5)::DOUBLE AS salary,
            i % 2 = 0 AS active,
            'Oslo ' || (i % 50)::VARCHAR AS city,
            strftime(TIMESTAMP '2020-01-01' + to_seconds(i), '%Y-%m-%dT%H:%M:%S')
              || CASE i % 3 WHEN 0 THEN 'Z' WHEN 1 THEN '+02:00' ELSE '-05:00' END AS born,
            strftime(DATE '2000-01-01' + (i % 9000)::INTEGER, '%Y-%m-%d') AS day,
            (i % {max(n // 100, 1)})::INTEGER AS dept_id,
            'g' || (i % 4)::VARCHAR AS g
          FROM range({n}) t(i)"""
    )
    con.execute(
        f"""CREATE TABLE dept AS SELECT i::INTEGER AS id, 'Dept ' || i::VARCHAR AS name
          FROM range({max(n // 100, 1)}) t(i)"""
    )
    return con


def count_triples(m) -> int:
    df = m.query(
        "SELECT (COUNT(*) AS ?c) WHERE { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } }"
    )
    return int(df["c"][0])


def run_r2rml(scenario: str, n: int, source: str) -> dict:
    import polars as pl  # noqa: F401
    from maplib import Model

    con = make_database(n)
    mapping = PREFIXES + MAPPINGS[scenario]
    kwargs = {"database": con}
    if source == "polars":
        kwargs = {"tables": {t: con.sql(f"SELECT * FROM {t}").pl() for t in ("person", "dept")}}
    rss_before = peak_rss_gb()
    m = Model()
    t0 = time.perf_counter()
    m.map_r2rml(mapping, **kwargs)
    total = time.perf_counter() - t0
    return {"total": total, "rss_before": rss_before, "triples": count_triples(m)}


def hand_built(scenario: str, person, dept):
    """The DataFrames and map_triples arguments that give the triples of a scenario."""
    import polars as pl

    s = pl.format(EX + "person/{}", pl.col("id")).alias("subject")

    def enc(e):
        return e.str.replace_all(" ", "%20", literal=True).str.replace_all("ø", "%C3%B8", literal=True)

    out = []  # (df, predicate, graph)
    if scenario in ("literals", "blank_nodes"):
        if scenario == "blank_nodes":
            s = pl.format(EX + "bnode/{}", pl.col("id")).alias("subject")
        cols = ["first", "age"] if scenario == "blank_nodes" else ["first", "age", "salary", "active"]
        for c in cols:
            out.append((person.select(s, pl.col(c).alias("object")), EX + c, None))
    elif scenario == "iri_templates":
        s2 = pl.format(EX + "person/{}/{}/{}", enc(pl.col("last")), enc(pl.col("first")), pl.col("id"))
        o = pl.format(EX + "city/{}/{}", enc(pl.col("city")), pl.col("dept_id"))
        out.append((person.select(s2.alias("subject"), o.alias("object")), EX + "city", None))
        out.append((person.select(s2.alias("subject"), s.alias("object")), EX + "same", None))
    elif scenario == "class_lang":
        out.append(
            (person.select(s, pl.lit(EX + "Person").alias("object")),
             "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", None)
        )
        out.append((person.select(s, pl.col("first").alias("object")), EX + "name", None))
    elif scenario == "datatypes":
        born = pl.col("born").str.to_datetime("%Y-%m-%dT%H:%M:%S%#z", time_zone="UTC")
        out.append((person.select(s, born.alias("object")), EX + "born", None))
        out.append((person.select(s, pl.col("day").str.to_date().alias("object")), EX + "day", None))
    elif scenario == "graphs":
        for g in sorted(person["g"].unique()):
            part = person.filter(pl.col("g") == g)
            for c in ("first", "age"):
                out.append((part.select(s, pl.col(c).alias("object")), EX + c, EX + "graph/" + g))
    elif scenario == "join":
        d = pl.format(EX + "dept/{}", pl.col("id"))
        out.append((dept.select(d.alias("subject"), pl.col("name").alias("object")), EX + "deptName", None))
        j = person.join(dept, left_on="dept_id", right_on="id", suffix="_d")
        out.append(
            (j.select(s, pl.format(EX + "dept/{}", pl.col("dept_id")).alias("object")), EX + "dept", None)
        )
    return out


def run_hand_built(scenario: str, n: int) -> dict:
    from maplib import Model

    con = make_database(n)
    person = con.sql("SELECT * FROM person").pl()
    dept = con.sql("SELECT * FROM dept").pl()
    rss_before = peak_rss_gb()
    t0 = time.perf_counter()
    batches = hand_built(scenario, person, dept)
    prep = time.perf_counter() - t0
    m = Model()
    t0 = time.perf_counter()
    for df, predicate, graph in batches:
        m.map_triples(df, predicate=predicate, graph=graph, validate_iris=False)
    expand = time.perf_counter() - t0
    return {"evaluate": prep, "expand": expand, "total": prep + expand, "rss_before": rss_before,
            "triples": count_triples(m)}


def child(args):
    if args.source == "hand_built":
        res = run_hand_built(args.scenario, args.rows)
    else:
        res = run_r2rml(args.scenario, args.rows, args.source)
    res["peak_rss"] = peak_rss_gb()
    print("RESULT " + json.dumps(res), flush=True)


def run_child(scenario, source, rows, rss_limit_gb):
    env = dict(os.environ)
    env["RUST_LOG"] = "warn,r2rml_timing=debug"
    cmd = [sys.executable, __file__, "--child", "--scenario", scenario, "--source", source,
           "--rows", str(rows)]
    if rss_limit_gb:
        # A cap on the address space does not work on macOS; ulimit -v is advisory there, so
        # the parent also checks the peak RSS each child reports.
        pass
    with tempfile.TemporaryFile("w+") as err:
        p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=err, text=True, env=env)
        err.seek(0)
        log = err.read()
    if p.returncode != 0:
        raise RuntimeError(f"{scenario}/{source} failed:\n{log[-3000:]}")
    res = json.loads([line for line in p.stdout.splitlines() if line.startswith("RESULT ")][-1][7:])
    for k, v in TIMING.findall(log):
        res[k] = res.get(k, 0.0) + float(v)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--scenarios", default=",".join(MAPPINGS))
    ap.add_argument("--sources", default="duckdb,polars,hand_built")
    ap.add_argument("--rss-limit-gb", type=float, default=0.0)
    ap.add_argument("--json", help="write all results to this file")
    ap.add_argument("--child", action="store_true")
    ap.add_argument("--scenario")
    ap.add_argument("--source")
    args = ap.parse_args()
    if args.child:
        return child(args)
    while busy := builds_running():
        print(f"waiting for builds: {busy[:3]}", flush=True)
        time.sleep(30)
    results = []
    cols = ["fetch", "evaluate", "expand", "total"]
    print(f"rows={args.rows:,}  (seconds; peak RSS and RSS before mapping in GB)")
    print(f"{'scenario':<14}{'source':<11}{'triples':>11}" + "".join(f"{c:>10}" for c in cols)
          + f"{'peak':>8}{'before':>8}")
    for scenario in args.scenarios.split(","):
        for source in args.sources.split(","):
            best = None
            for _ in range(args.repeats):
                r = run_child(scenario, source, args.rows, args.rss_limit_gb)
                if best is None or r["total"] < best["total"]:
                    best = r
                if args.rss_limit_gb and r["peak_rss"] > args.rss_limit_gb:
                    sys.exit(f"{scenario}/{source}: peak RSS {r['peak_rss']:.1f} GB over the limit")
            best.update(scenario=scenario, source=source, rows=args.rows)
            results.append(best)
            vals = "".join(
                f"{best[c]:>10.3f}" if c in best else f"{'':>10}" for c in cols
            )
            print(f"{scenario:<14}{source:<11}{best['triples']:>11,}{vals}"
                  f"{best['peak_rss']:>8.2f}{best['rss_before']:>8.2f}", flush=True)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
