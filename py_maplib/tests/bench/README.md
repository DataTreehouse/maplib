# Benchmarks

Scripts that measure maplib's speed and memory. They are not part of the pytest suite in
`py_maplib/tests` and are run by hand.

| Script | Measures | Needs |
|---|---|---|
| `rdf12_benchmark.py` | Statements about statements (plain triples, classic reification, RDF 1.2 `rdf:reifies`): load time, memory per triple, query latency | Only this repository |
| `run_maplib_benches.py` | The maplib_benches suite: workload memory and scaling, storage modes, BSBM, real datasets | A local checkout of **maplib_benches** (see below) |
| `r2rml_benchmark.py` | `Model.map_r2rml` against hand-built `map_triples` | `duckdb` |

## Before measuring

Measure a release build of the code you have checked out:

```sh
cd py_maplib
source .venv/bin/activate
maturin develop --release
```

Both `rdf12_benchmark.py` and `run_maplib_benches.py` refuse to run in two cases:

- while `cargo`, `rustc` or `maturin` is running;
- when the compiled maplib module is older than its Rust sources, for example after a `git pull`
  without a rebuild. `--allow-stale` overrides this.

Only compare results measured on the same machine.

## rdf12_benchmark.py

```sh
python rdf12_benchmark.py run --triples 1e5,1e6,1e7 --label main
python rdf12_benchmark.py compare rdf12_results/main.json rdf12_results/other.json
```

- Generated data goes to the system temporary folder.
- Results go to `rdf12_results/`, which git ignores.
- The `rdf12` encoding is recorded as an error until maplib can read RDF 1.2 triple terms.
- At 10⁷ triples, loading peaks at about 3.5 GB. Going beyond that needs more memory than 24 GB.

## run_maplib_benches.py and maplib_benches

[maplib_benches](https://github.com/DataTreehouse/maplib_benches) is a separate repository, about
4 GB with its test data. It is not copied into maplib. `run_maplib_benches.py` runs it from its own
checkout with the Python environment you start the script with.

### Setup (once)

1. Clone it next to this repository, so that `maplib` and `maplib_benches` share a parent folder.
   The large datasets are stored with Git LFS.

   ```sh
   cd ..   # the folder that holds maplib
   git lfs install
   git clone git@github.com:DataTreehouse/maplib_benches.git
   ```

   If it is somewhere else, pass `--benches-dir <path>` or set `MAPLIB_BENCHES=<path>`.

2. Install the suite's requirement into maplib's environment:

   ```sh
   pip install -r ../maplib_benches/tests/requirements.txt   # psutil
   ```

3. Check that the clone is complete:
   - `git -C ../maplib_benches status` should list no deleted files.
     If it lists every file as deleted, the clone stopped before writing its index; run
     `git reset` in `maplib_benches` to rebuild it, which leaves the files as they are.
   - `git -C ../maplib_benches lfs ls-files` should mark each dataset with `*`, meaning it is
     downloaded. `-` means only the LFS pointer is there; fetch the data with `git lfs pull`.

### Running and comparing

```sh
python run_maplib_benches.py run --tier quick --label main            # about 25 s
python run_maplib_benches.py run --tier full --scale 4 --label branch # larger workloads
python run_maplib_benches.py compare main branch
```

- **Tiers:**
  - `quick`: the workload and storage benchmarks.
  - `full`: the whole suite, about 2 minutes on an open-source build.
- **Scale:** `--scale` sets `WORKLOADS_SCALE`, which multiplies the sizes of the workload
  benchmarks. At 4 they reach 5.6 M triples. Compare only runs made at the same scale.
- **Results** go to `benches_results/<label>/`, which git ignores:
  - `meta.json`: the commits of both repositories, the build and the settings;
  - `bmf.json`: all measures;
  - the outcome of each test, and the raw workload metrics.
- **Compare** lists the tests whose outcome changed, and the measures that changed beyond the
  noise of two identical runs.
- **Enterprise Edition features:** hybrid and disk storage, SHACL validation and some serialization
  need maplib Enterprise Edition. On an open-source build, `maplib_benches_plugin.py` skips the
  tests that reach them ("needs maplib Enterprise Edition") instead of failing them. That is about
  two thirds of the suite.
