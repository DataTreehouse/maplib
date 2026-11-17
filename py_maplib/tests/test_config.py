"""The config graph (maplib.config): configuration as triples in the model."""

import pytest

from maplib import Model
from maplib.config import (
    CONFIG,
    CONFIG_GRAPH,
    IndexingConfig,
    QueryCacheConfig,
    VirtualizationConfig,
    VirtualizationResource,
    apply_config,
    apply_properties,
    apply_query_cache,
    read_indexing,
    read_query_cache,
    remove_query_cache,
    read_virtualizations,
    remove_virtualization,
    write_indexing,
    write_query_cache,
    write_virtualization,
)
from maplib.config import probe

TEMPLATE = """@prefix tpl: <http://example.org/templates#> .
@prefix ex: <http://example.org/> .
@prefix ottr: <http://ns.ottr.xyz/0.4/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
tpl:readings [ xsd:string ?id, xsd:dateTime ?timestamp, xsd:double ?value ] :: {
  ottr:Triple(?id, ex:hasValue, ?v) , ottr:Triple(?v, ex:timestamp, ?timestamp) , ottr:Triple(?v, ex:value, ?value)
} ."""


def snowflake(**kw) -> VirtualizationConfig:
    return VirtualizationConfig(
        iri="urn:example:readings",
        database="snowflake",
        label='Readings "north"\nsecond line',
        parameters={"account": "acme", "warehouse": "wh"},
        secrets={"password": "SNOWFLAKE_PASSWORD"},
        resources=[VirtualizationResource(name="readings", sql="SELECT id, ts AS timestamp, value FROM r", template=TEMPLATE)],
        **kw,
    )


def test_round_trip():
    m = Model()
    write_virtualization(m, snowflake())
    write_virtualization(
        m,
        VirtualizationConfig(
            iri="urn:example:cdf",
            database="cdf",
            parameters={"project": "p"},
            secrets={"client_secret": "CDF_SECRET"},
            resources=[VirtualizationResource(name="ts", template="urn:example:template", options={"id_type": "id"})],
            enabled=False,
        ),
    )
    got = {v.iri: v for v in read_virtualizations(m)}
    assert got["urn:example:readings"] == snowflake()
    cdf = got["urn:example:cdf"]
    assert cdf.database == "cdf" and cdf.enabled is False
    assert cdf.resources[0].template == "urn:example:template"
    assert cdf.resources[0].options == {"id_type": "id"}
    assert cdf.resources[0].sql is None


def test_the_configuration_is_in_its_own_graph():
    m = Model()
    write_virtualization(m, snowflake())
    in_config = m.query(f"SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ ?s ?p ?o }} }}")["n"][0]
    in_default = m.query("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }")["n"][0]
    assert in_config > 0 and in_default == 0
    types = m.query(
        f"SELECT ?t WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ <urn:example:readings> a ?t }} }} ORDER BY ?t"
    )["t"].to_list()
    assert types == [f"<{CONFIG}SnowflakeDatabase>", f"<{CONFIG}Virtualization>"]


def test_no_secret_values_are_stored():
    m = Model()
    write_virtualization(m, snowflake())
    values = m.query(f"SELECT ?o WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ ?s ?p ?o }} }}")["o"].to_list()
    # the name of the variable (quoted: the column mixes IRIs and literals), nothing else
    assert '"SNOWFLAKE_PASSWORD"' in values


def test_writing_replaces_and_removing_removes():
    m = Model()
    write_virtualization(m, snowflake())
    write_virtualization(m, snowflake(enabled=False))
    configs = read_virtualizations(m)
    assert len(configs) == 1 and configs[0].enabled is False
    remove_virtualization(m, "urn:example:readings")
    assert read_virtualizations(m) == []
    assert m.query(f"SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ ?s ?p ?o }} }}")["n"][0] == 0


def test_invalid_configurations():
    m = Model()
    with pytest.raises(ValueError, match="Unknown database"):
        write_virtualization(m, VirtualizationConfig(iri="urn:x", database="oracle"))
    with pytest.raises(ValueError, match="needs SQL"):
        write_virtualization(
            m, VirtualizationConfig(iri="urn:x", database="duckdb", resources=[VirtualizationResource(name="r", template=TEMPLATE)])
        )
    with pytest.raises(ValueError, match="different names"):
        r = VirtualizationResource(name="r", template="urn:t")
        write_virtualization(m, VirtualizationConfig(iri="urn:x", database="opcua", resources=[r, r]))


def test_indexing():
    m = Model()
    assert read_indexing(m) == IndexingConfig()
    write_indexing(m, IndexingConfig(fts=True))
    write_indexing(m, IndexingConfig(fts=True, geo=False))
    assert read_indexing(m) == IndexingConfig(fts=True, geo=False)


def test_query_cache():
    m = Model()
    assert read_query_cache(m) is None
    write_query_cache(m, QueryCacheConfig())
    assert read_query_cache(m) == QueryCacheConfig()
    c = QueryCacheConfig(enabled=False, max_entries=10, max_bytes=1 << 20, max_entry_bytes=1000,
                         ttl=30.5, min_run_time=0.01, cache_virtualized=True)
    write_query_cache(m, c)
    assert read_query_cache(m) == c
    # one item: writing again replaces it
    n = m.query(f"SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ ?c a <{CONFIG}QueryCache> }} }}")
    assert n["n"][0] == 1
    write_query_cache(m, QueryCacheConfig(ttl=None))
    assert read_query_cache(m).ttl is None
    remove_query_cache(m)
    assert read_query_cache(m) is None
    with pytest.raises(ValueError):
        write_query_cache(m, QueryCacheConfig(max_entries=-1))
    with pytest.raises(ValueError):
        write_query_cache(m, QueryCacheConfig(ttl=-1.0))


def test_query_cache_is_applied():
    m = Model()
    if not hasattr(m, "set_query_cache"):
        pytest.skip("this maplib has no query cache")
    m.reads('<urn:a> <urn:p> "x" .', format="ntriples")
    apply_config(m)
    assert not m.get_query_cache().enabled  # nothing configured: the model's setting stays
    write_query_cache(m, QueryCacheConfig(max_entries=7, ttl=60.0))
    assert apply_config(m) == []
    o = m.get_query_cache()
    assert (o.enabled, o.max_entries, o.ttl) == (True, 7, 60.0)
    m.query("SELECT * WHERE { ?s <urn:p> ?o }")
    m.query("SELECT * WHERE { ?s <urn:p> ?o }")
    assert m.query_cache_stats()["hits"] == 1
    apply_query_cache(m, QueryCacheConfig(enabled=False))
    assert not m.get_query_cache().enabled


def test_object_indexes():
    m = Model()
    m.reads('<urn:a> <urn:p> "x" .\n<urn:a> <urn:q> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .', format="ntriples")
    rdfs_label = "http://www.w3.org/2000/01/rdf-schema#label"
    write_indexing(m, IndexingConfig(object_sort_some=["urn:p", rdfs_label, "urn:p"]))
    assert read_indexing(m) == IndexingConfig(object_sort_some=["http://www.w3.org/2000/01/rdf-schema#label", "urn:p"])
    write_indexing(m, IndexingConfig(object_sort_all=True))
    assert read_indexing(m) == IndexingConfig(object_sort_all=True)
    # the configured indexes are created
    assert apply_config(m) == []
    assert m.query('SELECT ?s WHERE { ?s <urn:p> "x" }').height == 1


def test_a_missing_secret_is_named():
    m = Model()
    write_virtualization(m, snowflake())
    with pytest.raises(KeyError, match="SNOWFLAKE_PASSWORD"):
        apply_config(m, environ={})


def test_the_configuration_travels_with_the_model(tmp_path):
    m = Model()
    write_virtualization(m, snowflake())
    nq = tmp_path / "config.nt"
    m.write(str(nq), format="ntriples", graph=CONFIG_GRAPH)
    m2 = Model()
    m2.read(str(nq), graph=CONFIG_GRAPH)
    assert read_virtualizations(m2) == read_virtualizations(m)


CIM = "http://iec.ch/TC57/2013/CIM-schema-cim16#"
UNITS = {CIM + "Measurement.unitSymbol": CIM + "UnitSymbol.W", CIM + "Measurement.unitMultiplier": CIM + "UnitMultiplier.k"}


def duckdb_config(path: str = "unused.duckdb", units=None) -> VirtualizationConfig:
    return VirtualizationConfig(
        iri="urn:example:duck",
        database="duckdb",
        parameters={"database": path},
        resources=[
            VirtualizationResource(
                name="readings",
                sql="SELECT id, ts AS timestamp, value FROM r",
                template=TEMPLATE,
                properties=dict(UNITS if units is None else units),
            )
        ],
    )


def test_resource_properties_round_trip():
    m = Model()
    write_virtualization(m, duckdb_config())
    (got,) = read_virtualizations(m)
    assert got == duckdb_config()
    assert got.resources[0].properties == UNITS


def test_resource_properties_must_be_iris():
    m = Model()
    with pytest.raises(ValueError, match="absolute IRIs"):
        write_virtualization(m, duckdb_config(units={CIM + "Measurement.unitSymbol": "W"}))


def test_applying_properties_writes_them_on_the_resource_objects():
    m = Model()
    m.reads(
        """<urn:m1> <https://datatreehouse.github.io/maplib/vocab#hasResource> "readings" .
<urn:m2> <https://datatreehouse.github.io/maplib/vocab#hasResource> "readings" .
<urn:other> <https://datatreehouse.github.io/maplib/vocab#hasResource> "other" .""",
        format="ntriples",
    )
    apply_properties(m, duckdb_config())
    q = f"SELECT ?s ?u WHERE {{ ?s <{CIM}Measurement.unitSymbol> ?u }} ORDER BY ?s"
    df = m.query(q)
    assert df["s"].to_list() == ["<urn:m1>", "<urn:m2>"]
    assert set(df["u"].to_list()) == {f"<{CIM}UnitSymbol.W>"}
    # applying a changed unit replaces it, and applying twice adds nothing
    apply_properties(m, duckdb_config(units={CIM + "Measurement.unitSymbol": CIM + "UnitSymbol.V"}))
    apply_properties(m, duckdb_config(units={CIM + "Measurement.unitSymbol": CIM + "UnitSymbol.V"}))
    assert m.query(q)["u"].to_list() == [f"<{CIM}UnitSymbol.V>"] * 2
    # nothing in the config graph
    assert m.query(f"SELECT ?s WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ ?s ?p ?o }} }}").height == 0


def _virtualization_available() -> bool:
    try:
        import duckdb  # noqa: F401
        from maplib import virtualization  # noqa: F401

        return hasattr(virtualization, "DuckDBDatabase")
    except ImportError:
        return False


@pytest.mark.skipif(not _virtualization_available(), reason="needs maplib Enterprise Edition and duckdb")
@pytest.mark.parametrize(
    "kind,cast,datatype,expected",
    [
        ("Analog", "DOUBLE", "double", [480.31, 466.86]),
        ("Discrete", "BIGINT", "integer", [480, 467]),
        ("StringMeasurement", "VARCHAR", "string", ["480.31", "466.86"]),
    ],
)
def test_units_and_measurement_kinds_with_duckdb(tmp_path, kind, cast, datatype, expected):
    import duckdb

    path = str(tmp_path / "readings.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE r (meter_id VARCHAR, ts TIMESTAMPTZ, kw DOUBLE)")
    con.execute(
        "INSERT INTO r VALUES ('M-1', '2026-11-01 00:00:00+00', 480.31), ('M-1', '2026-11-01 00:15:00+00', 466.86)"
    )
    con.close()
    values = {"Analog": "AnalogValue", "Discrete": "DiscreteValue", "StringMeasurement": "StringMeasurementValue"}[kind]
    template = f"""@prefix tpl: <http://example.org/templates#> .
@prefix ottr: <http://ns.ottr.xyz/0.4/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix cim: <{CIM}> .
tpl:readings [ xsd:string ?id, xsd:dateTime ?timestamp, xsd:{datatype} ?value ] :: {{
  ottr:Triple(?id, cim:{kind}.{kind}Values, ?point) ,
  ottr:Triple(?point, cim:MeasurementValue.timeStamp, ?timestamp) ,
  ottr:Triple(?point, cim:{values}.value, ?value)
}} ."""
    config = VirtualizationConfig(
        iri="urn:example:duck",
        database="duckdb",
        parameters={"database": path, "timezone": "UTC"},
        resources=[
            VirtualizationResource(
                name="readings",
                sql=f"SELECT q.meter_id AS id, q.ts AS timestamp, CAST(q.kw AS {cast}) AS value FROM (SELECT * FROM r) AS q",
                template=template,
                properties=UNITS,
            )
        ],
    )
    m = Model()
    m.reads(
        """<urn:m1> <https://datatreehouse.github.io/maplib/vocab#hasResource> "readings" .
<urn:m1> <https://datatreehouse.github.io/maplib/vocab#hasExternalId> "M-1" .""",
        format="ntriples",
    )
    write_virtualization(m, config)
    assert apply_config(m) == ["urn:example:duck"]
    df = m.query(
        f"""PREFIX cim: <{CIM}>
SELECT ?u ?k ?t ?v WHERE {{
  ?m cim:Measurement.unitSymbol ?u ; cim:Measurement.unitMultiplier ?k ; cim:{kind}.{kind}Values ?p .
  ?p cim:MeasurementValue.timeStamp ?t ; cim:{values}.value ?v
}} ORDER BY ?t"""
    )
    assert df["u"].to_list() == [f"<{CIM}UnitSymbol.W>"] * 2
    assert df["k"].to_list() == [f"<{CIM}UnitMultiplier.k>"] * 2
    assert df["v"].to_list() == expected
    # saving and applying again (the template is in the model already) works, and changes units
    config.resources[0].properties = {CIM + "Measurement.unitSymbol": CIM + "UnitSymbol.V", CIM + "Measurement.unitMultiplier": CIM + "UnitMultiplier.k"}
    write_virtualization(m, config)
    assert apply_config(m) == ["urn:example:duck"]
    assert set(m.query(f"SELECT ?u WHERE {{ ?m <{CIM}Measurement.unitSymbol> ?u }}")["u"].to_list()) == {f"<{CIM}UnitSymbol.V>"}


def test_fabric_round_trip_probe_and_construction(monkeypatch):
    """Microsoft Fabric, with the database classes of maplib.virtualization replaced by fakes."""
    import types

    import maplib
    import polars as pl
    from maplib.config import _virtualized_database, probe

    made = []

    class FakeLakehouse:
        sql_dialect = "duckdb"

        def __init__(self, **kwargs):
            made.append(("lakehouse", kwargs))
            self.table_urls = {"readings": "abfss://...", "sensors": "abfss://..."}
            self.queries = []

        def query(self, sql):
            made.append(("lakehouse query", sql))
            return pl.DataFrame({"id": ["M-1"], "value": [1.5]})

    class FakeWarehouse:
        sql_dialect = "fabric"

        def __init__(self, **kwargs):
            made.append(("warehouse", kwargs))

        def query(self, sql):
            made.append(("warehouse query", sql))
            return pl.DataFrame({"ok": [1]})

    fake = types.SimpleNamespace(FabricLakehouseDatabase=FakeLakehouse, FabricDatabase=FakeWarehouse)
    monkeypatch.setattr(maplib, "virtualization", fake, raising=False)

    lakehouse = VirtualizationConfig(
        iri="urn:example:onelake",
        database="fabric_lakehouse",
        parameters={"workspace": "Plant", "lakehouse": "Sensors", "tenant_id": "t", "client_id": "c",
                    "authentication": "service_principal"},
        secrets={"client_secret": "FABRIC_CLIENT_SECRET"},
        resources=[VirtualizationResource(name="readings", sql="SELECT id, ts AS timestamp, value FROM readings",
                                          template=TEMPLATE)],
    )
    warehouse = VirtualizationConfig(
        iri="urn:example:warehouse",
        database="fabric_warehouse",
        parameters={"server": "abc.datawarehouse.fabric.microsoft.com", "database": "wh", "tenant_id": "t",
                    "client_id": "c", "authentication": "service_principal"},
        secrets={"client_secret": "FABRIC_CLIENT_SECRET"},
        resources=[VirtualizationResource(name="readings", sql="SELECT id, ts AS timestamp, value FROM dbo.r",
                                          template=TEMPLATE)],
    )
    m = Model()
    write_virtualization(m, lakehouse)
    write_virtualization(m, warehouse)
    got = {v.iri: v for v in read_virtualizations(m)}
    assert got["urn:example:onelake"] == lakehouse and got["urn:example:warehouse"] == warehouse
    ttl = m.writes(format="ntriples", graph=CONFIG_GRAPH)
    assert f"<{CONFIG}FabricLakehouseDatabase>" in ttl and f"<{CONFIG}FabricDatabase>" in ttl
    assert "s3cret" not in ttl

    environ = {"FABRIC_CLIENT_SECRET": "s3cret"}
    # without SQL, a lakehouse lists its tables
    r = probe(lakehouse, environ=environ)
    assert r["sample"] == [["readings"], ["sensors"]]
    assert made[0] == ("lakehouse", {**lakehouse.parameters, "client_secret": "s3cret"})
    r = probe(lakehouse, sql="SELECT * FROM readings;", environ=environ)
    assert made[-1] == ("lakehouse query", "SELECT * FROM (SELECT * FROM readings) AS q LIMIT 5")
    assert r["columns"] == [{"name": "id", "type": "String"}, {"name": "value", "type": "Float64"}]
    # T-SQL has TOP instead of LIMIT
    probe(warehouse, sql="SELECT * FROM dbo.r", environ=environ)
    assert made[-1] == ("warehouse query", "SELECT TOP 5 * FROM (SELECT * FROM dbo.r) AS q")
    assert made[-2] == ("warehouse", {**warehouse.parameters, "client_secret": "s3cret"})

    seen = {}

    class FakeVirtualizedDatabase:
        def __init__(self, database, resource_sql_map=None):
            seen["database"], seen["resource_sql_map"] = database, resource_sql_map

    monkeypatch.setattr(maplib, "VirtualizedDatabase", FakeVirtualizedDatabase)
    _virtualized_database(lakehouse, environ)
    assert isinstance(seen["database"], FakeLakehouse)
    assert seen["resource_sql_map"] == {"readings": "SELECT id, ts AS timestamp, value FROM readings"}
    _virtualized_database(warehouse, environ)
    assert isinstance(seen["database"], FakeWarehouse)


def test_kusto_round_trip_probe_and_construction(monkeypatch):
    """Azure Data Explorer / Fabric Eventhouse, with maplib.virtualization.KustoDatabase replaced by a fake."""
    import types

    import maplib
    import polars as pl
    from maplib.config import QUERY_LANGUAGES, _virtualized_database, probe

    made = []

    class FakeKusto:
        sql_dialect = "kusto"

        def __init__(self, **kwargs):
            made.append(("kusto", kwargs))

        def query(self, kql):
            made.append(("query", kql))
            return pl.DataFrame({"id": ["M-1"], "timestamp": [1], "value": [1.5]})

    monkeypatch.setattr(maplib, "virtualization", types.SimpleNamespace(KustoDatabase=FakeKusto), raising=False)
    kql = "Telemetry | project id=DeviceId, timestamp=Timestamp, value=Value"
    config = VirtualizationConfig(
        iri="urn:example:adx",
        database="kusto",
        parameters={"cluster": "https://plant.westeurope.kusto.windows.net", "database": "telemetry",
                    "tenant_id": "t", "client_id": "c"},
        secrets={"client_secret": "KUSTO_CLIENT_SECRET"},
        resources=[VirtualizationResource(name="readings", sql=kql, template=TEMPLATE)],
    )
    assert QUERY_LANGUAGES["kusto"] == "kql"
    m = Model()
    write_virtualization(m, config)
    assert read_virtualizations(m) == [config]
    nt = m.writes(format="ntriples", graph=CONFIG_GRAPH)
    assert f"<{CONFIG}KustoDatabase>" in nt and "s3cret" not in nt
    with pytest.raises(ValueError, match="needs KQL"):
        write_virtualization(m, VirtualizationConfig(
            iri="urn:example:adx2", database="kusto", parameters=config.parameters,
            resources=[VirtualizationResource(name="readings", template=TEMPLATE)]))

    environ = {"KUSTO_CLIENT_SECRET": "s3cret"}
    r = probe(config, environ=environ)
    assert r["message"] == "Connected"
    assert made[0] == ("kusto", {**config.parameters, "client_secret": "s3cret"})
    assert made[1] == ("query", "print ok = 1")
    r = probe(config, sql=kql + ";", environ=environ)
    assert made[-1] == ("query", f"({kql})\n| take 5")
    assert [c["name"] for c in r["columns"]] == ["id", "timestamp", "value"]

    seen = {}

    class FakeVirtualizedDatabase:
        def __init__(self, database, resource_sql_map=None):
            seen["database"], seen["resource_sql_map"] = database, resource_sql_map

    monkeypatch.setattr(maplib, "VirtualizedDatabase", FakeVirtualizedDatabase)
    _virtualized_database(config, environ)
    assert isinstance(seen["database"], FakeKusto)
    assert seen["resource_sql_map"] == {"readings": kql}


def test_ip21_is_applied_with_map_ip21_and_probed(monkeypatch):
    """IP.21 is self-describing: applying calls maplib.ip21.map_ip21 with typed arguments."""
    import sys
    import types

    from maplib.config import SELF_DESCRIBING, probe

    calls = {}

    def map_ip21(m, **kwargs):
        calls["map"] = kwargs

    class IP21Client:
        def __init__(self, url, data_source, **kwargs):
            calls["client"] = (url, data_source, kwargs)

        def browse(self, tag_filter, max_tags):
            calls["browse"] = (tag_filter, max_tags)
            return ["FIC101.PV", "TI200"]

    monkeypatch.setitem(sys.modules, "maplib.ip21", types.SimpleNamespace(map_ip21=map_ip21, IP21Client=IP21Client))
    assert "ip21" in SELF_DESCRIBING
    config = VirtualizationConfig(
        iri="urn:example:ip21",
        database="ip21",
        parameters={"url": "https://ip21/ProcessData/AtProcessDataREST.dll", "data_source": "IP21",
                    "tag_filter": "FIC*", "username": "u", "verify_ssl": "false", "read_metadata": "true",
                    "max_tags": "100", "timeout": "30"},
        secrets={"password": "IP21_PASSWORD"},
    )
    m = Model()
    write_virtualization(m, config)
    (read,) = read_virtualizations(m)
    assert read.database == "ip21" and read.secrets == {"password": "IP21_PASSWORD"}
    assert apply_config(m, environ={"IP21_PASSWORD": "secret"}) == ["urn:example:ip21"]
    assert calls["map"] == {
        "url": "https://ip21/ProcessData/AtProcessDataREST.dll", "data_source": "IP21", "tag_filter": "FIC*",
        "username": "u", "password": "secret", "verify_ssl": False, "read_metadata": True, "max_tags": 100,
        "timeout": 30.0,
    }
    result = probe(config, environ={"IP21_PASSWORD": "secret"})
    assert result["sample"] == [["FIC101.PV"], ["TI200"]]
    assert calls["browse"] == ("FIC*", 5)
    assert calls["client"][2]["password"] == "secret" and calls["client"][2]["verify_ssl"] is False


def pi_points_config() -> VirtualizationConfig:
    return VirtualizationConfig(
        iri="urn:example:pi",
        database="pi_points",
        label="PI historian",
        parameters={"url": "https://pi.example.com/piwebapi", "data_server": "PI1", "name_filter": "FIC*",
                    "point_attributes": "pointsource, location1", "username": "me", "verify_ssl": "false",
                    "default_start_time": "*-7d", "timeout": "30"},
        secrets={"password": "OSIPI_PASSWORD"},
    )


def test_pi_points_round_trip_and_apply():
    m = Model()
    write_virtualization(m, pi_points_config())
    assert read_virtualizations(m) == [pi_points_config()]

    class Recorder:
        """Stands in for the model, recording how map_pi_points is called."""

        def __init__(self):
            self.calls = []

        def map_pi_points(self, **kwargs):
            self.calls.append(kwargs)

    from maplib.config import apply_virtualization

    r = Recorder()
    apply_virtualization(r, pi_points_config(), environ={"OSIPI_PASSWORD": "secret"})
    assert r.calls == [{
        "url": "https://pi.example.com/piwebapi", "data_server": "PI1", "name_filter": "FIC*",
        "point_attributes": ["pointsource", "location1"], "username": "me", "verify_ssl": False,
        "default_start_time": "*-7d", "timeout": 30.0, "password": "secret",
    }]


def test_pi_points_probe():
    """probe lists a few points of the data server, from a mock PI Web API."""
    import base64
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import parse_qs, urlparse

    seen = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            url = urlparse(self.path)
            seen.append((url.path, parse_qs(url.query), self.headers.get("Authorization")))
            if url.path == "/piwebapi/dataservers":
                body = {"Items": [{"WebId": "DS0", "Name": "PI0", "Path": "\\\\PI0"},
                                  {"WebId": "DS1", "Name": "PI1", "Path": "\\\\PI1"}]}
            elif url.path == "/piwebapi/dataservers/DS1/points":
                body = {"Items": [{"Name": "FIC101.PV", "Descriptor": "Feed flow", "PointType": "Float32",
                                   "EngineeringUnits": "m3/h"}]}
            else:
                body = {"Errors": ["not found"]}
            data = json.dumps(body).encode()
            self.send_response(200 if "Items" in body else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        config = pi_points_config()
        config.parameters["url"] = f"http://127.0.0.1:{server.server_address[1]}/piwebapi"
        result = probe(config, environ={"OSIPI_PASSWORD": "secret"})
    finally:
        server.shutdown()
        server.server_close()
    assert result["message"] == "Connected to the PI Data Archive PI1"
    assert result["sample"] == [["FIC101.PV", "Feed flow", "Float32", "m3/h"]]
    path, query, authorization = seen[-1]
    assert path == "/piwebapi/dataservers/DS1/points"
    assert query == {"nameFilter": ["FIC*"], "maxCount": ["5"]}
    assert authorization == "Basic " + base64.b64encode(b"me:secret").decode()


def test_influxdb_round_trip_probe_and_construction(monkeypatch):
    """InfluxDB 3 (SQL) and InfluxDB 2 (Flux), with the database classes of maplib.virtualization replaced by fakes."""
    import types

    import maplib
    import polars as pl
    from maplib.config import _virtualized_database, probe

    made = []

    class FakeInfluxDB:
        sql_dialect = "influxdb"

        def __init__(self, **kwargs):
            made.append(("influxdb", kwargs))

        def query(self, sql):
            made.append(("influxdb query", sql))
            if "information_schema" in sql:
                return pl.DataFrame({"table_name": ["readings", "status"]})
            return pl.DataFrame({"id": ["M-1"], "value": [1.5]})

    class FakeRecord:
        def __init__(self, value):
            self.value = value

        def get_value(self):
            return self.value

    class FakeQueryApi:
        def query(self, flux, org=None):
            made.append(("flux", flux, org))
            return [types.SimpleNamespace(records=[FakeRecord("status"), FakeRecord("readings")])]

    class FakeInfluxDB2:
        def __init__(self, resources=None, **kwargs):
            made.append(("influxdb2", resources, kwargs))
            self.client = types.SimpleNamespace(query_api=FakeQueryApi)

    fake = types.SimpleNamespace(InfluxDBDatabase=FakeInfluxDB, InfluxDB2Database=FakeInfluxDB2)
    monkeypatch.setattr(maplib, "virtualization", fake, raising=False)

    v3 = VirtualizationConfig(
        iri="urn:example:influxdb",
        database="influxdb",
        parameters={"host": "http://localhost:8181", "database": "plant"},
        secrets={"token": "INFLUXDB_TOKEN"},
        resources=[VirtualizationResource(name="readings", sql="SELECT sensor AS id, time AS timestamp, value FROM readings",
                                          template=TEMPLATE)],
    )
    v2 = VirtualizationConfig(
        iri="urn:example:influxdb2",
        database="influxdb2",
        parameters={"url": "http://localhost:8086", "org": "acme", "bucket": "plant", "measurement": "readings",
                    "id": "sensor", "field": "value", "timeout": "30000"},
        secrets={"token": "INFLUXDB2_TOKEN"},
        resources=[
            VirtualizationResource(name="readings", template=TEMPLATE),
            VirtualizationResource(name="status", template=TEMPLATE, options={"measurement": "status", "id": "_field"}),
        ],
    )
    m = Model()
    write_virtualization(m, v3)
    write_virtualization(m, v2)
    got = {v.iri: v for v in read_virtualizations(m)}
    assert got["urn:example:influxdb"] == v3 and got["urn:example:influxdb2"] == v2
    ttl = m.writes(format="ntriples", graph=CONFIG_GRAPH)
    assert f"<{CONFIG}InfluxDBDatabase>" in ttl and f"<{CONFIG}InfluxDB2Database>" in ttl
    assert "s3cret" not in ttl
    with pytest.raises(ValueError, match="needs SQL"):
        write_virtualization(m, VirtualizationConfig(iri="urn:x", database="influxdb",
                                                     resources=[VirtualizationResource(name="r", template=TEMPLATE)]))
    with pytest.raises(ValueError, match="needs a measurement"):
        write_virtualization(m, VirtualizationConfig(iri="urn:x", database="influxdb2", parameters={"bucket": "b"},
                                                     resources=[VirtualizationResource(name="r", template=TEMPLATE)]))

    environ = {"INFLUXDB_TOKEN": "s3cret", "INFLUXDB2_TOKEN": "s3cret2"}
    # without SQL, InfluxDB 3 lists its measurements
    r = probe(v3, environ=environ)
    assert r["sample"] == [["readings"], ["status"]]
    assert made[0] == ("influxdb", {**v3.parameters, "token": "s3cret"})
    r = probe(v3, sql="SELECT * FROM readings;", environ=environ)
    assert made[-1] == ("influxdb query", "SELECT * FROM (SELECT * FROM readings) AS q LIMIT 5")
    assert r["columns"] == [{"name": "id", "type": "String"}, {"name": "value", "type": "Float64"}]
    # InfluxDB 2 lists the measurements of the bucket with Flux
    r = probe(v2, environ=environ)
    assert r["sample"] == [["readings"], ["status"]]
    assert made[-1] == ("flux", 'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "plant")', "acme")
    resources = {"readings": {"measurement": "readings", "id": "sensor", "field": "value"},
                 "status": {"measurement": "status", "id": "_field", "field": "value"}}
    assert made[-2] == ("influxdb2", resources, {"url": "http://localhost:8086", "org": "acme", "bucket": "plant",
                                                 "timeout": 30000, "token": "s3cret2"})

    seen = {}

    class FakeVirtualizedDatabase:
        def __init__(self, database, resource_sql_map=None):
            seen["database"], seen["resource_sql_map"] = database, resource_sql_map

    monkeypatch.setattr(maplib, "VirtualizedDatabase", FakeVirtualizedDatabase)
    _virtualized_database(v3, environ)
    assert isinstance(seen["database"], FakeInfluxDB)
    assert seen["resource_sql_map"] == {"readings": "SELECT sensor AS id, time AS timestamp, value FROM readings"}
    _virtualized_database(v2, environ)
    assert isinstance(seen["database"], FakeInfluxDB2) and seen["resource_sql_map"] is None


def test_insights_hub_round_trip_probe_and_apply():
    """Siemens Insights Hub: probed against a mock of its token and Asset Management endpoints, and
    applied through Model.map_insights_hub (replaced by a fake)."""
    import base64
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from maplib.config import apply_virtualization, probe

    seen = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0)).decode()
            seen.append(("POST", self.path, self.headers.get("Authorization"), body))
            self.reply(200, {"access_token": "tok", "expires_in": 1800})

        def do_GET(self):
            seen.append(("GET", self.path, self.headers.get("Authorization"), None))
            if self.headers.get("Authorization") != "Bearer tok":
                return self.reply(401, {})
            self.reply(200, {"_embedded": {"assets": [{"assetId": "a1", "name": "Pump1", "typeId": "plant.Pump"}]},
                             "page": {"size": 5, "totalElements": 42, "totalPages": 9, "number": 0}})

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    config = VirtualizationConfig(
        iri="urn:example:insights_hub",
        database="insights_hub",
        label="Insights Hub",
        parameters={"host": url, "tenant": "plant", "client_id": "plant-svc", "token_url": url + "/oauth/token",
                    "default_start_time": "P7D", "aggregate_pushdown": "true", "timeout": "10"},
        secrets={"client_secret": "INSIGHTS_HUB_CLIENT_SECRET"},
    )
    environ = {"INSIGHTS_HUB_CLIENT_SECRET": "s3cret"}
    try:
        m = Model()
        write_virtualization(m, config)
        assert read_virtualizations(m) == [config]
        ttl = m.writes(format="ntriples", graph=CONFIG_GRAPH)
        assert f"<{CONFIG}InsightsHubDatabase>" in ttl
        assert "s3cret" not in ttl

        r = probe(config, environ=environ)
        assert r["message"] == "Connected to the Insights Hub tenant plant, which has 42 assets"
        assert r["sample"] == [["a1", "Pump1", "plant.Pump"]]
        basic = "Basic " + base64.b64encode(b"plant-svc:s3cret").decode()
        assert seen[0] == ("POST", "/oauth/token", basic, "grant_type=client_credentials")
        assert seen[1][:3] == ("GET", "/api/assetmanagement/v3/assets?page=0&size=5", "Bearer tok")
    finally:
        server.shutdown()
        server.server_close()

    calls = []

    class FakeModel:
        def map_insights_hub(self, **kwargs):
            calls.append(kwargs)

    apply_virtualization(FakeModel(), config, environ)
    assert calls == [{"host": url, "tenant": "plant", "client_id": "plant-svc", "token_url": url + "/oauth/token",
                      "default_start_time": "P7D", "aggregate_pushdown": True, "timeout": 10.0,
                      "client_secret": "s3cret"}]


def test_timescale_round_trip_probe_and_construction(monkeypatch):
    """TimescaleDB, with maplib.virtualization.TimescaleDatabase replaced by a fake."""
    import types

    import maplib
    import polars as pl
    from maplib.config import DATABASE_PARAMETERS, DATABASE_SECRETS, _virtualized_database, probe

    made = []

    class FakeTimescale:
        sql_dialect = "timescale"

        def __init__(self, **kwargs):
            made.append(("timescale", kwargs))

        def query(self, sql):
            made.append(("query", sql))
            return pl.DataFrame({"id": ["M-1"], "timestamp": [1], "value": [1.5]})

        def close(self):
            made.append(("close",))

    monkeypatch.setattr(maplib, "virtualization", types.SimpleNamespace(TimescaleDatabase=FakeTimescale), raising=False)
    sql = "SELECT meter AS id, time AS timestamp, kwh AS value FROM readings"
    config = VirtualizationConfig(
        iri="urn:example:timescale",
        database="timescale",
        parameters={"host": "db.example.com", "port": "5432", "database": "plant", "user": "reader",
                    "sslmode": "require"},
        secrets={"password": "TIMESCALE_PASSWORD"},
        resources=[VirtualizationResource(name="readings", sql=sql, template=TEMPLATE)],
    )
    assert set(config.parameters) <= set(DATABASE_PARAMETERS["timescale"])
    assert DATABASE_SECRETS["timescale"] == ["password"]
    m = Model()
    write_virtualization(m, config)
    assert read_virtualizations(m) == [config]
    nt = m.writes(format="ntriples", graph=CONFIG_GRAPH)
    assert f"<{CONFIG}TimescaleDatabase>" in nt and "s3cret" not in nt
    with pytest.raises(ValueError):
        write_virtualization(m, VirtualizationConfig(
            iri="urn:example:timescale2", database="timescale", parameters=config.parameters,
            resources=[VirtualizationResource(name="readings", template=TEMPLATE)]))

    environ = {"TIMESCALE_PASSWORD": "s3cret"}
    r = probe(config, environ=environ)
    assert r["message"] == "Connected"
    assert made[:3] == [("timescale", {**config.parameters, "password": "s3cret"}), ("query", "SELECT 1 AS ok"),
                        ("close",)]
    r = probe(config, sql=sql + ";", environ=environ)
    assert made[-2:] == [("query", f"SELECT * FROM ({sql}) AS q LIMIT 5"), ("close",)]
    assert [c["name"] for c in r["columns"]] == ["id", "timestamp", "value"]

    seen = {}

    class FakeVirtualizedDatabase:
        def __init__(self, database, resource_sql_map=None):
            seen["database"], seen["resource_sql_map"] = database, resource_sql_map

    monkeypatch.setattr(maplib, "VirtualizedDatabase", FakeVirtualizedDatabase)
    _virtualized_database(config, environ)
    assert isinstance(seen["database"], FakeTimescale)
    assert seen["resource_sql_map"] == {"readings": sql}


# ── common questions ────────────────────────────────────────────────

from maplib.config import (  # noqa: E402
    QUESTION_DOMAINS,
    CommonQuestion,
    query_form,
    question_iri,
    read_questions,
    remove_question,
    write_question,
)

def question(**kw) -> CommonQuestion:
    return CommonQuestion(
        **{
            "iri": "urn:example:question:labels",
            "name": 'Labels "all"',
            "description": "Every label\nof the model",
            "query": "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n# the labels\nSELECT ?s ?label WHERE { ?s rdfs:label ?label }",
            **kw,
        }
    )


def test_question_round_trip():
    m = Model()
    write_question(m, question(applies_to=QUESTION_DOMAINS["cim"], order=3))
    [q] = read_questions(m)
    assert q == question(applies_to=QUESTION_DOMAINS["cim"], order=3)
    assert q.form == "select" and q.domain == "cim"
    # replaced whole, by IRI
    write_question(m, question(name="Labels", description="", query="ASK { ?s ?p ?o }"))
    [q] = read_questions(m)
    assert (q.name, q.description, q.query, q.applies_to, q.order, q.form) == (
        "Labels", "", "ASK { ?s ?p ?o }", None, None, "ask")
    remove_question(m, q.iri)
    assert read_questions(m) == []


def test_question_vocabulary():
    """SHACL-AF executables with Dublin Core titles and descriptions, in the config graph."""
    m = Model()
    write_question(m, question(applies_to=QUESTION_DOMAINS["era"], bundled=True, order=1))
    df = m.query(
        """PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mcfg: <https://datatreehouse.github.io/maplib/config#>
SELECT ?title ?label ?description ?select ?applies ?bundled ?order WHERE { GRAPH mcfg:ConfigGraph {
  ?q a mcfg:CommonQuestion , sh:SPARQLSelectExecutable ; dcterms:title ?title ; rdfs:label ?label ;
     dcterms:description ?description ; sh:select ?select ; mcfg:appliesTo ?applies ;
     mcfg:bundled ?bundled ; sh:order ?order } }"""
    )
    assert df.height == 1
    r = df.row(0, named=True)
    assert r["title"] == r["label"] == 'Labels "all"'
    assert r["applies"] == "<http://data.europa.eu/949/>"
    assert r["bundled"] is True and r["order"] == 1
    assert "SELECT ?s ?label" in r["select"]
    # the question is in the config graph only
    assert m.query("SELECT ?s WHERE { ?s ?p ?o }").height == 0


@pytest.mark.parametrize(
    "query,form",
    [
        ("SELECT * WHERE { ?s ?p ?o }", "select"),
        ("prefix ex: <http://example.org/#a>\n# a comment # with hashes\n  construct { ?s ?p ?o } WHERE { ?s ?p ?o }", "construct"),
        ("BASE <http://example.org/>\nPREFIX : <http://example.org/>\nASK { :a ?p ?o }", "ask"),
        ("DESCRIBE <http://example.org/a>", "describe"),
    ],
)
def test_query_form(query, form):
    assert query_form(query) == form
    m = Model()
    write_question(m, question(query=query))
    [q] = read_questions(m)
    assert q.query == query and q.form == form


@pytest.mark.parametrize(
    "kw",
    [
        {"name": " "},
        {"query": ""},
        {"query": "INSERT DATA { <urn:a> <urn:b> <urn:c> }"},
        {"query": "PREFIX ex: <http://example.org/>\nDELETE WHERE { ?s ?p ?o }"},
        {"iri": "not an iri"},
        {"applies_to": "http://example.org/ space"},
    ],
)
def test_question_rejected(kw):
    m = Model()
    with pytest.raises(ValueError):
        write_question(m, question(**kw))
    assert read_questions(m) == []


def test_read_questions_order_and_domain():
    m = Model()
    write_question(m, question(iri="urn:q:b", name="B"))
    write_question(m, question(iri="urn:q:a", name="a"))
    write_question(m, question(iri="urn:q:cim", name="Z", applies_to=QUESTION_DOMAINS["cim"], order=0))
    write_question(m, question(iri="urn:q:era", name="Y", applies_to=QUESTION_DOMAINS["era"], order=0))
    assert [q.iri for q in read_questions(m)] == ["urn:q:era", "urn:q:cim", "urn:q:a", "urn:q:b"]
    # a domain: its own questions and those for any model
    assert [q.iri for q in read_questions(m, "cim")] == ["urn:q:cim", "urn:q:a", "urn:q:b"]
    assert [q.iri for q in read_questions(m, QUESTION_DOMAINS["era"])] == ["urn:q:era", "urn:q:a", "urn:q:b"]


def test_question_iri():
    a, b = question_iri("Lines and their lengths!"), question_iri("Lines and their lengths!")
    assert a.startswith("urn:maplib:config:question:lines-and-their-lengths-") and a != b
    assert question_iri("§§").startswith("urn:maplib:config:question:question-")


# ── prefixes ────────────────────────────────────────────────────────

from maplib.config import (  # noqa: E402
    PREFIXES,
    PrefixConfig,
    apply_prefixes,
    generate_prefixes,
    namespace_of,
    prefix_name_for,
    read_prefixes,
    remove_prefix,
    write_prefix,
    write_prefixes,
)


def test_prefixes_are_shacl_declarations_in_the_config_graph():
    m = Model()
    write_prefix(m, PrefixConfig("ex", "http://example.org/"))
    write_prefix(m, PrefixConfig("plant", "http://example.org/plant/"))
    assert read_prefixes(m) == [
        PrefixConfig("ex", "http://example.org/"),
        PrefixConfig("plant", "http://example.org/plant/"),
    ]
    df = m.query(
        f"""PREFIX sh: <http://www.w3.org/ns/shacl#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?name (STR(?ns) AS ?namespace) WHERE {{ GRAPH <{CONFIG_GRAPH}> {{
  <{PREFIXES}> a owl:Ontology ; sh:declare ?d . ?d a sh:PrefixDeclaration ; sh:prefix ?name ; sh:namespace ?ns
}} }} ORDER BY ?name"""
    )
    assert df["name"].to_list() == ["ex", "plant"]
    assert df["namespace"].to_list() == ["http://example.org/", "http://example.org/plant/"]


def test_prefix_replace_and_remove():
    m = Model()
    write_prefix(m, PrefixConfig("ex", "http://example.org/a/"))
    write_prefix(m, PrefixConfig("ex", "http://example.org/b/"))
    assert read_prefixes(m) == [PrefixConfig("ex", "http://example.org/b/")]
    with pytest.raises(ValueError, match="already has the prefix ex"):
        write_prefix(m, PrefixConfig("other", "http://example.org/b/"))
    remove_prefix(m, "ex")
    assert read_prefixes(m) == []
    # nothing is left of the declaration, and the ontology declares nothing
    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    left = m.query(f"SELECT ?s ?p ?o WHERE {{ GRAPH <{CONFIG_GRAPH}> {{ ?s ?p ?o FILTER(?p != <{rdf_type}>) }} }}")
    assert left.height == 0


def test_prefixes_survive_serialization(tmp_path):
    m = Model()
    m.reads("<http://example.org/a> <http://example.org/p> 1 .", format="turtle")
    write_prefixes(m, [PrefixConfig("ex", "http://example.org/"), PrefixConfig("g", "urn:g:", generated=True)])
    path = tmp_path / "model.trig"
    m.write(str(path), format="trig")
    m2 = Model()
    m2.read(str(path))
    assert read_prefixes(m2) == [PrefixConfig("ex", "http://example.org/"), PrefixConfig("g", "urn:g:", generated=True)]


@pytest.mark.parametrize(
    "prefix",
    [PrefixConfig("", "http://example.org/"), PrefixConfig("1x", "http://example.org/"),
     PrefixConfig("a b", "http://example.org/"), PrefixConfig("ex.", "http://example.org/"),
     PrefixConfig("ex", "not an iri"), PrefixConfig("ex", "relative/")],
)
def test_prefix_rejected(prefix):
    with pytest.raises(ValueError):
        write_prefix(Model(), prefix)


def test_apply_prefixes():
    m = Model()
    m.reads("<http://example.org/a> <http://example.org/p> 1 .", format="turtle")
    write_prefix(m, PrefixConfig("ex", "http://example.org/"))
    apply_config(m)
    assert m.query("SELECT ?s WHERE { ?s ex:p 1 }").height == 1
    apply_prefixes(m, [PrefixConfig("ex2", "http://example.org/")])
    assert m.query("SELECT ?s WHERE { ?s ex2:p 1 }").height == 1


def test_namespace_of_and_prefix_names():
    assert namespace_of("http://example.org/plant/p1") == "http://example.org/plant/"
    assert namespace_of("http://example.org/onto#Pump") == "http://example.org/onto#"
    assert namespace_of("urn:acme:pump:1") == "urn:acme:pump:"
    assert namespace_of("http://example.org/") is None
    assert namespace_of("urn:x") is None
    assert prefix_name_for("http://example.org/plant/") == "plant"
    assert prefix_name_for("https://data.example.no/ontology/v2/") == "example"
    assert prefix_name_for("urn:acme:pump:") == "pump"
    assert prefix_name_for("http://example.org/meter-readings/") == "mr"
    assert prefix_name_for("http://www.w3.org/2004/02/skos/core#") == "skos"


def test_generate_prefixes():
    m = Model()
    m.reads(
        """@prefix ex: <http://example.org/plant/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix cim: <http://iec.ch/TC57/CIM100#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
ex:b1 a cim:Breaker ; rdfs:label "B1" ; cim:IdentifiedObject.name "b1" .
ex:b2 a cim:Breaker ; skos:prefLabel "B2" .
<urn:acme:pump:1> a <http://example.org/onto/Pump> .
""",
        format="turtle",
        graph="urn:graph:upload",
    )
    write_prefix(m, PrefixConfig("plant", "http://example.org/other/"))
    new = generate_prefixes(m, graph="urn:graph:upload")
    got = {p.namespace: p.name for p in new}
    assert all(p.generated for p in new)
    assert got["http://www.w3.org/1999/02/22-rdf-syntax-ns#"] == "rdf"
    assert got["http://www.w3.org/2000/01/rdf-schema#"] == "rdfs"
    assert got["http://www.w3.org/2004/02/skos/core#"] == "skos"
    # CGMES 3 gets cim: when it is free
    assert got["http://iec.ch/TC57/CIM100#"] == "cim"
    # plant is taken, so the namespace gets the next free name
    assert got["http://example.org/plant/"] == "plant2"
    assert got["urn:acme:pump:"] == "pump"
    assert {p.name: p.namespace for p in read_prefixes(m)}["plant2"] == "http://example.org/plant/"
    # again: everything has a prefix already
    assert generate_prefixes(m, graph="urn:graph:upload") == []
    # the default graph is empty
    assert generate_prefixes(Model()) == []


def test_generate_prefixes_limit_and_dry_run():
    m = Model()
    m.reads(
        "\n".join(f"<http://example.org/ns{i}/a> <http://example.org/ns{i}/p> {i} ." for i in range(5)),
        format="turtle",
    )
    new = generate_prefixes(m, max_generated=2, write=False)
    assert len(new) == 2
    assert read_prefixes(m) == []


def test_generate_prefixes_over_several_graphs():
    m = Model()
    m.reads("<http://example.org/a/x> <http://example.org/a/p> 1 .", format="turtle", graph="urn:g:a")
    m.reads("<http://example.org/b/x> <http://example.org/b/p> 1 .", format="turtle")
    got = {p.namespace: p.name for p in generate_prefixes(m, graph=["urn:g:a", None], write=False)}
    assert got == {"http://example.org/a/": "a", "http://example.org/b/": "b"}


# ── maplib's one table of default prefixes ──────────────────────────

from maplib import well_known_prefixes  # noqa: E402

CIM100 = "http://iec.ch/TC57/CIM100#"
CIM16 = "http://iec.ch/TC57/2013/CIM-schema-cim16#"


def test_well_known_prefixes_come_from_rust():
    known = well_known_prefixes()
    assert known["cim"] == CIM100
    assert known["cim16"] == CIM16
    assert known["rdfs"] == "http://www.w3.org/2000/01/rdf-schema#"
    assert known["opc"] == "http://opcfoundation.org/UA/"
    assert len(set(known.values())) == len(known)
    # maplib.config reads the same table, from namespace to name
    from maplib import config

    assert config.WELL_KNOWN_PREFIXES == {ns: name for name, ns in known.items()}
    assert not hasattr(config, "PREFERRED_PREFIXES")


def test_generated_prefixes_name_cim16_cim16():
    m = Model()
    m.reads(
        f"""@prefix c16: <{CIM16}> .
<http://example.org/grid/b1> a c16:Breaker ; c16:IdentifiedObject.name "b1" .""",
        format="turtle",
    )
    got = {p.namespace: p.name for p in generate_prefixes(m, write=False)}
    assert got[CIM16] == "cim16"
    assert "cim" not in got.values()


def test_rdfxml_names_well_known_namespaces():
    m = Model()
    m.reads(
        f"""@prefix c16: <{CIM16}> .
<http://example.org/grid/b1> a c16:Breaker ; c16:IdentifiedObject.name "b1" ;
    <http://example.org/other/p> "x" .""",
        format="turtle",
    )
    xml = m.writes(format="rdf/xml")
    assert f'xmlns:cim16="{CIM16}"' in xml
    assert "<cim16:IdentifiedObject.name>" in xml
    # namespaces maplib has no name for are still numbered
    assert 'xmlns:ns1="http://example.org/other/"' in xml
