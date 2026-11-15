"""The outbound connection policy (maplib.set_outbound_policy): cloud metadata services are refused
for every connector, allow-listed hosts are accepted, and redirects to refused hosts are not followed."""

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import maplib
from maplib import Model, OutboundError, set_outbound_policy
from maplib.config import VirtualizationConfig, probe
from maplib.outbound import check_host, check_outbound, check_url, connection_targets, get_outbound_policy

METADATA = ["169.254.169.254", "[fd00:ec2::254]", "100.100.100.200", "metadata.google.internal"]


@pytest.fixture(autouse=True)
def default_policy():
    set_outbound_policy()
    yield
    set_outbound_policy()


@contextmanager
def mock_server(routes):
    """An HTTP server on 127.0.0.1 answering GET and POST: routes maps a path to (status, headers, body)."""
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def answer(self):
            path = self.path.split("?", 1)[0]
            seen.append(path)
            status, headers, body = routes.get(path, (404, {}, {"error": "not found"}))
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = answer

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            self.answer()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", seen
    finally:
        server.shutdown()
        server.server_close()


def redirect_to(location):
    return 302, {"Location": location}, b""


# ── the policy itself ───────────────────────────────────────────────


@pytest.mark.parametrize("mode", ["any", "public", "allowlist"])
@pytest.mark.parametrize("host", METADATA)
def test_metadata_is_refused_in_every_mode(mode, host):
    # even when the allow list names it
    set_outbound_policy(mode, allow=[host.strip("[]"), "169.254.0.0/16", "100.64.0.0/10", "*.internal"])
    with pytest.raises(OutboundError, match="metadata"):
        check_url(f"http://{host}/latest/meta-data/")


def test_mapped_metadata_addresses_are_refused():
    for url in ["http://[::ffff:169.254.169.254]/", "http://[::ffff:a9fe:a9fe]/", "http://169.254.169.254.:80/"]:
        with pytest.raises(OutboundError):
            check_url(url)


def test_modes_and_allow_list():
    assert check_url("http://127.0.0.1:9/") == ["127.0.0.1"]
    set_outbound_policy("public")
    with pytest.raises(OutboundError, match="local or private"):
        check_url("http://127.0.0.1:9/")
    with pytest.raises(OutboundError, match="local or private"):
        check_host("10.1.2.3:5432", "postgresql")
    set_outbound_policy("public", allow=["127.0.0.0/8"])
    check_url("http://127.0.0.1:9/")
    set_outbound_policy("allowlist", allow="127.0.0.1, *.example.com")
    check_url("http://127.0.0.1:9/")
    with pytest.raises(OutboundError, match="not in the hosts"):
        check_outbound("https", "8.8.8.8", 443)
    assert get_outbound_policy().allow == ("127.0.0.1", "*.example.com")
    with pytest.raises(ValueError):
        set_outbound_policy("nowhere")
    with pytest.raises(ValueError):
        set_outbound_policy("public", allow=["10.0.0.0/40"])
    # a refused policy leaves the previous one in place
    assert get_outbound_policy().mode == "allowlist"


def test_check_function():
    calls = []

    def check(scheme, host, port):
        calls.append((scheme, host, port))
        if host == "127.0.0.2":
            raise OutboundError("not that one")

    set_outbound_policy(check)
    check_url("https://127.0.0.1/x")
    with pytest.raises(OutboundError, match="not that one"):
        check_url("opc.tcp://127.0.0.2")
    assert calls == [("https", "127.0.0.1", 443), ("opc.tcp", "127.0.0.2", 4840)]
    # metadata is refused before the function is asked
    with pytest.raises(OutboundError, match="metadata"):
        check_url("http://169.254.169.254/")
    assert len(calls) == 2


def test_connection_targets_are_the_final_hosts():
    assert connection_targets("snowflake", {"account": "xy12345.eu-west-1"}) == [
        ("https", "xy12345.eu-west-1.snowflakecomputing.com", 443)
    ]
    assert connection_targets("fabric_warehouse", {"server": "tcp:abc.datawarehouse.fabric.microsoft.com,1444"}) == [
        ("mssql", "abc.datawarehouse.fabric.microsoft.com", 1444)
    ]
    assert connection_targets("fabric_lakehouse", {"workspace": "w", "lakehouse": "l"}) == [
        ("https", "onelake.dfs.fabric.microsoft.com", 443)
    ]
    assert connection_targets("cdf", {"base_url": "https://api.cognitedata.com", "token_url": "https://login.example/t"}) == [
        ("https", "api.cognitedata.com", 443),
        ("https", "login.example", 443),
    ]
    assert connection_targets("insights_hub", {"host": "gateway.eu1.mindsphere.io", "tenant": "t"}) == [
        ("https", "gateway.eu1.mindsphere.io", 443),
        ("https", "t.piam.eu1.mindsphere.io", 443),
    ]
    assert connection_targets("timescale", {"url": "host=db1,db2 port=6432 dbname=x"}) == [
        ("postgresql", "db1", 6432),
        ("postgresql", "db2", 6432),
    ]
    assert connection_targets("sql", {"url": "sqlite:///local.db"}) == []
    assert connection_targets("duckdb", {"database": "x.duckdb"}) == []


# ── every connector refuses metadata services before connecting ─────────


CONNECTORS = {
    "sql": lambda h: {"url": f"postgresql://user@{h}:5432/db"},
    "snowflake": lambda h: {"account": "acme", "host": h, "user": "u"},
    "databricks": lambda h: {"server_hostname": h, "http_path": "/sql/1.0/warehouses/x"},
    "timescale": lambda h: {"host": h, "database": "db"},
    "fabric_lakehouse": lambda h: {"workspace": "w", "lakehouse": "l", "endpoint": h},
    "fabric_warehouse": lambda h: {"server": h, "database": "db"},
    "kusto": lambda h: {"cluster": f"https://{h}", "database": "db"},
    "influxdb": lambda h: {"host": f"http://{h}:8181", "database": "db"},
    "influxdb2": lambda h: {"url": f"http://{h}:8086", "bucket": "b", "measurement": "m"},
    "opcua": lambda h: {"endpoint": f"opc.tcp://{h}:4840/"},
    "cdf": lambda h: {"project": "p", "base_url": f"http://{h}", "token_url": "https://login.example/t",
                      "client_id": "c", "client_secret": "s"},
    "osipi": lambda h: {"url": f"http://{h}/piwebapi", "asset_database": "db"},
    "pi_points": lambda h: {"url": f"http://{h}/piwebapi"},
    "ip21": lambda h: {"url": f"http://{h}/ProcessData/AtProcessDataREST.dll", "data_source": "IP21"},
    "insights_hub": lambda h: {"host": h, "tenant": "t", "token": "x"},
}


@pytest.mark.parametrize("database", sorted(CONNECTORS))
@pytest.mark.parametrize("host", ["169.254.169.254", "[fd00:ec2::254]", "100.100.100.200", "metadata.google.internal"])
def test_probe_refuses_metadata(database, host):
    bare = host.strip("[]") if database in ("timescale", "fabric_warehouse", "databricks", "snowflake", "fabric_lakehouse") else host
    config = VirtualizationConfig(iri="urn:x", database=database, parameters=CONNECTORS[database](bare))
    with pytest.raises(OutboundError, match="metadata"):
        probe(config, sql="SELECT 1", environ={})


def test_apply_config_refuses_metadata():
    from maplib.config import apply_virtualization

    config = VirtualizationConfig(iri="urn:x", database="osipi", parameters=CONNECTORS["osipi"]("169.254.169.254"))
    with pytest.raises(OutboundError):
        apply_virtualization(Model(), config, environ={})


def test_mappings_refuse_metadata():
    m = Model()
    with pytest.raises(OutboundError):
        m.map_osipi("http://169.254.169.254/piwebapi")
    with pytest.raises(OutboundError):
        m.map_pi_points("http://[fd00:ec2::254]/piwebapi")
    with pytest.raises(OutboundError):
        m.map_insights_hub("metadata.google.internal", "tenant", token="x")
    with pytest.raises(OutboundError):
        m.map_insights_hub("gateway.eu1.mindsphere.io", "t", client_id="c", client_secret="s",
                           token_url="http://100.100.100.200/token")

    class Config:
        base_url = "http://169.254.169.254"
        credentials = None

    class Client:
        config = Config()

    with pytest.raises(OutboundError):
        m.map_cdf(Client())


def test_opcua_refuses_metadata():
    import asyncio

    from maplib.opcua import OPCUAHistoryDatabase

    db = OPCUAHistoryDatabase("opc.tcp://169.254.169.254:4840/")
    with pytest.raises(OutboundError):
        asyncio.run(db._read(["ns=2;i=1"], None, None))


def test_sparql_endpoints_refuse_metadata():
    m = Model()
    with pytest.raises(Exception, match="metadata"):
        m.query_external("SELECT * WHERE { ?s ?p ?o }", "http://169.254.169.254/sparql")
    with pytest.raises(Exception, match="metadata"):
        m.query("SELECT * WHERE { ?s ?p ?o }", endpoint="http://169.254.169.254:7878")


# ── allow-listed hosts are accepted ───────────────────────────────────


def pi_points_routes():
    return {
        "/piwebapi/dataservers": (200, {"Content-Type": "application/json"},
                                  {"Items": [{"WebId": "DS0", "Name": "PI0", "Path": "\\\\PI0"}]}),
        "/piwebapi/dataservers/DS0/points": (200, {"Content-Type": "application/json"},
                                             {"Items": [{"Name": "T1", "PointType": "Float32"}]}),
    }


def test_allow_listed_host_is_accepted():
    with mock_server(pi_points_routes()) as (base, seen):
        config = VirtualizationConfig(iri="urn:x", database="pi_points", parameters={"url": base + "/piwebapi"})
        set_outbound_policy("allowlist", allow=["127.0.0.1"])
        assert probe(config, environ={})["message"] == "Connected to the PI Data Archive PI0"
        set_outbound_policy("public")
        with pytest.raises(OutboundError):
            probe(config, environ={})
        set_outbound_policy("allowlist", allow=["10.0.0.0/8"])
        with pytest.raises(OutboundError):
            probe(config, environ={})
    assert seen == ["/piwebapi/dataservers", "/piwebapi/dataservers/DS0/points"]


# ── redirects to refused hosts are not followed ───────────────────────


@pytest.mark.parametrize("target", ["http://169.254.169.254/latest/meta-data/", "http://[fd00:ec2::254]/"])
def test_urllib_redirect_to_metadata_is_refused(target):
    with mock_server({"/piwebapi/": redirect_to(target), "/piwebapi": redirect_to(target)}) as (base, seen):
        config = VirtualizationConfig(iri="urn:x", database="osipi", parameters={"url": base + "/piwebapi"})
        with pytest.raises(OutboundError, match="metadata"):
            probe(config, environ={})
    assert seen


def test_urllib_redirect_to_unlisted_host_is_refused():
    with mock_server({"/piwebapi/": redirect_to("http://192.0.2.1:9/x")}) as (base, _):
        set_outbound_policy("allowlist", allow=["127.0.0.1"])
        config = VirtualizationConfig(iri="urn:x", database="osipi", parameters={"url": base + "/piwebapi"})
        with pytest.raises(OutboundError, match="192.0.2.1"):
            probe(config, environ={})


def test_reqwest_redirect_to_metadata_is_refused():
    with mock_server({"/sparql": redirect_to("http://169.254.169.254/sparql"),
                      "/arrow/query": redirect_to("http://169.254.169.254/arrow/query")}) as (base, seen):
        m = Model()
        with pytest.raises(Exception, match="metadata"):
            m.query_external("SELECT * WHERE { ?s ?p ?o }", base + "/sparql")
        with pytest.raises(Exception, match="metadata"):
            m.query("SELECT * WHERE { ?s ?p ?o }", endpoint=base)
    assert "/sparql" in seen and "/arrow/query" in seen


def test_reqwest_allow_list_applies_to_resolved_addresses():
    # localhost resolves to loopback addresses, which public mode refuses
    with mock_server({"/sparql": (200, {"Content-Type": "application/sparql-results+json"},
                                  {"head": {"vars": ["s"]}, "results": {"bindings": []}})}) as (base, seen):
        m = Model()
        assert m.query_external("SELECT ?s WHERE { ?s ?p ?o }", base + "/sparql").height == 0
        set_outbound_policy("public")
        with pytest.raises(Exception, match="outbound policy|local or private"):
            m.query_external("SELECT ?s WHERE { ?s ?p ?o }", base + "/sparql")
    assert seen == ["/sparql"]


def test_top_level_exports():
    assert maplib.set_outbound_policy is set_outbound_policy
    assert issubclass(maplib.OutboundError, PermissionError)
