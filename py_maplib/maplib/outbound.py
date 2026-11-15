"""
The outbound connection policy: one place where an application decides which hosts maplib may
connect to, against server-side request forgery (SSRF).

maplib checks the final scheme, host and port of every connection it makes for a configuration
or a query before connecting: the databases of ``maplib.config`` (``probe``,
``apply_virtualization``, ``apply_config``), the virtualized databases of ``maplib.virtualization``,
``Model.map_osipi`` / ``map_pi_points`` / ``map_insights_hub`` / ``map_cdf``, IP.21, OPC UA, and
SPARQL ``SERVICE`` and ``LOAD``. The host is resolved and every address it resolves to is checked.
Redirects are checked too, by the clients maplib controls (urllib, requests and reqwest), and
reqwest's DNS resolver checks the addresses it actually connects to (against DNS rebinding).
Connections made by the database's own client library (snowflake-connector, azure-kusto-data,
cognite-sdk, psycopg, the InfluxDB clients, DuckDB's azure extension, ...) are checked by the host
they are given before they connect; what such a library does afterwards (redirects, other hosts it
discovers) is up to it.

Cloud instance metadata services (169.254.169.254, fd00:ec2::254, 100.100.100.200,
metadata.google.internal, ...) are always refused, whatever the policy says.

>>> import maplib
>>> maplib.set_outbound_policy("public", allow=["*.snowflakecomputing.com", "10.20.0.0/16"])

A refused connection raises :class:`OutboundError`, a ``PermissionError``.
"""

import ipaddress
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Mapping, Optional, Tuple, Union

__all__ = [
    "OutboundError",
    "OutboundPolicy",
    "METADATA_HOSTS",
    "METADATA_ADDRESSES",
    "set_outbound_policy",
    "get_outbound_policy",
    "check_outbound",
    "check_url",
    "check_host",
    "urlopen",
    "guard_session",
    "connection_targets",
    "check_connection",
    "check_cognite_client",
]

#: Host names of cloud metadata services, refused in every mode.
METADATA_HOSTS = (
    "metadata.google.internal",
    "metadata.goog",
    "metadata",
    "instance-data",
    "instance-data.ec2.internal",
)

#: Addresses of cloud metadata services, refused in every mode.
METADATA_ADDRESSES = ("169.254.169.254", "169.254.170.2", "100.100.100.200", "192.0.0.192", "fd00:ec2::254")

#: The default port of the schemes maplib connects with, for URLs without a port.
DEFAULT_PORTS = {
    "http": 80,
    "https": 443,
    "ws": 80,
    "wss": 443,
    "opc.tcp": 4840,
    "postgres": 5432,
    "postgresql": 5432,
    "mysql": 3306,
    "mssql": 1433,
    "snowflake": 443,
    "databricks": 443,
    "bigquery": 443,
    "trino": 8080,
}

Check = Callable[[str, str, int], None]


class OutboundError(PermissionError):
    """A connection to a host the outbound policy does not allow."""


@dataclass(frozen=True)
class OutboundPolicy:
    """
    The outbound policy.

    :param mode: ``any`` (any host but cloud metadata services), ``public`` (no loopback, private,
        link-local or other local addresses) or ``allowlist`` (only the hosts in ``allow``).
    :param allow: Host names, ``*.domain`` (the domain and the hosts under it), addresses or CIDR
        ranges, allowed in every mode (but never metadata services). A range allows a host when
        every address the host resolves to is in the listed ranges.
    :param check: Called with the scheme, host and port of each connection made from Python,
        after the checks above; raise (e.g. OutboundError) to refuse it. Connections made by
        maplib's Rust clients (SPARQL SERVICE and LOAD, the PI Web API and Insights Hub mappings,
        Model.serve's endpoints) apply mode and allow, but do not call it.
    """

    mode: str = "any"
    allow: Tuple[str, ...] = ()
    check: Optional[Check] = None


_policy = OutboundPolicy()


def _native():
    from maplib import maplib as native

    return native


def set_outbound_policy(
    mode: Union[str, Check, OutboundPolicy, None] = "any",
    allow: Iterable[str] = (),
    check: Optional[Check] = None,
) -> OutboundPolicy:
    """
    Sets the process-wide outbound policy, for every connection maplib makes. See
    :class:`OutboundPolicy`. ``set_outbound_policy()`` (or ``None``) restores the default: any
    host but cloud metadata services.

    :param mode: ``any``, ``public`` or ``allowlist``; or a function ``check(scheme, host, port)``
        that raises to refuse a connection (the mode is then ``any``); or an OutboundPolicy.
    :param allow: Hosts, ``*.domain`` patterns, addresses and CIDR ranges always allowed (with
        ``allowlist`` the only ones allowed).
    :param check: A function ``check(scheme, host, port)`` called for each connection made from
        Python after the checks of mode and allow; it raises to refuse the connection.
    :return: The policy set.
    """
    global _policy
    if isinstance(mode, OutboundPolicy):
        policy = mode
    elif mode is None:
        policy = OutboundPolicy()
    elif callable(mode):
        policy = OutboundPolicy("any", tuple(allow), mode)
    else:
        if isinstance(allow, str):
            allow = [a for a in allow.replace(",", " ").split() if a]
        policy = OutboundPolicy(str(mode).strip().lower() or "any", tuple(str(a).strip() for a in allow), check)
    try:
        _native()._set_outbound_policy(policy.mode, list(policy.allow))
    except ValueError as e:
        raise ValueError(str(e)) from None
    _policy = policy
    return policy


def get_outbound_policy() -> OutboundPolicy:
    """The process-wide outbound policy."""
    return _policy


def _is_metadata(host: str) -> bool:
    h = host.strip().strip("[]").rstrip(".").lower()
    if h in METADATA_HOSTS:
        return True
    try:
        a = ipaddress.ip_address(h)
    except ValueError:
        return False
    if isinstance(a, ipaddress.IPv6Address) and a.ipv4_mapped is not None:
        a = a.ipv4_mapped
    return str(a) in METADATA_ADDRESSES


def check_outbound(scheme: str, host: str, port: Optional[int] = None) -> List[str]:
    """
    Checks a connection to the host with the outbound policy: refuses cloud metadata services,
    resolves the host and checks its addresses with the mode and allow list, then calls the
    policy's check function. Raises OutboundError when the connection is not allowed.

    :param scheme: The scheme, e.g. https, or a database name; for the check function and messages.
    :param host: The host name or address.
    :param port: The port, by default the scheme's.
    :return: The addresses the host resolved to (empty when it could not be resolved and the
        mode does not need them).
    """
    scheme = (scheme or "").lower()
    host = (host or "").strip()
    if port is None:
        port = DEFAULT_PORTS.get(scheme, 0)
    if not host:
        raise OutboundError(f"No host to connect to ({scheme or 'no scheme'})")
    if _is_metadata(host):
        raise OutboundError(f"Connections to {host} (a cloud metadata service) are not allowed")
    try:
        addresses = _native()._check_outbound(scheme, host, int(port))
    except PermissionError as e:
        raise OutboundError(str(e)) from None
    if _policy.check is not None:
        _policy.check(scheme, host.strip("[]"), int(port))
    return list(addresses)


def _split(url: str):
    url = url.strip()
    parts = urllib.parse.urlsplit(url if "://" in url else "//" + url)
    try:
        port = parts.port
    except ValueError as e:
        raise OutboundError(f"{url!r} is not a URL maplib can connect to: {e}") from None
    return parts.scheme.lower(), parts.hostname or "", port


def check_url(url: str, default_port: Optional[int] = None) -> List[str]:
    """
    Checks a connection to the URL (or ``host[:port]``) with the outbound policy, see
    :func:`check_outbound`.
    """
    scheme, host, port = _split(str(url))
    if not host:
        raise OutboundError(f"{url!r} has no host to connect to")
    return check_outbound(scheme, host, port if port is not None else default_port)


def check_host(host: str, scheme: str = "", port: Optional[int] = None) -> List[str]:
    """
    Checks a host given in any of the forms configurations use: a URL, ``host:port`` or a bare
    host name.
    """
    host = str(host).strip()
    if "://" in host or "/" in host or (host.count(":") == 1 and not host.startswith("[")):
        s, h, p = _split(host)
        return check_outbound(s or scheme, h, p if p is not None else port)
    return check_outbound(scheme, host, port)


_HOST_KEYS = ("url", "host", "endpoint", "server", "server_hostname", "cluster", "base_url", "token_url")


def _target(value: str, scheme: str = "", port: Optional[int] = None) -> Optional[Tuple[str, str, Optional[int]]]:
    value = str(value or "").strip()
    if not value:
        return None
    if "://" in value or "/" in value or (value.count(":") == 1 and not value.startswith("[")):
        s, h, p = _split(value)
        return (s or scheme, h, p if p is not None else port) if h else None
    return scheme, value, port


def _conninfo(value: str) -> Dict[str, str]:
    """The keywords of a libpq conninfo string, e.g. host=db port=5432 dbname=x."""
    out = {}
    for part in value.split():
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip().lower()] = v.strip().strip("'")
    return out


def connection_targets(database: str, parameters: Mapping[str, object]) -> List[Tuple[str, str, Optional[int]]]:
    """
    The (scheme, host, port) a database of ``maplib.config`` connects to with the parameters:
    the final hosts, e.g. ``<account>.snowflakecomputing.com`` for a Snowflake account, the
    OneLake endpoint of a Fabric lakehouse, or both the base_url and the token_url of Cognite
    Data Fusion. Local databases (DuckDB, SQLite) have none.
    """
    kw = {k: str(v).strip() for k, v in parameters.items() if v is not None and str(v).strip()}
    db = (database or "").lower()
    targets: List[Optional[Tuple[str, str, Optional[int]]]] = []
    if db == "sql":
        url = kw.get("url", "")
        scheme = url.split("://", 1)[0].split("+", 1)[0].lower() if "://" in url else ""
        if scheme not in ("sqlite", "duckdb") and url:
            targets.append(_target(url, scheme))
    elif db == "duckdb":
        pass
    elif db == "snowflake":
        if kw.get("host"):
            targets.append(_target(kw["host"], "https", int(kw["port"]) if kw.get("port") else 443))
        elif kw.get("account"):
            account = kw["account"].lower()
            if not account.endswith((".snowflakecomputing.com", ".snowflakecomputing.cn")):
                account += ".snowflakecomputing.com"
            targets.append(("https", account, int(kw["port"]) if kw.get("port") else 443))
    elif db == "bigquery":
        targets.append(("https", "bigquery.googleapis.com", 443))
    elif db == "databricks":
        targets.append(_target(kw.get("server_hostname", ""), "https", 443))
    elif db == "timescale":
        url = kw.get("url", "")
        if "://" in url:
            targets.append(_target(url, "postgresql", 5432))
        elif url:
            info = _conninfo(url)
            for h in info.get("host", "").split(","):
                if h and not h.startswith("/"):
                    targets.append(("postgresql", h, int(info["port"]) if info.get("port", "").isdigit() else 5432))
        if kw.get("host") and not kw["host"].startswith("/"):
            targets.append(("postgresql", kw["host"], int(kw["port"]) if kw.get("port", "").isdigit() else 5432))
    elif db == "fabric_lakehouse":
        root = kw.get("root", "")
        if "://" in root:
            parts = urllib.parse.urlsplit(root)
            host = parts.netloc.rsplit("@", 1)[-1]
            if host:
                targets.append(_target(host, "https", 443))
        elif not root:
            targets.append(_target(kw.get("endpoint") or "onelake.dfs.fabric.microsoft.com", "https", 443))
    elif db == "fabric_warehouse":
        server = kw.get("server", "")
        if server.lower().startswith("tcp:"):
            server = server[4:]
        host, _, port = server.partition(",")
        if host:
            targets.append(("mssql", host.strip(), int(port) if port.strip().isdigit() else 1433))
    elif db == "kusto":
        targets.append(_target(kw.get("cluster", ""), "https", 443))
    elif db == "influxdb":
        targets.append(_target(kw.get("host", ""), "https", 443))
    elif db == "influxdb2":
        targets.append(_target(kw.get("url", ""), "http", 8086))
    elif db == "opcua":
        targets.append(_target(kw.get("endpoint", ""), "opc.tcp", 4840))
    elif db == "cdf":
        targets.append(_target(kw.get("base_url", ""), "https", 443))
        targets.append(_target(kw.get("token_url", ""), "https", 443))
    elif db in ("osipi", "pi_points", "ip21"):
        targets.append(_target(kw.get("url", ""), "https", 443))
    elif db == "insights_hub":
        host = kw.get("host", "")
        targets.append(_target(host if "://" in host else "https://" + host, "https", 443) if host else None)
        if kw.get("token_url"):
            targets.append(_target(kw["token_url"], "https", 443))
        elif not kw.get("token") and not kw.get("app_name") and host:
            hostname = _split(host if "://" in host else "https://" + host)[1]
            if hostname.startswith("gateway.") and kw.get("tenant"):
                targets.append(("https", f"{kw['tenant']}.piam.{hostname[len('gateway.'):]}", 443))
    else:
        for key in _HOST_KEYS:
            if kw.get(key):
                targets.append(_target(kw[key]))
    return [t for t in targets if t is not None and t[1]]


def check_connection(database: str, parameters: Mapping[str, object]) -> None:
    """Checks every host the database connects to with the parameters (see
    :func:`connection_targets`) with the outbound policy. Raises OutboundError."""
    for scheme, host, port in connection_targets(database, parameters):
        check_outbound(scheme, host, port)


def check_cognite_client(client) -> None:
    """Checks the hosts a cognite-sdk client connects to (its base_url, and the token_url of
    OAuth client credentials) with the outbound policy."""
    config = getattr(client, "config", None)
    base_url = getattr(config, "base_url", None)
    if base_url:
        check_url(str(base_url), 443)
    token_url = getattr(getattr(config, "credentials", None), "token_url", None)
    if isinstance(token_url, str) and token_url:
        check_url(token_url, 443)


class _GuardedRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        check_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def urlopen(request, timeout: Optional[float] = None, context=None):
    """``urllib.request.urlopen`` with the outbound policy applied to the URL and every redirect."""
    url = request.full_url if isinstance(request, urllib.request.Request) else str(request)
    check_url(url)
    handlers: list = [_GuardedRedirectHandler()]
    if context is not None:
        handlers.append(urllib.request.HTTPSHandler(context=context))
    opener = urllib.request.build_opener(*handlers)
    if timeout is None:
        return opener.open(request)
    return opener.open(request, timeout=timeout)


def guard_session(session):
    """
    Applies the outbound policy to a ``requests.Session``: every request it sends, redirects
    included, is checked first. Returns the session.
    """
    from requests.adapters import HTTPAdapter

    class _GuardedAdapter(HTTPAdapter):
        def send(self, request, *args, **kwargs):
            check_url(request.url)
            return super().send(request, *args, **kwargs)

    for prefix in ("http://", "https://"):
        old = session.adapters.get(prefix)
        adapter = _GuardedAdapter()
        if old is not None and hasattr(old, "max_retries"):
            adapter.max_retries = old.max_retries
        session.mount(prefix, adapter)
    return session
