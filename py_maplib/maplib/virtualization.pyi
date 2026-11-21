"""
Ready-made databases and SQL dialects for virtualizing data in SQL databases with VirtualizedDatabase,
and KustoDatabase for Azure Data Explorer and Microsoft Fabric Eventhouses, which are queried in KQL.

Not available in the open source build: raises NotImplementedError.
"""
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Type, Union

import polars as pl

from maplib.vq import VirtualizedQuery, XSDDuration

class SQLDatabase:
    """
    A database that runs SQL and returns polars DataFrames.
    Subclass it to virtualize another database: set sql_dialect and implement query.
    """

    sql_dialect: Union[str, "SQLDialect", None]

    def query(self, sql: str) -> pl.DataFrame:
        """Run the SQL and return the result as a polars DataFrame."""

class DuckDBDatabase(SQLDatabase):
    """A DuckDB database."""

    connection: Any

    sandbox: bool

    def __init__(self, connection: Any = None, database: str = ":memory:", timezone: Optional[str] = "UTC",
                 sql_dialect: Union[str, "SQLDialect"] = "duckdb", sandbox: Union[bool, str, None] = None,
                 allowed_directories: Union[str, List[str], None] = None,
                 allowed_paths: Union[str, List[str], None] = None):
        """
        :param connection: A duckdb connection. When not given, duckdb.connect(database) is used.
        :param database: The database file to connect to when no connection is given.
        :param timezone: The time zone set for the connection, so that timestamps with time zone come back in it.
            None leaves the connection's time zone as is.
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of DuckDBDialect.
        :param sandbox: Turn off DuckDB's external access and lock the configuration before any resource SQL
            runs, so that the SQL can not read local files, ATTACH databases, COPY to files, install extensions or
            turn it back on. The database file and the allowed directories and paths stay usable. None (the
            default): sandboxed when the environment variable MAPLIB_DUCKDB_SANDBOX is set. Recommended when the
            resource SQL comes from users who should not read the process's files.
        :param allowed_directories: Directories (with subdirectories) still readable in the sandbox: a list, or
            a string with paths separated by commas or new lines.
        :param allowed_paths: Files still readable in the sandbox, as for allowed_directories.

        MAPLIB_DUCKDB_SANDBOX=1 enforces the sandbox: sandbox=False is refused, the database file must exist,
        and the database file and allowed directories and paths must lie in MAPLIB_DUCKDB_ALLOWED_DIRECTORIES
        (separated by os.pathsep); without it, no further directories or paths can be allowed.
        """

class SnowflakeDatabase(SQLDatabase):
    """A Snowflake database, queried with snowflake-connector-python. Generating the SQL needs snowflake-sqlalchemy."""

    connection: Any

    def __init__(self, connection: Any = None, sql_dialect: Union[str, "SQLDialect"] = "snowflake", **connect_kwargs: Any):
        """
        :param connection: A snowflake.connector connection. When not given, the keyword arguments are passed to
            snowflake.connector.connect (e.g. account, user, warehouse, database, schema).
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of SnowflakeDialect.
        """

class BigQueryDatabase(SQLDatabase):
    """A BigQuery database, queried with google-cloud-bigquery. Generating the SQL needs sqlalchemy-bigquery."""

    client: Any

    def __init__(self, client: Any = None, sql_dialect: Union[str, "SQLDialect"] = "bigquery", **client_kwargs: Any):
        """
        :param client: A google.cloud.bigquery.Client. When not given, one is created with the keyword arguments
            (e.g. project, credentials, location).
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of BigQueryDialect.
        """

class DatabricksDatabase(SQLDatabase):
    """A Databricks SQL warehouse, queried with databricks-sql-connector. Generating the SQL needs databricks-sqlalchemy."""

    connection: Any

    def __init__(self, connection: Any = None, sql_dialect: Union[str, "SQLDialect"] = "databricks", **connect_kwargs: Any):
        """
        :param connection: A databricks.sql connection. When not given, the keyword arguments are passed to
            databricks.sql.connect (server_hostname, http_path, access_token, ...).
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of DatabricksDialect.
        """

class InfluxDBDatabase(SQLDatabase):
    """
    InfluxDB 3 (Core, Enterprise, Cloud Dedicated, Cloud Serverless, Clustered), queried with SQL over Arrow Flight
    with influxdb3-python. The resource SQL selects from measurements, e.g.
    ``SELECT sensor AS id, time AS timestamp, value FROM readings``. Timestamps without a time zone are returned as UTC.
    """

    client: Any

    def __init__(self, client: Any = None, host: Optional[str] = None, database: Optional[str] = None,
                 token: Optional[str] = None, org: Optional[str] = None,
                 sql_dialect: Union[str, "SQLDialect"] = "influxdb", **client_kwargs: Any):
        """
        :param client: An influxdb_client_3.InfluxDBClient3, or an object with query(sql, language="sql") returning
            a pyarrow Table. When not given, one is created with host, database, token, org and the keyword arguments.
        :param host: The InfluxDB URL, e.g. http://localhost:8181
        :param database: The database (bucket) to query.
        :param token: The API token.
        :param org: The organization, only needed by some InfluxDB Cloud versions.
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of InfluxDBDialect.
        """

class FluxRequest:
    """What InfluxDB2Database reads with Flux for a basic query: the times are [start_ns, stop_ns)."""

    bucket: str
    measurement: str
    id_column: str
    field: Optional[str]
    ids: List[str]
    start_ns: int
    stop_ns: int
    extra_columns: List[str]

    def flux(self) -> str:
        """The Flux query."""

class InfluxDB2Database:
    """
    InfluxDB 2, InfluxDB Cloud (TSM) or InfluxDB 1.8 (2.0 compatible API), queried with Flux through influxdb-client.
    Use it without a resource_sql_map. Each resource is a measurement:
    ``resources={"my_resource": {"measurement": "readings", "id": "sensor", "field": "value"}}``, where id is the
    column with the external ids (a tag, "_field" (the default) or "_measurement") and field optionally restricts
    the values to one field. The resource templates have the parameters id, timestamp and value; other parameters
    are read from the tags with the same names. The time range of the filters and the ids are sent to InfluxDB,
    everything else is evaluated by maplib.
    """

    pushdowns: List[str]
    client: Any
    bucket: str
    resources: Dict[str, Dict[str, Optional[str]]]

    def __init__(self, url: Optional[str] = None, token: Optional[str] = None, org: Optional[str] = None,
                 bucket: Optional[str] = None, resources: Optional[Mapping[str, Mapping[str, str]]] = None,
                 client: Any = None, timeout: int = 60000, timestamp_parameter: str = "timestamp",
                 value_parameter: str = "value", **client_kwargs: Any):
        """
        :param url: The InfluxDB URL, e.g. http://localhost:8086
        :param token: The API token. For InfluxDB 1.8, "username:password".
        :param org: The organization.
        :param bucket: The bucket to read (for InfluxDB 1.8, "database/retention_policy").
        :param resources: The measurement, id column and field of each resource.
        :param client: An influxdb_client.InfluxDBClient. When not given, one is created with url, token, org,
            timeout and the keyword arguments.
        :param timeout: Request timeout in milliseconds.
        :param timestamp_parameter: The name of the timestamp parameter of the resource templates.
        :param value_parameter: The name of the value parameter of the resource templates.
        """
    def query(self, vq: VirtualizedQuery) -> pl.DataFrame: ...
    def flux_request(self, basic: VirtualizedQuery, filters: List[Any]) -> Optional[FluxRequest]:
        """What to read for a basic query and the filters above it, or None when nothing can match."""

class FabricLakehouseDatabase(DuckDBDatabase):
    """
    The Delta tables of a Microsoft Fabric Lakehouse, read directly from OneLake with DuckDB's delta and azure
    extensions, signed in to with Microsoft Entra ID. Each table is a view in the DuckDB connection, so resource SQL
    is DuckDB SQL over the table names, e.g. "SELECT sensor_id AS id, ts AS timestamp, value FROM readings".
    Tables of lakehouses with schemas are views in that schema ("dbo.readings"), and also have their plain name
    when it is unique.
    """

    workspace: Optional[str]
    lakehouse: Optional[str]
    root: str
    endpoint: str
    table_urls: Dict[str, str]

    def __init__(self, workspace: Optional[str] = None, lakehouse: Optional[str] = None,
                 authentication: Optional[str] = None, tenant_id: Optional[str] = None,
                 client_id: Optional[str] = None, client_secret: Optional[str] = None, chain: str = "cli",
                 tables: Union[str, Sequence[str], None] = None, root: Optional[str] = None,
                 endpoint: str = "onelake.dfs.fabric.microsoft.com", azure_transport: Optional[str] = None,
                 connection: Any = None, timezone: Optional[str] = "UTC",
                 sql_dialect: Union[str, "SQLDialect"] = "duckdb"):
        """
        :param workspace: The workspace name or id.
        :param lakehouse: The lakehouse name or id (a name may end with ".Lakehouse").
        :param authentication: "service_principal" (with tenant_id, client_id and client_secret), "credential_chain"
            (one Azure credential, chosen with chain), "azure_cli", "managed_identity" or "none". By default
            "service_principal" when a client secret is given, otherwise "credential_chain".
        :param tenant_id: The Microsoft Entra tenant (directory) id.
        :param client_id: The application (client) id of the service principal.
        :param client_secret: The client secret of the service principal.
        :param chain: The credential of "credential_chain": "cli", "managed_identity", "env" or "workload_identity".
        :param tables: The tables to make views of, as a list or a comma separated string ("readings, dbo/sensors").
            By default all tables in the lakehouse.
        :param root: The folder with the tables, instead of the lakehouse's Tables folder in OneLake
            (e.g. a local folder of Delta tables).
        :param endpoint: The OneLake host name.
        :param azure_transport: The azure extension's azure_transport_option_type ("default" or "curl").
        :param connection: A duckdb connection to use. When not given, an in-memory database is used.
        :param timezone: The time zone of the connection.
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of DuckDBDialect.
        """
    def workspace_url(self) -> str:
        """The OneLake URL of the workspace."""
    def table_url(self, table: str) -> str:
        """The URL (or path) of a table of the lakehouse, e.g. "readings" or "dbo/readings"."""
    def list_tables(self) -> Sequence[str]:
        """The tables in the lakehouse ("name" or "schema/name"), found by listing the Tables folder."""
    def add_tables(self, tables: Sequence[str]) -> None:
        """Make views of the given tables."""

class FabricDatabase(SQLDatabase):
    """
    A Microsoft Fabric Warehouse, or the SQL analytics endpoint of a Fabric Lakehouse, signed in to with
    Microsoft Entra ID. Resource SQL is T-SQL. Needs azure-identity, and mssql-python or pyodbc.
    """

    server: Optional[str]
    database: Optional[str]
    driver: str
    timezone: Optional[str]

    def __init__(self, server: Optional[str] = None, database: Optional[str] = None,
                 authentication: str = "default", tenant_id: Optional[str] = None,
                 client_id: Optional[str] = None, client_secret: Optional[str] = None,
                 driver: str = "mssql-python", odbc_driver: str = "ODBC Driver 18 for SQL Server",
                 timezone: Optional[str] = "UTC", credential: Any = None, connection: Any = None,
                 sql_dialect: Union[str, "SQLDialect"] = "fabric"):
        """
        :param server: The SQL endpoint host name, e.g. <id>.datawarehouse.fabric.microsoft.com, optionally with
            ",port" (default 1433).
        :param database: The warehouse or lakehouse name.
        :param authentication: "service_principal" (with tenant_id, client_id and client_secret), "default"
            (azure-identity's DefaultAzureCredential; client_id selects a user-assigned managed identity),
            "interactive" (a browser window) or "azure_cli".
        :param tenant_id: The Microsoft Entra tenant (directory) id.
        :param client_id: The application (client) id of the service principal.
        :param client_secret: The client secret of the service principal.
        :param driver: "mssql-python" (includes the ODBC driver) or "pyodbc" (needs ODBC Driver 18 for SQL Server).
        :param odbc_driver: The ODBC driver name used with pyodbc.
        :param timezone: The time zone given to timestamps without one in the results (Fabric's datetime2).
            None leaves them without a time zone.
        :param credential: An azure-identity credential to use instead of the authentication arguments.
        :param connection: A DB-API connection to use instead of connecting.
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of FabricDialect.
        """
    def connection_string(self) -> str:
        """The connection string, without credentials."""
    def connect(self) -> Any:
        """Connect to the SQL endpoint, and return the DB-API connection."""
    @property
    def connection(self) -> Any: ...
    def close(self) -> None: ...

class TimescaleDatabase(SQLDatabase):
    """
    A TimescaleDB (PostgreSQL) database, queried with psycopg (version 3).
    Time buckets are computed with TimescaleDB's time_bucket.
    """

    connection: Any

    def __init__(self, url: Optional[str] = None, host: Optional[str] = None, port: Union[int, str, None] = None,
                 database: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None,
                 sslmode: Optional[str] = None, connection: Any = None, timezone: Optional[str] = "UTC",
                 sql_dialect: Union[str, "SQLDialect"] = "timescale", **connect_kwargs: Any):
        """
        :param url: A connection URI or conninfo string, e.g. postgresql://user:password@host:5432/database.
            The other connection arguments are added to it, and take precedence.
        :param host: The host name of the server.
        :param port: The port of the server (5432 when not given).
        :param database: The database to connect to.
        :param user: The user to connect as.
        :param password: The password of the user.
        :param sslmode: The libpq sslmode, e.g. require or verify-full.
        :param connection: A DB-API connection (e.g. from psycopg.connect) to use instead of connecting.
        :param timezone: The time zone set for the session, so that timestamps with time zone come back in it.
            None leaves the session's time zone as is.
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of TimescaleDialect.
        :param connect_kwargs: Other keyword arguments for psycopg.connect (e.g. connect_timeout, sslrootcert).
        """
    def close(self) -> None:
        """Close the connection."""

class KustoDatabase:
    """
    An Azure Data Explorer (Kusto) database or a Microsoft Fabric Eventhouse KQL database, queried in KQL
    with the azure-kusto-data package (and azure-identity for default credentials).

    The resources in resource_sql_map are KQL tabular expressions with an id column of type string and a
    column for each of the other template parameters, e.g.
    "Telemetry | project id=DeviceId, timestamp=Timestamp, value=Value".
    """

    cluster: Optional[str]
    database: str
    client: Any
    sql_dialect: "KQLDialect"
    max_ids_per_query: Optional[int]

    def __init__(self, cluster: Optional[str] = None, database: Optional[str] = None,
                 tenant_id: Optional[str] = None, client_id: Optional[str] = None,
                 client_secret: Optional[str] = None, authentication: Optional[str] = None,
                 client: Any = None, sql_dialect: Optional["KQLDialect"] = None,
                 max_ids_per_query: Optional[int] = 2000):
        """
        :param cluster: The cluster or query URI, e.g. https://mycluster.westeurope.kusto.windows.net,
            or the query URI of a Fabric Eventhouse (https://<id>.<region>.kusto.fabric.microsoft.com).
        :param database: The database (in Fabric: the KQL database).
        :param tenant_id: The Microsoft Entra ID tenant, for a service principal.
        :param client_id: The application (client) id of a service principal.
        :param client_secret: The client secret of the service principal. Without client_id and client_secret,
            DefaultAzureCredential from azure-identity is used (environment, managed identity, Azure CLI, ...).
        :param authentication: "service_principal", "default", "azure_cli" or "none" (for the Kusto emulator).
            Chosen from the other arguments when not given.
        :param client: An azure.kusto.data.KustoClient to use instead of connecting.
        :param sql_dialect: Override the KQL dialect, e.g. with a subclass of KQLDialect.
        :param max_ids_per_query: The most time series ids one KQL query names. The ids (and the groups of a
            pushed down GROUP BY) are written into the query text, and Kusto refuses queries longer than 2 MB,
            so a query over more time series is sent as several queries whose answers are combined.
            None sends each query whole.
        """
    def query(self, kql: str) -> pl.DataFrame:
        """Run a KQL query and return the result as a polars DataFrame."""
    def column_types(self, resource: str, kql: str) -> Dict[str, str]:
        """The Kusto types of the columns of a resource expression, from getschema."""

class KQLDialect:
    """
    The translation from virtualized queries to KQL. Subclass it to change the KQL that is generated,
    then pass an instance as sql_dialect.
    """

    name: str
    query_language: str

    def __init__(self, column_types: Optional[Callable[[str, str], Optional[Mapping[str, str]]]] = None):
        """
        :param column_types: Optional function from (resource name, resource KQL) to the Kusto types of the
            columns of the resource. They let the translation convert numbers where KQL needs equal types.
        """
    def string_literal(self, value: str) -> str: ...
    def datetime_literal(self, value: Any) -> str: ...
    def date_literal(self, value: Any) -> str: ...
    def timespan_literal(self, value: XSDDuration) -> str: ...
    def real_literal(self, value: float) -> str: ...
    def decimal_literal(self, value: Any) -> str: ...

def translate_kql(vq: VirtualizedQuery, dialect: Union[str, KQLDialect, None], resource_kql_map: Mapping[str, str]) -> str:
    """Translate a virtualized query to KQL."""

class SQLDialect:
    """
    The database specific parts of the translation from virtualized queries to SQL.
    Subclass a dialect and override its methods to change the SQL that is generated,
    then pass an instance as sql_dialect.
    """

    name: str
    limit_for_offset_only: Optional[int]
    offset_requires_order_by: bool

    def sqlalchemy_dialect(self) -> Any:
        """The SQLAlchemy dialect used to compile queries."""
    def compile(self, statement: Any) -> str: ...
    def string_literal(self, value: str) -> Any: ...
    def timestamp_literal(self, value: Any) -> Any: ...
    def date_literal(self, value: Any) -> Any: ...
    def duration_literal(self, value: XSDDuration) -> Any: ...
    def id_grouping_table(self, rows: Sequence[Tuple[str, int]], grouping_column_name: str, name: str) -> Any: ...
    def date_part(self, part: str, timestamp: Any) -> Any: ...
    def epoch_seconds(self, timestamp: Any) -> Any: ...
    def from_epoch_seconds(self, seconds: Any) -> Any: ...
    def floor_to_seconds_interval(self, timestamp: Any, seconds: Any) -> Any: ...
    def date_bin(self, stride: XSDDuration, timestamp: Any, origin: Any) -> Any: ...
    def contains(self, haystack: Any, needle: Any) -> Any: ...
    def starts_with(self, s: Any, prefix: Any) -> Any: ...
    def ends_with(self, s: Any, suffix: Any) -> Any: ...
    def string_agg(self, expression: Any, separator: str) -> Any: ...
    def any_value(self, expression: Any) -> Any: ...
    def avg_argument(self, expression: Any) -> Any: ...
    def ceil(self, expression: Any) -> Any: ...
    def round(self, expression: Any) -> Any: ...
    def as_value(self, expression: Any) -> Any:
        """An expression used as a selected value (BIND), e.g. a condition as a value where there are no booleans."""

class PostgresDialect(SQLDialect): ...
class TimescaleDialect(PostgresDialect): ...
class DuckDBDialect(PostgresDialect): ...
class BigQueryDialect(SQLDialect): ...
class SnowflakeDialect(SQLDialect): ...
class DatabricksDialect(SQLDialect): ...
class InfluxDBDialect(PostgresDialect):
    """InfluxDB 3 SQL (Apache DataFusion), by the names "influxdb", "influxdb3" and "datafusion"."""
class MSSQLDialect(SQLDialect):
    """T-SQL for SQL Server 2022 and later, Azure SQL Database and Managed Instance ("mssql", "sqlserver", "tsql")."""
class FabricDialect(MSSQLDialect):
    """T-SQL for Microsoft Fabric Warehouses and Lakehouse SQL analytics endpoints ("fabric")."""

DIALECTS: Dict[str, Type[SQLDialect]]

def get_dialect(dialect: Union[str, SQLDialect, Type[SQLDialect]]) -> SQLDialect:
    """Look up a dialect by name, or return the SQLDialect given."""

def translate_sql(vq: VirtualizedQuery, dialect: Union[str, SQLDialect], resource_sql_map: Mapping[str, Any]) -> str:
    """Translate a virtualized query to SQL in the given dialect."""
