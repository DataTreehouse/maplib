"""
Ready-made databases and SQL dialects for virtualizing data in SQL databases with VirtualizedDatabase.

Not available in the open source build: raises NotImplementedError.
"""
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Type, Union

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

    def __init__(self, connection: Any = None, database: str = ":memory:", timezone: Optional[str] = "UTC",
                 sql_dialect: Union[str, "SQLDialect"] = "duckdb"):
        """
        :param connection: A duckdb connection. When not given, duckdb.connect(database) is used.
        :param database: The database file to connect to when no connection is given.
        :param timezone: The time zone set for the connection, so that timestamps with time zone come back in it.
            None leaves the connection's time zone as is.
        :param sql_dialect: Override the SQL dialect, e.g. with a subclass of DuckDBDialect.
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

class SQLDialect:
    """
    The database specific parts of the translation from virtualized queries to SQL.
    Subclass a dialect and override its methods to change the SQL that is generated,
    then pass an instance as sql_dialect.
    """

    name: str

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

class PostgresDialect(SQLDialect): ...
class DuckDBDialect(PostgresDialect): ...
class BigQueryDialect(SQLDialect): ...
class SnowflakeDialect(SQLDialect): ...
class DatabricksDialect(SQLDialect): ...

DIALECTS: Dict[str, Type[SQLDialect]]

def get_dialect(dialect: Union[str, SQLDialect, Type[SQLDialect]]) -> SQLDialect:
    """Look up a dialect by name, or return the SQLDialect given."""

def translate_sql(vq: VirtualizedQuery, dialect: Union[str, SQLDialect], resource_sql_map: Mapping[str, Any]) -> str:
    """Translate a virtualized query to SQL in the given dialect."""
