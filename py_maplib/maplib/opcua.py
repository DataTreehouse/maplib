"""
A virtualized database reading raw time series values from an OPC UA server with History Access.

Requires the asyncua package (pip install asyncua).

Usage::

    from maplib import Model, VirtualizedDatabase
    from maplib.opcua import OPCUAHistoryDatabase

    vdb = VirtualizedDatabase(database=OPCUAHistoryDatabase("opc.tcp://localhost:4840/"))
    m = Model()
    m.add_virtualization(virtualized_database=vdb, resources=resources)

The external ids of the time series are OPC UA node ids, e.g. "ns=2;s=ts1".
The resource templates must have the parameters id, timestamp and value.
Conditions on the timestamp narrow the time range that is read from the server.
All other conditions, and all aggregations, are evaluated by maplib afterwards.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import polars as pl

if TYPE_CHECKING:
    from maplib.vq import Expression, VirtualizedQuery

# The server reads up to but not including the end time, so we widen it a little.
_END_MARGIN = timedelta(milliseconds=1)
_MIN_TIME = datetime(1601, 1, 2, tzinfo=timezone.utc)
_MAX_TIME = datetime(9999, 12, 31, tzinfo=timezone.utc)
_DATETIME_TYPES = {
    "http://www.w3.org/2001/XMLSchema#dateTime",
    "http://www.w3.org/2001/XMLSchema#dateTimeStamp",
}


class OPCUAHistoryDatabase:
    # Read by VirtualizedDatabase when it is not given pushdowns.
    # Only conditions are pushed down, and they are evaluated again by maplib,
    # so it is enough that we read a superset of the values asked for.
    pushdowns = ["value_conditions"]

    def __init__(
        self,
        endpoint: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: float = 30.0,
        timestamp_parameter: str = "timestamp",
        value_parameter: str = "value",
    ):
        """
        :param endpoint: The OPC UA endpoint, e.g. opc.tcp://localhost:4840/
        :param username: Optional user name for authentication.
        :param password: Optional password for authentication.
        :param timeout: Request timeout in seconds.
        :param timestamp_parameter: The name of the timestamp parameter of the resource templates.
        :param value_parameter: The name of the value parameter of the resource templates.
        """
        self.endpoint = endpoint
        self.username = username
        self.password = password
        self.timeout = timeout
        self.timestamp_parameter = timestamp_parameter
        self.value_parameter = value_parameter

    def query(self, vq: VirtualizedQuery) -> pl.DataFrame:
        filters = []
        basic = _find_basic(vq, filters)
        timestamp_col = None
        value_col = None
        for param, col in basic.column_mapping.items():
            if param == self.timestamp_parameter:
                timestamp_col = col
            elif param == self.value_parameter:
                value_col = col
            else:
                raise ValueError(
                    f"OPC UA resource templates may only have the parameters id, "
                    f"{self.timestamp_parameter} and {self.value_parameter}, got {param}"
                )
        start, end = None, None
        if timestamp_col is not None:
            for f in filters:
                start, end = _restrict(f, timestamp_col, start, end)

        if basic.ids and not (start is not None and end is not None and start > end):
            # The query method is called from a thread without a running event loop.
            history = asyncio.run(self._read(basic.ids, start, end))
        else:
            history = {}

        ids, timestamps, values = [], [], []
        for node_id in basic.ids:
            for dv in history.get(node_id, []):
                ts = dv.SourceTimestamp or dv.ServerTimestamp
                if ts is None or dv.Value is None or dv.Value.Value is None:
                    continue
                ids.append(node_id)
                timestamps.append(_utc(ts))
                values.append(dv.Value.Value)

        columns = {basic.identifier_name: pl.Series(ids, dtype=pl.String)}
        if timestamp_col is not None:
            columns[timestamp_col] = pl.Series(
                timestamps, dtype=pl.Datetime("ns", "UTC")
            )
        if value_col is not None:
            columns[value_col] = (
                pl.Series(values, strict=False)
                if values
                else pl.Series([], dtype=pl.Float64)
            )
        return pl.DataFrame(columns)

    async def _read(
        self, node_ids: List[str], start: Optional[datetime], end: Optional[datetime]
    ) -> Dict[str, list]:
        from asyncua import Client

        starttime = start if start is not None else _MIN_TIME
        endtime = end + _END_MARGIN if end is not None else _MAX_TIME
        client = Client(self.endpoint, timeout=self.timeout)
        if self.username is not None:
            client.set_user(self.username)
        if self.password is not None:
            client.set_password(self.password)
        async with client:
            results = await asyncio.gather(
                *[
                    client.get_node(n).read_raw_history(
                        starttime, endtime, return_bounds=False
                    )
                    for n in node_ids
                ]
            )
        return dict(zip(node_ids, results))


def _find_basic(vq: VirtualizedQuery, filters: List[Expression]) -> VirtualizedQuery:
    t = vq.type_name()
    if t == "Basic":
        return vq
    if t == "Filtered":
        filters.append(vq.filter)
        return _find_basic(vq.query, filters)
    if t in ("ExpressionAs", "Sliced", "Ordered"):
        # Evaluated by maplib afterwards, they do not change the columns we return.
        return _find_basic(vq.query, filters)
    raise NotImplementedError(
        f"{t} queries are not supported for OPC UA, "
        f"do not override the pushdowns of OPCUAHistoryDatabase"
    )


def _restrict(
    e: Expression,
    timestamp_col: str,
    start: Optional[datetime],
    end: Optional[datetime],
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Narrow the time range using the conditions in the top level conjunction."""
    t = e.expression_type()
    if t == "And":
        start, end = _restrict(e.left, timestamp_col, start, end)
        return _restrict(e.right, timestamp_col, start, end)
    if t not in ("Greater", "GreaterOrEqual", "Less", "LessOrEqual", "Equal"):
        return start, end
    left = _datetime_bound(e.right, e.left, timestamp_col)  # ?t op literal
    right = _datetime_bound(e.left, e.right, timestamp_col)  # literal op ?t
    lower = upper = None
    if t in ("Greater", "GreaterOrEqual"):
        lower, upper = left, right
    elif t in ("Less", "LessOrEqual"):
        lower, upper = right, left
    else:
        lower = upper = left or right
    if lower is not None and (start is None or lower > start):
        start = lower
    if upper is not None and (end is None or upper < end):
        end = upper
    return start, end


def _datetime_bound(
    lit: Expression, var: Expression, timestamp_col: str
) -> Optional[datetime]:
    if var.expression_type() != "Variable" or var.variable.name != timestamp_col:
        return None
    if lit.expression_type() != "Literal":
        return None
    if lit.literal.datatype.iri not in _DATETIME_TYPES:
        return None
    native = lit.literal.to_native()
    return _utc(native) if isinstance(native, datetime) else None


def _utc(dt: datetime) -> datetime:
    # OPC UA times are UTC, as are datetime literals without a time zone.
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
