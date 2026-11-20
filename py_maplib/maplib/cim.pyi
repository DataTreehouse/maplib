"""
The CIM grid tools of maplib Enterprise Edition bound to a model, available as ``m.cim``.

Each method is the function of the ``cim`` package that maplib Enterprise Edition ships, with the
model as its first argument: ``m.cim.cut_to_graph(...)`` is ``cim.cut_to_graph(m, ...)``. The
functions that need no model (``cim.serves_rulesets``, ``cim.meter_readings_template``,
``cim.measurement_template``, ``cim.meter_readings_sql``, ...) and the constants are in ``cim``.
In open source maplib, ``m.cim`` raises an AttributeError.
"""

from typing import Any, Dict, Iterable, List, Optional, Sequence, Union

from polars import DataFrame

from maplib import IRI, Model, VirtualizedDatabase

class CIM:
    """The CIM grid tools of a Model (maplib Enterprise Edition), available as ``m.cim``."""

    def cut_to_graph(
        self,
        interior_equipment_iris: Sequence[str],
        border_equipment_iris: Sequence[str] = (),
        target_graph: Optional[str] = None,
        graphs: Optional[Sequence[Optional[str]]] = None,
        normal_topology: bool = True,
        max_distance: Optional[int] = None,
        context_hops: int = 10,
        context_predicate_iris: Optional[Sequence[str]] = None,
        backwards_predicate_iris: Optional[Sequence[str]] = None,
        cim_prefix: Optional[str] = None,
        work_graph: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Cut the grid around the interior equipment, stopping at the border equipment (left out),
        into `target_graph`. The cut holds the equipment reached through terminals and connectivity
        nodes (by default the normal topology: normally open switches and equipment normally out of
        service are not passed), their terminals and connectivity nodes, and the resources they refer
        to up to `context_hops` steps away (containers, base voltages, locations, ...).

        `graphs` are read as one dataset (None or the default graph: `None` in the list), so the
        instance data, `rdf:type` triples, profiles and CIM vocabulary may be in different graphs.
        `target_graph` must be a new graph (not one that is read); without one nothing is written
        except the work graph, which is kept when `work_graph` is given and dropped otherwise.

        Usage:

        >>> m.read("EQ.xml", format="cim/xml")
        >>> counts = m.cim.cut_to_graph(["urn:uuid:busbar-in-the-area"], ["urn:uuid:line-leaving-the-area"],
        ...                             target_graph="urn:graph:cut")

        :return: Counts: members, equipment, terminals, connectivity_nodes, non_members, context,
            boundary_equipment (equipment outside with a terminal on a connectivity node of the cut) and
            triples (in the target graph).
        """

    def cut(
        self,
        border_equipment_iris: List[str],
        interior_equipment_iri: Union[str, List[str]],
        backwards_predicate_iris: Optional[List[str]] = None,
        normal_topology: bool = True,
        max_distance: Optional[int] = None,
        fail_distance: int = 1000,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        max_iterations: Optional[int] = None,
        graphs: Optional[Sequence[Optional[str]]] = None,
        context_hops: int = 10,
    ) -> Model:
        """
        Cut the grid around the interior equipment (an IRI or a list), stopping at the border
        equipment, and return the cut as a new Model (in its default graph). See `cut_to_graph`.
        `fail_distance` and `max_iterations` are no longer used.
        """

    def add_serves(
        self,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        states: Iterable[str] = ("normal", "actual"),
        primary_source_classes: Optional[List[str]] = None,
        backup_source_classes: Optional[List[str]] = None,
        graph: Optional[str] = None,
        keep_helpers: bool = False,
        graphs: Optional[Sequence[Optional[str]]] = None,
        target_graph: Optional[str] = None,
    ) -> Dict[str, Dict[str, float]]:
        """
        Add the serves relations (maplib:normallyServesSameVoltage, maplib:normallyServesDecreasedVoltage,
        maplib:normallyServesIncreasedVoltage and the corresponding maplib:actuallyServes... relations),
        from the primary and backup sources down through the grid. See cim.add_serves.

        Usage:

        >>> m.read("grid_EQ.xml", format="cim/xml")
        >>> m.cim.add_serves(cim_prefix="http://iec.ch/TC57/CIM100#")

        :param cim_prefix: The CIM namespace, e.g. "http://iec.ch/TC57/CIM100#" (CGMES 3) or
            "http://iec.ch/TC57/2013/CIM-schema-cim16#" (CGMES 2.4).
        :param states: The states to derive the relations of, "normal" and/or "actual".
        :param primary_source_classes: Local names of the classes of the primary sources.
        :param backup_source_classes: Local names of the classes of the backup sources.
        :param graph: The graph with the CIM data and where the relations are added.
        :param keep_helpers: Keep the helper triples (remove them later with remove_serves_helpers).
        :param graphs: The graphs read as one dataset, instead of `graph`.
        :param target_graph: The graph the relations are added to, with `graphs`.
        :return: The time taken by each step, per state.
        """

    def remove_serves_helpers(
        self,
        states: Iterable[str] = ("normal", "actual"),
        graph: Optional[str] = None,
    ) -> None:
        """Delete the helper triples of add_serves (it does so itself unless keep_helpers=True)."""

    def add_serves_helper_predicates(
        self,
        minimum_input_voltage: float,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        include_potential: bool = False,
    ) -> None:
        """
        Add the helper predicates of the older serves relations (dt:normallyServes... with
        dt: = https://www.data-treehouse.com/cim/), from the equipment fed at `minimum_input_voltage`
        or above. Superseded by add_serves.
        """

    def add_serves_transformer_shortcut(self, cim_prefix: str = "http://iec.ch/TC57/CIM100#") -> None:
        """Add dt:normallyServesPowerTransformer from each power transformer to the power transformers it normally serves at a lower voltage (needs add_serves_helper_predicates)."""

    def add_transformer_conform_load_shortcut(self, cim_prefix: str = "http://iec.ch/TC57/CIM100#") -> None:
        """Add dt:normallyServesConformLoads from each power transformer to a shortcut node linking the conform loads it normally serves at a lower voltage (needs add_serves_helper_predicates)."""

    def add_reading_types(
        self,
        reading_types: Optional[DataFrame] = None,
        iri_prefix: str = "https://www.data-treehouse.com/cim/",
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        custom_types: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        """
        Add CIM reading types (IEC 61968-9) to the model, by default the common ones that the cim
        package ships, else those of the DataFrame (with the single column ReadingTypeInstance, the
        dotted reading type codes), as IRIs under `iri_prefix`.
        """

    def add_meter_reading_templates(
        self,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        resource: str = "cim_meter_readings",
    ) -> None:
        """
        Add the meter reading templates (tmpl:MeterReadingTimeseries and the templates it uses) to the model.

        :param cim_prefix: The CIM namespace of the graph.
        :param resource: The default value of the resource parameter.
        """

    def add_meter_reading_timeseries(
        self,
        df: DataFrame,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        resource: str = "cim_meter_readings",
        graph: Optional[str] = None,
    ) -> None:
        """
        Add virtualized meter reading time series to a CIM graph using tmpl:MeterReadingTimeseries.
        Each row adds a cim:MeterReading of the meter and a cim:IntervalBlock of the reading type, annotated
        so that its interval readings are the rows of the database where the id column is externalId.

        :param df: A DataFrame with the IRI columns meterReading, meter, intervalBlock, readingType and the
            string column externalId, and optionally the string column resource.
        :param cim_prefix: The CIM namespace of the graph.
        :param resource: The resource name used when the DataFrame has no resource column.
        :param graph: The graph to add the triples to.
        """

    def add_meter_readings_shapes(
        self,
        graph: Optional[str] = None,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        resource: str = "cim_meter_readings",
    ) -> None:
        """
        Add the SHACL shapes for the meter readings virtualization to the model, then use m.validate(shape_graph=graph).

        :param graph: The shape graph.
        :param cim_prefix: The CIM namespace of the graph.
        :param resource: The resource name (the value of maplib:hasResource).
        """

    def add_meter_readings_virtualization(
        self,
        virtualized_database: VirtualizedDatabase,
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        resource: str = "cim_meter_readings",
        value_datatype: Optional[IRI] = None,
    ) -> None:
        """
        Virtualize the interval readings of the annotated interval blocks in the model, queried with
        cim:IntervalBlock.IntervalReadings, cim:MeasurementValue.timeStamp and cim:BaseReading.value.
        The resource_sql_map of the virtualized database must map the resource name to a SQL with the columns
        id, timestamp and value, e.g. from cim.meter_readings_sql.

        :param virtualized_database: The virtualized database.
        :param cim_prefix: The CIM namespace of the graph.
        :param resource: The resource name (the value of maplib:hasResource).
        :param value_datatype: The datatype of the value column, defaults to xsd:double.
        """

    def add_measurement_timeseries(
        self,
        df: DataFrame,
        kind: str = "analog",
        resource: Optional[str] = None,
        graph: Optional[str] = None,
    ) -> None:
        """
        Annotate CIM measurements so that their values are the rows of the database where the id
        column is externalId: each gets maplib:hasResource and maplib:hasExternalId.

        :param df: A DataFrame with the IRI column measurement and the string column externalId, and
            optionally the string column resource.
        :param kind: "analog", "discrete", "accumulator" or "string", for the default resource name.
        :param resource: The resource name used when the DataFrame has no resource column, defaults to
            cim.measurement_resource(kind), e.g. "cim_analog_values".
        :param graph: The graph to add the triples to.
        """

    def add_measurement_virtualization(
        self,
        virtualized_database: VirtualizedDatabase,
        kind: Union[str, Sequence[str]] = "analog",
        cim_prefix: str = "http://iec.ch/TC57/CIM100#",
        resource: Optional[str] = None,
        value_datatype: Optional[IRI] = None,
    ) -> None:
        """
        Virtualize the values of the annotated CIM measurements (IEC 61970-301 Meas) of one or more
        kinds, so that they are queried with CIM predicates only, e.g. for analogs
        cim:Analog.AnalogValues, cim:MeasurementValue.timeStamp and cim:AnalogValue.value, and likewise
        for discretes, accumulators and string measurements. These are the templates of the CIM
        measurement values pattern of the Model portal's Connect page.
        The resource_sql_map of the virtualized database must map each resource name (by default
        cim.measurement_resource(kind), e.g. "cim_analog_values") to a SQL with the columns id,
        timestamp and value. When one database holds several kinds, give the class of the
        measurements in queries (``?a a cim:Analog``): chrontext otherwise also asks the other
        kinds' resources for the values.

        Usage:

        >>> m.cim.add_measurement_timeseries(pl.DataFrame({"measurement": [...], "externalId": [...]}))
        >>> vdb = VirtualizedDatabase(database=db, resource_sql_map={
        ...     "cim_analog_values": 'SELECT tag AS id, ts AS "timestamp", val AS value FROM analogs'},
        ...     sql_dialect="postgres")
        >>> m.cim.add_measurement_virtualization(vdb, kind="analog")

        :param virtualized_database: The virtualized database.
        :param kind: A kind of measurement ("analog", "discrete", "accumulator" or "string"), or a list of kinds.
        :param cim_prefix: The CIM namespace of the graph.
        :param resource: The resource name of a single kind, defaults to cim.measurement_resource(kind).
        :param value_datatype: The datatype of the value column of a single kind, defaults to that of the
            kind (xsd:double for analogs, xsd:integer for discretes and accumulators, xsd:string for
            string measurements).
        """
