import time

from maplib import Mapping
import polars as pl
import os
import pathlib
from pathlib import Path
import pytest

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
GTFS_PATH = PATH_HERE / "testdata" / "gtfs"


@pytest.fixture(scope="function")
def mapping() -> Mapping:
    time_start = time.time()
    mapping_prefixes = """
    @prefix rr:<http://www.w3.org/ns/r2rml#>.
    @prefix foaf:<http://xmlns.com/foaf/0.1/>.
    @prefix xsd:<http://www.w3.org/2001/XMLSchema#>.
    @prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>.
    @prefix dc:<http://purl.org/dc/elements/1.1/>.
    @prefix rev:<http://purl.org/stuff/rev#>.
    @prefix gtfs:<http://vocab.gtfs.org/terms#>.
    @prefix geo:<http://www.w3.org/2003/01/geo/wgs84_pos#>.
    @prefix schema:<http://schema.org/>.
    @prefix dct:<http://purl.org/dc/terms/>.
    @prefix rml:<http://semweb.mmlab.be/ns/rml#>.
    @prefix ql:<http://semweb.mmlab.be/ns/ql#>.
    @prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
    @prefix t:<https://github.com/magbak/maplib/benchmark/template#>.
    """

    i = 5

    path = GTFS_PATH / "datasets" / "csv" / f"{i}"

    frequencies = pl.scan_csv(path / "FREQUENCIES.csv")
    calendar_dates = pl.scan_csv(path / "CALENDAR_DATES.csv")
    trips = pl.scan_csv(path / "TRIPS.csv")
    stop_times = pl.scan_csv(path / "STOP_TIMES.csv")
    stops = pl.scan_csv(path / "STOPS.csv")
    routes = pl.scan_csv(path / "ROUTES.csv")
    feed_info = pl.scan_csv(path / "FEED_INFO.csv")
    agency = pl.scan_csv(path / "AGENCY.csv")
    calendar = pl.scan_csv(path / "CALENDAR.csv")
    shapes = pl.scan_csv(path / "SHAPES.csv")

    TRANS_LINKED_ES = "http://transport.linkeddata.es/"
    STOPTIMES_PREFIX = TRANS_LINKED_ES + "madrid/metro/stoptimes/"
    PICKUP_TYPE_PREFIX = TRANS_LINKED_ES + "resource/PickupType/"
    DROPOFF_TYPE_PREFIX = TRANS_LINKED_ES + "resource/DropOffType/"
    TRIP_ID_PREFIX = TRANS_LINKED_ES + "madrid/metro/trips/"
    ROUTE_ID_PREFIX = TRANS_LINKED_ES + "madrid/metro/routes/"
    ROUTE_TYPE_PREFIX = TRANS_LINKED_ES + "resource/RouteType/"
    STOP_ID_PREFIX = TRANS_LINKED_ES + "madrid/metro/stops/"
    SERVICE_ID_PREFIX = TRANS_LINKED_ES + "madrid/metro/services/"
    AGENCY_ID_PREFIX = TRANS_LINKED_ES + "madrid/agency/"
    SHAPE_ID_PREFIX = TRANS_LINKED_ES + "madrid/metro/shape/"
    SHAPE_POINT_PREFIX = TRANS_LINKED_ES + "madrid/metro/shape_point/"
    FREQUENCY_ID_PREFIX = TRANS_LINKED_ES + "madrid/metro/frequency/"
    WHEELCHAIR_ACCESSIBLE_PREFIX = TRANS_LINKED_ES + "resource/WheelchairBoardingStatus/"
    LOCATION_TYPE_PREFIX = TRANS_LINKED_ES + "resource/LocationType/"
    CALENDAR_DATE_RULES_PREFIX = TRANS_LINKED_ES + "madrid/metro/calendar_date_rule/"
    CALENDAR_RULES_PREFIX = TRANS_LINKED_ES + "madrid/metro/calendar_rules/"
    FEED_PUBLISHER_PREFIX = TRANS_LINKED_ES + "madrid/metro/feed/"

    stop_times_mut = stop_times.with_columns([
        pl.col("stop_sequence").cast(pl.Int32),
        (STOPTIMES_PREFIX + pl.col("trip_id") + '-' + pl.col("stop_id") + "-" + pl.col("arrival_time")).alias(
            "subject"),
        (PICKUP_TYPE_PREFIX + pl.col("pickup_type")).alias("pickup_type"),
        (DROPOFF_TYPE_PREFIX + pl.col("drop_off_type")).alias("drop_off_type"),
        (TRIP_ID_PREFIX + pl.col("trip_id")).alias("trip_id"),
        (STOP_ID_PREFIX + pl.col("stop_id")).alias("stop_id")
    ]).collect()

    stop_times_mapping = mapping_prefixes + """
    t:StopTimes [xsd:anyURI ?trip_id, ?arrival_time, ?departure_time, xsd:anyURI ?stop_id, ??stop_sequence, 
                 ?stop_headsign, ?pickup_type, ?drop_off_type, ?shape_dist_traveled,  xsd:anyURI ?subject ] :: { 
      ottr:Triple(?subject,rdf:type, gtfs:StopTime),
      ottr:Triple(?subject,gtfs:trip,?trip_id),
      ottr:Triple(?subject,gtfs:arrivalTime,?arrival_time),
      ottr:Triple(?subject,gtfs:departureTime,?departure_time),
      ottr:Triple(?subject,gtfs:stop,?stop_id),
      ottr:Triple(?subject,gtfs:stopSequence,?stop_sequence),
      ottr:Triple(?subject,gtfs:headsign,?stop_headsign),
      ottr:Triple(?subject,gtfs:pickupType,?pickup_type),
      ottr:Triple(?subject,gtfs:dropoffType,?drop_off_type),
      ottr:Triple(?subject,gtfs:distanceTraveled,?shape_dist_traveled)
    }.
    """

    trips_mut = trips.with_columns([
        (TRIP_ID_PREFIX + pl.col("trip_id")).alias("trip_id"),
        (WHEELCHAIR_ACCESSIBLE_PREFIX + pl.col("wheelchair_accessible")).alias("wheelchair_accessible"),
        (SERVICE_ID_PREFIX + pl.col("service_id")).alias("service_id"),
        (ROUTE_ID_PREFIX + pl.col("route_id")).alias("route_id"),
        (SHAPE_ID_PREFIX + pl.col("shape_id")).alias("shape_id")
    ]).collect()

    trips_mapping = mapping_prefixes + """
    t:Trips [xsd:anyURI ?route_id, xsd:anyURI ?service_id,  xsd:anyURI ?trip_id, ?trip_headsign, ?trip_short_name, 
             xsd:anyURI ?direction_id, xsd:anyURI ?block_id, xsd:anyURI ?shape_id, ?wheelchair_accessible ] :: {
      ottr:Triple(?trip_id, rdf:type, gtfs:Trip) ,
      ottr:Triple(?trip_id, gtfs:route, ?route_id) ,
      ottr:Triple(?trip_id, gtfs:service, ?service_id) ,
      ottr:Triple(?trip_id, gtfs:headsign, ?trip_headsign) ,
      ottr:Triple(?trip_id, gtfs:shortName, ?trip_short_name) ,
      ottr:Triple(?trip_id, gtfs:direction, ?direction_id) ,
      ottr:Triple(?trip_id, gtfs:block, ?block_id) ,
      ottr:Triple(?trip_id, gtfs:shape, ?shape_id) ,
      ottr:Triple(?trip_id, gtfs:wheelchairAccessible, ?wheelchair_accessible)
    } . """

    routes_mut = routes.with_columns([
        (ROUTE_ID_PREFIX + pl.col("route_id")).alias("route_id"),
        (ROUTE_TYPE_PREFIX + pl.col("route_type")).alias("route_type"),
        (AGENCY_ID_PREFIX + pl.col("agency_id")).alias("agency_id")
    ]).collect()

    routes_mapping = mapping_prefixes + """
    t:Routes [ xsd:anyURI ?route_id, xsd:anyURI ?agency_id, ?route_short_name, ?route_long_name, ?route_desc, 
               ?route_type, xsd:anyURI ?route_url, ?route_color, ?route_text_color ] :: {
      ottr:Triple(?route_id, rdf:type, gtfs:Route) ,
      ottr:Triple(?route_id, gtfs:agency, ?agency_id) ,
      ottr:Triple(?route_id, gtfs:shortName, ?route_short_name) ,
      ottr:Triple(?route_id, gtfs:longName, ?route_long_name) ,
      ottr:Triple(?route_id, dct:description, ?route_desc) ,
      ottr:Triple(?route_id, gtfs:routeType, ?route_type) ,
      ottr:Triple(?route_id, gtfs:routeUrl, ?route_url) ,
      ottr:Triple(?route_id, gtfs:color, ?route_color) ,
      ottr:Triple(?route_id, gtfs:textColor, ?route_text_color)
    } . 
    """

    agency_mut = agency.with_columns([
        (AGENCY_ID_PREFIX + pl.col("agency_id")).alias("agency_id"),
        pl.col("agency_name").cast(pl.String)
    ]).collect()

    agency_mapping = mapping_prefixes + """
    t:Agency [ xsd:anyURI ?agency_id, ?agency_name, xsd:anyURI ?agency_url, ?agency_timezone, ?agency_lang, 
               ?agency_phone, xsd:anyURI ?agency_fare_url ] :: {
      ottr:Triple(?agency_id,rdf:type, gtfs:Agency) ,
      ottr:Triple(?agency_id,foaf:name, ?agency_name) ,
      ottr:Triple(?agency_id,foaf:page, ?agency_url) ,
      ottr:Triple(?agency_id,gtfs:timeZone, ?agency_timezone) ,
      ottr:Triple(?agency_id,dct:language,?agency_lang) ,
      ottr:Triple(?agency_id,foaf:phone,?agency_phone) ,
      ottr:Triple(?agency_id,gtfs:fareUrl,?agency_fare_url)
    } ."""

    stops_mut = stops.with_columns([
        (STOP_ID_PREFIX + pl.col("stop_id")).alias("stop_id"),
        (WHEELCHAIR_ACCESSIBLE_PREFIX + pl.col("wheelchair_boarding")).alias("wheelchair_boarding"),
        (LOCATION_TYPE_PREFIX + pl.col("location_type")).alias("location_type"),
        (STOP_ID_PREFIX + pl.col("parent_station")).alias("parent_station"),
        pl.col("stop_lat").cast(pl.Float64),
        pl.col("stop_lon").cast(pl.Float64)
    ]).collect()

    stops_mapping = mapping_prefixes + """
    t:Stops [ xsd:anyURI ?stop_id, ?stop_code, ?stop_name, ?stop_desc, ?stop_lat, ?stop_lon, ??zone_id, 
              xsd:anyURI ?stop_url, xsd:anyURI ?location_type, ? xsd:anyURI ?parent_station, ??stop_timezone, 
              ?wheelchair_boarding ] :: {
      ottr:Triple(?stop_id, rdf:type, gtfs:Stop) ,
      ottr:Triple(?stop_id, gtfs:code, ?stop_code) ,
      ottr:Triple(?stop_id, foaf:name, ?stop_name) ,
      ottr:Triple(?stop_id, dct:description, ?stop_desc) ,
      ottr:Triple(?stop_id, geo:lat, ?stop_lat) ,
      ottr:Triple(?stop_id, geo:long, ?stop_lon) ,
      ottr:Triple(?stop_id, gtfs:zone, ?zone_id) ,
      ottr:Triple(?stop_id, foaf:page, ?stop_url) ,
      ottr:Triple(?stop_id, gtfs:locationType, ?location_type) ,
      ottr:Triple(?stop_id, gtfs:parentStation, ?parent_station) ,
      ottr:Triple(?stop_id, gtfs:timeZone, ?stop_timezone) ,
      ottr:Triple(?stop_id, gtfs:wheelchairAccessible, ?wheelchair_boarding)
    } . 
    """
    #  ottr:Triple(?stop_id, dct:identifier, ?stop_id) ,

    calendar_dates_mut = calendar_dates.with_columns([
        (CALENDAR_DATE_RULES_PREFIX + pl.col("service_id") + "-" + pl.col("date")).alias("subject"),
        (SERVICE_ID_PREFIX + pl.col("service_id")).alias("service_id"),
        (pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
    ]).select(["subject", "service_id", "date", "exception_type"
               ]).collect()

    calendar_dates_mapping = mapping_prefixes + """
    t:CalendarDateRules [?date, ?exception_type,  xsd:anyURI ?subject, xsd:anyURI ?service_id ] :: {
      ottr:Triple(?service_id, rdf:type, gtfs:Service),
      ottr:Triple(?service_id, gtfs:serviceRule, ?subject) ,
      ottr:Triple(?subject,rdf:type,gtfs:CalendarDateRule) ,
      ottr:Triple(?subject,dct:date,?date) ,
      ottr:Triple(?subject,gtfs:dateAddition,?exception_type)
    } . """

    tobools = []
    for d in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
        tobools.append(pl.col(d).cast(pl.Boolean))

    calendar_mut = calendar.with_columns([
                                             (CALENDAR_RULES_PREFIX + pl.col("service_id")).alias("subject"),
                                             (SERVICE_ID_PREFIX + pl.col("service_id")).alias("service_id"),
                                             (pl.col("start_date").str.strptime(pl.Date, "%Y-%m-%d")),
                                             (pl.col("end_date").str.strptime(pl.Date, "%Y-%m-%d")),
                                         ] + tobools).collect()

    calendar_mapping = mapping_prefixes + """
    t:CalendarRules [ xsd:anyURI ?subject, xsd:anyURI ?service_id, ?monday, ?tuesday, ?wednesday, ?thursday, ?friday, 
                      ?saturday, ?sunday, ?start_date, ?end_date ] :: {
      ottr:Triple(?service_id, rdf:type, gtfs:Service),
      ottr:Triple(?service_id, gtfs:serviceRule, ?subject) ,
      ottr:Triple(?subject,rdf:type, gtfs:CalendarRule) ,
      ottr:Triple(?subject,gtfs:monday, ?monday) ,
      ottr:Triple(?subject,gtfs:tuesday, ?tuesday) ,
      ottr:Triple(?subject,gtfs:wednesday, ?wednesday) ,
      ottr:Triple(?subject,gtfs:thursday, ?thursday) ,
      ottr:Triple(?subject,gtfs:friday, ?friday) ,
      ottr:Triple(?subject,gtfs:saturday, ?saturday) ,
      ottr:Triple(?subject,gtfs:sunday, ?sunday) ,
      ottr:Triple(?subject,schema:startDate, ?start_date) ,
      ottr:Triple(?subject,schema:endDate, ?end_date)
    } . 
    """

    feed_info_mut = feed_info.with_columns([
        (FEED_PUBLISHER_PREFIX + pl.col("feed_publisher_name")).alias("subject"),
        (pl.col("feed_start_date").str.strptime(pl.Date, "%Y-%m-%d")),
        (pl.col("feed_end_date").str.strptime(pl.Date, "%Y-%m-%d"))]
    ).collect()

    feed_info_mapping = mapping_prefixes + """
    t:Feed [?feed_publisher_name, xsd:anyURI ?feed_publisher_url, ?feed_lang, ?feed_start_date, ?feed_end_date, 
            ?feed_version,  xsd:anyURI ?subject ] :: {
      ottr:Triple(?subject,rdf:type, gtfs:Feed) ,
      ottr:Triple(?subject,dct:publisher,?feed_publisher_name) ,
      ottr:Triple(?subject,foaf:page,?feed_publisher_url) ,
      ottr:Triple(?subject,dct:language,?feed_lang) ,
      ottr:Triple(?subject,schema:startDate,?feed_start_date) ,
      ottr:Triple(?subject,schema:endDate,?feed_end_date) ,
      ottr:Triple(?subject,schema:version,?feed_version)
    } . 
    """

    shapes_mut = shapes.with_columns([
        (SHAPE_POINT_PREFIX + pl.col("shape_id") + "-" + pl.col("shape_pt_sequence")).alias("point"),
        (SHAPE_ID_PREFIX + pl.col("shape_id")).alias("shape_id")
    ]).select(["shape_id", "point"]).collect()

    shapes_mapping = mapping_prefixes + """
    t:Shapes [ xsd:anyURI ?shape_id, xsd:anyURI ?point] :: {
      ottr:Triple(?shape_id, rdf:type, gtfs:Shape) ,
      ottr:Triple(?shape_id, gtfs:shapePoint, ?point) 
      } . 
    """

    shape_points_mut = shapes.with_columns([
        (SHAPE_POINT_PREFIX + pl.col("shape_id") + "-" + pl.col("shape_pt_sequence")).alias("subject"),
        pl.col("shape_pt_lat").cast(pl.Float64),
        pl.col("shape_pt_lon").cast(pl.Float64)
    ]).drop(["shape_id"]).collect()

    shape_points_mapping = mapping_prefixes + """
    t:ShapePoints [?shape_pt_lat, ?shape_pt_lon, ?shape_pt_sequence, ?shape_dist_traveled,  xsd:anyURI ?subject ] :: {
      ottr:Triple(?subject, rdf:type, gtfs:ShapePoint) ,
      ottr:Triple(?subject,geo:lat,?shape_pt_lat) ,
      ottr:Triple(?subject,geo:long,?shape_pt_lon) ,
      ottr:Triple(?subject,gtfs:pointSequence,?shape_pt_sequence) ,
      ottr:Triple(?subject,gtfs:distanceTraveled,?shape_dist_traveled)
    } . 
    """

    frequencies_mut = frequencies.with_columns([
        (FREQUENCY_ID_PREFIX + pl.col("trip_id") + "-" + pl.col("start_time")).alias("subject"),
        (TRIP_ID_PREFIX + pl.col("trip_id")).alias("trip_id"),
        pl.col("exact_times").cast(pl.Boolean)
    ]).collect()

    frequencies_mapping = mapping_prefixes + """
    t:Frequencies [xsd:anyURI ?trip_id, ?start_time, ?end_time, ?headway_secs, ?exact_times,  xsd:anyURI ?subject ] :: {
      ottr:Triple(?subject,rdf:type, gtfs:Frequency) ,
      ottr:Triple(?subject,gtfs:trip,?trip_id) ,
      ottr:Triple(?subject,gtfs:startTime,?start_time) ,
      ottr:Triple(?subject,gtfs:endTime,?end_time) ,
      ottr:Triple(?subject,gtfs:headwaySeconds,?headway_secs) ,
      ottr:Triple(?subject,gtfs:exactTimes,?exact_times)
    } . 
    """

    m = Mapping([stop_times_mapping,
                 trips_mapping,
                 routes_mapping,
                 agency_mapping,
                 stops_mapping,
                 calendar_dates_mapping,
                 calendar_mapping,
                 feed_info_mapping,
                 shapes_mapping,
                 shape_points_mapping,
                 frequencies_mapping])

    m.expand("t:StopTimes", stop_times_mut, ["subject"])
    m.expand("t:Trips", trips_mut, ["trip_id"])
    m.expand("t:Routes", routes_mut, ["route_id"])
    m.expand("t:Agency", agency_mut, ["agency_id"])
    m.expand("t:Stops", stops_mut, ["stop_id"])
    m.expand("t:CalendarDateRules", calendar_dates_mut, ["subject"])
    m.expand("t:CalendarRules", calendar_mut, ["service_id"])
    m.expand("t:Feed", feed_info_mut, ["subject"])
    m.expand("t:Shapes", shapes_mut, ["point"])
    m.expand("t:ShapePoints", shape_points_mut, ["subject"])
    m.expand("t:Frequencies", frequencies_mut, ["subject"])

    time_end = time.time()
    print(f"Mapping took {round(time_end-time_start, 2)} seconds")
    return m


@pytest.mark.skip("dataset not yet in git")
def test_q15(mapping):
    query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX gtfs: <http://vocab.gtfs.org/terms#>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT * WHERE { 
	?stop a gtfs:Stop .
	?stop ?p ?str .
	FILTER regex (?str, '000000000000000002')
}
"""
    print(mapping.query(query))

@pytest.mark.skip("dataset not yet in git")
def test_q7(mapping):
    query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX gtfs: <http://vocab.gtfs.org/terms#>
PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX gtfsroute: <http://transport.linkeddata.es/madrid/metro/routes/>
PREFIX gtfsaccessible: <http://transport.linkeddata.es/resource/WheelchairBoardingStatus/>

SELECT DISTINCT ?routeShortName ?routeDescription ?tripShortName ?stopDescription ?stopLat ?stopLong   
WHERE {
	?route a gtfs:Route .
	OPTIONAL { ?route gtfs:shortName ?routeShortName . }
	OPTIONAL { ?route dct:description ?routeDescription . }

	?trip a gtfs:Trip .
	OPTIONAL { ?trip gtfs:shortName ?tripShortName . }
	?trip gtfs:service ?service .
	?trip gtfs:route ?route .

	?stopTime a gtfs:StopTime . 
	?stopTime gtfs:trip ?trip . 
	?stopTime gtfs:stop ?stop . 

	?stop a gtfs:Stop . 
	OPTIONAL { ?stop dct:description ?stopDescription . }
	OPTIONAL {
		?stop geo:lat ?stopLat . 
		?stop geo:long ?stopLong . 
	}
	?stop gtfs:wheelchairAccessible gtfsaccessible:1 .
    FILTER (?route=gtfsroute:0000000000000000000c)
}
"""
    print(mapping.query(query))
@pytest.mark.skip("dataset not yet in git")
def test_queries(mapping):
    query_path = GTFS_PATH / "queries"
    errors = 0
    perf = []
    #10 is not relevant as the data type is wrong in the test data
    #7 is crashing with sigkill
    for fname in os.listdir(query_path):
        if fname in [f'q{str(i)}.rq' for i in [1,2,3,4,5,6,8,9,11,12,13,14,15,16,17,18]]:
            with open(query_path / fname) as f:
                q = f.read()
                print(fname.replace(".rq", "\n"))
                print(q)
                start = time.time()
                try:
                    res = mapping.query(q)
                    used_time = round(time.time()-start,2)
                    perf.append((fname, used_time))
                    print(f"Took {used_time} seconds")
                    print(f"Ok {res.height}")

                except:
                    print("Error")
                    errors+=1
                print("\n")


    print(f"N errors {errors}\n")
    print(f"Perf: {perf}")


