from maplib import Mapping
import pytest
from polars.testing import assert_frame_equal
import polars as pl
from math import floor
import pathlib
import time

pl.Config.set_fmt_str_lengths(300)

PATH_HERE = pathlib.Path(__file__).parent
TESTDATA_PATH = PATH_HERE / "testdata"

@pytest.fixture(scope="session")
def windpower_mapping():
    instance_mapping = """
    @prefix tpl:<https://github.com/magbak/chrontext/templates#>.
    @prefix rds:<https://github.com/magbak/chrontext/rds_power#>.
    @prefix ct:<https://github.com/magbak/chrontext#>.
    @prefix rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>.
    @prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>.
    
    tpl:Site [?SiteIRI, ?SiteName] :: {
        ottr:Triple(?SiteIRI, rdfs:label, ?SiteName),
        ottr:Triple(?SiteIRI, rdf:type, rds:Site)
        } .
    
    tpl:FunctionalAspect [?SourceIRI, ?TargetIRI, ?TargetAspectNodeIRI, ?Label] :: {
        ottr:Triple(?SourceIRI, rds:hasFunctionalAspect, ?TargetAspectNodeIRI),
        ottr:Triple(?TargetIRI, rds:hasFunctionalAspectNode, ?TargetAspectNodeIRI),
        ottr:Triple(?TargetAspectNodeIRI, rdfs:label, ?Label)
    } .
    
    tpl:RDSSystem [?SystemIRI, ottr:IRI ?RDSType, ?Label] :: {
        ottr:Triple(?SystemIRI, rdf:type, ?RDSType),
        ottr:Triple(?SystemIRI, rdfs:label, ?Label)
    } .
    
    tpl:StaticProperty [?ParentIRI, ?ValueNodeIRI, ?Label, ?Value] :: {
        ottr:Triple(?ParentIRI, ct:hasStaticProperty, ?ValueNodeIRI),
        ottr:Triple(?ValueNodeIRI, rdfs:label, ?Label),
        ottr:Triple(?ValueNodeIRI, ct:hasStaticValue, ?Value)
    } .
    
    tpl:Timeseries [?ParentIRI, ?TimeseriesNodeIRI, ?Label, ?ExternalId, ottr:IRI ?Datatype] :: {
        ottr:Triple(?ParentIRI, ct:hasTimeseries, ?TimeseriesNodeIRI),
        ottr:Triple(?TimeseriesNodeIRI, ct:hasExternalId, ?ExternalId),
        ottr:Triple(?TimeseriesNodeIRI, ct:hasDatatype, ?Datatype),
        ottr:Triple(?TimeseriesNodeIRI, rdfs:label, ?Label)
    } .
    """

    n = 40

    mapping = Mapping([instance_mapping])
    # Used as a prefix
    wpex = "https://github.com/magbak/chrontext/windpower_example#"
    rds = "https://github.com/magbak/chrontext/rds_power#"
    site_iris = [wpex + "Site" + str(i) for i in range(4)]
    sites = pl.DataFrame({"SiteName": ["Wind Mountain", "Gale Valley", "Gusty Plains", "Breezy Field"],
                          "SiteIRI": site_iris})
    mapping.expand("tpl:Site", sites)

    wind_turbine_iris = [wpex + "WindTurbine" + str(i) for i in range(1, n + 1)]
    wind_turbines = pl.DataFrame(
        {
            "RDSType": [rds + "A"] * n,
            "Label": ["Wind turbine " + str(i) for i in range(1, n + 1)],
            "SystemIRI": wind_turbine_iris
        })

    mapping.expand("tpl:RDSSystem", wind_turbines)
    operating_external_ids = ["oper" + str(i) for i in range(1, n + 1)]
    operating = pl.DataFrame({
        "Label": ["Operating"] * n,
        "ParentIRI": wind_turbine_iris,
        "ExternalId": operating_external_ids,
        "Datatype": ["http://www.w3.org/2001/XMLSchema#boolean"] * n
    })
    operating = operating.with_columns(
        pl.Series("TimeseriesNodeIRI", [wpex] * operating.height) + operating["ExternalId"])
    mapping.expand("tpl:Timeseries", operating)
    maximum_power_values = [[5_000_000, 10_000_000, 15_000_000][i % 3] for i in range(1, n + 1)]
    maximum_power_node_iri = [wpex + "WindTurbineMaximumPower" + str(i) for i in range(1, n + 1)]
    maximum_power = pl.DataFrame({
        "Label": ["MaximumPower"] * n,
        "Value": maximum_power_values,
        "ParentIRI": wind_turbine_iris,
        "ValueNodeIRI": maximum_power_node_iri
    })
    mapping.expand("tpl:StaticProperty",
                   df=maximum_power)

    def add_aspect_labeling_by_source(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
        label_df = df.group_by("SourceIRI", maintain_order=True).map_groups(
            lambda x: pl.DataFrame({"Label": [prefix + str(i) for i in range(1, x.height + 1)]}))
        df = df.with_columns(label_df["Label"])
        return df

    target_aspect_node_iri = [wpex + "WindTurbineFunctionalAspect" + str(i) for i in range(1, n + 1)]

    site_has_wind_turbine = pl.DataFrame({
        "SourceIRI": [site_iris[floor(i / (n / sites.height))] for i in range(0, n)],
        "TargetIRI": wind_turbine_iris,
        "TargetAspectNodeIRI": target_aspect_node_iri
    })
    site_has_wind_turbine = add_aspect_labeling_by_source(site_has_wind_turbine, "A")
    mapping.expand("tpl:FunctionalAspect", site_has_wind_turbine)

    generator_system_iris = [wpex + "GeneratorSystem" + str(i) for i in range(1, n + 1)]
    generator_systems = pl.DataFrame(
        {
            "SystemIRI": generator_system_iris,
            "Label": ["Generator system"] * n,
            "RDSType": [rds + "RA"] * n
        })
    mapping.expand("tpl:RDSSystem", generator_systems)
    wind_turbine_has_generator_system = pl.DataFrame({
        "SourceIRI": wind_turbine_iris,
        "TargetIRI": generator_system_iris,
        "TargetAspectNodeIRI": [wpex + "GeneratorSystemFunctionalAspect" + str(i) for i in range(1, n + 1)]
    })
    wind_turbine_has_generator_system = add_aspect_labeling_by_source(wind_turbine_has_generator_system, "RA")
    mapping.expand("tpl:FunctionalAspect", wind_turbine_has_generator_system)

    generator_iris = [wpex + "Generator" + str(i) for i in range(1, n + 1)]
    generators = pl.DataFrame(
        {
            "SystemIRI": generator_iris,
            "RDSType": [rds + "GAA"] * n,
            "Label": ["Generator"] * n
        })
    mapping.expand("tpl:RDSSystem", generators)
    energy_production_external_ids = ["ep" + str(i) for i in range(1, n + 1)]
    energy_production = pl.DataFrame({
        "Label": ["Production"] * n,
        "ParentIRI": generator_iris,
        "ExternalId": energy_production_external_ids,
        "Datatype": ["http://www.w3.org/2001/XMLSchema#double"] * n
    })
    energy_production = energy_production.with_columns(
        pl.Series("TimeseriesNodeIRI", [wpex] * energy_production.height) + energy_production["ExternalId"])
    mapping.expand("tpl:Timeseries", energy_production)
    generator_system_has_generator = pl.DataFrame({
        "SourceIRI": generator_system_iris,
        "TargetIRI": generator_iris,
        "TargetAspectNodeIRI": [wpex + "GeneratorFunctionalAspect" + str(i) for i in range(1, n + 1)]
    })
    generator_system_has_generator = add_aspect_labeling_by_source(generator_system_has_generator, "GAA")

    mapping.expand("tpl:FunctionalAspect", generator_system_has_generator)

    wms_iris = [wpex + "WeatherMeasuringSystem" + str(i) for i in range(1, n + 1)]
    wms = pl.DataFrame(
        {
            "SystemIRI": wms_iris,
            "RDSType": [rds + "LE"] * n,
            "Label": ["Weather Measuring System"] * n
        })
    mapping.expand("tpl:RDSSystem", wms)
    wind_speed_external_ids = ["wsp" + str(i) for i in range(1, n + 1)]
    wind_speed = pl.DataFrame({
        "Label": ["Windspeed"] * n,
        "ParentIRI": wms_iris,
        "ExternalId": wind_speed_external_ids,
        "Datatype": ["http://www.w3.org/2001/XMLSchema#double"] * n
    })
    wind_speed = wind_speed.with_columns(
        pl.Series("TimeseriesNodeIRI", [wpex] * wind_speed.height) + wind_speed["ExternalId"])

    mapping.expand("tpl:Timeseries", wind_speed)
    wind_direction_external_ids = ["wdir" + str(i) for i in range(1, n + 1)]
    wind_direction = pl.DataFrame({
        "Label": ["WindDirection"] * n,
        "ParentIRI": wms_iris,
        "ExternalId": wind_direction_external_ids,
        "Datatype": ["http://www.w3.org/2001/XMLSchema#double"] * n
    })
    wind_direction = wind_direction.with_columns(
        pl.Series("TimeseriesNodeIRI", [wpex] * wind_direction.height) + wind_direction["ExternalId"])

    mapping.expand("tpl:Timeseries", wind_direction)
    wind_turbine_has_wms = pl.DataFrame({
        "SourceIRI": wind_turbine_iris,
        "TargetIRI": wms_iris,
        "TargetAspectNodeIRI": [wpex + "WMSFunctionalAspect" + str(i) for i in range(1, n + 1)]
    })
    wind_turbine_has_wms = add_aspect_labeling_by_source(wind_turbine_has_wms, "LE")

    mapping.expand("tpl:FunctionalAspect",
                   df=wind_turbine_has_wms)
    return mapping


def test_simple_query(windpower_mapping):
    df = windpower_mapping.query("SELECT ?a ?b WHERE {?a a ?b}").sort(["a", "b"])
    filename = TESTDATA_PATH / "simple_query.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(["a", "b"]).collect()
    pl.testing.assert_frame_equal(df, expected_df)

def test_larger_query(windpower_mapping):
    query = """PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/magbak/chrontext#>
PREFIX wp:<https://github.com/magbak/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/chrontext/rds_power#>
SELECT ?site_label ?wtur_label ?ts ?ts_label WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site rds:hasFunctionalAspect ?wtur_asp .
    ?wtur_asp rdfs:label ?wtur_label .
    ?wtur rds:hasFunctionalAspectNode ?wtur_asp .
    ?wtur rds:hasFunctionalAspect ?gensys_asp .
    ?wtur a rds:A .
    ?gensys rds:hasFunctionalAspectNode ?gensys_asp .
    ?gensys a rds:RA .
    ?gensys rds:hasFunctionalAspect ?generator_asp .
    ?generator rds:hasFunctionalAspectNode ?generator_asp .
    ?generator a rds:GAA .
    ?generator ct:hasTimeseries ?ts .
    ?ts rdfs:label ?ts_label .
    FILTER(?wtur_label = "A1" && ?site_label = "Wind Mountain") .
}"""
    by = ["site_label", "wtur_label", "ts", "ts_label"]
    df = windpower_mapping.query(query).sort(by)
    filename = TESTDATA_PATH / "larger_query.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)

def test_simple_property_path_query(windpower_mapping):
    query = """PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/magbak/chrontext#>
PREFIX wp:<https://github.com/magbak/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/chrontext/rds_power#>
SELECT ?site_label ?node WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site rds:hasFunctionalAspect / ^rds:hasFunctionalAspectNode ?node .
}"""
    by = ["site_label", "node"]
    df = windpower_mapping.query(query).sort(by)
    filename = TESTDATA_PATH / "simple_property_path_query.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    pl.testing.assert_frame_equal(df, expected_df)

def test_iterated_property_path_query(windpower_mapping):
    query = """PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/magbak/chrontext#>
PREFIX wp:<https://github.com/magbak/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/chrontext/rds_power#>
SELECT ?site_label ?node WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site (rds:hasFunctionalAspect / ^rds:hasFunctionalAspectNode)+ ?node .
}"""
    by = ["site_label", "node"]
    start = time.time()
    df = windpower_mapping.query(query).sort(by)
    end = time.time()
    print(f"Took {round(end-start, 3)}")
    filename = TESTDATA_PATH / "iterated_property_path_query.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    pl.testing.assert_frame_equal(df, expected_df)

def test_iterated_property_path_constant_object_query(windpower_mapping):
    query = """PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/magbak/chrontext#>
PREFIX wp:<https://github.com/magbak/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/chrontext/rds_power#>
SELECT ?site_label WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site (rds:hasFunctionalAspect / ^rds:hasFunctionalAspectNode)+ <https://github.com/magbak/chrontext/windpower_example#Generator40> .
}"""
    by = ["site_label"]
    start = time.time()
    df = windpower_mapping.query(query).sort(by)
    end = time.time()
    print(f"Took {round(end-start, 3)}")
    filename = TESTDATA_PATH / "iterated_property_path_constant_object_query.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    pl.testing.assert_frame_equal(df, expected_df)

def test_iterated_property_path_constant_subject_query(windpower_mapping):
    query = """PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/magbak/chrontext#>
PREFIX wp:<https://github.com/magbak/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/chrontext/rds_power#>
SELECT ?node WHERE {
    <https://github.com/magbak/chrontext/windpower_example#Site3> (rds:hasFunctionalAspect / ^rds:hasFunctionalAspectNode)+ ?node .
}"""
    by = ["node"]
    start = time.time()
    df = windpower_mapping.query(query).sort(by)
    end = time.time()
    print(f"Took {round(end-start, 3)}")
    filename = TESTDATA_PATH / "iterated_property_path_constant_subject_query.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    pl.testing.assert_frame_equal(df, expected_df)


def test_iterated_property_path_query_with_bug(windpower_mapping):
    query = """PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/magbak/chrontext#>
PREFIX wp:<https://github.com/magbak/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/magbak/chrontext/rds_power#>
SELECT ?site_label ?node_label WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site (rds:hasFunctionalAspect / ^rds:hasFunctionalAspectNode)+ ?node .
    ?node rdfs:label ?node_label .
}"""
    by = ["site_label", "node_label"]
    start = time.time()
    df = windpower_mapping.query(query).sort(by)
    end = time.time()
    print(f"Took {round(end-start, 3)}")
    filename = TESTDATA_PATH / "iterated_property_path_query_with_bug.csv"
    #df.write_csv(filename)
    expected_df = pl.scan_csv(filename).sort(by).collect()
    assert_frame_equal(df, expected_df)

def test_simple_construct_query(windpower_mapping):
    dfs = windpower_mapping.query("""
    PREFIX ct:<https://github.com/magbak/chrontext#>
    CONSTRUCT {
    ?a a ct:something.
    ?b a ct:nothing. 
    } WHERE {?a a ?b}""")
    something = dfs[0].sort(["subject", "object"])
    nothing = dfs[1].sort(["subject", "object"])
    filename_something = TESTDATA_PATH / "simple_construct_query_something.csv"
    #something.write_csv(filename_something)
    filename_nothing = TESTDATA_PATH / "simple_construct_query_nothing.csv"
    #nothing.write_csv(filename_nothing)
    expected_something_df = pl.scan_csv(filename_something).sort(["subject", "object"]).collect()
    assert_frame_equal(something, expected_something_df)
    expected_nothing_df = pl.scan_csv(filename_nothing).sort(["subject", "object"]).collect()
    assert_frame_equal(nothing, expected_nothing_df)

def test_simple_insert_construct_query(windpower_mapping):
    windpower_mapping.insert("""
    PREFIX ct:<https://github.com/magbak/chrontext#>
    CONSTRUCT {
    ?a a ct:somethingTestit.
    ?b a ct:nothingTestit. 
    } WHERE {?a a ?b}""")

    something = windpower_mapping.query("""
        PREFIX ct:<https://github.com/magbak/chrontext#>
        SELECT ?a
        WHERE {
        ?a a ct:somethingTestit .
        }
    """).sort(["a"])
    nothing = windpower_mapping.query("""
            PREFIX ct:<https://github.com/magbak/chrontext#>
            SELECT ?a
            WHERE {
            ?a a ct:nothingTestit .
            }
        """).sort(["a"])
    filename_something = TESTDATA_PATH / "simple_insert_query_something.csv"
    #something.write_csv(filename_something)
    filename_nothing = TESTDATA_PATH / "simple_insert_query_nothing.csv"
    #nothing.write_csv(filename_nothing)
    expected_something_df = pl.scan_csv(filename_something).sort(["a"]).collect()
    assert_frame_equal(something, expected_something_df)
    expected_nothing_df = pl.scan_csv(filename_nothing).sort(["a"]).collect()
    assert_frame_equal(nothing, expected_nothing_df)