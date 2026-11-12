import polars as pl
from maplib import Model


def _lang_df(lang_field: str) -> pl.DataFrame:
    df = pl.DataFrame(
        {
            "id": ["urn:ex:a", "urn:ex:b"],
            "value": ["abc", "def"],
            lang_field: ["en", "fr"],
        }
    )
    return df.select(
        "id", pl.struct(["value", lang_field]).alias("label")
    )


def test_map_default_lang_string_struct_value_language():
    m = Model()
    m.map_default(_lang_df("language"), "id")
    df = m.query("SELECT ?a ?c WHERE { ?a ?b ?c } ORDER BY ?a")
    assert df.get_column("c").to_list() == ['"abc"@en', '"def"@fr']


def test_map_default_lang_string_struct_value_lang():
    m = Model()
    m.map_default(_lang_df("lang"), "id")
    df = m.query("SELECT ?a ?c WHERE { ?a ?b ?c } ORDER BY ?a")
    assert df.get_column("c").to_list() == ['"abc"@en', '"def"@fr']


def test_template_lang_string_struct():
    m = Model()
    m.add_template(
        """
        @prefix ex: <http://example.net/ns#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:LabelTemplate [ ottr:IRI ?id, rdf:langString ?label ] :: {
            ottr:Triple(?id, ex:label, ?label)
        } .
        """
    )
    m.map("http://example.net/ns#LabelTemplate", _lang_df("language"))
    df = m.query("SELECT ?a ?c WHERE { ?a ?b ?c } ORDER BY ?a")
    assert df.get_column("c").to_list() == ['"abc"@en', '"def"@fr']
