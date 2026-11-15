"""map_xml keeps the text at entity and character references.

quick-xml reports `&ref;` and `&#N;` as separate events. map_xml joins them with the
surrounding text and CDATA into one text child per run of text.
"""

from maplib import Model


def text_children(xml: str) -> list:
    m = Model()
    m.map_xml(xml)
    df = m.query(
        """
        PREFIX fx: <http://sparql.xyz/facade-x/ns/>
        SELECT ?val WHERE { ?s fx:child ?val FILTER(isLiteral(?val)) }
        """
    )
    return sorted(df.get_column("val").to_list())


def test_map_xml_predefined_entities_and_char_refs():
    assert text_children(
        "<doc><item>Pump &amp; motor &#65;</item></doc>"
    ) == ["Pump & motor A"]


def test_map_xml_lt_gt_quot_apos():
    assert text_children(
        "<doc><item>a &lt; b &gt; c &quot;d&quot; &apos;e&apos;</item></doc>"
    ) == ["a < b > c \"d\" 'e'"]


def test_map_xml_hex_char_ref():
    assert text_children("<doc><item>&#x41;&#x42;C</item></doc>") == ["ABC"]


def test_map_xml_reference_only_text():
    assert text_children("<doc><item>&amp;</item></doc>") == ["&"]


def test_map_xml_text_mixed_with_cdata():
    assert text_children(
        "<doc><item>  x &lt; <![CDATA[<y> & z]]> &#65;  </item></doc>"
    ) == ["x < <y> & z A"]


def test_map_xml_mixed_content_keeps_separate_runs():
    assert text_children(
        "<doc><item>before &amp; <b>bold</b> after &#65;</item></doc>"
    ) == ["after A", "before &", "bold"]


def test_map_xml_whitespace_between_elements_is_not_text():
    assert text_children(
        "<doc>\n  <item>a</item>\n  <!-- c -->\n  <item>b</item>\n</doc>"
    ) == ["a", "b"]


def test_map_xml_undeclared_entity_is_dropped():
    assert text_children("<doc><item>a &nbsp;b</item></doc>") == ["a b"]


def test_map_xml_custom_entity_is_dropped_not_expanded():
    xml = """<?xml version="1.0"?>
<!DOCTYPE doc [ <!ENTITY custom "EXPANDED"> ]>
<doc><item>x &custom; y</item></doc>"""
    assert text_children(xml) == ["x  y"]
