import polars as pl
import pytest
from datetime import datetime
from polars.testing import assert_frame_equal
from maplib import Model, MaplibException

pl.Config.set_fmt_str_lengths(300)


def test_incorrect_prefix_syntax_fails():
    '''Test that incorrect :prefix syntax fails'''
    doc = '''
    :prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
       ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    '''
    m = Model()
    with pytest.raises(MaplibException):
        m.add_template(doc)


def test_missing_ottr_prefix_fails():
    '''Test that missing ottr prefix definition fails'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    
    ex:ExampleTemplate [?MyValue] :: {
       ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    '''
    m = Model()
    with pytest.raises(MaplibException):
        m.add_template(doc)


def test_all_iri_case():
    '''Test IRI types in stOTTR templates'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:ExampleTemplate [ottr:IRI ?myVar1] :: {
        ottr:Triple(ex:anObject, ex:relatesTo, ?myVar1)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'myVar1': [
            'http://example.net/ns#OneThing',
            'http://example.net/ns#AnotherThing'
        ]
    })
    
    m.map('ex:ExampleTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?o WHERE {
            ex:anObject ex:relatesTo ?o .
        }
        ORDER BY ?o
    ''')
    
    assert result.height == 2
    assert result['o'][0] == '<http://example.net/ns#AnotherThing>'
    assert result['o'][1] == '<http://example.net/ns#OneThing>'


def test_string_language_tag_cases():
    '''Test strings and language tags'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:ExampleTemplate [?myString] :: {
        ottr:Triple(ex:anObject, ex:hasString, ?myString),
        ottr:Triple(ex:anotherObject, ex:hasString, ""@ar-SA)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({'myString': ['one', 'two']})
    
    m.map('ex:ExampleTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?o WHERE {
            ?s ex:hasString ?o .
        }
    ''')
    
    # Should have 2 rows from myString + 1 row from empty language tagged literal
    assert result.height == 3


def test_const_list_case():
    '''Test constant list expansion'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:ExampleTemplate [ottr:IRI ?var1] :: {
        cross | ottr:Triple(?var1, ex:hasNumber, ++(1, 2))
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'var1': [
            'http://example.net/ns#OneThing',
            'http://example.net/ns#AnotherThing'
        ]
    })
    
    m.map('ex:ExampleTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?o WHERE {
            ?s ex:hasNumber ?o .
        }
        ORDER BY ?s ?o
    ''')
    
    # 2 subjects × 2 numbers = 4 triples
    assert result.height == 4


def test_nested_templates():
    '''Test template calling another template'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:ExampleTemplate [?myVar1, ?myVar2] :: {
        ex:Nested(?myVar1),
        ottr:Triple(ex:anObject, ex:hasOtherNumber, ?myVar2)
    } .
    
    ex:Nested [?myVar] :: {
        ottr:Triple(ex:anObject, ex:hasNumber, ?myVar)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'myVar1': [1, 2],
        'myVar2': [3, 4]
    })
    
    m.map('ex:ExampleTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?p ?o WHERE {
            ex:anObject ?p ?o .
        }
        ORDER BY ?p ?o
    ''')
    
    # Should have 4 triples: 2 hasNumber + 2 hasOtherNumber
    assert result.height == 4


def test_derived_datatypes():
    '''Test various Polars datatypes being mapped to RDF'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:ExampleTemplate [
        ?Boolean,
        ?UInt32,
        ?UInt64,
        ?Int32,
        ?Int64,
        ?Float32,
        ?Float64,
        ?String,
        ?Datetime
    ] :: {
        ottr:Triple(ex:anObject, ex:hasVal, ?Boolean),
        ottr:Triple(ex:anObject, ex:hasVal, ?UInt32),
        ottr:Triple(ex:anObject, ex:hasVal, ?UInt64),
        ottr:Triple(ex:anObject, ex:hasVal, ?Int32),
        ottr:Triple(ex:anObject, ex:hasVal, ?Int64),
        ottr:Triple(ex:anotherObject, ex:hasValVal, ?Float32),
        ottr:Triple(ex:anotherObject, ex:hasValVal, ?Float64),
        ottr:Triple(ex:yetAnotherObject, ex:hasString, ?String),
        ottr:Triple(ex:yetAnotherObject, ex:hasDateTime, ?Datetime)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'Boolean': [True, False],
        'UInt32': pl.Series([5, 6], dtype=pl.UInt32),
        'UInt64': pl.Series([7, 8], dtype=pl.UInt64),
        'Int32': pl.Series([-13, -14], dtype=pl.Int32),
        'Int64': pl.Series([-15, -16], dtype=pl.Int64),
        'Float32': pl.Series([17.18, 19.20], dtype=pl.Float32),
        'Float64': [21.22, 23.24],
        'String': ['abcde', 'fghij'],
        'Datetime': [
            datetime(2022, 7, 3, 10, 6, 20, 123000),
            datetime(2022, 7, 3, 10, 6, 21, 456000)
        ]
    })
    
    m.map('ex:ExampleTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?p ?o WHERE {
            ?s ?p ?o .
        }
    ''')
    
    # Each row maps to 9 triples = 18 triples total
    assert result.height == 18


def test_list_arguments():
    '''Test list expansion with grouping'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:AnotherExampleTemplate [ottr:IRI ?object, ottr:IRI ?predicate, ?myList] :: {
        cross | ottr:Triple(?object, ?predicate, ++?myList)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    # Create data with groups
    df = pl.DataFrame({
        'object': [
            'http://example.net/ns#obj1',
            'http://example.net/ns#obj1',
            'http://example.net/ns#obj2',
            'http://example.net/ns#obj2'
        ],
        'predicate': [
            'http://example.net/ns#hasNumberFromList1',
            'http://example.net/ns#hasNumberFromList1',
            'http://example.net/ns#hasNumberFromList2',
            'http://example.net/ns#hasNumberFromList2'
        ],
        'myList': [1, 2, 3, 4]
    })
    
    # Group by object and predicate
    df = df.group_by(['object', 'predicate'], maintain_order=True).agg(
        pl.col('myList')
    )
    
    m.map('ex:AnotherExampleTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?p ?o WHERE {
            ?s ?p ?o .
        }
        ORDER BY ?s ?p ?o
    ''')
    
    # obj1 with 2 values, obj2 with 2 values = 4 triples
    assert result.height == 4


def test_two_list_arguments():
    '''Test expansion of two lists'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:TwoListTemplate [ottr:IRI ?object, ?myList1, ?myList2] :: {
        cross | ottr:Triple(?object, ex:hasNumber, ++?myList1),
        cross | ottr:Triple(?object, ex:hasOtherNumber, ++?myList2)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'object': ['http://example.net/ns#obj1', 'http://example.net/ns#obj2'],
        'myList1': [[1, 2], [3, 4]],
        'myList2': [[5, 6], [7, 8, 9]]
    })
    
    m.map('ex:TwoListTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?p ?o WHERE {
            ?s ?p ?o .
        }
        ORDER BY ?s ?p ?o
    ''')
    
    # obj1: 2 hasNumber + 2 hasOtherNumber = 4
    # obj2: 2 hasNumber + 3 hasOtherNumber = 5
    # Total = 9 triples
    assert result.height == 9


def test_default():
    '''Test default mapping without explicit template'''
    m = Model()
    
    df = pl.DataFrame({
        'subject': [
            'http://example.net/ns#obj1',
            'http://example.net/ns#obj1',
            'http://example.net/ns#obj2'
        ],
        'myVar1': [1, 2, None],
        'myVar2': [5, 6, 7]
    })
    
    m.map_default(df, 'subject')
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?p ?o WHERE {
            ?s ?p ?o .
        }
        ORDER BY ?s ?p ?o
    ''')
    
    # obj1: 2 myVar1 values + 2 myVar2 values = 4 triples
    # obj2: 0 myVar1 values (null) + 1 myVar2 value = 1 triple
    # Total = 5 triples
    assert result.height == 5


def test_default_list():
    '''Test default mapping with lists'''
    m = Model()
    
    df = pl.DataFrame({
        'subject': [
            'http://example.net/ns#obj1',
            'http://example.net/ns#obj1',
            'http://example.net/ns#obj2'
        ],
        'myList1': [1, 2, None],
        'myList2': [5, 6, 7]
    })
    
    # Group by subject and aggregate into lists
    df = df.group_by('subject', maintain_order=True).agg([
        pl.col('myList1').drop_nulls(),
        pl.col('myList2')
    ])
    
    m.map_default(df, 'subject')
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?s ?p ?o WHERE {
            ?s ?p ?o .
        }
        ORDER BY ?s ?p ?o
    ''')
    
    # obj1: [1, 2] for myList1 + [5, 6] for myList2 = 4 triples
    # obj2: [] for myList1 (nulls dropped) + [7] for myList2 = 1 triple
    # Total = 5 triples
    assert result.height == 5


def test_comments_in_template():
    '''Test that comments are properly ignored'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    # This is a comment at the start
    ex:ExampleTemplate [?value] :: {
        # Comment before triple
        ottr:Triple(ex:subject, ex:property, ?value)  # inline comment
        # Comment after triple
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({'value': ['test']})
    m.map('ex:ExampleTemplate', df)
    
    result = m.query('SELECT ?o WHERE { ?s ?p ?o }')
    assert result.height == 1


def test_optional_parameters():
    '''Test optional parameters with ? prefix'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    ex:OptionalTemplate [xsd:string ?required, ? xsd:string ?optional] :: {
        ottr:Triple(ex:subject, ex:hasRequired, ?required),
        ottr:Triple(ex:subject, ex:hasOptional, ?optional)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    # Test with both values present
    df1 = pl.DataFrame({
        'required': ['value1'],
        'optional': ['value2']
    })
    m.map('ex:OptionalTemplate', df1)
    
    # Test with optional missing (null)
    df2 = pl.DataFrame({
        'required': ['value3'],
        'optional': [None]
    })
    m.map('ex:OptionalTemplate', df2)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?p ?o WHERE {
            ex:subject ?p ?o .
        }
        ORDER BY ?p ?o
    ''')
    
    # Should have: 2 required + 1 optional (null is skipped) = 3 triples
    assert result.height == 3


def test_typed_literals():
    '''Test literals with explicit type annotations'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    ex:TypedLiteralTemplate [xsd:string ?id] :: {
        ottr:Triple(ex:subject, ex:country, 'United Kingdom'^^xsd:string),
        ottr:Triple(ex:subject, ex:hasId, ?id)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({'id': ['test']})
    m.map('ex:TypedLiteralTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?o WHERE {
            ex:subject ex:country ?o .
        }
    ''')
    
    assert result.height == 1
    assert result['o'][0] == 'United Kingdom'


def test_multiline_parameters():
    '''Test parameter lists split across multiple lines'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    ex:MultilineParamsTemplate [
        xsd:string ?param1,
        xsd:string ?param2,
        ? xsd:string ?param3
    ] :: {
        ottr:Triple(ex:subject, ex:prop1, ?param1),
        ottr:Triple(ex:subject, ex:prop2, ?param2),
        ottr:Triple(ex:subject, ex:prop3, ?param3)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'param1': ['a'],
        'param2': ['b'],
        'param3': ['c']
    })
    m.map('ex:MultilineParamsTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?p ?o WHERE {
            ex:subject ?p ?o .
        }
    ''')
    
    assert result.height == 3


def test_multiline_instances():
    '''Test instance lists split across multiple lines'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    
    ex:MultilineInstancesTemplate [?val] :: {
        ottr:Triple(
            ex:subject,
            ex:property,
            ?val
        ),
        ottr:Triple(
            ex:subject,
            ex:anotherProperty,
            ?val
        )
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({'val': ['test']})
    m.map('ex:MultilineInstancesTemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?p WHERE {
            ex:subject ?p ?o .
        }
        ORDER BY ?p
    ''')
    
    assert result.height == 2


def test_xsd_anyuri_type():
    '''Test xsd:anyURI parameter type'''
    doc = '''
    @prefix ex: <http://example.net/ns#> .
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    ex:URITemplate [? xsd:anyURI ?website] :: {
        ottr:Triple(ex:subject, ex:hasWebsite, ?website)
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({
        'website': ['https://example.com', None]
    })
    m.map('ex:URITemplate', df)
    
    result = m.query('''
        PREFIX ex: <http://example.net/ns#>
        SELECT ?o WHERE {
            ex:subject ex:hasWebsite ?o .
        }
    ''')
    
    # Only non-null value should appear
    assert result.height == 1
    assert 'example.com' in result['o'][0]


def test_long_iri_prefixes():
    '''Test templates with full IRI names instead of prefix abbreviations'''
    doc = '''
    @prefix ottr: <http://ns.ottr.xyz/0.4/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    
    <https://example.org/very/long/path/to/template#MyTemplate> [xsd:string ?value] :: {
        ottr:Triple(
            <https://example.org/very/long/path/to/entity#test>,
            <https://example.org/very/long/path/to/property#hasValue>,
            ?value
        )
    } .
    '''
    m = Model()
    m.add_template(doc)
    
    df = pl.DataFrame({'value': ['test']})
    m.map('https://example.org/very/long/path/to/template#MyTemplate', df)
    
    result = m.query('''
        SELECT ?s WHERE {
            ?s <https://example.org/very/long/path/to/property#hasValue> 'test' .
        }
    ''')
    
    assert result.height == 1
