## maplib: High-performance RDF knowledge graph construction, SHACL validation and SPARQL-based enrichment in Python
maplib is a knowledge graph construction library for building RDF knowledge graphs using template expansion ([OTTR](https://ottr.xyz/) Templates). Maplib features SPARQL- and SHACL-engines that are available as the graph is being constructed, allowing enrichment and validation. It can construct and validate knowledge graphs with millions of nodes in seconds.

maplib allows you to leverage your existing skills with Pandas or Polars to extract and wrangle data from existing databases and spreadsheets, before applying simple templates to them to build a knowledge graph. 

Template expansion is typically zero-copy and nearly instantaneous, and the built-in SPARQL and SHACL engines means you can query, inspect, enrich and validate the knowledge graph immediately.      

maplib is written in Rust, it is built on [Apache Arrow](https://arrow.apache.org/) using [Pola.rs](https://www.pola.rs/) and uses libraries from [Oxigraph](https://github.com/oxigraph/oxigraph) for handling linked data as well as parsing SPARQL queries.

## Installing
The package is published on [PyPi](https://pypi.org/project/maplib/) and the API documented [here](https://datatreehouse.github.io/maplib/maplib/maplib.html):
```shell
pip install maplib
```
Please send us a message, e.g. on LinkedIn (search for Data Treehouse) or on our [webpage](https://www.data-treehouse.com/contact-8) if you want to try out SHACL.  

## Mapping
We can easily map DataFrames to RDF-graphs using the Python library. Below is a reproduction of the example in the paper [1]. Assume that we have a DataFrame given by: 

```python
from maplib import Mapping, Prefix, Template, Argument, Parameter, Variable, RDFType, triple, a
import polars as pl
pl.Config.set_fmt_str_lengths(150)

pi = "https://github.com/DataTreehouse/maplib/pizza#"
df = pl.DataFrame({
    "p":[pi + "Hawaiian", pi + "Grandiosa"],
    "c":[pi + "CAN", pi + "NOR"],
    "ings": [[pi + "Pineapple", pi + "Ham"],
             [pi + "Pepper", pi + "Meat"]]
})
print(df)
```
That is, our DataFrame is:

| p                             | c                              | ings                                     |
|-------------------------------|--------------------------------|------------------------------------------|
| str                           | str                            | list[str]                                |
| "https://.../pizza#Hawaiian"  | "https://.../maplib/pizza#CAN" | [".../pizza#Pineapple", ".../pizza#Ham"] |
| "https://.../pizza#Grandiosa" | "https://.../maplib/pizza#NOR" | [".../pizza#Pepper", ".../pizza#Meat"]   |

Then we can define a OTTR template, and create our knowledge graph by expanding this template with our DataFrame as input:
```python
from maplib import Mapping, Prefix, Template, Argument, Parameter, Variable, RDFType, triple, a
pi = Prefix("pi", pi)

p_var = Variable("p")
c_var = Variable("c")
ings_var = Variable("ings")

template = Template(
    iri= pi.suf("PizzaTemplate"),
    parameters= [
        Parameter(variable=p_var, rdf_type=RDFType.IRI()),
        Parameter(variable=c_var, rdf_type=RDFType.IRI()),
        Parameter(variable=ings_var, rdf_type=RDFType.Nested(RDFType.IRI()))
    ],
    instances= [
        triple(p_var, a(), pi.suf("Pizza")),
        triple(p_var, pi.suf("fromCountry"), c_var),
        triple(
            p_var, 
            pi.suf("hasIngredient"), 
            Argument(term=ings_var, list_expand=True), 
            list_expander="cross")
    ]
)

m = Mapping()
m.expand(template, df)
hpizzas = """
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
    CONSTRUCT { ?p a pi:HeterodoxPizza } 
    WHERE {
        ?p a pi:Pizza .
        ?p pi:hasIngredient pi:Pineapple .
    }"""
m.insert(hpizzas)
return m
```

We can immediately query the mapped knowledge graph:

```python
m.query("""
PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
SELECT ?p ?i WHERE {
?p a pi:Pizza .
?p pi:hasIngredient ?i .
}
""")
```

The query gives the following result (a DataFrame):

| p                               | i                                     |
|---------------------------------|---------------------------------------|
| str                             | str                                   |
| "<https://.../pizza#Grandiosa>" | "<https://.../pizza#Meat>"      |
| "<https://.../pizza#Grandiosa>" | "<https://.../pizza#Pepper>"    |
| "<https://.../pizza#Hawaiian>"  | "<https://.../pizza#Pineapple>" |
| "<https://.../pizza#Hawaiian>"  | "<https://.../pizza#Ham>"       |

Next, we are able to perform a construct query, which creates new triples but does not insert them. 

```python
hpizzas = """
PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
CONSTRUCT { ?p a pi:UnorthodoxPizza } 
WHERE {
    ?p a pi:Pizza .
    ?p pi:hasIngredient pi:Pineapple .
}"""
res = m.query(hpizzas)
res[0]
```

The resulting triples are given below:

| subject                        | verb                                 | object                                |
|--------------------------------|--------------------------------------|---------------------------------------|
| str                            | str                                  | str                                   |
| "<https://.../pizza#Hawaiian>" | "<http://.../22-rdf-syntax-ns#type>" | "<https://.../pizza#UnorthodoxPizza>" |

If we are happy with the output of this construct-query, we can insert it in the mapping state. Afterwards we check that the triple is added with a query.

```python
m.insert(hpizzas)
m.query("""
PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>

SELECT ?p WHERE {
?p a pi:UnorthodoxPizza
}
""")
```

Indeed, we have added the triple: 

| p                                                          |
|------------------------------------------------------------|
| str                                                        |
| "<https://github.com/DataTreehouse/maplib/pizza#Hawaiian>" |

## API
The [API](https://datatreehouse.github.io/maplib/maplib/maplib.html) is simple, and contains only one class and a few methods for:
- expanding templates
- querying with SPARQL
- validating SHACL
- importing triples (Turtle, RDF/XML, NTriples)
- writing triples (NTriples)
- creating a new Mapping object (sprout) based on queries over the current Mapping object.

The API is documented [HERE](https://datatreehouse.github.io/maplib/maplib/maplib.html)

## References
There is an associated paper [1] with associated benchmarks showing superior performance and scalability that can be found [here](https://ieeexplore.ieee.org/document/10106242). OTTR is described in [2].

[1] M. Bakken, "maplib: Interactive, literal RDF model mapping for industry," in IEEE Access, doi: 10.1109/ACCESS.2023.3269093.

[2] M. G. Skjæveland, D. P. Lupp, L. H. Karlsen, and J. W. Klüwer, “Ottr: Formal templates for pattern-based ontology engineering.” in WOP (Book),
2021, pp. 349–377.

## Licensing
All code produced since August 1st. 2023 is copyrighted to [Data Treehouse AS](https://www.data-treehouse.com/) with an Apache 2.0 license unless otherwise noted. 

All code which was produced before August 1st. 2023 copyrighted to [Prediktor AS](https://www.prediktor.com/) with an Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree. The code at this state is archived in the repository at [https://github.com/magbak/maplib](https://github.com/magbak/maplib).
