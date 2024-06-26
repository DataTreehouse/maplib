## maplib: High-performance RDF knowledge graph construction, validation and enrichment in Python
maplib is a knowledge graph construction library for building RDF knowledge graphs using template expansion ([OTTR](https://ottr.xyz/) Templates). Maplib features SPARQL- and SHACL-engines that are available as the graph is being constructed, allowing enrichment and validation. It can construct and validate knowledge graphs with millions of nodes in seconds.

maplib allows you to leverage your existing skills with Pandas or Polars to extract and wrangle data from existing databases and spreadsheets, before applying simple templates to them to build a knowledge graph. 

Template expansion is typically zero-copy and nearly instantaneous, and the built-in SPARQL and SHACL engines means you can query, inspect, enrich and validate the knowledge graph immediately.      

maplib is written in Rust, it is built on [Apache Arrow](https://arrow.apache.org/) using [Pola.rs](https://www.pola.rs/) and uses libraries from [Oxigraph](https://github.com/oxigraph/oxigraph) for handling linked data as well as parsing SPARQL queries.

## Installing
The package is published on [PyPi](https://pypi.org/project/maplib/) and the API documented [here](https://datatreehouse.github.io/maplib/maplib/maplib.html):
```shell
pip install maplib
```
Please send us a message on our [webpage](https://www.data-treehouse.com/contact-8) if you want to try out SHACL.  

## Mapping
We can easily map DataFrames to RDF-graphs using the Python library. Below is a reproduction of the example in the paper [1]. Assume that we have a DataFrame given by: 

```python
ex = "https://github.com/DataTreehouse/maplib/example#"
co = "https://github.com/DataTreehouse/maplib/countries#"
pi = "https://github.com/DataTreehouse/maplib/pizza#"
ing = "https://github.com/DataTreehouse/maplib/pizza/ingredients#"

import polars as pl
df = pl.DataFrame({"p":[pi + "Hawaiian", pi + "Grandiosa"],
                   "c":[co + "CAN", co + "NOR"],
                   "is": [[ing + "Pineapple", ing + "Ham"],
                          [ing + "Pepper", ing + "Meat"]]})
df
```
That is, our DataFrame is:

| p                             | c                                  | is                                                   |
|-------------------------------|------------------------------------|------------------------------------------------------|
| str                           | str                                | list[str]                                            |
| "https://.../pizza#Hawaiian"  | "https://.../maplib/countries#CAN" | [".../ingredients#Pineapple", ".../ingredients#Ham"] |
| "https://.../pizza#Grandiosa" | "https://.../maplib/countries#NOR" | [".../ingredients#Pepper", ".../ingredients#Meat"]   |

Then we can define a OTTR template, and create our knowledge graph by expanding this template with our DataFrame as input:
```python
from maplib import Mapping
pl.Config.set_fmt_str_lengths(150)

doc = """
@prefix pizza:<https://github.com/DataTreehouse/maplib/pizza#>.
@prefix xsd:<http://www.w3.org/2001/XMLSchema#>.
@prefix ex:<https://github.com/DataTreehouse/maplib/pizza#>.

ex:Pizza[?p, xsd:anyURI ?c, List<xsd:anyURI> ?is] :: {
ottr:Triple(?p, a, pizza:Pizza),
ottr:Triple(?p, pizza:fromCountry, ?c),
cross | ottr:Triple(?p, pizza:hasIngredient, ++?is)
}.
"""

m = Mapping([doc])
m.expand("ex:Pizza", df)
```

We can immediately query the mapped knowledge graph:

```python
m.query("""
PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>
SELECT ?p ?i WHERE {
?p a pizza:Pizza .
?p pizza:hasIngredient ?i .
}
""")
```

The query gives the following result (a DataFrame):

| p                               | i                                     |
|---------------------------------|---------------------------------------|
| str                             | str                                   |
| "<https://.../pizza#Grandiosa>" | "<https://.../ingredients#Meat>"      |
| "<https://.../pizza#Grandiosa>" | "<https://.../ingredients#Pepper>"    |
| "<https://.../pizza#Hawaiian>"  | "<https://.../ingredients#Pineapple>" |
| "<https://.../pizza#Hawaiian>"  | "<https://.../ingredients#Ham>"       |

Next, we are able to perform a construct query, which creates new triples but does not insert them. 

```python
hpizzas = """
PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>
PREFIX ing:<https://github.com/DataTreehouse/maplib/pizza/ingredients#>
CONSTRUCT { ?p a pizza:UnorthodoxPizza } 
WHERE {
    ?p a pizza:Pizza .
    ?p pizza:hasIngredient ing:Pineapple .
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
PREFIX pizza:<https://github.com/DataTreehouse/maplib/pizza#>

SELECT ?p WHERE {
?p a pizza:UnorthodoxPizza
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
- querying
- validating
- importing triples
- writing triples

The API is documented [HERE](https://datatreehouse.github.io/maplib/maplib/maplib.html)

## References
There is an associated paper [1] with associated benchmarks showing superior performance and scalability that can be found [here](https://ieeexplore.ieee.org/document/10106242). OTTR is described in [2].

[1] M. Bakken, "maplib: Interactive, literal RDF model mapping for industry," in IEEE Access, doi: 10.1109/ACCESS.2023.3269093.

[2] M. G. Skjæveland, D. P. Lupp, L. H. Karlsen, and J. W. Klüwer, “Ottr: Formal templates for pattern-based ontology engineering.” in WOP (Book),
2021, pp. 349–377.

## Licensing
All code produced since August 1st. 2023 is copyrighted to [Data Treehouse AS](https://www.data-treehouse.com/) with an Apache 2.0 license unless otherwise noted. 

All code which was produced before August 1st. 2023 copyrighted to [Prediktor AS](https://www.prediktor.com/) with an Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree. The code at this state is archived in the repository at [https://github.com/magbak/maplib](https://github.com/magbak/maplib).
