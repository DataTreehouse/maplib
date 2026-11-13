## maplib: High-performance RDF knowledge graph construction, SHACL validation, SPARQL and Datalog in Python
maplib is written in Rust, it is built on [Apache Arrow](https://arrow.apache.org/) using [Pola.rs](https://www.pola.rs/) and uses libraries from [Oxigraph](https://github.com/oxigraph/oxigraph) for handling linked data as well as parsing SPARQL queries.

maplib allows you to leverage your existing skills with Pandas or Polars to extract and wrangle data from existing databases and spreadsheets, before applying simple templates to them to build a knowledge graph. 
You can also read knowledge graphs extremely quickly from a wide variety of serialization formats. 
Using the built-in SPARQL, SHACL and Datalog engines means you can query, inspect, enrich and validate and then serialize the knowledge graph immediately. 
All query results are Polars Dataframes that are transferred zero-copy from Rust to Python. 
Currently, maplib is in-memory and supports around 100M triples on 32GB of RAM. 

The core functionality of maplib (mapping, querying, serialization) is open source, but SHACL and Datalog functionality are not.
Please send us a message, e.g. on LinkedIn (search for Data Treehouse) or on email (magnus at data-treehouse.com) if you want to try out these features.
See our roadmap for upcoming features.

## Installing
The package is published on [PyPi](https://pypi.org/project/maplib/) and the API documented [here](https://datatreehouse.github.io/maplib/maplib.html):
```shell
pip install maplib
```
## Model
We can easily map DataFrames to RDF-graphs using the Python library. Below is a reproduction of the example in the paper [1]. Assume that we have a DataFrame given by: 

```python
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
from maplib import Model, Prefix, Template, Argument, Parameter, Variable, RDFType, Triple, a
pi = Prefix(pi)

p_var = Variable("p")
c_var = Variable("c")
ings_var = Variable("ings")

template = Template(
    iri= pi.suf("PizzaTemplate"),
    parameters= [
        Parameter(variable=p_var, rdf_type=RDFType.IRI),
        Parameter(variable=c_var, rdf_type=RDFType.IRI),
        Parameter(variable=ings_var, rdf_type=RDFType.Nested(RDFType.IRI))
    ],
    instances= [
        Triple(p_var, a, pi.suf("Pizza")),
        Triple(p_var, pi.suf("fromCountry"), c_var),
        Triple(
            p_var, 
            pi.suf("hasIngredient"), 
            Argument(term=ings_var, list_expand=True), 
            list_expander="cross")
    ]
)

m = Model()
m.map(template, df)
hpizzas = """
    PREFIX pi:<https://github.com/DataTreehouse/maplib/pizza#>
    CONSTRUCT { ?p a pi:HeterodoxPizza } 
    WHERE {
        ?p a pi:Pizza .
        ?p pi:hasIngredient pi:Pineapple .
    }"""
m.insert(hpizzas)
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

If we are happy with the output of this construct-query, we can insert it in the model state. Afterwards we check that the triple is added with a query.

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

## Streaming large inputs
The mapping methods `map`, `map_triples` and `map_default` accept a DataFrame, but also a Polars LazyFrame (consumed in one pass with `collect_batches()`) or any iterable of DataFrames, LazyFrames or SolutionMappings. Each batch is mapped into the model as it arrives, so the input never has to fit in memory as a whole:

```python
m.map(template, pl.scan_parquet("pizzas/*.parquet"))
```

The triples still end up in the model. When the result is too large for that, or when you only want to transform data from one format to another, pass `stream=True`. You then get a lazy `ModelStream` instead, and add steps to it that run batch by batch when you call `collect()`:

```python
(
    m.map(template, pl.scan_parquet("pizzas/*.parquet"), stream=True)
    .insert(hpizzas)                          # CONSTRUCT, as Model.insert
    .update(some_sparql_update)               # SPARQL Update, as Model.update
    .infer(rules)                             # Datalog, as Model.infer
    .write("pizzas.nt")                       # N-Triples (the default)
    .write("pizzas.ttl", format="turtle")
    .collect()
)
```

For each batch, `collect()`:
1. maps the batch into a fresh in-memory model that has the templates, prefixes and UDFs of `m`,
2. runs the steps on it in the order you added them; a `write` step appends the triples of the batch to its file (not those in the transient graph), so writing before and after a step gives you both versions,
3. drops the batch model before the next batch.

Memory use therefore depends on the batch size and not on the size of the input. Things to be aware of:
- The steps only see the triples of the current batch. A query that joins triples from different batches finds nothing, so split the input so that what belongs together is in the same batch.
- `m` itself is not changed: its triples are neither read nor written by the stream.
- Each step returns a new `ModelStream`, so a partial pipeline can be reused; nothing runs before `collect()`. Iterators given as input are consumed by `collect()`.
- Write steps support `"ntriples"`, `"turtle"` (prefixes are declared once at the top) and `"nquads"`. Files are created, or overwritten, when `collect()` starts.
- Blank nodes, both from templates and from SPARQL updates, get labels that are unique across batches.
- `map_default(..., stream=True)` creates the default template from the first batch.
- Datalog (`infer`) requires maplib Enterprise Edition.

### JSON and XML documents
`map_json` and `map_xml` accept a single document (a string or a path) or an iterable of documents, which are mapped one at a time. Without a `uuid_namespace`, IRIs are generated from the path of a document, or randomly for a document given as a string. With a `uuid_namespace`, document number `i` of an iterable uses the namespace `<uuid_namespace>/<i>`, so the documents get different but reproducible IRIs.

With `stream=True`, `documents_per_batch` documents (default 1) are mapped into each batch. A useful pattern is to map the documents into the transient graph, construct the triples you want, and write only those. For example, converting a JSON Lines file with one record per line:

```python
records = """
PREFIX fx:  <http://sparql.xyz/facade-x/ns/>
PREFIX xyz: <http://sparql.xyz/facade-x/data/>
PREFIX ex:  <http://example.net/>
CONSTRUCT { ?r a ex:Record ; ex:name ?name } WHERE { ?r a fx:root ; xyz:name ?name }
"""
from maplib import TRANSIENT_GRAPH

with open("records.jsonl") as lines:
    (
        Model()
        .map_json(lines, graph=TRANSIENT_GRAPH, stream=True, documents_per_batch=10_000)
        .insert(records)
        .write("records.nt")
        .collect()
    )
```

## API
The [API](https://datatreehouse.github.io/maplib/maplib.html) is simple, and contains only one class and a few methods for:
- expanding templates, also from LazyFrames and iterables of batches
- streaming pipelines that map, transform and write large inputs batch by batch
- querying with SPARQL
- validating with SHACL
- reading JSON and XML to triples using Façade-X, also from iterables of documents
- importing triples (Turtle, RDF/XML, NTriples, JSON-LD, HDT)
- writing triples (Turtle, RDF/XML, NTriples)
- creating a new Model from a named graph

The API is documented [HERE](https://datatreehouse.github.io/maplib/maplib.html)

## Roadmap of features and optimizations
Spring 2026
- SHACL Rules
- Disk based storage and internal serialization format
- Jelly

Roadmap is subject to changes,particularly user and customer requests. 

## References
There is an associated paper [1] with associated benchmarks showing superior performance and scalability that can be found [here](https://ieeexplore.ieee.org/document/10106242). OTTR is described in [2].

[1] M. Bakken, "maplib: Interactive, literal RDF model model for industry," in IEEE Access, doi: 10.1109/ACCESS.2023.3269093.

[2] M. G. Skjæveland, D. P. Lupp, L. H. Karlsen, and J. W. Klüwer, “Ottr: Formal templates for pattern-based ontology engineering.” in WOP (Book),
2021, pp. 349–377.

## Contributing
Any contributions will fall under the Apache 2.0 license of maplib. 

## Licensing
All code produced since August 1st. 2023 is copyrighted to [Data Treehouse AS](https://www.data-treehouse.com/) with an Apache 2.0 license unless otherwise noted. 

All code which was produced before August 1st. 2023 copyrighted to [Prediktor AS](https://www.prediktor.com/) with an Apache 2.0 license unless otherwise noted, and has been financed by [The Research Council of Norway](https://www.forskningsradet.no/en/) (grant no. 316656) and [Prediktor AS](https://www.prediktor.com/) as part of a PhD Degree. The code at this state is archived in the repository at [https://github.com/magbak/maplib](https://github.com/magbak/maplib).
