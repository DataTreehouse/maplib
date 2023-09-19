# maplib API Documentation

The mapper implements the [stOTTR](https://dev.spec.ottr.xyz/stOTTR/) language with extensions for mapping asset structures inspired by the [Epsilon Transformation Language](https://www.eclipse.org/epsilon/doc/etl/). 
Implemented with [Apache Arrow](https://arrow.apache.org/) in Rust using [Pola.rs](https://www.pola.rs/). 
We provide a Python wrapper for the library, which allows us to create mappings using DataFrames. 

## API
The API is simple, and contains only one class and a few methods.
```python
from maplib import Mapping
import polars as pl
```

Mapping-objects are initialized using a list of stOttr documents (each a string). 
```python
doc = """
    @prefix ex:<http://example.net/ns#>.
    ex:ExampleTemplate [?MyValue] :: {
    ottr:Triple(ex:myObject, ex:hasValue, ?MyValue)
    } .
    """
mapping = Mapping([doc])
```

In order to extend this template, we provide a DataFrame with a particular signature.
We use the _extend_-method of the mapping-object.

```python
from typing import Optional, Dict
def extend(self, 
           template: str,
           df: pl.DataFrame,
           language_tags: Optional[Dict[String, String]]=None
           ) 
```
The _template_-argument specifies the template we want to expand. 
We are allowed to use the prefixes in the stOttr documents when referring to these templates unless there are conflicting prefix-definitions. 
The parameters of the templates must be provided as identically-named columns. To provide a null-argument, just make a column of nulls.

## Exporting
Multiple alternatives exist to export the mapped triples. The fastest way to serialize is the _write_ntriples_-method.
```python
def write_ntriples(&self, file:str)
```