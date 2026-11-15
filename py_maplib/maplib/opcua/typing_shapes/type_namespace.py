from typing import List

from maplib import (
    Model,
    Template,
    Variable,
    Parameter,
    RDFType,
    xsd,
    Triple,
    Prefix,
)
import polars as pl


def is_type_namespace(
    m: Model, type_namespaces: List[str] = None, instance_namespaces: List[str] = None
):

    if type_namespaces is None and instance_namespaces is None:
        q = """
            PREFIX ua:<http://opcfoundation.org/UA/>
            SELECT DISTINCT ?ns ?node_class WHERE {
                ?n ua:Namespace ?ns .
                ?n ua:NodeClass ?node_class .
            }
        """
        df = m.query(q, solution_mappings=True)
        df = (
            df.mappings.with_columns(
                pl.col("node_class").str.contains("Type").alias("has_types")
            )
            .group_by(pl.col("ns"))
            .agg(pl.col("has_types").any().alias("type_ns"))
        )
    elif type_namespaces is not None:
        q = """
            PREFIX ua:<http://opcfoundation.org/UA/>
            SELECT DISTINCT ?ns WHERE {
                ?n ua:Namespace ?ns .
            }
            """
        df = m.query(q, solution_mappings=True)
        df = df.mappings.with_columns(
            pl.col("ns").is_in(type_namespaces).alias("type_ns")
        )
    elif instance_namespaces is not None:
        q = """
            PREFIX ua:<http://opcfoundation.org/UA/>
            SELECT DISTINCT ?ns WHERE {
                ?n ua:Namespace ?ns .
            }
            """
        df = m.query(q, solution_mappings=True)
        df = df.mappings.with_columns(
            pl.col("ns").is_in(instance_namespaces).not_().alias("type_ns")
        )
    else:
        raise RuntimeError("Should never happen")

    ns = Variable("ns")
    type_ns = Variable("type_ns")
    uamapper = Prefix("https://github.com/DataTreehouse/uamapper#")
    template = Template(
        uamapper.suf("IsTypeNamespaceTemplate"),
        [
            Parameter(ns, rdf_type=RDFType.IRI),
            Parameter(type_ns, rdf_type=RDFType.Literal(xsd.boolean)),
        ],
        [Triple(ns, uamapper.suf("typeNamespace"), type_ns)],
    )
    m.map(template, df)
    return m
