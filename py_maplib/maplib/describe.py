"""Model.describe: a summary of what is in each graph of a model.

A prototype in Python on top of Model.analyze and SPARQL, to settle the shape of the summary
before the counts move into the engine.

The tables of a ModelDescription hold everything that was computed. Their size is bounded by the
schema (graphs, predicates and classes), never by the number of triples. The text and dict forms
are bounded further: see ModelDescription.to_text.
"""
from typing import Dict, List, Optional, Sequence, Union

import polars as pl

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
XSD = "http://www.w3.org/2001/XMLSchema#"
RDF_TYPE = RDF + "type"
_STRING_TYPES = (XSD + "string", RDF + "langString")

#: Made from the statistics the model keeps, and one count per class.
DEFAULT_SECTIONS = ("predicates", "classes", "namespaces")
#: Each of these reads the triples of the described graphs.
EXTRA_SECTIONS = (
    "subjects",
    "class_predicates",
    "links",
    "literals",
    "examples",
    "dangling",
    "overlap",
    "vocabulary",
    "iri_namespaces",
)
SECTIONS = DEFAULT_SECTIONS + EXTRA_SECTIONS

_DECLARING = {
    OWL + "Class": "class",
    RDFS + "Class": "class",
    RDF + "Property": "predicate",
    OWL + "ObjectProperty": "predicate",
    OWL + "DatatypeProperty": "predicate",
    OWL + "AnnotationProperty": "predicate",
    OWL + "FunctionalProperty": "predicate",
}

_S, _U = pl.String, pl.UInt64
_SCHEMAS = {
    "classes": {"graph": _S, "class": _S, "instances": _U},
    "class_predicates": {
        "graph": _S, "class": _S, "predicate": _S, "instances": _U, "subjects": _U,
        "coverage": pl.Float64, "triples": _U, "min_per_subject": _U, "max_per_subject": _U,
    },
    "links": {"graph": _S, "subject_class": _S, "predicate": _S, "object_class": _S, "triples": _U},
    "literals": {
        "graph": _S, "predicate": _S, "datatype": _S, "min": _S, "max": _S,
        "min_length": _U, "max_length": _U,
    },
    "languages": {"graph": _S, "predicate": _S, "language": _S, "triples": _U},
    "dangling": {"graph": _S, "predicate": _S, "objects": _U, "triples": _U, "in_other_graphs": _U},
    "overlap": {
        "graph_a": _S, "graph_b": _S, "shared_predicates": _U, "shared_classes": _U,
        "shared_subjects": _U, "a_refers_to_b": _U, "b_refers_to_a": _U,
    },
    "namespaces": {"graph": _S, "namespace": _S, "prefix": _S, "role": _S, "terms": _U},
}


def namespace_of(iri: str) -> str:
    """The namespace of an IRI: up to and including the last '#', else the last '/', else the
    last ':'."""
    for sep in "#/:":
        i = iri.rfind(sep)
        if i >= 0:
            return iri[: i + 1]
    return iri


def _cap(n: Optional[int]) -> int:
    return n if n is not None else 1 << 40


def _graph_name(g: Optional[str]) -> str:
    return "default graph" if g is None else f"<{g}>"


def _more(shown: int, total: int, what: str) -> List[str]:
    return [f"    ... {total - shown} more {what}"] if total > shown else []


class DescriptionDiff:
    """What changed between two descriptions, from ModelDescription.diff: the predicates and classes
    whose number of triples or instances differ, with before, after and change."""

    def __init__(self, predicates: pl.DataFrame, classes: pl.DataFrame):
        self.predicates = predicates
        self.classes = classes

    def __repr__(self) -> str:
        out = []
        for name, df, key in (("predicates", self.predicates, "predicate"), ("classes", self.classes, "class")):
            out.append(f"{name}: {df.height} changed")
            for r in df.head(30).iter_rows(named=True):
                out.append(
                    f"  {_graph_name(r['graph'])} <{r[key]}>: {r['before']:,} -> {r['after']:,} ({r['change']:+,})"
                )
            if df.height > 30:
                out.append(f"  ... {df.height - 30} more")
        return "\n".join(out)


class ModelDescription:
    """The result of Model.describe. Printing it gives a text summary of bounded length (see
    to_text); the tables are polars DataFrames with everything that was computed. The table of a
    section that was not asked for is empty.

    sections: the sections that were computed.
    graphs: one row per graph: graph, triples, predicates, classes, namespaces, and with the
        subjects section subjects, untyped_subjects and blank_subjects.
    predicates: one row per graph and predicate: graph, predicate, triples, distinct_subjects,
        distinct_objects, exact (whether the distinct counts are exact), subject_kinds (IRI, Blank),
        object_types (IRI, Blank or the datatype IRI), object_type_triples, max_triples_per_subject,
        max_triples_per_object, functional (at most one object per subject) and inverse_functional
        (at most one subject per object), both None when the statistics cannot tell,
        common_objects and common_object_counts (the most common objects, as N-Triples terms),
        example (examples section), declared and label (vocabulary section).
    classes: graph, class, instances, and with the vocabulary section declared and label.
    namespaces: graph, namespace, prefix, role (predicate, class, and with the iri_namespaces
        section subject and object) and terms (the distinct IRIs of that role in the namespace).
    class_predicates: for each class, the predicates its instances have: graph, class, predicate,
        instances, subjects (instances with the predicate), coverage (subjects / instances),
        triples, min_per_subject and max_per_subject (among the instances that have it).
    links: which classes are linked: graph, subject_class, predicate, object_class, triples.
    literals: graph, predicate, datatype, min and max (as strings, not for strings), min_length
        and max_length (strings only).
    languages: graph, predicate, language, triples.
    dangling: IRI objects that are not a subject in their graph: graph, predicate, objects,
        triples, in_other_graphs (how many of the objects are a subject in another graph).
    overlap: for pairs of the largest graphs: graph_a, graph_b, shared_predicates, shared_classes,
        shared_subjects, a_refers_to_b (IRI objects of a that are subjects in b), b_refers_to_a.
    """

    def __init__(self, tables: Dict[str, pl.DataFrame], sections: Sequence[str], prefixes: Dict[str, str]):
        self.sections = list(sections)
        self.graphs = tables["graphs"]
        self.predicates = tables["predicates"]
        self.classes = tables["classes"]
        self.namespaces = tables["namespaces"]
        self.class_predicates = tables["class_predicates"]
        self.links = tables["links"]
        self.literals = tables["literals"]
        self.languages = tables["languages"]
        self.dangling = tables["dangling"]
        self.overlap = tables["overlap"]
        self._prefixes = prefixes
        self._by_namespace = sorted(((ns, p) for p, ns in prefixes.items()), key=lambda x: -len(x[0]))

    _TABLES = (
        "graphs", "predicates", "classes", "namespaces", "class_predicates", "links", "literals",
        "languages", "dangling", "overlap",
    )

    def _short(self, iri: Optional[str]) -> str:
        if iri is None:
            return ""
        if iri in ("IRI", "Blank"):
            return iri
        for ns, p in self._by_namespace:
            if iri.startswith(ns):
                return f"{p}:{iri[len(ns):]}"
        return f"<{iri}>"

    def _expand(self, name: str) -> str:
        """An IRI from an IRI, <IRI> or prefix:local with a well-known prefix."""
        if name.startswith("<") and name.endswith(">"):
            return name[1:-1]
        prefix, sep, local = name.partition(":")
        if sep and prefix in self._prefixes and not local.startswith("//"):
            return self._prefixes[prefix] + local
        return name

    def _of(self, df: pl.DataFrame, graph: Optional[str], col: str = "graph") -> pl.DataFrame:
        return df.filter(pl.col(col).eq_missing(graph))

    # Graphs with the same predicates are described once.
    def _signatures(self) -> Dict[Optional[str], frozenset]:
        return {
            g: frozenset(p)
            for g, p in self.predicates.group_by("graph", maintain_order=True)
            .agg("predicate")
            .iter_rows()
        }

    def to_dict(self, max_rows: Optional[int] = None, max_graphs: Optional[int] = None) -> dict:
        """The summary as plain dicts and lists, e.g. for JSON or as context for a language model.

        :param max_rows: Keep at most this many rows per table and graph (the largest first).
        :param max_graphs: Keep only the this many largest graphs.
        """
        keep = self.graphs["graph"].to_list()
        if max_graphs is not None:
            keep = keep[:max_graphs]
        keep_s = pl.Series(keep, dtype=pl.String).implode()
        out = {}
        for name in self._TABLES:
            df = getattr(self, name)
            for col in ("graph", "graph_a", "graph_b"):
                if col in df.columns:
                    df = df.filter(pl.col(col).is_in(keep_s, nulls_equal=True))
            if max_rows is not None and "graph" in df.columns and name != "graphs":
                df = df.filter(pl.int_range(pl.len()).over("graph") < max_rows)
            out[name] = df.to_dicts()
        out["graphs_total"] = self.graphs.height
        return out

    def diff(self, other: "ModelDescription") -> DescriptionDiff:
        """What changed from other (before) to this description (after), e.g. around a mapping or
        an inference run: the predicates and classes whose triples or instances differ."""

        def changed(before: pl.DataFrame, after: pl.DataFrame, key: str, n: str) -> pl.DataFrame:
            b = before.select("graph", key, pl.col(n).cast(pl.Int64).alias("before"))
            a = after.select("graph", key, pl.col(n).cast(pl.Int64).alias("after"))
            return (
                b.join(a, on=["graph", key], how="full", coalesce=True, nulls_equal=True)
                .with_columns(pl.col("before").fill_null(0), pl.col("after").fill_null(0))
                .with_columns((pl.col("after") - pl.col("before")).alias("change"))
                .filter(pl.col("change") != 0)
                .sort(pl.col("change").abs(), descending=True, maintain_order=True)
            )

        return DescriptionDiff(
            changed(other.predicates, self.predicates, "predicate", "triples"),
            changed(other.classes, self.classes, "class", "instances"),
        )

    def __repr__(self) -> str:
        return self.to_text(max_chars=20_000)

    def to_text(
            self,
            graphs: Optional[List[Optional[str]]] = None,
            sections: Optional[List[str]] = None,
            max_graphs: Optional[int] = 5,
            max_rows: Optional[int] = 15,
            max_chars: Optional[int] = None,
    ) -> str:
        """The text summary. Its length does not grow with the model: an overview line per graph
        (the largest max_rows of them), then the details of the largest max_graphs graphs with at
        most max_rows rows per section, the largest first. Graphs with the same predicates as a
        graph already detailed are only named. What is left out is counted in "... n more" lines.
        Printing the description is to_text(max_chars=20_000).

        To see more, raise the limits (None for no limit), ask for one graph or section, read
        the tables, or use show_class and show_predicate.

        :param graphs: Detail these graphs (None in the list is the default graph) instead of the
            largest.
        :param sections: Only these sections of the details.
        :param max_graphs: The number of graphs to detail.
        :param max_rows: The number of rows per section, and of graphs in the overview.
        :param max_chars: Halve max_rows, then max_graphs, until the text is at most this long.
        """
        text = self._text(graphs, sections, max_graphs, max_rows)
        if max_chars is None:
            return text
        rows = max_rows if max_rows is not None else 1 << 20
        n_graphs = max_graphs if max_graphs is not None else self.graphs.height
        while len(text) > max_chars and (rows > 3 or n_graphs > 1):
            if rows > 3:
                rows = max(3, rows // 2)
            else:
                n_graphs = max(1, n_graphs // 2)
            text = self._text(graphs, sections, n_graphs, rows)
        if len(text) > max_chars:
            text = text[:max_chars] + "\n... cut at max_chars"
        return text

    def _text(self, graphs, sections, max_graphs, max_rows) -> str:
        show = set(sections) if sections is not None else set(self.sections)
        out = []
        all_graphs = self.graphs["graph"].to_list()
        total = int(self.graphs["triples"].sum() or 0)
        out.append(f"{len(all_graphs)} graphs, {total:,} triples")
        for g in self.graphs.head(_cap(max_rows)).iter_rows(named=True):
            line = (
                f"  {_graph_name(g['graph'])}: {g['triples']:,} triples, {g['predicates']} predicates, "
                f"{g['classes']} classes"
            )
            if g["subjects"] is not None:
                line += (
                    f", {g['subjects']:,} subjects ({g['untyped_subjects']:,} untyped, "
                    f"{g['blank_subjects']:,} blank)"
                )
            out.append(line)
        if max_rows is not None and len(all_graphs) > max_rows:
            rest = int(self.graphs["triples"][max_rows:].sum())
            out.append(f"  ... {len(all_graphs) - max_rows} more graphs with {rest:,} triples")
        out.append("")

        signatures = self._signatures()
        detailed: Dict[frozenset, Optional[str]] = {}
        same: Dict[Optional[str], List[Optional[str]]] = {}
        if graphs is not None:
            chosen = [g for g in graphs if g in signatures]
        else:
            chosen = []
            for g in all_graphs:
                first = detailed.get(signatures[g])
                if first is not None or (signatures[g] in detailed):
                    same.setdefault(first, []).append(g)
                elif max_graphs is None or len(chosen) < max_graphs:
                    detailed[signatures[g]] = g
                    chosen.append(g)
        for g in chosen:
            out.extend(self._graph_text(g, show, max_rows))
            twins = same.get(g, [])
            if twins:
                names = ", ".join(_graph_name(t) for t in twins[:5])
                more = f" and {len(twins) - 5} more" if len(twins) > 5 else ""
                out.append(f"  same predicates in {len(twins)} more graphs: {names}{more}")
            out.append("")
        if graphs is None:
            skipped = len(all_graphs) - len(chosen) - sum(len(v) for v in same.values())
            if skipped > 0:
                out.append(f"... {skipped} more graphs not detailed (to_text(graphs=[...]) or max_graphs)")
                out.append("")

        if "overlap" in show and self.overlap.height:
            out.append("overlap between graphs:")
            ov = self.overlap.filter(
                (pl.col("shared_subjects") > 0) | (pl.col("a_refers_to_b") > 0) | (pl.col("b_refers_to_a") > 0)
            )
            for r in ov.head(_cap(max_rows)).iter_rows(named=True):
                out.append(
                    f"  {_graph_name(r['graph_a'])} / {_graph_name(r['graph_b'])}: "
                    f"{r['shared_subjects']:,} shared subjects, {r['a_refers_to_b']:,} references a to b, "
                    f"{r['b_refers_to_a']:,} b to a, {r['shared_predicates']} shared predicates, "
                    f"{r['shared_classes']} shared classes"
                )
            if not ov.height:
                out.append(f"  none among the {self.overlap.height} pairs compared")
            elif max_rows is not None:
                out.extend(_more(max_rows, ov.height, "pairs"))
            out.append("")
        return "\n".join(out)

    def _labelled(self, iri: str, label: Optional[str]) -> str:
        return self._short(iri) + (f' "{label}"' if label else "")

    def _predicate_line(self, r: dict) -> str:
        approx = "" if r["exact"] else "~"
        many = len(r["object_types"]) > 1
        types = ", ".join(
            self._short(t) + (f" ({n:,})" if many else "")
            for t, n in zip(r["object_types"], r["object_type_triples"])
        )
        notes = []
        if r["functional"]:
            notes.append("functional")
        if r["inverse_functional"]:
            notes.append("key")
        if r.get("declared") is False:
            notes.append("undeclared")
        if r.get("example") is not None:
            notes.append(f"e.g. {r['example'][:40]}")
        return (
            f"    {self._labelled(r['predicate'], r.get('label')):<36} {r['triples']:>12,} "
            f"{approx + format(r['distinct_subjects'], ','):>12} "
            f"{approx + format(r['distinct_objects'], ','):>12}  {types}"
            + (f"  [{'; '.join(notes)}]" if notes else "")
        )

    def _graph_text(self, g: Optional[str], show: set, max_rows: Optional[int]) -> List[str]:
        out = [f"{_graph_name(g)}"]
        cap = max_rows if max_rows is not None else 1 << 40

        ns = self._of(self.namespaces, g)
        if "namespaces" in show and ns.height:
            roles = ns.group_by("namespace", "prefix", maintain_order=True).agg(
                pl.format("{} {}", "terms", "role").alias("r")
            )
            out.append(f"  namespaces ({roles.height}):")
            for r in roles.head(_cap(max_rows)).iter_rows(named=True):
                prefix = f"{r['prefix']}:" if r["prefix"] is not None else ""
                out.append(f"    {prefix:<10} {r['namespace']}  ({', '.join(r['r'])})")
            out.extend(_more(cap, roles.height, "namespaces"))

        cl = self._of(self.classes, g)
        if "classes" in show and cl.height:
            out.append(f"  classes ({cl.height}):")
            for r in cl.head(_cap(max_rows)).iter_rows(named=True):
                flag = "  [undeclared]" if r.get("declared") is False else ""
                out.append(f"    {self._labelled(r['class'], r.get('label')):<40} {r['instances']:>12,}{flag}")
            if cl.height > cap:
                rest = int(cl["instances"][cap:].sum())
                out.append(f"    ... {cl.height - cap} more classes with {rest:,} instances")

        pr = self._of(self.predicates, g)
        if "predicates" in show and pr.height:
            out.append(f"  predicates ({pr.height}):")
            out.append(f"    {'':<36} {'triples':>12} {'subjects':>12} {'objects':>12}  object types")
            for r in pr.head(_cap(max_rows)).iter_rows(named=True):
                out.append(self._predicate_line(r))
            if pr.height > cap:
                rest = int(pr["triples"][cap:].sum())
                share = 100 * rest / max(int(pr["triples"].sum()), 1)
                out.append(f"    ... {pr.height - cap} more predicates with {rest:,} triples ({share:.1f}%)")

        lit = self._of(self.literals, g)
        if "literals" in show and lit.height:
            out.append(f"  literals ({lit.height}):")
            langs = {
                p: ", ".join(f"{l} ({n:,})" for l, n in zip(ls, ns_))
                for p, ls, ns_ in self._of(self.languages, g)
                .group_by("predicate", maintain_order=True)
                .agg("language", "triples")
                .iter_rows()
            }
            for r in lit.head(_cap(max_rows)).iter_rows(named=True):
                if r["min_length"] is not None:
                    rng = f"length {r['min_length']}..{r['max_length']}"
                    if r["datatype"] == RDF + "langString" and r["predicate"] in langs:
                        rng += f", languages {langs[r['predicate']]}"
                else:
                    rng = f"{r['min']} .. {r['max']}"
                out.append(f"    {self._short(r['predicate']):<36} {self._short(r['datatype']):<18} {rng}")
            out.extend(_more(cap, lit.height, "literal ranges"))

        cp = self._of(self.class_predicates, g)
        if "class_predicates" in show and cp.height:
            n_classes = cp["class"].n_unique()
            class_cap = max(3, cap // 3)
            out.append(f"  class profiles ({n_classes}; coverage of instances, objects per instance):")
            for c in cp["class"].unique(maintain_order=True).head(class_cap).to_list():
                out.extend("  " + line for line in self._class_lines(g, c, max_rows))
            out.extend(_more(class_cap, n_classes, "classes (show_class)"))

        li = self._of(self.links, g)
        if "links" in show and li.height:
            out.append(f"  links between classes ({li.height}):")
            for r in li.head(_cap(max_rows)).iter_rows(named=True):
                out.append(
                    f"    {self._short(r['subject_class'])} -[{self._short(r['predicate'])}]-> "
                    f"{self._short(r['object_class'])}  {r['triples']:,}"
                )
            out.extend(_more(cap, li.height, "links"))

        da = self._of(self.dangling, g)
        if "dangling" in show and da.height:
            out.append(f"  IRI objects that are not subjects in this graph ({da.height} predicates):")
            for r in da.head(_cap(max_rows)).iter_rows(named=True):
                out.append(
                    f"    {self._short(r['predicate']):<36} {r['objects']:>12,} objects, "
                    f"{r['in_other_graphs']:,} of them subjects in other graphs"
                )
            out.extend(_more(cap, da.height, "predicates"))
        return out

    def _class_lines(self, g: Optional[str], cls: str, max_rows: Optional[int]) -> List[str]:
        cp = self._of(self.class_predicates, g).filter(pl.col("class") == cls)
        if not cp.height:
            return []
        out = [f"  {self._short(cls)} ({cp['instances'][0]:,} instances)"]
        for r in cp.head(_cap(max_rows)).iter_rows(named=True):
            low = r["min_per_subject"] if r["subjects"] == r["instances"] else 0
            out.append(
                f"    {self._short(r['predicate']):<36} {100 * r['coverage']:>5.1f}%  "
                f"[{low}..{r['max_per_subject']}]"
            )
        if max_rows is not None:
            out.extend("  " + line.strip() for line in _more(max_rows, cp.height, "predicates"))
        return out

    def show_class(self, cls: str, graph: Union[str, None, List[Optional[str]]] = "*") -> str:
        """The whole profile of one class: its instances, its predicates with coverage and
        cardinality, and its links to and from other classes. Needs the class_predicates section
        for the predicates and the links section for the links.

        :param cls: The class as an IRI or prefix:local with a well-known prefix.
        :param graph: A graph (None for the default graph); every graph with the class by default.
        """
        iri = self._expand(cls)
        graphs = self.classes.filter(pl.col("class") == iri)["graph"].to_list()
        if graph != "*":
            graphs = [g for g in graphs if g in (graph if isinstance(graph, list) else [graph])]
        out = []
        for g in graphs:
            n = self._of(self.classes, g).filter(pl.col("class") == iri)["instances"][0]
            out.append(f"{self._short(iri)} in {_graph_name(g)}: {n:,} instances")
            out.extend(self._class_lines(g, iri, None)[1:])
            li = self._of(self.links, g)
            for r in li.filter(pl.col("subject_class") == iri).iter_rows(named=True):
                out.append(f"  -[{self._short(r['predicate'])}]-> {self._short(r['object_class'])}  {r['triples']:,}")
            for r in li.filter(pl.col("object_class") == iri).iter_rows(named=True):
                out.append(f"  <-[{self._short(r['predicate'])}]- {self._short(r['subject_class'])}  {r['triples']:,}")
        return "\n".join(out) if out else f"no instances of {self._short(iri)}"

    def show_predicate(self, predicate: str) -> str:
        """Everything about one predicate in every graph that has it: counts, object types, the
        most common objects, literal ranges and languages, and the classes that use it.

        :param predicate: The predicate as an IRI or prefix:local with a well-known prefix.
        """
        iri = self._expand(predicate)
        out = []
        for r in self.predicates.filter(pl.col("predicate") == iri).iter_rows(named=True):
            g = r["graph"]
            out.append(f"{self._short(iri)} in {_graph_name(g)}")
            out.append(self._predicate_line(r))
            out.append(
                f"    at most {r['max_triples_per_subject']:,} per subject, "
                f"{r['max_triples_per_object']:,} per object"
            )
            for o, n in zip(r["common_objects"], r["common_object_counts"]):
                out.append(f"    common object {o}  {n:,}")
            for l in self._of(self.literals, g).filter(pl.col("predicate") == iri).iter_rows(named=True):
                rng = (
                    f"length {l['min_length']}..{l['max_length']}"
                    if l["min_length"] is not None
                    else f"{l['min']} .. {l['max']}"
                )
                out.append(f"    {self._short(l['datatype'])}: {rng}")
            for l in self._of(self.languages, g).filter(pl.col("predicate") == iri).iter_rows(named=True):
                out.append(f"    language {l['language']}: {l['triples']:,}")
            for c in self._of(self.class_predicates, g).filter(pl.col("predicate") == iri).iter_rows(named=True):
                out.append(
                    f"    on {self._short(c['class'])}: {100 * c['coverage']:.1f}% of {c['instances']:,} instances"
                )
        return "\n".join(out) if out else f"no triples with {self._short(iri)}"


def _iri_cols(df: pl.DataFrame, *cols: str) -> pl.DataFrame:
    return df.with_columns(
        [pl.col(c).cast(pl.String).str.strip_prefix("<").str.strip_suffix(">") for c in cols]
    )


def _frame(name: str, frames: List[pl.DataFrame]) -> pl.DataFrame:
    schema = _SCHEMAS[name]
    frames = [f.select([pl.col(c).cast(t) for c, t in schema.items()]) for f in frames if f.height]
    return pl.concat(frames) if frames else pl.DataFrame(schema=schema)


def _with_graph(df: pl.DataFrame, g: Optional[str]) -> pl.DataFrame:
    return df.with_columns(pl.lit(g, dtype=pl.String).alias("graph"))


def describe(
        m,
        graphs: Optional[List[Optional[str]]] = None,
        sections: Union[None, str, Sequence[str]] = None,
        exact: bool = False,
        max_overlap_graphs: int = 6,
) -> ModelDescription:
    """See Model.describe."""
    from . import DEFAULT_GRAPH, well_known_prefixes

    if sections is None:
        want = set(DEFAULT_SECTIONS)
    elif sections == "all":
        want = set(SECTIONS)
    else:
        unknown = set(sections) - set(SECTIONS)
        if unknown:
            raise ValueError(f"Unknown sections {sorted(unknown)}, the sections are {list(SECTIONS)}")
        want = set(DEFAULT_SECTIONS) | set(sections)

    all_tables = m.analyze().with_columns(
        pl.when(pl.col("graph") == "default graph")
        .then(None)
        .otherwise(pl.col("graph").str.strip_prefix("<").str.strip_suffix(">"))
        .alias("graph"),
        pl.col("predicate").str.strip_prefix("<").str.strip_suffix(">"),
        pl.col("object_type").str.strip_prefix("<").str.strip_suffix(">"),
    ).filter(pl.col("triples") > 0)
    tables = all_tables
    if graphs is not None:
        wanted = pl.Series(graphs, dtype=pl.String)
        tables = tables.filter(pl.col("graph").is_in(wanted.implode(), nulls_equal=True))

    # Subjects of different kinds, and objects of different types, are different terms, so their
    # distinct counts add up. A subject may have objects of several types, though, and an object
    # may have subjects of both kinds, so a predicate stored as several tables gets an upper bound.
    per_object_type = (
        tables.group_by("graph", "predicate", "object_type", maintain_order=True)
        .agg(pl.col("triples").sum())
        .sort("triples", descending=True, maintain_order=True)
    )
    common = (
        tables.select("graph", "predicate", "most_common_objects", "most_common_object_counts")
        .explode("most_common_objects", "most_common_object_counts")
        .drop_nulls("most_common_objects")
        .sort("most_common_object_counts", descending=True, maintain_order=True)
        .group_by("graph", "predicate", maintain_order=True)
        .agg(
            pl.col("most_common_objects").head(5).alias("common_objects"),
            pl.col("most_common_object_counts").head(5).alias("common_object_counts"),
        )
    )
    single = (pl.len() == 1) & pl.col("exact").all()
    predicates = (
        tables.group_by("graph", "predicate", maintain_order=True)
        .agg(
            pl.col("triples").sum(),
            pl.col("distinct_subjects").sum(),
            pl.col("distinct_objects").sum(),
            single.alias("exact"),
            pl.col("subject_type").unique().sort().alias("subject_kinds"),
            pl.col("max_triples_per_subject").max(),
            pl.col("max_triples_per_object").max(),
            # More than one somewhere is certain; at most one only when the predicate is one table.
            pl.when(pl.col("max_triples_per_subject").max() > 1).then(False)
            .when(single).then(True).otherwise(None).alias("functional"),
            pl.when(pl.col("max_triples_per_object").max() > 1).then(False)
            .when(single).then(True).otherwise(None).alias("inverse_functional"),
        )
        .join(
            per_object_type.group_by("graph", "predicate", maintain_order=True).agg(
                pl.col("object_type").alias("object_types"),
                pl.col("triples").alias("object_type_triples"),
            ),
            on=["graph", "predicate"],
            nulls_equal=True,
        )
        .join(common, on=["graph", "predicate"], how="left", nulls_equal=True)
        .with_columns(
            pl.col("common_objects").fill_null([]),
            pl.col("common_object_counts").fill_null([]),
        )
        .select(
            "graph", "predicate", "triples", "distinct_subjects", "distinct_objects", "exact",
            "subject_kinds", "object_types", "object_type_triples",
            "max_triples_per_subject", "max_triples_per_object", "functional", "inverse_functional",
            "common_objects", "common_object_counts",
        )
    )
    graph_sizes = (
        predicates.group_by("graph")
        .agg(pl.col("triples").sum().alias("_g"))
        .sort("_g", "graph", descending=[True, False])
    )
    graph_list = graph_sizes["graph"].to_list()
    predicates = (
        predicates.join(graph_sizes, on="graph", nulls_equal=True)
        .sort(["_g", "graph", "triples", "predicate"], descending=[True, False, True, False])
        .drop("_g")
    )

    def q(query: str, g: Optional[str], *iri_cols: str) -> pl.DataFrame:
        return _with_graph(_iri_cols(m.query(query, graph=g), *iri_cols), g)

    def by_graph(query: str, *iri_cols: str) -> List[pl.DataFrame]:
        return [q(query, g, *iri_cols) for g in graph_list]

    T = f"<{RDF_TYPE}>"

    if exact:
        # One query per graph that has a predicate whose counts are not exact yet.
        inexact = set(predicates.filter(~pl.col("exact"))["graph"].to_list())
        fixed = [
            q(
                "SELECT ?predicate (COUNT(DISTINCT ?s) AS ?s_n) (COUNT(DISTINCT ?o) AS ?o_n) "
                "WHERE { ?s ?predicate ?o } GROUP BY ?predicate",
                g,
                "predicate",
            )
            for g in graph_list
            if g in inexact
        ]
        if fixed:
            fixed_df = pl.concat(fixed).select(
                "graph", "predicate", pl.col("s_n").cast(_U), pl.col("o_n").cast(_U)
            )
            predicates = (
                predicates.join(fixed_df, on=["graph", "predicate"], how="left", nulls_equal=True)
                .with_columns(
                    pl.coalesce("s_n", "distinct_subjects").alias("distinct_subjects"),
                    pl.coalesce("o_n", "distinct_objects").alias("distinct_objects"),
                    pl.lit(True).alias("exact"),
                )
                .drop("s_n", "o_n")
            )

    classes = _frame(
        "classes",
        by_graph(
            f"SELECT ?class (COUNT(?s) AS ?instances) WHERE {{ ?s {T} ?class }} "
            f"GROUP BY ?class ORDER BY DESC(?instances) ?class",
            "class",
        ),
    )

    subject_counts = {}
    if "subjects" in want:
        for g in graph_list:
            a = m.query(
                "SELECT (COUNT(DISTINCT ?s) AS ?n) (COUNT(DISTINCT ?b) AS ?blank) WHERE { ?s ?p ?o "
                "BIND(IF(isBlank(?s), ?s, ?unbound) AS ?b) }",
                graph=g,
            )
            u = m.query(
                f"SELECT (COUNT(DISTINCT ?s) AS ?n) WHERE {{ ?s ?p ?o FILTER NOT EXISTS {{ ?s {T} ?c }} }}",
                graph=g,
            )
            subject_counts[g] = (a["n"][0], u["n"][0], a["blank"][0])

    class_predicates = _frame("class_predicates", [])
    if "class_predicates" in want:
        cp = _frame_any(
            by_graph(
                f"SELECT ?class ?predicate (COUNT(?s) AS ?subjects) (SUM(?n) AS ?triples) "
                f"(MIN(?n) AS ?min_per_subject) (MAX(?n) AS ?max_per_subject) WHERE {{ "
                f"SELECT ?class ?predicate ?s (COUNT(?o) AS ?n) WHERE {{ ?s {T} ?class . ?s ?predicate ?o }} "
                f"GROUP BY ?class ?predicate ?s }} GROUP BY ?class ?predicate",
                "class",
                "predicate",
            )
        )
        if cp is not None:
            cp = (
                cp.filter(pl.col("predicate") != RDF_TYPE)
                .join(classes, on=["graph", "class"], nulls_equal=True)
                .with_columns((pl.col("subjects") / pl.col("instances")).alias("coverage"))
                .with_columns(pl.col("graph").replace_strict(
                    {g: i for i, g in enumerate(graph_list)}, return_dtype=pl.Int64, default=None
                ).fill_null(graph_list.index(None) if None in graph_list else 0).alias("_g"))
                .sort(["_g", "instances", "class", "subjects", "predicate"],
                      descending=[False, True, False, True, False])
            )
            class_predicates = _frame("class_predicates", [cp])

    links = _frame(
        "links",
        by_graph(
            f"SELECT ?subject_class ?predicate ?object_class (COUNT(?s) AS ?triples) WHERE {{ "
            f"?s ?predicate ?o . ?s {T} ?subject_class . ?o {T} ?object_class "
            f"FILTER(?predicate != {T}) }} "
            f"GROUP BY ?subject_class ?predicate ?object_class ORDER BY DESC(?triples)",
            "subject_class", "predicate", "object_class",
        ) if "links" in want else [],
    )

    literals, languages = _frame("literals", []), _frame("languages", [])
    if "literals" in want:
        strings = ", ".join(f"<{t}>" for t in _STRING_TYPES)
        frames = []
        for g in graph_list:
            ranges = q(
                "SELECT ?predicate ?datatype (STR(MIN(?o)) AS ?min) (STR(MAX(?o)) AS ?max) WHERE { "
                "?s ?predicate ?o FILTER(isLiteral(?o)) BIND(DATATYPE(?o) AS ?datatype) "
                f"FILTER(?datatype NOT IN ({strings})) }} GROUP BY ?predicate ?datatype",
                g, "predicate", "datatype",
            ).with_columns(
                pl.lit(None, dtype=_U).alias("min_length"), pl.lit(None, dtype=_U).alias("max_length")
            )
            lengths = q(
                "SELECT ?predicate ?datatype (MIN(STRLEN(STR(?o))) AS ?min_length) "
                "(MAX(STRLEN(STR(?o))) AS ?max_length) WHERE { ?s ?predicate ?o FILTER(isLiteral(?o)) "
                f"BIND(DATATYPE(?o) AS ?datatype) FILTER(?datatype IN ({strings})) }} "
                "GROUP BY ?predicate ?datatype",
                g, "predicate", "datatype",
            ).with_columns(pl.lit(None, dtype=_S).alias("min"), pl.lit(None, dtype=_S).alias("max"))
            frames += [ranges, lengths]
        literals = _frame("literals", frames).sort("predicate", "datatype", maintain_order=True)
        literals = literals.sort(
            pl.col("graph").map_elements(graph_list.index, return_dtype=pl.Int64, skip_nulls=False),
            maintain_order=True,
        ) if literals.height else literals
        languages = _frame(
            "languages",
            by_graph(
                "SELECT ?predicate ?language (COUNT(?s) AS ?triples) WHERE { ?s ?predicate ?o "
                'FILTER(isLiteral(?o)) BIND(LANG(?o) AS ?language) FILTER(?language != "") } '
                "GROUP BY ?predicate ?language ORDER BY ?predicate DESC(?triples)",
                "predicate",
            ),
        )

    if "examples" in want:
        ex = _frame_any(
            by_graph(
                "SELECT ?predicate (STR(SAMPLE(?o)) AS ?example) WHERE { ?s ?predicate ?o } GROUP BY ?predicate",
                "predicate",
            )
        )
        if ex is not None:
            predicates = predicates.join(
                ex.select("graph", "predicate", pl.col("example").cast(_S)),
                on=["graph", "predicate"], how="left", nulls_equal=True,
            )
    if "example" not in predicates.columns:
        predicates = predicates.with_columns(pl.lit(None, dtype=_S).alias("example"))

    dangling = _frame("dangling", [])
    if "dangling" in want:
        frames = []
        for g in graph_list:
            here = q(
                f"SELECT ?predicate (COUNT(DISTINCT ?o) AS ?objects) (COUNT(?s) AS ?triples) WHERE {{ "
                f"?s ?predicate ?o FILTER(isIRI(?o) && ?predicate != {T}) "
                f"FILTER NOT EXISTS {{ ?o ?p2 ?o2 }} }} GROUP BY ?predicate ORDER BY DESC(?objects)",
                g, "predicate",
            )
            if not here.height:
                continue
            others = [h for h in set(all_tables["graph"].to_list()) if h != g]
            elsewhere = None
            if others:
                union = " UNION ".join(
                    f"{{ GRAPH <{DEFAULT_GRAPH if h is None else h}> {{ ?o ?p3 ?o3 }} }}"
                    for h in (others if len(others) <= 20 else [None] if None in others else [])
                )
                if len(others) > 20:
                    named = "{ GRAPH ?g3 { ?o ?p3 ?o3 } }"
                    union = f"{union} UNION {named}" if union else named
                elsewhere = q(
                    f"SELECT ?predicate (COUNT(DISTINCT ?o) AS ?in_other_graphs) WHERE {{ "
                    f"?s ?predicate ?o FILTER(isIRI(?o) && ?predicate != {T}) "
                    f"FILTER NOT EXISTS {{ ?o ?p2 ?o2 }} FILTER EXISTS {{ {union} }} }} GROUP BY ?predicate",
                    g, "predicate",
                ).select("predicate", pl.col("in_other_graphs").cast(_U))
            if elsewhere is not None and elsewhere.height:
                here = here.join(elsewhere, on="predicate", how="left").with_columns(
                    pl.col("in_other_graphs").fill_null(0)
                )
            else:
                here = here.with_columns(pl.lit(0, dtype=_U).alias("in_other_graphs"))
            frames.append(here)
        dangling = _frame("dangling", frames)

    overlap = _frame("overlap", [])
    if "overlap" in want and len(graph_list) > 1:
        top = graph_list[:max_overlap_graphs]
        pred_sets = {g: set(predicates.filter(pl.col("graph").eq_missing(g))["predicate"].to_list()) for g in top}
        class_sets = {g: set(classes.filter(pl.col("graph").eq_missing(g))["class"].to_list()) for g in top}

        def name(g):
            return f"<{DEFAULT_GRAPH if g is None else g}>"

        def count(a, b, var) -> int:
            return m.query(
                f"SELECT (COUNT(DISTINCT ?{var}) AS ?n) WHERE {{ ?s ?p ?o FILTER(isIRI(?{var})) "
                f"FILTER EXISTS {{ GRAPH {name(b)} {{ ?{var} ?p2 ?o2 }} }} }}",
                graph=a,
            )["n"][0]

        rows = []
        for i, a in enumerate(top):
            for b in top[i + 1:]:
                rows.append((
                    a, b, len(pred_sets[a] & pred_sets[b]), len(class_sets[a] & class_sets[b]),
                    count(a, b, "s"), count(a, b, "o"), count(b, a, "o"),
                ))
        overlap = pl.DataFrame(rows, schema=_SCHEMAS["overlap"], orient="row")

    if "vocabulary" in want:
        values = " ".join(f"<{k}>" for k in _DECLARING)
        found = [
            _iri_cols(
                m.query(
                    f"SELECT ?term ?kind (STR(?l) AS ?label) WHERE {{ VALUES ?kind {{ {values} }} "
                    f"?term {T} ?kind OPTIONAL {{ ?term <{RDFS}label> ?l }} }}",
                    graph=g,
                ),
                "term", "kind",
            )
            # Also graphs that are not described: the ontology is often in a graph of its own.
            for g in all_tables.filter(pl.col("predicate") == RDF_TYPE)["graph"].unique().to_list()
        ]
        found = [f.select(pl.col("term"), pl.col("kind"), pl.col("label").cast(_S)) for f in found if f.height]
        vocab = (
            pl.concat(found)
            if found
            else pl.DataFrame(schema={"term": _S, "kind": _S, "label": _S})
        ).with_columns(pl.col("kind").replace_strict(_DECLARING, return_dtype=_S))

        def declared(df: pl.DataFrame, col: str) -> pl.DataFrame:
            v = (
                vocab.filter(pl.col("kind") == col)
                .group_by("term")
                .agg(pl.col("label").drop_nulls().sort().first())
                .rename({"term": col})
                .with_columns(pl.lit(True).alias("declared"))
            )
            # Not declared is only said of terms outside rdf, rdfs and owl, and only when the
            # model declares some term at all.
            builtin = pl.any_horizontal([pl.col(col).str.starts_with(ns) for ns in (RDF, RDFS, OWL)])
            return df.join(v, on=col, how="left").with_columns(
                pl.when(pl.col("declared").is_not_null()).then(True)
                .when(builtin | (vocab.height == 0)).then(None)
                .otherwise(False).alias("declared")
            )

        predicates = declared(predicates, "predicate")
        classes = declared(classes, "class")

    def term_namespaces(df: pl.DataFrame, col: str, role: str) -> pl.DataFrame:
        return (
            df.select("graph", pl.col(col).map_elements(namespace_of, return_dtype=_S).alias("namespace"))
            .group_by("graph", "namespace", maintain_order=True)
            .agg(pl.len().cast(_U).alias("terms"))
            .with_columns(pl.lit(role).alias("role"))
        )

    ns_frames = [
        term_namespaces(predicates, "predicate", "predicate"),
        term_namespaces(classes, "class", "class"),
    ]
    if "iri_namespaces" in want:
        for role, var in (("subject", "s"), ("object", "o")):
            for g in graph_list:
                ns_frames.append(
                    q(
                        f"SELECT ?namespace (COUNT(DISTINCT ?{var}) AS ?terms) WHERE {{ ?s ?p ?o "
                        f'FILTER(isIRI(?{var})) BIND(REPLACE(STR(?{var}), "[^/#:]*$", "") AS ?namespace) }} '
                        f"GROUP BY ?namespace",
                        g,
                    ).with_columns(pl.lit(role).alias("role"))
                )
    prefixes = dict(well_known_prefixes())
    by_ns = {ns: p for p, ns in prefixes.items()}
    ns_frames = [
        f.with_columns(
            pl.col("namespace").cast(_S).map_elements(by_ns.get, return_dtype=_S).alias("prefix")
        )
        for f in ns_frames
    ]
    role_order = pl.col("role").replace_strict(
        {"predicate": 0, "class": 1, "subject": 2, "object": 3}, return_dtype=pl.Int8
    )
    namespaces = _frame("namespaces", ns_frames)
    namespaces = (
        namespaces.with_columns(pl.col("terms").sum().over("graph", "namespace").alias("_all"))
        .sort(["graph", "_all", "namespace", role_order], descending=[False, True, False, False])
        .select(list(_SCHEMAS["namespaces"]))
    )

    def per_graph(col: str, i: int) -> pl.Expr:
        return (
            pl.col("graph")
            .map_elements(lambda g: (subject_counts.get(g) or (None,) * 3)[i], return_dtype=_U, skip_nulls=False)
            .alias(col)
        )

    graphs_df = (
        predicates.group_by("graph", maintain_order=True)
        .agg(pl.col("triples").sum(), pl.len().cast(_U).alias("predicates"))
        .join(
            classes.group_by("graph").agg(pl.len().cast(_U).alias("classes")),
            on="graph", how="left", nulls_equal=True,
        )
        .join(
            namespaces.group_by("graph").agg(pl.col("namespace").n_unique().cast(_U).alias("namespaces")),
            on="graph", how="left", nulls_equal=True,
        )
        .with_columns(
            pl.col("classes").fill_null(0),
            per_graph("subjects", 0),
            per_graph("untyped_subjects", 1),
            per_graph("blank_subjects", 2),
        )
        .select(
            "graph", "triples", "predicates", "classes", "namespaces",
            "subjects", "untyped_subjects", "blank_subjects",
        )
    )
    return ModelDescription(
        {
            "graphs": graphs_df, "predicates": predicates, "classes": classes, "namespaces": namespaces,
            "class_predicates": class_predicates, "links": links, "literals": literals,
            "languages": languages, "dangling": dangling, "overlap": overlap,
        },
        [s for s in SECTIONS if s in want],
        prefixes,
    )


def _frame_any(frames: List[pl.DataFrame]) -> Optional[pl.DataFrame]:
    frames = [f for f in frames if f.height]
    return pl.concat(frames, how="vertical_relaxed") if frames else None
