import re
from typing import Dict, List, Optional, Set, Tuple

from .ordering import topological_sort

from maplib import Model, Variable, RDFType, Parameter, Triple, IRI, Template

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
OWL = "http://www.w3.org/2002/07/owl#"
XSD = "http://www.w3.org/2001/XMLSchema#"

RDF_TYPE = RDF + "type"
RDFS_LITERAL = RDFS + "Literal"

# Property types that imply that the object of the property is a resource (IRI or blank node).
OBJECT_PROPERTY_TYPES = {
    OWL + "ObjectProperty",
    OWL + "TransitiveProperty",
    OWL + "SymmetricProperty",
    OWL + "AsymmetricProperty",
    OWL + "ReflexiveProperty",
    OWL + "IrreflexiveProperty",
    OWL + "InverseFunctionalProperty",
}
DATATYPE_PROPERTY_TYPES = {OWL + "DatatypeProperty"}

# Datatypes that are not in the XSD namespace.
NON_XSD_DATATYPES = {
    RDFS_LITERAL,
    RDF + "langString",
    RDF + "dirLangString",
    RDF + "HTML",
    RDF + "XMLLiteral",
    RDF + "JSON",
    RDF + "PlainLiteral",
    OWL + "real",
    OWL + "rational",
}

SUBJECT_VARIABLE_NAME = "id"
NOTYPE_SUFFIX = "_notype"

PREFIXES = f"""
PREFIX rdf: <{RDF}>
PREFIX rdfs: <{RDFS}>
PREFIX owl: <{OWL}>
"""

# A slot is a property in a direction: "out" means ?id <property> ?value, "in" means ?value <property> ?id.
Slot = Tuple[str, str]
OUT = "out"
IN = "in"


def generate_templates(
    m: Model,
    graph: Optional[str] = None,
    prefixes: Optional[Dict[str, str]] = None,
) -> Dict[str, Template]:
    """Generate templates for instantiating the classes in an ontology

    :param m: The model where the ontology is stored. We rely on owl:Class/rdfs:Class declarations, rdfs:subClassOf, rdfs:domain, rdfs:range (also via owl:unionOf), owl:Restriction and the declared types of properties.
    :param graph: The named graph where the ontology is stored. Defaults to the default graph.
    :param prefixes: Prefixes (prefix name to namespace IRI) used to name the parameters, e.g. {"ex": "http://example.net/"}.
        Common prefixes such as rdf, rdfs, owl, xsd, skos and dcterms are known already.
        Namespaces without a prefix get the generated prefixes ns1, ns2, ...

    :return A dictionary of templates for instantiating the classes in the ontology, where the keys are the class IRIs.
        For each class there is a template with the class IRI, which also produces the rdf:type triple, and one with
        the IRI of the class suffixed by "_notype", which does not. The subject is the parameter "id",
        the other (optional) parameters are named by the prefixed IRIs of the properties, e.g. "ex:name",
        so the columns of the data frame you map should be named like that.
        Properties that have the class as their range are included in the reverse direction, with the suffix
        "_inverse" on the local name (e.g. "ex:hasPart_inverse"). Remaining name clashes get the suffixes "_2", "_3", ...
        Templates of subclasses instantiate the "_notype" templates of their superclasses,
        so all templates must be added to the Model you want to populate.

    Usage example - note that it is important to add the templates to the Model you want to populate.
    >>> from maplib import Model, generate_templates
    >>>
    >>> m_ont = Model()
    >>> m_ont.read("my_ontology.ttl")
    >>> templates = generate_templates(m_ont, prefixes={"ex": "https://example.net/"})
    >>> m = Model()
    >>> for t in templates.values():
    >>>     m.add_template(t)
    >>> m.map("https://example.net/MyClass", df)
    """
    if not isinstance(m, Model):
        raise TypeError(f"Expected a Model, got {type(m).__name__}")
    if graph is not None and not isinstance(graph, str):
        raise TypeError(f"Expected graph to be a str or None, got {type(graph).__name__}")
    known_prefixes = dict(WELL_KNOWN_PREFIXES)
    if prefixes is not None:
        if not isinstance(prefixes, dict):
            raise TypeError(f"Expected prefixes to be a dict or None, got {type(prefixes).__name__}")
        for name, ns in prefixes.items():
            if not isinstance(name, str) or not isinstance(ns, str):
                raise TypeError("Prefixes must map prefix names (str) to namespace IRIs (str)")
            if not _is_pn_prefix(name):
                raise ValueError(f"Invalid prefix name: {name!r}")
            # User prefixes take precedence for their namespace.
            for k in [k for k, v in known_prefixes.items() if v == ns or k == name]:
                del known_prefixes[k]
            known_prefixes[name] = ns

    declared_classes = _column(_query(m, _CLASSES_QUERY, graph), "c")
    datatypes = set(_column(_query(m, _DATATYPES_QUERY, graph), "d"))
    subclass_edges = {
        (r["child"], r["parent"])
        for r in _query(m, _SUBCLASS_QUERY, graph)
        if r.get("child") is not None and r.get("parent") is not None
    }

    def is_datatype(iri: str) -> bool:
        return iri.startswith(XSD) or iri in NON_XSD_DATATYPES or iri in datatypes

    property_types: Dict[str, Set[str]] = {}
    for r in _query(m, _PROPERTY_TYPES_QUERY, graph):
        property_types.setdefault(r["property"], set()).add(r["property_type"])
    property_domains: Dict[str, Set[str]] = {}
    for r in _query(m, _DOMAINS_QUERY, graph):
        property_domains.setdefault(r["property"], set()).add(r["domain"])
    property_ranges: Dict[str, Set[str]] = {}
    for r in _query(m, _RANGES_QUERY, graph):
        property_ranges.setdefault(r["property"], set()).add(r["range"])
    # (class, property) -> range hints from the restriction
    restrictions: Dict[Tuple[str, str], Set[str]] = {}
    for r in _query(m, _RESTRICTIONS_QUERY, graph):
        hints = restrictions.setdefault((r["domain"], r["property"]), set())
        if r.get("range") is not None:
            hints.add(r["range"])

    def kind(prop: str, ranges: Set[str]) -> Optional[str]:
        types = property_types.get(prop, set())
        is_object = bool(types & OBJECT_PROPERTY_TYPES)
        is_datatype_prop = bool(types & DATATYPE_PROPERTY_TYPES)
        if is_object and not is_datatype_prop:
            return "iri"
        if is_datatype_prop and not is_object:
            return "literal"
        if is_object and is_datatype_prop:
            return None
        if ranges:
            if all(is_datatype(r) for r in ranges):
                return "literal"
            if not any(is_datatype(r) for r in ranges):
                return "iri"
        return None

    def out_type(prop: str, ranges: Set[str]) -> Optional[RDFType]:
        k = kind(prop, ranges)
        if k == "iri":
            return RDFType.IRI
        if k == "literal":
            dts = {r for r in ranges if is_datatype(r) and r != RDFS_LITERAL}
            if len(dts) == 1 and not (ranges - dts - {RDFS_LITERAL}):
                return RDFType.Literal(dts.pop())
        return None

    own_slots: Dict[str, Dict[Slot, Optional[RDFType]]] = {}

    for prop, domains in property_domains.items():
        ranges = property_ranges.get(prop, set())
        for d in domains:
            own_slots.setdefault(d, {})[(prop, OUT)] = out_type(prop, ranges)
    for (d, prop), hints in restrictions.items():
        slots = own_slots.setdefault(d, {})
        if (prop, OUT) not in slots or slots[(prop, OUT)] is None:
            ranges = property_ranges.get(prop) or hints
            slots[(prop, OUT)] = out_type(prop, ranges)
    for prop, ranges in property_ranges.items():
        if kind(prop, ranges) != "iri":
            continue
        for r in ranges:
            if is_datatype(r):
                continue
            slots = own_slots.setdefault(r, {})
            # A property with the class as both domain and range is only included in the forward direction.
            if (prop, OUT) not in slots:
                slots[(prop, IN)] = RDFType.IRI

    classes = set(declared_classes) | set(own_slots)
    for c, p in subclass_edges:
        classes.add(c)
        classes.add(p)
    classes = {c for c in classes if not is_datatype(c)}
    edges = [(c, p) for c, p in subclass_edges if c in classes and p in classes]
    ordering, parents_of = topological_sort(classes, edges)

    all_properties = {prop for slots in own_slots.values() for prop, _ in slots}
    property_names = _PrefixedNamer(known_prefixes, all_properties)

    templates_without_typing: Dict[str, Template] = {}
    slot_layouts: Dict[str, List[Tuple[Slot, str, Optional[RDFType]]]] = {}
    subj = Variable(SUBJECT_VARIABLE_NAME)
    for c in ordering:
        own = own_slots.get(c, {})
        own_ordered = sorted(own, key=lambda s: (s[1] != OUT, s[0]))
        parents = parents_of.get(c, [])
        # Own slots take precedence over inherited ones, and forward slots get names before
        # reverse slots, so that a plain property name always refers to the forward direction.
        candidates: List[Tuple[Slot, List[str], Optional[RDFType]]] = []
        seen: Set[Slot] = set()
        for slot in own_ordered:
            seen.add(slot)
            prop, direction = slot
            base = property_names.name(prop)
            names_to_try = [base] if direction == OUT else [base, base + "_inverse"]
            candidates.append((slot, names_to_try, own[slot]))
        for p in parents:
            for slot, parent_name, t in slot_layouts[p]:
                if slot in seen:
                    continue
                seen.add(slot)
                names_to_try = [parent_name]
                if slot[1] == IN:
                    names_to_try.append(property_names.name(slot[0]) + "_inverse")
                candidates.append((slot, names_to_try, t))
        candidates.sort(key=lambda x: x[0][1] != OUT)

        used_names = {SUBJECT_VARIABLE_NAME}
        layout: List[Tuple[Slot, str, Optional[RDFType]]] = []
        names: Dict[Slot, str] = {}
        for slot, names_to_try, t in candidates:
            name = _unique_name(names_to_try, used_names)
            names[slot] = name
            layout.append((slot, name, t))

        parameters = [Parameter(variable=subj, optional=False, rdf_type=RDFType.IRI)]
        for _, name, t in layout:
            parameters.append(Parameter(variable=Variable(name), optional=True, rdf_type=t))

        instances = []
        for slot in own_ordered:
            prop, direction = slot
            v = Variable(names[slot])
            if direction == OUT:
                instances.append(Triple(subj, IRI(prop), v))
            else:
                instances.append(Triple(v, IRI(prop), subj))

        # Each inherited slot is passed to the first superclass that has it,
        # so that triples are not produced more than once.
        handled: Set[Slot] = set(own)
        for p in parents:
            arguments = [subj]
            for slot, _, _ in slot_layouts[p]:
                if slot in handled:
                    arguments.append(None)
                else:
                    handled.add(slot)
                    arguments.append(Variable(names[slot]))
            instances.append(templates_without_typing[p].instance(arguments))

        slot_layouts[c] = layout
        templates_without_typing[c] = Template(
            IRI(c + NOTYPE_SUFFIX), parameters=parameters, instances=instances
        )

    templates: Dict[str, Template] = {}
    for c, template in templates_without_typing.items():
        templates[c + NOTYPE_SUFFIX] = template
    for c, template in templates_without_typing.items():
        instances = template.instances
        instances.append(Triple(subj, IRI(RDF_TYPE), IRI(c)))
        templates[c] = Template(
            iri=IRI(c), parameters=template.parameters, instances=instances
        )
    return templates


_CLASSES_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?c WHERE {
    { ?c a owl:Class } UNION { ?c a rdfs:Class }
    FILTER(ISIRI(?c))
}
"""
)

_DATATYPES_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?d WHERE {
    ?d a rdfs:Datatype .
    FILTER(ISIRI(?d))
}
"""
)

_SUBCLASS_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?child ?parent WHERE {
    { ?child rdfs:subClassOf ?parent }
    UNION
    { ?child rdfs:subClassOf/owl:intersectionOf/rdf:rest*/rdf:first ?parent }
    FILTER(ISIRI(?child) && ISIRI(?parent))
}
"""
)

_PROPERTY_TYPES_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?property ?property_type WHERE {
    ?property a ?property_type .
    { ?property rdfs:domain ?x } UNION { ?property rdfs:range ?x } UNION { ?r owl:onProperty ?property }
    FILTER(ISIRI(?property) && ISIRI(?property_type))
}
"""
)

_DOMAINS_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?property ?domain WHERE {
    { ?property rdfs:domain ?domain }
    UNION
    { ?property rdfs:domain/owl:unionOf/rdf:rest*/rdf:first ?domain }
    FILTER(ISIRI(?property) && ISIRI(?domain))
}
"""
)

_RANGES_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?property ?range WHERE {
    { ?property rdfs:range ?range }
    UNION
    { ?property rdfs:range/owl:unionOf/rdf:rest*/rdf:first ?range }
    FILTER(ISIRI(?property) && ISIRI(?range))
}
"""
)

_RESTRICTIONS_QUERY = (
    PREFIXES
    + """
SELECT DISTINCT ?domain ?property ?range WHERE {
    {
        ?domain rdfs:subClassOf ?restr .
    } UNION {
        ?domain rdfs:subClassOf/owl:intersectionOf/rdf:rest*/rdf:first ?restr .
    } UNION {
        ?domain owl:equivalentClass/owl:intersectionOf/rdf:rest*/rdf:first ?restr .
    }
    ?restr a owl:Restriction .
    ?restr owl:onProperty ?property .
    OPTIONAL {
        ?restr owl:someValuesFrom|owl:allValuesFrom|owl:onClass|owl:onDataRange ?range .
        FILTER(ISIRI(?range))
    }
    FILTER(ISIRI(?domain) && ISIRI(?property))
}
"""
)


def _query(m: Model, query: str, graph: Optional[str]) -> List[dict]:
    res = m.query(query, solution_mappings=True, graph=graph)
    df = res.mappings
    if df.height == 0:
        return []
    return df.to_dicts()


def _column(rows: List[dict], name: str) -> List[str]:
    return [r[name] for r in rows if r.get(name) is not None]


WELL_KNOWN_PREFIXES = {
    "rdf": RDF,
    "rdfs": RDFS,
    "owl": OWL,
    "xsd": XSD,
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dcterms": "http://purl.org/dc/terms/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "foaf": "http://xmlns.com/foaf/0.1/",
    "prov": "http://www.w3.org/ns/prov#",
    "sh": "http://www.w3.org/ns/shacl#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "time": "http://www.w3.org/2006/time#",
    "geo": "http://www.opengis.net/ont/geosparql#",
    "schema": "https://schema.org/",
}

_PN_CHARS_BASE = (
    "A-Za-z\u00C0-\u00D6\u00D8-\u00F6\u00F8-\u02FF\u0370-\u037D\u037F-\u1FFF"
    "\u200C-\u200D\u2070-\u218F\u2C00-\u2FEF\u3001-\uD7FF\uF900-\uFDCF\uFDF0-\uFFFD"
    "\U00010000-\U000EFFFF"
)
_PN_CHARS_U = _PN_CHARS_BASE + "_"
_PN_CHARS = _PN_CHARS_U + "\\-0-9\u00B7\u0300-\u036F\u203F-\u2040"
_PN_PREFIX_RE = re.compile(f"[{_PN_CHARS_BASE}]([{_PN_CHARS}.]*[{_PN_CHARS}])?")
_PN_LOCAL_RE = re.compile(f"[{_PN_CHARS_U}:0-9]([{_PN_CHARS}.:]*[{_PN_CHARS}:])?")
_NOT_PN_LOCAL_CHAR = re.compile(f"[^{_PN_CHARS}.:]")


def _is_pn_prefix(s: str) -> bool:
    return _PN_PREFIX_RE.fullmatch(s) is not None


def _sanitize_local(local: str) -> str:
    """Make a valid local name (PN_LOCAL without escapes) from the local part of an IRI."""
    local = _NOT_PN_LOCAL_CHAR.sub("_", local)
    if not local:
        return "property"
    if _PN_LOCAL_RE.fullmatch(local[0]) is None:
        local = "_" + local
    if local.endswith("."):
        local = local[:-1] + "_"
    return local


def _split_iri(iri: str) -> Tuple[str, str]:
    """Split an IRI into a namespace and a local name at the last #, / or :."""
    stripped = iri.rstrip("/#")
    for sep in ("#", "/", ":"):
        idx = stripped.rfind(sep)
        if idx >= 0:
            return iri[: idx + 1], stripped[idx + 1 :]
    return "", stripped


class _PrefixedNamer:
    """Names properties by prefixed IRIs, generating prefixes ns1, ns2, ... for unknown namespaces."""

    def __init__(self, prefixes: Dict[str, str], iris: Set[str]):
        self.prefixes = dict(prefixes)
        # Longest namespaces first, so that the most specific prefix is used.
        self.by_namespace = sorted(
            ((ns, p) for p, ns in self.prefixes.items()), key=lambda x: (-len(x[0]), x[1])
        )
        self.names: Dict[str, str] = {}
        unknown_namespaces = set()
        for iri in iris:
            if self._known(iri) is None:
                unknown_namespaces.add(_split_iri(iri)[0])
        i = 1
        for ns in sorted(unknown_namespaces):
            while f"ns{i}" in self.prefixes:
                i += 1
            self.prefixes[f"ns{i}"] = ns
            self.by_namespace.append((ns, f"ns{i}"))
            i += 1
        self.by_namespace.sort(key=lambda x: (-len(x[0]), x[1]))

    def _known(self, iri: str) -> Optional[str]:
        for ns, p in self.by_namespace:
            if iri.startswith(ns) and len(iri) > len(ns):
                return f"{p}:{_sanitize_local(iri[len(ns):])}"
        return None

    def name(self, iri: str) -> str:
        if iri not in self.names:
            name = self._known(iri)
            if name is None:
                # Only happens for IRIs that are their own namespace.
                ns, local = _split_iri(iri)
                p = next(p for n, p in self.by_namespace if n == ns)
                name = f"{p}:{_sanitize_local(local)}"
            self.names[iri] = name
        return self.names[iri]


def _unique_name(candidates: List[str], used: Set[str]) -> str:
    for c in candidates:
        if c not in used:
            used.add(c)
            return c
    base = candidates[0]
    i = 2
    while f"{base}_{i}" in used:
        i += 1
    name = f"{base}_{i}"
    used.add(name)
    return name
