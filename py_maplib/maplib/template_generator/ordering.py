import warnings
from typing import Dict, Iterable, List, Set, Tuple


def topological_sort(
    classes: Iterable[str], edges: Iterable[Tuple[str, str]]
) -> Tuple[List[str], Dict[str, List[str]]]:
    """Order classes so that every superclass comes before its subclasses.

    :param classes: All classes that should be in the ordering, also those without any subclass edges.
    :param edges: (child, parent) pairs from rdfs:subClassOf.
    :return: The ordering, and for each class the parents that its template should instantiate.
        Self-loops are ignored. Cycles are broken deterministically (with a warning), since classes in a
        subclass cycle are equivalent and would otherwise instantiate each other forever.
        Redundant parents (those that are also ancestors of another parent) are pruned, so that
        the triples of a superclass are produced only once.
    """
    nodes: Set[str] = set(classes)
    parents: Dict[str, Set[str]] = {}
    for child, parent in edges:
        nodes.add(child)
        nodes.add(parent)
        if child == parent:
            continue
        parents.setdefault(child, set()).add(parent)

    children: Dict[str, Set[str]] = {}
    for child, ps in parents.items():
        for p in ps:
            children.setdefault(p, set()).add(child)

    remaining_in_degree = {n: len(parents.get(n, ())) for n in nodes}
    placed: Set[str] = set()
    ordering: List[str] = []
    ready = sorted(n for n, d in remaining_in_degree.items() if d == 0)
    broken_cycle_members: List[str] = []

    def place(n: str):
        placed.add(n)
        ordering.append(n)
        newly_ready = []
        for c in children.get(n, ()):
            if c in placed:
                continue
            remaining_in_degree[c] -= 1
            if remaining_in_degree[c] == 0:
                newly_ready.append(c)
        return newly_ready

    while len(ordering) < len(nodes):
        if not ready:
            # Only cycles remain, break one by placing its smallest member.
            n = min(x for x in nodes if x not in placed)
            broken_cycle_members.append(n)
            ready = [n]
        n = ready.pop(0)
        if n in placed:
            continue
        ready = sorted(set(ready) | set(place(n)))

    if broken_cycle_members:
        warnings.warn(
            "Cyclic rdfs:subClassOf hierarchy, ignoring the subclass edges into: "
            + ", ".join(broken_cycle_members)
        )

    position = {n: i for i, n in enumerate(ordering)}
    # Only keep edges to parents that were placed before the child (drops cycle edges).
    direct_parents: Dict[str, List[str]] = {}
    for n in ordering:
        direct_parents[n] = sorted(
            (p for p in parents.get(n, ()) if position[p] < position[n]),
            key=lambda p: position[p],
        )

    ancestors: Dict[str, Set[str]] = {}
    for n in ordering:
        a: Set[str] = set()
        for p in direct_parents[n]:
            a.add(p)
            a |= ancestors[p]
        ancestors[n] = a

    pruned: Dict[str, List[str]] = {}
    for n in ordering:
        ps = direct_parents[n]
        pruned[n] = [
            p for p in ps if not any(p in ancestors[q] for q in ps if q != p)
        ]
    return ordering, pruned
