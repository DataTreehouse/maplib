import polars as pl
from typing import List


def topological_sort(subclasses: pl.DataFrame) -> List[str]:
    in_degree_df = subclasses.group_by("child").agg([pl.col("parent").count()])
    in_degree_dict = {}
    for d in in_degree_df.iter_rows(named=True):
        in_degree_dict[d["child"]] = d["parent"]

    out_edges_df = subclasses.group_by("parent").agg([pl.col("child")])
    out_edges_dict = {}
    for d in out_edges_df.iter_rows(named=True):
        out_edges_dict[d["parent"]] = d["child"]

    sorting = []

    # First, we can add those parents who have no parents.
    for p in subclasses.select("parent").unique().iter_rows(named=True):
        if p["parent"] not in in_degree_dict:
            sorting.append(p["parent"])
            for out_edge in out_edges_dict[p["parent"]]:
                in_degree_dict[out_edge] = in_degree_dict[out_edge] - 1

    # Next, a topological sort defines ordering.
    while len(in_degree_dict) > 0:
        found_p = None
        for p, i in in_degree_dict.items():
            if i == 0:
                found_p = p
                break
        if found_p is None:
            assert False, "Error in the algorithm.. "

        sorting.append(found_p)
        in_degree_dict.pop(found_p)
        if found_p in out_edges_dict:
            for out_edge in out_edges_dict[found_p]:
                in_degree_dict[out_edge] = in_degree_dict[out_edge] - 1
    return sorting
