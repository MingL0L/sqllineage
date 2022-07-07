from typing import Any, Dict, List

from networkx import DiGraph
from sqlparse import format as sql_fm


def get_nodelist(res):
    list_n = []

    def _appe(l, v):
        l.append(v) if v not in l and not v.endswith(".*") else None

    for e in res:
        _appe(list_n, str(e[0]))
        _appe(list_n, str(e[1]))
        _appe(list_n, str(e[0].parent))
        _appe(list_n, str(e[1].parent))
    return list_n


def find_target_edge(edges_src, target, raw_name=None):
    res = []
    new_target = []
    for edge in edges_src:
        if str(edge[1]) in target:
            new_target.append(str(edge[0]))
            new_target.append(str(edge[0].parent) + '.*')
            new_target.append(str(edge[0].parent) + '.' + raw_name)
            res.append(edge)
    if new_target:
        res.extend(find_target_edge(edges_src, new_target, raw_name))
    return res


def fmt_sql(sql):
    if sql == 'FLOAT64':
        return ''
    # return sql_fm(sql, reindent=True, wrap_after=True, keyword_case='upper')
    else:
        return sql


def to_cytoscape(graph: DiGraph, compound=False) -> List[Dict[str, Dict[str, Any]]]:
    """
    compound nodes is used to group nodes together to their parent.
    See https://js.cytoscape.org/#notation/compound-nodes for reference.
    """
    edges_target_col = find_target_edge(graph.edges, ['db_dest.table_dest.ca_facture'], 'ca_facture')
    if compound:
        parents_dict = {
            node.parent: {
                "name": str(node.parent) if node.parent is not None else "<unknown>",
                "where": fmt_sql(str(node.parent.where)) if hasattr(node.parent, 'alias') else "",
                "groupby": fmt_sql(str(node.parent.groupby)) if hasattr(node.parent, 'alias') else "",
                "type": type(node.parent).__name__ if node.parent is not None else "Table or SubQuery",
                "write": node.parent.write
            }
            for node in graph.nodes
        }
        nodes = [
            {
                "data": {
                    "id": str(node),
                    "formula": fmt_sql(node.formula) if node.formula else node.raw_name,
                    "where": fmt_sql(str(node.parent.where)) if hasattr(node.parent, 'alias') and node.parent.write else "",
                    "groupby": fmt_sql(str(node.parent.groupby)) if hasattr(node.parent, 'alias') and node.parent.write else "",
                    "parent": parents_dict[node.parent]["name"],
                    "parent_candidates": [
                        {"name": str(p), "type": type(p).__name__}
                        for p in node.parent_candidates
                    ],
                    "type": type(node).__name__,
                }
            }
            for node in graph.nodes
        ]
        nodes += [
            {
                "data": {
                    "id": attr["name"],
                    "type": attr["type"],
                    "where": attr["where"],
                    "groupby": attr["groupby"]
                }
            }
            for _, attr in parents_dict.items()
        ]

        # filter nodes
        nodes_target = [n for n in nodes if n["data"]["id"] in get_nodelist(edges_target_col)]
        for _, attr in parents_dict.items():
            if attr["write"] and attr["where"]:
                nodes_target.append({
                    "data": {
                        "id": attr["where"],
                        "type": 'Where',
                        "val": attr["where"],
                        "len": len(attr["where"]),
                        "parent": attr["name"],
                        "parent_candidates": [
                            {"name": attr["name"], "type": attr["type"]}
                        ]
                    }
                })

            if attr["write"] and attr["groupby"]:
                nodes_target.append({
                    "data": {
                        "id": attr["groupby"],
                        "type": 'Groupby',
                        "len": len(attr["groupby"]),
                        "val": attr["groupby"],
                        "parent": attr["name"],
                        "parent_candidates": [
                            {"name": attr["name"], "type": attr["type"]}
                        ]
                    }
                })

    else:
        nodes = [{"data": {"id": str(node), "formula": str(node), "condition": ''}} for node in graph.nodes]

    edges: List[Dict[str, Dict[str, Any]]] = [
        {
            "data": {
                "id": f"e{i}",
                "source": str(edge[0]),
                "target": str(edge[1])
            }
        }
        for i, edge in enumerate(graph.edges)
    ]

    edges_target_tbl = []
    for e in edges_target_col:
        tmp = [e[0].parent, e[1].parent]
        if tmp not in edges_target_tbl:
            edges_target_tbl.append(tmp)

    edges_target = edges_target_col
    # edges_filtered: List[Dict[str, Dict[str, Any]]] = [
    #     {
    #         "data": {
    #             "id": f"e{i}",
    #             "source": str(edge[0]),
    #             "target": str(edge[1])
    #         }
    #     }
    #     for i, edge in enumerate(edges_target)
    # ]
    edges_filtered: List[Dict[str, Dict[str, Any]]] = []
    for i, edge in enumerate(edges_target):
        if str(edge[0]).endswith(".*") and str(edge[1]).endswith(".*"):
            edges_filtered.append({
                "data": {
                    "id": f"e{i}",
                    "source": str(edge[0]).replace("*", 'ca_facture'),
                    "target": str(edge[1]).replace("*", 'ca_facture'),
                }
            })
        else:
            edges_filtered.append({
                "data": {
                    "id": f"e{i}",
                    "source": str(edge[0]),
                    "target": str(edge[1])
                }
            })

    if compound:
        # print('compound')
        #print(nodes_target + edges_filtered)
        return nodes_target + edges_filtered
    else:
        #print('no compound')
        #print(nodes + edges)
        return nodes + edges
    # return nodes_target + edges_filtered
