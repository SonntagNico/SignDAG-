#!/usr/bin/env python3
"""
Enumerate conditional independencies and dependencies implied by a DAG.

Dependencies are reported as d-connections. Interpreting d-connection as a
statistical dependency requires the faithfulness assumption.

Optional sign prediction support:
- each directed edge can be treated as monotonic positive (+) by default
- selected edges can be marked negative (-)
- for each d-connected (X, Y | Z), association sign is:
  - positive: all active paths are positive
  - negative: all active paths are negative
  - indeterminate: active paths have mixed signs
Path sign rule implemented from the provided method:
positive iff (#negative edges on path + #controlled colliders on path) is even.
"""

from __future__ import annotations

import argparse
import itertools
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Literal, Sequence, Set, Tuple


Edge = Tuple[str, str]


def parse_edge(token: str) -> Edge:
    if "->" in token:
        left, right = token.split("->", 1)
    elif "," in token:
        left, right = token.split(",", 1)
    else:
        parts = token.split()
        if len(parts) != 2:
            raise ValueError(f"Cannot parse edge token: {token!r}")
        left, right = parts
    left = left.strip()
    right = right.strip()
    if not left or not right:
        raise ValueError(f"Invalid edge token: {token!r}")
    return left, right


def parse_negative_edges(tokens: Sequence[str]) -> Set[Edge]:
    negatives: Set[Edge] = set()
    for tok in tokens:
        negatives.add(parse_edge(tok))
    return negatives


def parse_signed_edge(token: str) -> Tuple[Edge, str]:
    pat = r"^\s*([A-Za-z0-9_]+)\s*-\(\s*([+-])\s*\)->\s*([A-Za-z0-9_]+)\s*$"
    m = re.match(pat, token)
    if not m:
        raise ValueError(f"Cannot parse signed edge token: {token!r}")
    u, sign, v = m.group(1), m.group(2), m.group(3)
    return (u, v), sign


def parse_signed_models_file(path: Path) -> List[Dict[str, object]]:
    """
    Parse models in notation:
      "ModelName";
      A-(+)->B;
      C;
    """
    tokens = [t.strip() for t in path.read_text(encoding="utf-8-sig").split(";")]
    models: List[Dict[str, object]] = []
    current: Dict[str, object] | None = None

    for token in tokens:
        if not token:
            continue
        name_match = re.match(r'^"([^"]+)"$', token)
        if name_match:
            current = {
                "name": name_match.group(1),
                "nodes": set(),
                "edges": [],
                "negative_edges": set(),
            }
            models.append(current)
            continue

        if current is None:
            raise ValueError(f"Statement before model name: {token!r}")

        if "->" in token:
            edge, sign = parse_signed_edge(token)
            current["edges"].append(edge)  # type: ignore[index]
            current["nodes"].add(edge[0])  # type: ignore[index]
            current["nodes"].add(edge[1])  # type: ignore[index]
            if sign == "-":
                current["negative_edges"].add(edge)  # type: ignore[index]
        else:
            node = token.strip()
            if not re.match(r"^[A-Za-z0-9_]+$", node):
                raise ValueError(f"Cannot parse node token: {token!r}")
            current["nodes"].add(node)  # type: ignore[index]

    if not models:
        raise ValueError(f"No models found in {path}")

    # Normalize mutable containers to plain lists for stable downstream handling.
    normalized: List[Dict[str, object]] = []
    for m in models:
        normalized.append(
            {
                "name": m["name"],
                "nodes": sorted(m["nodes"]),  # type: ignore[index]
                "edges": list(m["edges"]),  # type: ignore[index]
                "negative_edges": set(m["negative_edges"]),  # type: ignore[index]
            }
        )
    return normalized


def parse_trace_prediction(spec: str) -> Tuple[str, str, Tuple[str, ...]]:
    raw = spec.strip()
    if "|" in raw:
        pair_part, cond_part = raw.split("|", 1)
    else:
        pair_part, cond_part = raw, ""
    pair = [p.strip() for p in pair_part.split(",") if p.strip()]
    if len(pair) != 2:
        raise ValueError(f"Trace prediction must be X,Y|Z1,Z2 (got {spec!r})")
    x, y = pair
    if cond_part:
        cond = cond_part.strip()
        if cond.startswith("{") and cond.endswith("}"):
            cond = cond[1:-1]
        z = tuple(sorted([c.strip() for c in cond.split(",") if c.strip() and c.strip() != "{}"]))
    else:
        z = tuple()
    return x, y, z


def check_node_name(name: str) -> bool:
    return bool(re.match(r"^[A-Za-z0-9_]+$", name))


def read_edges_from_file(path: Path) -> List[Edge]:
    edges: List[Edge] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        edges.append(parse_edge(line))
    return edges


def build_graph(nodes: Sequence[str], edges: Sequence[Edge]) -> Tuple[List[str], Set[Edge], Dict[str, Set[str]], Dict[str, Set[str]], Dict[str, Set[str]]]:
    node_set: Set[str] = set(nodes)
    for u, v in edges:
        node_set.add(u)
        node_set.add(v)

    sorted_nodes = sorted(node_set)
    edge_set = set(edges)

    parents: Dict[str, Set[str]] = defaultdict(set)
    children: Dict[str, Set[str]] = defaultdict(set)
    undirected: Dict[str, Set[str]] = defaultdict(set)

    for u, v in edge_set:
        parents[v].add(u)
        children[u].add(v)
        undirected[u].add(v)
        undirected[v].add(u)

    # Force keys to exist for isolated nodes.
    for n in sorted_nodes:
        parents[n]
        children[n]
        undirected[n]

    return sorted_nodes, edge_set, parents, children, undirected


def has_cycle(nodes: Sequence[str], children: Dict[str, Set[str]]) -> bool:
    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n: WHITE for n in nodes}

    def dfs(u: str) -> bool:
        color[u] = GRAY
        for v in children[u]:
            if color[v] == GRAY:
                return True
            if color[v] == WHITE and dfs(v):
                return True
        color[u] = BLACK
        return False

    return any(color[n] == WHITE and dfs(n) for n in nodes)


def compute_descendants(nodes: Sequence[str], children: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    descendants: Dict[str, Set[str]] = {n: set() for n in nodes}
    for n in nodes:
        stack = list(children[n])
        seen: Set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            descendants[n].add(cur)
            stack.extend(children[cur])
    return descendants


def all_simple_paths(undirected: Dict[str, Set[str]], src: str, dst: str) -> Iterable[List[str]]:
    stack: List[Tuple[str, List[str], Set[str]]] = [(src, [src], {src})]
    while stack:
        node, path, visited = stack.pop()
        if node == dst:
            yield path
            continue
        for nbr in undirected[node]:
            if nbr in visited:
                continue
            stack.append((nbr, path + [nbr], visited | {nbr}))


def is_collider(a: str, b: str, c: str, edge_set: Set[Edge]) -> bool:
    return (a, b) in edge_set and (c, b) in edge_set


def is_path_active(path: Sequence[str], z: Set[str], edge_set: Set[Edge], descendants: Dict[str, Set[str]]) -> bool:
    if len(path) <= 2:
        return True
    for i in range(1, len(path) - 1):
        a, b, c = path[i - 1], path[i], path[i + 1]
        collider = is_collider(a, b, c, edge_set)
        if collider:
            if b not in z and not (descendants[b] & z):
                return False
        else:
            if b in z:
                return False
    return True


def d_connected(x: str, y: str, z: Set[str], edge_set: Set[Edge], undirected: Dict[str, Set[str]], descendants: Dict[str, Set[str]]) -> bool:
    for path in all_simple_paths(undirected, x, y):
        if is_path_active(path, z, edge_set, descendants):
            return True
    return False


def active_paths(x: str, y: str, z: Set[str], edge_set: Set[Edge], undirected: Dict[str, Set[str]], descendants: Dict[str, Set[str]]) -> List[List[str]]:
    out: List[List[str]] = []
    for path in all_simple_paths(undirected, x, y):
        if is_path_active(path, z, edge_set, descendants):
            out.append(path)
    return out


def count_negative_edges_on_path(path: Sequence[str], negative_edges: Set[Edge], edge_set: Set[Edge]) -> int:
    count = 0
    for i in range(len(path) - 1):
        a, b = path[i], path[i + 1]
        if (a, b) in edge_set:
            directed = (a, b)
        elif (b, a) in edge_set:
            directed = (b, a)
        else:
            raise ValueError(f"Path uses non-edge: {a}-{b}")
        if directed in negative_edges:
            count += 1
    return count


def negative_directed_edges_on_path(path: Sequence[str], negative_edges: Set[Edge], edge_set: Set[Edge]) -> List[Edge]:
    out: List[Edge] = []
    for i in range(len(path) - 1):
        a, b = path[i], path[i + 1]
        if (a, b) in edge_set:
            directed = (a, b)
        elif (b, a) in edge_set:
            directed = (b, a)
        else:
            raise ValueError(f"Path uses non-edge: {a}-{b}")
        if directed in negative_edges:
            out.append(directed)
    return out


def count_controlled_colliders_on_path(path: Sequence[str], z: Set[str], edge_set: Set[Edge]) -> int:
    count = 0
    for i in range(1, len(path) - 1):
        a, b, c = path[i - 1], path[i], path[i + 1]
        if is_collider(a, b, c, edge_set) and b in z:
            count += 1
    return count


def controlled_colliders_on_path(path: Sequence[str], z: Set[str], edge_set: Set[Edge]) -> List[str]:
    out: List[str] = []
    for i in range(1, len(path) - 1):
        a, b, c = path[i - 1], path[i], path[i + 1]
        if is_collider(a, b, c, edge_set) and b in z:
            out.append(b)
    return out


def path_sign(path: Sequence[str], z: Set[str], edge_set: Set[Edge], negative_edges: Set[Edge]) -> Literal["positive", "negative"]:
    parity = (
        count_negative_edges_on_path(path, negative_edges, edge_set)
        + count_controlled_colliders_on_path(path, z, edge_set)
    ) % 2
    return "positive" if parity == 0 else "negative"


def association_sign_from_paths(paths: Sequence[Sequence[str]], z: Set[str], edge_set: Set[Edge], negative_edges: Set[Edge]) -> Literal["independent", "positive", "negative", "indeterminate"]:
    if not paths:
        return "independent"
    signs = {path_sign(p, z, edge_set, negative_edges) for p in paths}
    if signs == {"positive"}:
        return "positive"
    if signs == {"negative"}:
        return "negative"
    return "indeterminate"


def conditioning_subsets(items: Sequence[str], max_size: int | None) -> Iterable[Tuple[str, ...]]:
    upper = len(items) if max_size is None else min(max_size, len(items))
    for r in range(0, upper + 1):
        for combo in itertools.combinations(items, r):
            yield combo


def fmt_set(items: Iterable[str]) -> str:
    vals = sorted(items)
    if not vals:
        return "{}"
    return "{" + ", ".join(vals) + "}"


def enumerate_implications(
    nodes: Sequence[str],
    edge_set: Set[Edge],
    undirected: Dict[str, Set[str]],
    descendants: Dict[str, Set[str]],
    max_cond_size: int | None,
    negative_edges: Set[Edge],
) -> Tuple[
    List[Tuple[str, str, Tuple[str, ...]]],
    List[Tuple[str, str, Tuple[str, ...], str]],
]:
    independencies: List[Tuple[str, str, Tuple[str, ...]]] = []
    dependencies: List[Tuple[str, str, Tuple[str, ...], str]] = []

    for x, y in itertools.combinations(nodes, 2):
        others = [n for n in nodes if n not in {x, y}]
        for z_tuple in conditioning_subsets(others, max_cond_size):
            z = set(z_tuple)
            paths = active_paths(x, y, z, edge_set, undirected, descendants)
            if not paths:
                independencies.append((x, y, z_tuple))
            else:
                sign = association_sign_from_paths(paths, z, edge_set, negative_edges)
                dependencies.append((x, y, z_tuple, sign))

    return independencies, dependencies


def prepare_graph(
    nodes: Sequence[str],
    edges: Sequence[Edge],
    negative_edges: Set[Edge],
) -> Tuple[List[str], Set[Edge], Dict[str, Set[str]], Dict[str, Set[str]]]:
    nodes2, edge_set, _parents, children, undirected = build_graph(nodes, edges)
    if not negative_edges.issubset(edge_set):
        missing = sorted(f"{u}->{v}" for (u, v) in (negative_edges - edge_set))
        raise ValueError(f"Negative edges not found in DAG: {', '.join(missing)}")
    if has_cycle(nodes2, children):
        raise ValueError("Input graph has a directed cycle; expected a DAG.")
    descendants = compute_descendants(nodes2, children)
    return nodes2, edge_set, undirected, descendants


def trace_prediction_payload(
    nodes: Sequence[str],
    edges: Sequence[Edge],
    negative_edges: Set[Edge],
    trace_spec: str,
) -> Dict[str, object]:
    x, y, z_tuple = parse_trace_prediction(trace_spec)
    for var in (x, y, *z_tuple):
        if not check_node_name(var):
            raise ValueError(f"Invalid variable name in trace prediction: {var!r}")

    nodes2, edge_set, undirected, descendants = prepare_graph(nodes, edges, negative_edges)
    missing_vars = sorted([v for v in (x, y, *z_tuple) if v not in set(nodes2)])
    if missing_vars:
        raise ValueError(f"Trace variables not in DAG: {', '.join(missing_vars)}")

    z = set(z_tuple)
    paths = active_paths(x, y, z, edge_set, undirected, descendants)
    overall = association_sign_from_paths(paths, z, edge_set, negative_edges)
    details: List[Dict[str, object]] = []
    for p in paths:
        neg_edges = negative_directed_edges_on_path(p, negative_edges, edge_set)
        controlled_cols = controlled_colliders_on_path(p, z, edge_set)
        neg_count = len(neg_edges)
        col_count = len(controlled_cols)
        parity = (neg_count + col_count) % 2
        details.append(
            {
                "path": p,
                "path_str": " -> ".join(p),
                "negative_edges": [{"from": u, "to": v} for (u, v) in neg_edges],
                "negative_edge_count": neg_count,
                "controlled_colliders": controlled_cols,
                "controlled_collider_count": col_count,
                "parity_total": parity,
                "path_sign": "positive" if parity == 0 else "negative",
            }
        )

    return {
        "query": {"x": x, "y": y, "given": list(z_tuple)},
        "d_connected": bool(paths),
        "overall_sign_prediction": overall,
        "active_path_count": len(paths),
        "active_paths": details,
    }


def build_payload(
    nodes: Sequence[str],
    edges: Sequence[Edge],
    negative_edges: Set[Edge],
    max_conditioning_size: int | None,
) -> Dict[str, object]:
    nodes2, edge_set, undirected, descendants = prepare_graph(nodes, edges, negative_edges)
    independencies, dependencies = enumerate_implications(
        nodes2,
        edge_set,
        undirected,
        descendants,
        max_conditioning_size,
        negative_edges,
    )
    return {
        "nodes": nodes2,
        "edges": sorted([{"from": u, "to": v} for u, v in edge_set], key=lambda d: (d["from"], d["to"])),
        "negative_edges": sorted([{"from": u, "to": v} for u, v in negative_edges], key=lambda d: (d["from"], d["to"])),
        "independencies": [{"x": x, "y": y, "given": list(z)} for x, y, z in independencies],
        "dependencies_d_connected": [
            {"x": x, "y": y, "given": list(z), "sign_prediction": s} for x, y, z, s in dependencies
        ],
        "note": "Dependencies are d-connections; statistical dependence needs faithfulness.",
        "sign_note": "Sign rule: parity of (#negative edges + #controlled colliders in Z) across active paths.",
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="List conditional independencies and d-connected dependencies for a DAG.")
    p.add_argument("--nodes", nargs="*", default=[], help="Optional node names.")
    p.add_argument("--edges", nargs="*", default=[], help="Edges as A->B (or A,B).")
    p.add_argument(
        "--negative-edges",
        nargs="*",
        default=[],
        help="Subset of directed edges with negative direct effect sign, e.g. A->B C->D. All others are positive.",
    )
    p.add_argument("--edge-file", type=Path, help="Text file with one edge per line, e.g. A->B.")
    p.add_argument(
        "--signed-models-file",
        type=Path,
        help='File with multiple signed DAGs in notation: "Model"; A-(+)->B; C;',
    )
    p.add_argument(
        "--trace-prediction",
        action="append",
        default=[],
        help='Trace one prediction (repeatable), format: X,Y|Z1,Z2 or X,Y|{}',
    )
    p.add_argument("--max-conditioning-size", type=int, default=None, help="Maximum |Z| to enumerate (default: all).")
    p.add_argument("--output-json", type=Path, help="Optional path to write JSON output.")
    p.add_argument("--json", action="store_true", help="Emit JSON instead of plain text.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.signed_models_file:
        models = parse_signed_models_file(args.signed_models_file)
        if args.trace_prediction:
            traces = []
            for m in models:
                model_traces = []
                for spec in args.trace_prediction:
                    model_traces.append(
                        trace_prediction_payload(
                            m["nodes"],  # type: ignore[arg-type]
                            m["edges"],  # type: ignore[arg-type]
                            m["negative_edges"],  # type: ignore[arg-type]
                            spec,
                        )
                    )
                traces.append({"name": m["name"], "traces": model_traces})
            out = {
                "source_file": str(args.signed_models_file),
                "model_count": len(models),
                "trace_predictions": list(args.trace_prediction),
                "models": traces,
            }
            text = json.dumps(out, indent=2)
            if args.output_json:
                args.output_json.write_text(text, encoding="utf-8")
            else:
                print(text)
            return

        out = {
            "source_file": str(args.signed_models_file),
            "model_count": len(models),
            "models": [],
        }
        for m in models:
            payload = build_payload(
                m["nodes"],  # type: ignore[arg-type]
                m["edges"],  # type: ignore[arg-type]
                m["negative_edges"],  # type: ignore[arg-type]
                args.max_conditioning_size,
            )
            out["models"].append({"name": m["name"], **payload})

        text = json.dumps(out, indent=2)
        if args.output_json:
            args.output_json.write_text(text, encoding="utf-8")
        else:
            print(text)
        return

    edges: List[Edge] = [parse_edge(tok) for tok in args.edges]
    if args.edge_file:
        edges.extend(read_edges_from_file(args.edge_file))
    negative_edges = parse_negative_edges(args.negative_edges)

    if not edges and not args.nodes:
        raise SystemExit("Provide --edges/--edge-file (or use --signed-models-file).")
    if args.trace_prediction:
        traces = [
            trace_prediction_payload(args.nodes, edges, negative_edges, spec)
            for spec in args.trace_prediction
        ]
        out = {"trace_predictions": list(args.trace_prediction), "traces": traces}
        text = json.dumps(out, indent=2)
        if args.output_json:
            args.output_json.write_text(text, encoding="utf-8")
        else:
            print(text)
        return

    payload = build_payload(args.nodes, edges, negative_edges, args.max_conditioning_size)

    if args.json:
        text = json.dumps(payload, indent=2)
        if args.output_json:
            args.output_json.write_text(text, encoding="utf-8")
        else:
            print(text)
        return
    if args.output_json:
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    nodes = payload["nodes"]  # type: ignore[assignment]
    edges_json = payload["edges"]  # type: ignore[assignment]
    negatives_json = payload["negative_edges"]  # type: ignore[assignment]
    independencies = payload["independencies"]  # type: ignore[assignment]
    dependencies = payload["dependencies_d_connected"]  # type: ignore[assignment]

    print("Nodes:", ", ".join(nodes))
    print("Edges:", ", ".join(f'{e["from"]}->{e["to"]}' for e in edges_json))
    print("Negative edges:", ", ".join(f'{e["from"]}->{e["to"]}' for e in negatives_json) if negatives_json else "(none)")
    print()

    print(f"Conditional independencies (count={len(independencies)}):")
    for row in independencies:
        print(f'  {row["x"]} _||_ {row["y"]} | {fmt_set(row["given"])}')

    print()
    print(f"D-connected dependencies (count={len(dependencies)}):")
    for row in dependencies:
        print(f'  {row["x"]} !|| {row["y"]} | {fmt_set(row["given"])}    sign={row["sign_prediction"]}')

    print()
    print("Note: D-connected entries are graph-allowed dependencies; interpreting them as guaranteed")
    print("dependencies requires a faithfulness assumption.")
    print("Sign note: path sign is positive iff (#negative edges + #controlled colliders in Z) is even.")


if __name__ == "__main__":
    main()
