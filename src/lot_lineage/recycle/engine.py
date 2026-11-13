"""Exact steady-state origin accounting over a process graph with recycle loops.

Algorithm outline
-----------------
1. Split the internal transfer graph into strongly connected components (SCCs)
   with an iterative Kosaraju traversal (self-loops and parallel edges are
   kept).  Components are numbered deterministically and the condensation DAG
   is processed in topological order.
2. Inside one SCC the origin fractions share one coefficient matrix::

       M_v * x[v,o] - sum(m[u,v] * x[u,o] for internal u->v) = b[v,o]

   where ``b[v,o]`` collects the direct external feeds of origin ``o`` plus
   the already-solved origin mass entering from upstream SCCs.  The matrix is
   factorised once; the multiple origin right-hand sides are solved together
   as separate columns of one augmented matrix.  No recycle path is ever
   enumerated, no flow is iterated gram by gram, and no floating point is
   used: every operation is an exact :class:`fractions.Fraction`.
3. Only SCCs that actually contain a cycle (a self-loop counts) need the
   linear solve.  Acyclic SCCs contain one node without a self-loop and reduce
   to the same weighted-average calculation as the acyclic batch pipeline.
4. Uniqueness, non-negativity, unit row sums, equation residuals and the
   three conservation ledgers are verified with exact arithmetic; any
   violation fails the whole run instead of returning a guess.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from fractions import Fraction

from .model import SteadyInput


class NonUniqueError(ValueError):
    """Raised when the steady-state equations have no unique legal solution."""


@dataclass
class SCC:
    scc_id: int
    members: list[str]
    cyclic: bool


@dataclass
class SteadyResult:
    data: SteadyInput
    sccs: list[SCC]
    node_scc: dict[str, int]
    condensation_order: list[int]
    # origin fractions: fractions[process_id][origin_id] = exact Fraction
    fractions: dict[str, dict[str, Fraction]]
    # per transfer/sink origin attribution: id -> {origin_id: Fraction}
    transfer_origins: dict[str, dict[str, Fraction]] = field(default_factory=dict)
    sink_origins: dict[str, dict[str, Fraction]] = field(default_factory=dict)
    # terminal totals per origin
    feed_total: dict[str, int] = field(default_factory=dict)
    product_total: dict[str, dict[str, Fraction]] = field(default_factory=dict)
    loss_total: dict[str, dict[str, Fraction]] = field(default_factory=dict)


def _kosaraju(nodes: list[str], edges: list[tuple[str, str, int]]) -> list[set[str]]:
    forward: dict[str, list[str]] = {node: [] for node in nodes}
    reverse: dict[str, list[str]] = {node: [] for node in nodes}
    for src, dst, _mass in edges:
        forward[src].append(dst)
        reverse[dst].append(src)

    visited: set[str] = set()
    finish_order: list[str] = []
    for start in nodes:
        if start in visited:
            continue
        visited.add(start)
        stack: list[tuple[str, int]] = [(start, 0)]
        while stack:
            node, index = stack[-1]
            neighbors = forward[node]
            if index < len(neighbors):
                stack[-1] = (node, index + 1)
                nxt = neighbors[index]
                if nxt not in visited:
                    visited.add(nxt)
                    stack.append((nxt, 0))
            else:
                finish_order.append(node)
                stack.pop()

    components: list[set[str]] = []
    assigned: set[str] = set()
    for start in reversed(finish_order):
        if start in assigned:
            continue
        component: set[str] = set()
        stack = [start]
        assigned.add(start)
        while stack:
            node = stack.pop()
            component.add(node)
            for nxt in reverse[node]:
                if nxt not in assigned:
                    assigned.add(nxt)
                    stack.append(nxt)
        components.append(component)
    return components


def _gauss_jordan(matrix: list[list[Fraction]], rhs_count: int) -> list[list[Fraction]]:
    """Solve A X = B in place.

    ``matrix`` has the coefficient columns followed by ``rhs_count`` RHS
    columns.  Pivot rows are chosen by smallest row index among non-zero
    candidates so the computation is fully deterministic.  Raises
    :class:`NonUniqueError` when the coefficient part is singular.
    """
    n = len(matrix)
    for col in range(n):
        pivot = next((row for row in range(col, n) if matrix[row][col] != 0), None)
        if pivot is None:
            raise NonUniqueError(
                f"steady-state equations are singular at variable {col}; origin proportions "
                "cannot be determined uniquely from the given feeds and flows"
            )
        if pivot != col:
            matrix[col], matrix[pivot] = matrix[pivot], matrix[col]
        pivot_value = matrix[col][col]
        matrix[col] = [value / pivot_value for value in matrix[col]]
        for row in range(n):
            if row == col:
                continue
            factor = matrix[row][col]
            if factor != 0:
                matrix[row] = [
                    value - factor * pivot_row_value
                    for value, pivot_row_value in zip(matrix[row], matrix[col])
                ]
    return [row[n:] for row in matrix]


def compute(data: SteadyInput) -> SteadyResult:
    nodes = list(data.processes)
    edges = [(t.from_process, t.to_process, t.mass_g) for t in data.transfers]

    raw_components = _kosaraju(nodes, edges)
    # Deterministic component ids: order SCCs by their lexicographically
    # smallest member, then number members by their own id.
    raw_components.sort(key=lambda component: min(component))
    sccs: list[SCC] = []
    node_scc: dict[str, int] = {}
    for scc_id, component in enumerate(raw_components, start=1):
        members = sorted(component)
        internal_pairs = {
            (t.from_process, t.to_process) for t in data.transfers
            if t.from_process in component and t.to_process in component
        }
        cyclic = any(src == dst for src, dst in internal_pairs) or len(component) > 1
        scc = SCC(scc_id=scc_id, members=members, cyclic=cyclic)
        sccs.append(scc)
        for member in members:
            node_scc[member] = scc_id

    # Condensation DAG and deterministic topological order (Kahn, sorted seeds).
    scc_by_id = {scc.scc_id: scc for scc in sccs}
    deps: dict[int, set[int]] = {scc.scc_id: set() for scc in sccs}
    dependents: dict[int, set[int]] = {scc.scc_id: set() for scc in sccs}
    for transfer in data.transfers:
        src = node_scc[transfer.from_process]
        dst = node_scc[transfer.to_process]
        if src != dst:
            deps[dst].add(src)
            dependents[src].add(dst)
    indegree = {scc_id: len(upstream) for scc_id, upstream in deps.items()}
    ready = sorted(scc_id for scc_id, degree in indegree.items() if degree == 0)
    condensation_order: list[int] = []
    while ready:
        scc_id = ready.pop(0)
        condensation_order.append(scc_id)
        for downstream in sorted(dependents[scc_id]):
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                ready.append(downstream)
                ready.sort()
    if len(condensation_order) != len(sccs):  # pragma: no cover - impossible for a condensation DAG
        raise NonUniqueError("condensation graph is not acyclic")

    origins = list(data.origins)
    fractions: dict[str, dict[str, Fraction]] = {node: {} for node in nodes}

    # Direct feed mass per (process, origin), aggregating split feeds.
    feed_mass: dict[tuple[str, str], int] = defaultdict(int)
    for feed in data.feeds:
        feed_mass[(feed.to_process, feed.origin_id)] += feed.mass_g

    incoming: dict[str, list[object]] = defaultdict(list)
    outgoing: dict[str, list[object]] = defaultdict(list)
    for transfer in data.transfers:
        incoming[transfer.to_process].append(transfer)
        outgoing[transfer.from_process].append(transfer)

    for scc_id in condensation_order:
        scc = scc_by_id[scc_id]
        members = scc.members
        index_of = {node: index for index, node in enumerate(members)}
        n = len(members)

        # Effective external forcing for every origin RHS.
        rhs: list[list[Fraction]] = [[Fraction(0) for _ in origins] for _ in members]
        for row, node in enumerate(members):
            for col, origin in enumerate(origins):
                rhs[row][col] = Fraction(feed_mass.get((node, origin), 0))
            for transfer in incoming[node]:
                if node_scc[transfer.from_process] == scc_id:
                    continue
                for col, origin in enumerate(origins):
                    rhs[row][col] += Fraction(transfer.mass_g) * fractions[transfer.from_process][origin]

        if not scc.cyclic:
            # Single node without self-loop: M_v x = b, plain weighted average.
            node = members[0]
            total = Fraction(data.mass_in[node])
            if total <= 0:  # pragma: no cover - rejected during validation
                raise NonUniqueError(f"process {node!r} has no inflow")
            for col, origin in enumerate(origins):
                fractions[node][origin] = rhs[0][col] / total
            continue

        # M_v on the diagonal, minus internal transfer masses for the
        # contributing rows.  The matrix is shared by every origin RHS.
        matrix: list[list[Fraction]] = [
            [Fraction(0) for _ in range(n)] + list(rhs_row)
            for rhs_row in rhs
        ]
        for row, node in enumerate(members):
            matrix[row][row] = Fraction(data.mass_in[node])
        for transfer in data.transfers:
            if node_scc[transfer.from_process] != scc_id or node_scc[transfer.to_process] != scc_id:
                continue
            row = index_of[transfer.to_process]
            col = index_of[transfer.from_process]
            matrix[row][col] -= Fraction(transfer.mass_g)

        solutions = _gauss_jordan(matrix, len(origins))
        for row, node in enumerate(members):
            for col, origin in enumerate(origins):
                value = solutions[row][col]
                if value < 0:
                    raise NonUniqueError(
                        f"illegal negative origin proportion for {origin!r} at process {node!r}: {value}"
                    )
                fractions[node][origin] = value

    result = SteadyResult(
        data=data,
        sccs=sccs,
        node_scc=node_scc,
        condensation_order=condensation_order,
        fractions=fractions,
    )
    _verify_and_attribute(result, feed_mass, incoming, outgoing, origins)
    return result


def _verify_and_attribute(
    result: SteadyResult,
    feed_mass: dict[tuple[str, str], int],
    incoming: dict[str, list[object]],
    outgoing: dict[str, list[object]],
    origins: list[str],
) -> None:
    data = result.data

    # Exact equation residuals and unit-sum / non-negativity checks per node.
    for node in data.processes:
        total = sum(result.fractions[node].values(), Fraction(0))
        if total != 1:
            raise NonUniqueError(
                f"process {node!r}: origin proportions sum to {total}, expected exactly 1"
            )
        for origin in origins:
            value = result.fractions[node][origin]
            if value < 0:
                raise NonUniqueError(
                    f"process {node!r}: negative proportion {value} for origin {origin!r}"
                )
            lhs = Fraction(data.mass_in[node]) * value
            rhs = Fraction(feed_mass.get((node, origin), 0))
            for transfer in incoming[node]:
                rhs += Fraction(transfer.mass_g) * result.fractions[transfer.from_process][origin]
            if lhs != rhs:
                raise NonUniqueError(
                    f"equation residual at process {node!r} origin {origin!r}: "
                    f"{lhs} != {rhs}"
                )

    # Attribution for every internal transfer and sink using the source
    # process proportions of the flow's from_process.
    for transfer in data.transfers:
        attributed = {
            origin: Fraction(transfer.mass_g) * result.fractions[transfer.from_process][origin]
            for origin in origins
        }
        if sum(attributed.values(), Fraction(0)) != transfer.mass_g:
            raise NonUniqueError(
                f"transfer {transfer.transfer_id!r}: attributed origin mass does not equal flow mass"
            )
        result.transfer_origins[transfer.transfer_id] = attributed

    product_total: dict[str, dict[str, Fraction]] = {o: defaultdict(Fraction) for o in origins}
    loss_total: dict[str, dict[str, Fraction]] = {o: defaultdict(Fraction) for o in origins}
    for sink in data.sinks:
        attributed = {
            origin: Fraction(sink.mass_g) * result.fractions[sink.from_process][origin]
            for origin in origins
        }
        if sum(attributed.values(), Fraction(0)) != sink.mass_g:
            raise NonUniqueError(
                f"sink {sink.sink_id!r}: attributed origin mass does not equal sink mass"
            )
        result.sink_origins[sink.sink_id] = attributed
        bucket = product_total if sink.kind == "product" else loss_total
        for origin, value in attributed.items():
            bucket[origin][sink.sink_id] = value

    # Layer 1: per node, per origin inflow equals outflow exactly.
    for node in data.processes:
        for origin in origins:
            inflow = Fraction(feed_mass.get((node, origin), 0))
            for transfer in incoming[node]:
                inflow += result.transfer_origins[transfer.transfer_id][origin]
            outflow = Fraction(0)
            for transfer in outgoing[node]:
                outflow += result.transfer_origins[transfer.transfer_id][origin]
            for sink in data.sinks:
                if sink.from_process == node:
                    outflow += result.sink_origins[sink.sink_id][origin]
            if inflow != outflow:
                raise NonUniqueError(
                    f"process {node!r} origin {origin!r}: per-origin balance broken "
                    f"({inflow} in != {outflow} out)"
                )

    # Layer 3: per external origin, feeds equal product plus loss; internal
    # transfers never appear in this ledger.
    feed_total: dict[str, int] = defaultdict(int)
    for feed in data.feeds:
        feed_total[feed.origin_id] += feed.mass_g
    for origin in origins:
        product = sum(product_total[origin].values(), Fraction(0))
        loss = sum(loss_total[origin].values(), Fraction(0))
        if Fraction(feed_total[origin]) != product + loss:
            raise NonUniqueError(
                f"origin {origin!r}: external feed {feed_total[origin]} g != "
                f"product {product} g + loss {loss} g"
            )

    result.feed_total = dict(feed_total)
    result.product_total = {origin: dict(values) for origin, values in product_total.items()}
    result.loss_total = {origin: dict(values) for origin, values in loss_total.items()}
