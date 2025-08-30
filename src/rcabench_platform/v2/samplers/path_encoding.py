"""
Path encoding utilities for trace sampling comprehensiveness metrics.

Based on TracePicker paper's path encoding mechanism for handling parallel calls
through breadth-first search with sorted nodes at same depth.
"""

from collections import defaultdict, deque

import polars as pl

from ..logging import logger


def build_trace_path_encoding(trace_df: pl.DataFrame) -> str:
    """
    Build path encoding for a single trace using BFS with sorted nodes at same depth.

    Args:
        trace_df: DataFrame containing spans for a single trace

    Returns:
        String representation of the execution path
    """
    if len(trace_df) == 0:
        return ""

    # Build parent-child relationships
    spans_data = {
        row["span_id"]: {
            "parent_span_id": row["parent_span_id"],
            "service_name": row["service_name"],
            "span_name": row["span_name"],
        }
        for row in trace_df.select(["span_id", "parent_span_id", "service_name", "span_name"]).iter_rows(named=True)
    }

    # Find root spans (no parent or parent not in trace)
    root_spans = []
    children_map = defaultdict(list)

    for span_id, span_info in spans_data.items():
        parent_id = span_info["parent_span_id"]

        if parent_id is None or parent_id == "" or parent_id not in spans_data:
            root_spans.append(span_id)
        else:
            children_map[parent_id].append(span_id)

    if not root_spans:
        logger.warning("No root spans found in trace")
        return ""

    # BFS traversal with depth-based sorting
    path_elements = []
    queue = deque()

    # Start with root spans (sorted by service:operation label)
    root_spans_labeled = [
        (span_id, f"{spans_data[span_id]['service_name']}:{spans_data[span_id]['span_name']}") for span_id in root_spans
    ]
    root_spans_labeled.sort(key=lambda x: x[1])  # Sort by label

    for span_id, label in root_spans_labeled:
        queue.append((span_id, 0))  # (span_id, depth)
        path_elements.append(label)

    visited = set(span_id for span_id, _ in root_spans_labeled)

    # BFS with sorting at each depth level
    current_depth = 0
    depth_nodes = []

    while queue:
        span_id, depth = queue.popleft()

        # Process nodes at same depth together
        if depth != current_depth:
            # Sort and add nodes from previous depth
            if depth_nodes:
                depth_nodes.sort(key=lambda x: x[1])  # Sort by label
                path_elements.extend([label for _, label in depth_nodes])

            depth_nodes = []
            current_depth = depth

        # Get children of current span
        children = children_map.get(span_id, [])

        for child_id in children:
            if child_id not in visited:
                visited.add(child_id)
                child_info = spans_data[child_id]
                child_label = f"{child_info['service_name']}:{child_info['span_name']}"

                queue.append((child_id, depth + 1))
                depth_nodes.append((child_id, child_label))

    # Process any remaining nodes at the last depth
    if depth_nodes:
        depth_nodes.sort(key=lambda x: x[1])
        path_elements.extend([label for _, label in depth_nodes])

    return " -> ".join(path_elements)


def calculate_path_coverage(traces_df: pl.DataFrame, sampled_trace_ids: set[str]) -> dict[str, float]:
    """
    Calculate path coverage metrics for sampled traces.

    Args:
        traces_df: All traces data
        sampled_trace_ids: Set of sampled trace IDs

    Returns:
        Dictionary containing path coverage metrics
    """
    # Group traces by trace_id
    trace_groups = traces_df.partition_by("trace_id", as_dict=True)

    # Build path encodings for all traces
    logger.info(f"Building path encodings for {len(trace_groups)} traces")

    all_paths = set()
    sampled_paths = set()

    for (trace_id,), trace_df in trace_groups.items():
        if not trace_id:
            continue

        path_encoding = build_trace_path_encoding(trace_df)
        if path_encoding:
            all_paths.add(path_encoding)

            if trace_id in sampled_trace_ids:
                sampled_paths.add(path_encoding)

    total_path_types = len(all_paths)
    sampled_path_types = len(sampled_paths)

    path_coverage = sampled_path_types / total_path_types if total_path_types > 0 else 0.0

    logger.info(f"Path coverage: {sampled_path_types}/{total_path_types} = {path_coverage:.4f}")

    return {
        "total_path_types": total_path_types,
        "sampled_path_types": sampled_path_types,
        "path_coverage": path_coverage,
    }
