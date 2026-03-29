"""
Normalize near-correct entries from main_dataset_alpaca.json into the
canonical Blender node graph format and append them to dataset_alpaca_clean.json.

Handles two fixable cases:
  1. nodes-dict with extra 'connections' key  →  strip 'connections', add 'groups: {}'
  2. nodes-dict with per-node socket data + 'links' list  →  convert links to update_lists,
     strip 'inputs'/'outputs'/'links', add missing scaffold keys

All other formats (nodes-as-list, VTK nodes, etc.) are skipped — not worth the conversion risk.
"""

import json
import hashlib
from pathlib import Path
from collections import defaultdict

MAIN_PATH  = Path(__file__).parent / "main_dataset_alpaca.json"
CLEAN_PATH = Path(__file__).parent / "dataset_alpaca_clean.json"

EXPECTED = {"export_version", "framed_nodes", "groups", "nodes", "update_lists"}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def output_hash(output_str: str) -> str:
    return hashlib.md5(output_str.encode()).hexdigest()


def normalize_connections_entry(out: dict) -> dict | None:
    """Case 1: nodes-dict, extra 'connections' key, missing 'groups'."""
    fixed = {k: v for k, v in out.items() if k != "connections"}
    if "groups" not in fixed:
        fixed["groups"] = {}
    unexpected = set(fixed.keys()) - EXPECTED
    if unexpected:
        return None  # something else unexpected — skip
    return fixed


def build_update_lists(nodes: dict, links: list) -> list:
    """
    Convert a flat links list  [{"from_node": NAME, "from_socket": IDX,
                                  "to_node": NAME, "to_socket": IDX}, …]
    into the update_lists chain format used by the canonical schema.

    update_lists is a list of dependency chains: each chain is a list of
    node-name strings ordered so that each node depends on the previous one.
    We build a DAG from links and emit chains via topological walk.
    """
    # Build adjacency: node -> set of nodes it feeds into
    predecessors: dict[str, set] = defaultdict(set)
    successors:   dict[str, set] = defaultdict(set)
    all_nodes = set(nodes.keys())

    for link in links:
        fn = link.get("from_node") or link.get("from_node_name", "")
        tn = link.get("to_node")   or link.get("to_node_name", "")
        if fn in all_nodes and tn in all_nodes:
            successors[fn].add(tn)
            predecessors[tn].add(fn)

    # Topological sort (Kahn's algorithm)
    in_degree = {n: len(predecessors[n]) for n in all_nodes}
    queue = sorted(n for n, d in in_degree.items() if d == 0)
    topo_order = []
    while queue:
        node = queue.pop(0)
        topo_order.append(node)
        for succ in sorted(successors[node]):
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)

    # Group into chains: one chain per connected component in the DAG
    visited = set()
    chains = []
    for start in topo_order:
        if start in visited:
            continue
        chain = []
        stack = [start]
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            chain.append(n)
            stack.extend(sorted(successors[n]))
        if len(chain) > 1:  # only include chains with actual connections
            chains.append(chain)

    return chains


def normalize_links_entry(out: dict) -> dict | None:
    """Case 2: nodes-dict with per-node socket data + 'links' list."""
    nodes = out.get("nodes")
    links = out.get("links")
    if not isinstance(nodes, dict) or not isinstance(links, list):
        return None

    # Strip per-node inputs/outputs from each node (keep only structural fields)
    clean_nodes = {}
    for name, node in nodes.items():
        if not isinstance(node, dict) or "bl_idname" not in node:
            return None  # unexpected node structure — bail
        clean_node = {k: v for k, v in node.items() if k not in ("inputs", "outputs")}
        clean_nodes[name] = clean_node

    update_lists = build_update_lists(clean_nodes, links)

    fixed = {
        "export_version": out.get("export_version", 1),
        "framed_nodes":   out.get("framed_nodes", {}),
        "groups":         out.get("groups", {}),
        "nodes":          clean_nodes,
        "update_lists":   update_lists,
    }
    return fixed


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print(f"Reading {MAIN_PATH.name} …")
    with open(MAIN_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Load existing clean dataset so we can skip already-present outputs
    existing_hashes: set[str] = set()
    clean: list[dict] = []
    if CLEAN_PATH.exists():
        with open(CLEAN_PATH, "r", encoding="utf-8") as f:
            clean = json.load(f)
        existing_hashes = {output_hash(x["output"]) for x in clean}
        print(f"Existing clean examples: {len(clean):,}")

    # Deduplicate source data
    seen_src: set[str] = set()
    unique_src = []
    for item in data:
        h = output_hash(item["output"])
        if h not in seen_src:
            seen_src.add(h)
            unique_src.append(item)

    added = 0
    skipped_dup = 0
    skipped_format = 0

    for item in unique_src:
        try:
            out = json.loads(item["output"])
        except Exception:
            continue

        if not isinstance(out, dict):
            skipped_format += 1
            continue

        keys = set(out.keys())
        extra = keys - EXPECTED
        fixed_out = None

        # Already correct format
        if not extra and isinstance(out.get("nodes"), dict) and "nodes" in keys:
            if output_hash(item["output"]) not in existing_hashes:
                clean.append(item)
                existing_hashes.add(output_hash(item["output"]))
                added += 1
            else:
                skipped_dup += 1
            continue

        # Case 1: extra 'connections' key only
        if extra == {"connections"} and isinstance(out.get("nodes"), dict):
            fixed_out = normalize_connections_entry(out)

        # Case 2: nodes-dict + links + inputs/outputs
        elif isinstance(out.get("nodes"), dict) and "links" in extra:
            fixed_out = normalize_links_entry(out)

        if fixed_out is None:
            skipped_format += 1
            continue

        new_output_str = json.dumps(fixed_out, ensure_ascii=False, separators=(",", ":"))
        h = output_hash(new_output_str)
        if h in existing_hashes:
            skipped_dup += 1
            continue

        existing_hashes.add(h)
        clean.append({
            "instruction": item["instruction"],
            "input":       item["input"],
            "output":      new_output_str,
        })
        added += 1

    print(f"  Added (new or normalized): {added:,}")
    print(f"  Skipped (already present): {skipped_dup:,}")
    print(f"  Skipped (unhandled format): {skipped_format:,}")
    print(f"  Total clean examples now: {len(clean):,}")

    with open(CLEAN_PATH, "w", encoding="utf-8") as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)
    out_mb = CLEAN_PATH.stat().st_size / (1024 ** 2)
    print(f"Written to {CLEAN_PATH.name}  ({out_mb:.2f} MB)")

    print()
    print("=" * 50)
    print("ENTRY COUNT SUMMARY")
    print("=" * 50)
    before = len(clean) - added
    print(f"  Before this run : {before:>10,}")
    print(f"  After this run  : {len(clean):>10,}")
    print(f"  Difference      : {added:>+10,}")
    print("=" * 50)


if __name__ == "__main__":
    main()
