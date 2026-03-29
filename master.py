"""
master.py — Full dataset pipeline: joins → merge → convert+filter → dedupe → augment → analysis.

Steps run conditionally:
  - JOINS/MERGE   : skipped if no joins/ folder or no merge.jsonl
  - DEDUPE        : skipped if main_dataset_alpaca.json has zero duplicates
  - AUGMENT       : skipped if deduped file (or fallback alpaca file) doesn't exist
  - ANALYSIS      : skipped if dataset_alpaca_father.json doesn't exist
"""

from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import random
import shutil
import sys
import tempfile
import zipfile
from collections import Counter, defaultdict
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any

# ── stdout encoding (Windows) ─────────────────────────────────────────────────
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── File paths ────────────────────────────────────────────────────────────────
_script_dir  = os.path.dirname(os.path.abspath(__file__))
JOINS_DIR    = os.path.join(_script_dir, "joins")
merge_path   = os.path.join(_script_dir, "merge.jsonl")
main_path    = os.path.join(_script_dir, "blender_nodes_dataset_main.jsonl")
alpaca_path  = Path(_script_dir) / "main_dataset_alpaca.json"
deduped_path = Path(_script_dir) / "main_dataset_alpaca_deduped.json"
augmented_path = Path(_script_dir) / "dataset_alpaca_father.json"
manifest_path  = Path(_script_dir) / "dataset_alpaca_father.manifest.json"

GOLD_PATH = Path(_script_dir) / "gold_dataset.json"

# ── Blender scoring constants (module-level for Windows multiprocessing) ──────
_BLENDER_NODE_TREES = frozenset({
    "ShaderNodeTree", "GeometryNodeTree", "CompositorNodeTree",
    "TextureNodeTree", "SverchCustomTreeType", "SverchGroupTreeType",
})

_BLENDER_KEYWORDS = (
    "bl_idname", "ShaderNodeTree", "GeometryNodeTree", "CompositorNodeTree",
    "SverchCustomTreeType", "NodeGroupInput", "NodeGroupOutput",
)


def _process_chunk(lines):
    """
    Worker: parse JSONL lines, convert to Alpaca format, and score in one pass.
    Returns (kept_entries, converted, skipped, tier_counts)
    Must be module-level for Windows multiprocessing (spawn).
    """
    kept = []
    converted = skipped = t1 = t2 = t3 = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            skipped += 1
            continue

        content = entry.get("content")
        if content is None:
            skipped += 1
            continue

        converted += 1
        output_str = json.dumps(content, ensure_ascii=False, separators=(',', ':'))

        score = 0
        if isinstance(content, dict):
            nodes   = content.get("nodes")
            links   = content.get("links")
            top_bl  = content.get("bl_idname", "")
            if (top_bl in _BLENDER_NODE_TREES and
                    isinstance(nodes, list) and nodes and isinstance(links, list)):
                score = 3
            elif isinstance(nodes, dict) and nodes:
                sample = next(iter(nodes.values()))
                if isinstance(sample, dict) and "bl_idname" in sample:
                    score = 2
            elif any(kw in output_str for kw in _BLENDER_KEYWORDS):
                if isinstance(nodes, (list, dict)) and nodes:
                    score = 1

        if score == 3:
            t3 += 1
        elif score == 2:
            t2 += 1
        elif score == 1:
            t1 += 1

        if score > 0:
            kept.append({
                "instruction": "Generate a Blender shader node graph.",
                "input":       "",
                "output":      output_str,
            })

    return kept, converted, skipped, {1: t1, 2: t2, 3: t3}


# ─────────────────────────────────────────────────────────────────────────────
# Console colour helpers
# ─────────────────────────────────────────────────────────────────────────────
R  = "\033[0m"
B  = "\033[1m"
GR = "\033[92m"
YL = "\033[93m"
RD = "\033[91m"
CY = "\033[96m"
BL = "\033[94m"

def _hdr(t):  print(f"\n{B}{CY}{'═'*68}{R}\n{B}{CY}{'═'*((68-len(t)-2)//2)} {t} {'═'*(68-(68-len(t)-2)//2-len(t)-2)}{R}\n{B}{CY}{'═'*68}{R}")
def _sec(t):  print(f"\n{B}{BL}── {t} {'─'*(58-len(t))}{R}")
def _ok(m):   print(f"  {GR}✔  {m}{R}")
def _warn(m): print(f"  {YL}⚠  {m}{R}")
def _err(m):  print(f"  {RD}✘  {m}{R}")
def _info(m): print(f"     {m}")


# =============================================================================
# STEP 0 + 1: JOINS & MERGE  (from logic.py)
# =============================================================================

def _extract_data_files(zip_path, temp_dir, depth=0):
    """Recursively extract .json/.jsonl files from a zip (and any nested zips)."""
    found  = []
    indent = "  " * (depth + 1)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for member in zf.namelist():
                basename = os.path.basename(member)
                if not basename:
                    continue
                ext = os.path.splitext(basename)[1].lower()
                if ext in (".json", ".jsonl"):
                    dest = os.path.join(temp_dir, basename)
                    if os.path.exists(dest):
                        stem, sfx = os.path.splitext(basename)
                        dest = os.path.join(temp_dir, f"{stem}__{len(found)}{sfx}")
                    with zf.open(member) as src, open(dest, "wb") as out_f:
                        out_f.write(src.read())
                    print(f"{indent}Extracted: {basename}")
                    found.append(dest)
                elif ext == ".zip":
                    nested_dest = os.path.join(temp_dir, f"__nested_{len(found)}_{basename}")
                    with zf.open(member) as src, open(nested_dest, "wb") as out_f:
                        out_f.write(src.read())
                    print(f"{indent}Descending into nested zip: {basename}")
                    found.extend(_extract_data_files(nested_dest, temp_dir, depth + 1))
                    os.remove(nested_dest)
    except zipfile.BadZipFile:
        print(f"{indent}Warning: {os.path.basename(zip_path)} is not a valid zip, skipping")
    return found


def _write_join_sources(all_jsonl, all_json, out_path):
    """Write all jsonl/json sources into out_path, return total count."""
    jsonl_count = json_count = 0
    write_mode = "w"

    if all_jsonl:
        with open(out_path, write_mode, encoding="utf-8") as out_f:
            for path in all_jsonl:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            out_f.write(line + "\n")
                            jsonl_count += 1
        write_mode = "a"

    if all_json:
        with open(out_path, write_mode, encoding="utf-8") as out_f:
            for path in all_json:
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        for entry in data:
                            out_f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                            json_count += 1
                    elif isinstance(data, dict):
                        out_f.write(json.dumps(data, ensure_ascii=False) + "\n")
                        json_count += 1
                    else:
                        print(f"  Warning: {os.path.basename(path)} has unexpected root type, skipping")
                except json.JSONDecodeError as e:
                    print(f"  Warning: {os.path.basename(path)} invalid JSON ({e}), skipping")

    return jsonl_count + json_count


def run_joins():
    """STEP 0 — collect files from joins/ into merge.jsonl."""
    print("=" * 50)
    print("STEP 0: JOINS")
    print("=" * 50)

    if not os.path.isdir(JOINS_DIR):
        print("No 'joins' folder found, skipping.")
        print()
        return

    all_jsonl, all_json, zip_paths = [], [], []
    for root, dirs, files in os.walk(JOINS_DIR):
        for filename in files:
            path = os.path.join(root, filename)
            ext  = os.path.splitext(filename)[1].lower()
            if ext == ".zip":
                zip_paths.append(path)
            elif ext == ".jsonl":
                all_jsonl.append(path)
            elif ext == ".json":
                all_json.append(path)

    if zip_paths:
        print(f"Found {len(zip_paths)} zip file(s) across joins/ and subfolders")
        with tempfile.TemporaryDirectory() as temp_dir:
            for zip_path in zip_paths:
                rel = os.path.relpath(zip_path, JOINS_DIR)
                print(f"  Processing: {rel}")
                extracted = _extract_data_files(zip_path, temp_dir)
                for p in extracted:
                    (all_jsonl if p.lower().endswith(".jsonl") else all_json).append(p)

            if not all_jsonl and not all_json:
                print("No .json/.jsonl files found anywhere in 'joins', skipping.")
            else:
                print(f"Processing {len(all_jsonl)} .jsonl and {len(all_json)} .json source(s)...")
                total = _write_join_sources(all_jsonl, all_json, merge_path)
                print(f"Joins: {total} entries written to merge.jsonl")
    elif all_jsonl or all_json:
        print(f"Processing {len(all_jsonl)} .jsonl and {len(all_json)} .json source(s)...")
        total = _write_join_sources(all_jsonl, all_json, merge_path)
        print(f"Joins: {total} entries written to merge.jsonl")
    else:
        print("No .json/.jsonl files found in 'joins' folder, skipping.")

    # Cleanup joins/
    deleted = 0
    for root, dirs, files in os.walk(JOINS_DIR, topdown=False):
        for filename in files:
            path = os.path.join(root, filename)
            try:
                os.remove(path)
                print(f"  Deleted: {os.path.relpath(path, JOINS_DIR)}")
                deleted += 1
            except OSError as e:
                print(f"  Warning: could not delete {filename} — {e}")
        if root != JOINS_DIR:
            try:
                os.rmdir(root)
            except OSError:
                pass
    if deleted:
        print(f"Cleanup: {deleted} file(s) deleted from joins/")
    print()


def run_merge():
    """STEP 1 — append merge.jsonl onto main JSONL file."""
    print("=" * 50)
    print("STEP 1: MERGE")
    print("=" * 50)

    if not os.path.exists(merge_path):
        print(f"No merge.jsonl found, skipping merge (proceeding with existing {os.path.basename(main_path)})")
        print()
        return

    if not os.path.exists(main_path):
        print(f"Error: {os.path.basename(main_path)} not found in {_script_dir}")
        sys.exit(1)

    with open(merge_path, "r", encoding="utf-8") as f:
        lines = [line for line in f if line.strip()]

    with open(main_path, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(line if line.endswith("\n") else line + "\n")

    os.remove(merge_path)
    print(f"Merge successful: {len(lines)} entries merged onto {os.path.basename(main_path)}")
    print()


def run_convert_filter():
    """STEP 2+3 — parallel convert + filter, write main_dataset_alpaca.json."""
    print("=" * 50)
    print("STEP 2+3: CONVERT + FILTER")
    print("=" * 50)

    if not os.path.exists(main_path):
        print(f"Error: {os.path.basename(main_path)} not found in {_script_dir}")
        sys.exit(1)

    _before_count = 0
    if alpaca_path.exists():
        try:
            with open(alpaca_path, "r", encoding="utf-8") as f:
                _before_count = len(json.load(f))
        except Exception:
            _before_count = 0

    N_WORKERS = cpu_count()
    READ_BUF  = 8 * 1024 * 1024

    print(f"Reading {os.path.basename(main_path)} into memory...")
    with open(main_path, "r", encoding="utf-8", buffering=READ_BUF) as f:
        all_lines = f.readlines()

    total_lines = len(all_lines)
    print(f"Loaded {total_lines} lines. Dispatching to {N_WORKERS} worker processes...")

    n_chunks   = N_WORKERS * 4
    chunk_size = max(1, (total_lines + n_chunks - 1) // n_chunks)
    chunks     = [all_lines[i:i + chunk_size] for i in range(0, total_lines, chunk_size)]

    with Pool(N_WORKERS) as pool:
        results = pool.map(_process_chunk, chunks)

    kept        = []
    converted   = skipped = 0
    tier_counts = {1: 0, 2: 0, 3: 0}
    for chunk_kept, chunk_conv, chunk_skip, chunk_tiers in results:
        kept.extend(chunk_kept)
        converted += chunk_conv
        skipped   += chunk_skip
        for t in (1, 2, 3):
            tier_counts[t] += chunk_tiers[t]

    if skipped:
        print(f"Skipped {skipped} entries (missing 'content' or invalid JSON)")
    print(f"Converted: {converted} | Kept after filter: {len(kept)}")

    gold = []
    if GOLD_PATH.exists():
        with open(GOLD_PATH, "r", encoding="utf-8") as f:
            gold = json.load(f)

    final    = kept + gold
    WRITE_BUF = 8 * 1024 * 1024
    BATCH     = 2000
    last_idx  = len(final) - 1

    print(f"Writing {len(final)} entries to {alpaca_path.name}...")
    with open(alpaca_path, "w", encoding="utf-8", buffering=WRITE_BUF) as f:
        f.write("[\n")
        buf = []
        for i, entry in enumerate(final):
            line = "  " + json.dumps(entry, ensure_ascii=False) + (",\n" if i < last_idx else "\n")
            buf.append(line)
            if len(buf) >= BATCH:
                f.writelines(buf)
                buf.clear()
        if buf:
            f.writelines(buf)
        f.write("]\n")

    print()
    print("=" * 50)
    print("FILTER RESULTS")
    print("=" * 50)
    print(f"Total entries before : {converted + skipped}")
    print(f"Entries removed      : {converted - len(kept)}")
    print(f"Entries kept         : {len(kept)}")
    print(f"Gold entries added   : {len(gold)}")
    print(f"Final total          : {len(final)}")
    print()
    print("QUALITY BREAKDOWN (filtered entries)")
    print("-" * 50)
    print(f"  Tier 3 - Gold      : {tier_counts[3]:>5}  (ShaderNodeTree/GeometryNodeTree with nodes+links)")
    print(f"  Tier 2 - Good      : {tier_counts[2]:>5}  (Sverchok-style node graphs)")
    print(f"  Tier 1 - Acceptable: {tier_counts[1]:>4}  (Blender nodes, non-standard format)")
    print(f"  gold_dataset.json  : {len(gold):>5}  (permanent fixture, always included)")
    print()
    if len(final) > 0:
        total_tier3 = tier_counts[3] + len(gold)
        print(f"  Gold share (incl. fixture) : {total_tier3 / len(final) * 100:.1f}%")
        print(f"  Good share                 : {tier_counts[2] / len(final) * 100:.1f}%")
        print(f"  Acceptable share           : {tier_counts[1] / len(final) * 100:.1f}%")
    print()
    print(f"Saved to {alpaca_path.name}")

    _after_count = len(final)
    _diff        = _after_count - _before_count
    print()
    print("=" * 50)
    print("ENTRY COUNT SUMMARY")
    print("=" * 50)
    print(f"  Before this run : {_before_count:>10,}")
    print(f"  After this run  : {_after_count:>10,}")
    print(f"  Difference      : {_diff:>+10,}")
    print("=" * 50)
    print()


# =============================================================================
# STEP 4: DEDUPE  (from dedupe.py)
# Skipped automatically if zero duplicates are detected.
# =============================================================================

def _hash_item(item: dict) -> str:
    key = json.dumps({k: item[k] for k in sorted(item)}, ensure_ascii=False)
    return hashlib.md5(key.encode()).hexdigest()


def run_dedupe() -> bool:
    """
    Deduplicate main_dataset_alpaca.json → main_dataset_alpaca_deduped.json.
    Returns True if the deduped file was written, False if skipped (no dupes).
    """
    print("=" * 50)
    print("STEP 4: DEDUPE")
    print("=" * 50)

    if not alpaca_path.exists():
        print(f"  {alpaca_path.name} not found, skipping dedupe.")
        print()
        return False

    print(f"Reading {alpaca_path.name} ...")
    with open(alpaca_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = len(data)
    seen: set[str] = set()
    unique: list[dict] = []

    for item in data:
        h = _hash_item(item)
        if h not in seen:
            seen.add(h)
            unique.append(item)

    removed = total - len(unique)

    if removed == 0:
        print(f"  No duplicates found in {total:,} examples — skipping dedupe step.")
        print()
        return False

    before_count = 0
    if deduped_path.exists():
        try:
            with open(deduped_path, "r", encoding="utf-8") as f:
                before_count = len(json.load(f))
        except Exception:
            before_count = 0

    print(f"  Total examples    : {total:,}")
    print(f"  Unique examples   : {len(unique):,}")
    print(f"  Duplicates removed: {removed:,}  ({removed/total*100:.1f}%)")
    print(f"\nWriting {deduped_path.name} ...")

    with open(deduped_path, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    out_mb = deduped_path.stat().st_size / (1024 ** 2)
    print(f"  Saved {len(unique):,} examples  ({out_mb:.2f} MB)")

    after_count = len(unique)
    diff        = after_count - before_count
    print()
    print("=" * 50)
    print("ENTRY COUNT SUMMARY")
    print("=" * 50)
    print(f"  Before this run : {before_count:>10,}")
    print(f"  After this run  : {after_count:>10,}")
    print(f"  Difference      : {diff:>+10,}")
    print("=" * 50)
    print()
    return True


# =============================================================================
# STEP 5: AUGMENT  (from augment.py)
# Skipped if the input file (deduped or alpaca fallback) doesn't exist.
# =============================================================================

# ── Node-type knowledge ───────────────────────────────────────────────────────
BL_CATEGORY: list[tuple[str, str]] = [
    ("Math",        "mathematical operations"),
    ("Vector",      "vector mathematics"),
    ("Matrix",      "matrix transformations"),
    ("Circle",      "circle geometry"),
    ("Cylinder",    "cylinder geometry"),
    ("Sphere",      "sphere geometry"),
    ("Torus",       "torus geometry"),
    ("Line",        "line/edge generation"),
    ("Plane",       "plane geometry"),
    ("Curve",       "curve operations"),
    ("Mesh",        "mesh processing"),
    ("Bmesh",       "BMesh operations"),
    ("Viewer",      "data visualisation"),
    ("Stethoscope", "data inspection"),
    ("Range",       "range generation"),
    ("Random",      "random value generation"),
    ("Noise",       "noise generation"),
    ("Color",       "colour adjustment"),
    ("Frame",       "node organisation"),
    ("List",        "list manipulation"),
    ("Logic",       "logic operations"),
    ("Sort",        "data sorting"),
    ("Join",        "data joining"),
    ("Split",       "data splitting"),
    ("Map",         "value mapping"),
    ("Float",       "float values"),
    ("Integer",     "integer values"),
    ("Boolean",     "boolean logic"),
    ("String",      "string handling"),
    ("Monad",       "node group/monad"),
    ("Group",       "node group"),
    ("Script",      "scripted nodes"),
    ("VTK",         "VTK pipeline"),
    ("vtk",         "VTK pipeline"),
    ("Shader",      "shader nodes"),
    ("Compositor",  "compositor nodes"),
    ("Geometry",    "geometry nodes"),
]

COMPLEXITY_LABEL = {
    (1,  5):   "minimal",
    (6,  10):  "simple",
    (11, 20):  "moderate",
    (21, 35):  "complex",
    (36, 999): "advanced",
}

DOMAIN_MAP = {
    "Generate a Blender shader node graph.":     ("shader",     "shader network"),
    "Generate a Blender node node graph.":        ("node",       "node graph"),
    "Generate a Blender compositor node graph.":  ("compositor", "compositor network"),
    "Generate a Blender geometry node graph.":    ("geometry",   "geometry node graph"),
}

TEMPLATES_A = [
    "Create a {complexity} Blender {domain} node graph for {purpose}.",
    "Build a {domain} node network that performs {purpose}.",
    "Generate a {complexity} {network} involving {feature}.",
    "Construct a Blender {domain} setup that handles {purpose}.",
    "Design a {complexity} node graph using {feature}.",
    "Set up a {domain} node graph that demonstrates {purpose}.",
    "Produce a {network} with {feature} for {purpose}.",
    "Assemble a {complexity} {domain} node configuration for {purpose}.",
]

TEMPLATES_B = [
    "How do I build a {complexity} {domain} node graph for {purpose}?",
    "What node setup achieves {purpose} in Blender?",
    "Can you show me a {network} that uses {feature}?",
    "What is the best way to set up {purpose} with {domain} nodes?",
    "How would you create a {complexity} {network} for {purpose}?",
    "What Blender nodes are needed for {purpose} involving {feature}?",
]

TEMPLATES_C = [
    "I need a {domain} node graph that does {purpose}.",
    "Help me build a {complexity} {network} with {feature}.",
    "I am trying to create {purpose} using Blender nodes.",
    "Show me a {complexity} example of a {domain} node graph for {purpose}.",
    "I want to set up nodes for {purpose} — can you help?",
    "Give me a {domain} node graph that demonstrates {feature}.",
]

TEMPLATES_D = [
    "Create a {purpose} {domain} node graph optimised for {constraint}.",
    "Build a {complexity} {network} suited to {constraint}.",
    "Generate a {constraint} version of a {domain} node graph for {purpose}.",
    "Design a {complexity} {domain} setup for {purpose}, targeting {constraint}.",
    "Produce a {network} with {feature} that works well for {constraint}.",
]

ALL_TEMPLATES = TEMPLATES_A + TEMPLATES_B + TEMPLATES_C + TEMPLATES_D

CONTEXT_SUFFIXES: dict[str, list[str]] = {
    "render_engine": ["optimised for Cycles rendering",
                      "compatible with Eevee real-time preview",
                      "usable in both Cycles and Eevee"],
    "performance":   ["with minimal node count for performance",
                      "prioritising render speed",
                      "computationally efficient for real-time use"],
    "skill_level":   ["suitable for beginners",
                      "designed for intermediate users",
                      "with advanced parameter control"],
    "quality":       ["achieving photorealistic quality",
                      "for a stylised non-photorealistic look",
                      "with fine detail control"],
    "use_case":      ["for game asset creation",
                      "for architectural visualisation",
                      "for product rendering",
                      "for motion graphics and animation"],
}

PURPOSE_PHRASES = [
    "procedural shape generation",
    "vector field manipulation",
    "mathematical surface generation",
    "parametric mesh construction",
    "data-driven geometry",
    "noise-based deformation",
    "colour-mapped visualisation",
    "range-based value distribution",
    "list-driven topology",
    "matrix-based transformation",
    "curve interpolation",
    "modular node composition",
    "randomised variation",
    "iterative subdivision",
    "boolean geometry operations",
]

SEED = 42
random.seed(SEED)


def _classify_schema(output_str: str) -> str:
    try:
        out = json.loads(output_str)
    except json.JSONDecodeError:
        return "parse-error"
    if not isinstance(out, dict):
        return "non-dict-root"
    nodes = out.get("nodes")
    if nodes is None:
        return "no-nodes"
    if isinstance(nodes, list):
        return "list-nodes"
    if isinstance(nodes, dict):
        return "dict-nodes"
    return "no-nodes"


def _get_complexity_label(node_count: int) -> str:
    for (lo, hi), label in COMPLEXITY_LABEL.items():
        if lo <= node_count <= hi:
            return label
    return "advanced"


def _extract_features(example: dict) -> dict:
    instr  = example["instruction"]
    domain, network = DOMAIN_MAP.get(instr, ("node", "node graph"))
    schema = _classify_schema(example["output"])

    features: dict[str, Any] = {
        "domain":     domain,
        "network":    network,
        "complexity": "moderate",
        "feature":    "node-based processing",
        "categories": [],
        "node_count": 0,
        "schema":     schema,
    }

    if schema not in ("dict-nodes", "list-nodes"):
        return features

    try:
        out   = json.loads(example["output"])
        nodes = out.get("nodes", {})
        if isinstance(nodes, dict):
            node_list = [v for v in nodes.values() if isinstance(v, dict)]
        elif isinstance(nodes, list):
            node_list = [v for v in nodes if isinstance(v, dict)]
        else:
            node_list = []

        features["node_count"] = len(node_list)
        features["complexity"] = _get_complexity_label(len(node_list))

        cats: Counter = Counter()
        for nd in node_list:
            bl = nd.get("bl_idname", nd.get("name", ""))
            for substr, cat in BL_CATEGORY:
                if substr in bl:
                    cats[cat] += 1
                    break

        features["categories"] = [c for c, _ in cats.most_common(3)]
        features["feature"] = features["categories"][0] if features["categories"] else "procedural node operations"

    except Exception:
        pass

    return features


def _fill_template(template: str, feat: dict, purpose: str, constraint: str | None) -> str:
    result = (
        template
        .replace("{domain}",     feat["domain"])
        .replace("{network}",    feat["network"])
        .replace("{complexity}", feat["complexity"])
        .replace("{feature}",    feat["feature"])
        .replace("{purpose}",    purpose)
        .replace("{constraint}", constraint or "general use")
    )
    return result.strip()


def _generate_instruction_variants(example: dict, features: dict, count: int = 13) -> list[str]:
    variants: list[str] = [example["instruction"]]
    purpose = features["categories"][0] if features["categories"] else random.choice(PURPOSE_PHRASES)

    shuffled = ALL_TEMPLATES[:]
    random.shuffle(shuffled)

    used_templates: set[str] = set()
    context_budget = max(1, int((count - 1) * 0.35))

    for tmpl in shuffled:
        if len(variants) >= count:
            break
        if tmpl in used_templates:
            continue
        used_templates.add(tmpl)

        use_context = (context_budget > 0) and ("{constraint}" in tmpl or random.random() < 0.35)
        constraint: str | None = None
        if use_context:
            ctx_type   = random.choice(list(CONTEXT_SUFFIXES))
            constraint = random.choice(CONTEXT_SUFFIXES[ctx_type])
            context_budget -= 1

        candidate = _fill_template(tmpl, features, purpose, constraint)
        if "{" in candidate or len(candidate) < 15 or candidate in variants:
            continue
        variants.append(candidate)

    return variants[:count]


PARAM_JITTER = 0.18
INT_JITTER   = 0.12
POS_JITTER   = 50.0
COLOR_JITTER = 0.08


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _jitter_float(v: float, frac: float = PARAM_JITTER,
                  lo: float = -1e9, hi: float = 1e9) -> float:
    return _clamp(v + v * frac * random.uniform(-1, 1), lo, hi)


def _jitter_int(v: int, frac: float = INT_JITTER) -> int:
    if v == 0:
        return 0
    return int(round(v + abs(v) * frac * random.uniform(-1, 1)))


def _jitter_color(rgb: list) -> list:
    result = []
    for ch in rgb:
        if isinstance(ch, (int, float)):
            result.append(round(_clamp(ch + ch * COLOR_JITTER * random.uniform(-1, 1), 0.0, 1.0), 6))
        else:
            result.append(ch)
    return result


def _augment_param(val: Any) -> Any:
    if isinstance(val, bool):
        return val
    if isinstance(val, float):
        lo, hi = (-1e9, 1e9) if not (0.0 <= val <= 1.0) else (0.0, 1.0)
        return round(_jitter_float(val, PARAM_JITTER, lo, hi), 6)
    if isinstance(val, int):
        return _jitter_int(val, INT_JITTER)
    if isinstance(val, str):
        return val
    if isinstance(val, list) and val and isinstance(val[0], (int, float)):
        return [_augment_param(x) for x in val]
    if isinstance(val, dict):
        return val
    return val


def _augment_node_dict(node: dict) -> dict:
    n   = copy.deepcopy(node)
    loc = n.get("location")
    if isinstance(loc, list) and len(loc) == 2:
        n["location"] = [
            round(loc[0] + random.uniform(-POS_JITTER, POS_JITTER), 4),
            round(loc[1] + random.uniform(-POS_JITTER, POS_JITTER), 4),
        ]
    col = n.get("color")
    if isinstance(col, list) and len(col) == 3:
        n["color"] = _jitter_color(col)
    params = n.get("params", {})
    if isinstance(params, dict):
        n["params"] = {pk: _augment_param(pv) for pk, pv in params.items()}
    return n


def _augment_node_list_item(node: dict) -> dict:
    n   = copy.deepcopy(node)
    loc = n.get("location")
    if isinstance(loc, list) and len(loc) == 2:
        n["location"] = [
            round(loc[0] + random.uniform(-POS_JITTER, POS_JITTER), 4),
            round(loc[1] + random.uniform(-POS_JITTER, POS_JITTER), 4),
        ]
    for key in list(n.keys()):
        if key in ("location", "name", "bl_idname"):
            continue
        v = n[key]
        if isinstance(v, float):
            n[key] = round(_jitter_float(v, PARAM_JITTER), 6)
        elif isinstance(v, int) and not isinstance(v, bool):
            n[key] = _jitter_int(v, INT_JITTER)
    return n


def _augment_output(output_str: str, schema: str) -> str | None:
    try:
        out = json.loads(output_str)
    except json.JSONDecodeError:
        return None

    if schema == "dict-nodes":
        nodes = out.get("nodes", {})
        if not isinstance(nodes, dict):
            return None
        out["nodes"] = {
            name: (_augment_node_dict(node) if isinstance(node, dict) else node)
            for name, node in nodes.items()
        }
    elif schema == "list-nodes":
        nodes = out.get("nodes", [])
        if not isinstance(nodes, list):
            return None
        out["nodes"] = [
            (_augment_node_list_item(n) if isinstance(n, dict) else n) for n in nodes
        ]
    else:
        return output_str

    try:
        return json.dumps(out, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return None


def _validate_augmented(original_str: str, augmented_str: str, schema: str) -> tuple[bool, str]:
    try:
        aug = json.loads(augmented_str)
    except Exception:
        return False, "augmented output is not valid JSON"
    try:
        orig = json.loads(original_str)
    except Exception:
        return True, ""

    if schema == "dict-nodes":
        orig_nodes = orig.get("nodes", {})
        aug_nodes  = aug.get("nodes",  {})
        if not isinstance(orig_nodes, dict) or not isinstance(aug_nodes, dict):
            return True, ""
        if len(orig_nodes) != len(aug_nodes):
            return False, f"node count changed {len(orig_nodes)}→{len(aug_nodes)}"
        for k in orig_nodes:
            if k not in aug_nodes:
                return False, f"node '{k}' disappeared"
            ob = orig_nodes[k].get("bl_idname") if isinstance(orig_nodes[k], dict) else None
            ab = aug_nodes[k].get("bl_idname")  if isinstance(aug_nodes[k], dict)  else None
            if ob != ab:
                return False, f"bl_idname changed for '{k}': {ob}→{ab}"
    elif schema == "list-nodes":
        orig_nodes = orig.get("nodes", [])
        aug_nodes  = aug.get("nodes",  [])
        if len(orig_nodes) != len(aug_nodes):
            return False, f"node count changed {len(orig_nodes)}→{len(aug_nodes)}"

    return True, ""


def _source_hash(example: dict) -> str:
    key = json.dumps(
        {k: v for k, v in example.items() if not k.startswith("_")},
        sort_keys=True, ensure_ascii=False,
    )
    return hashlib.md5(key.encode()).hexdigest()


def run_augment(augment_input: Path) -> None:
    """STEP 5 — augment the deduped (or alpaca) dataset."""
    _hdr("STEP 5: AUGMENTATION PIPELINE")

    if not augment_input.exists():
        _warn(f"{augment_input.name} not found, skipping augmentation.")
        print()
        return

    existing_data: list[dict] = []
    if augmented_path.exists():
        try:
            with open(augmented_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
        except Exception:
            existing_data = []
    before_count = len(existing_data)

    processed_hashes: set[str] = set()
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                processed_hashes = set(json.load(f))
        except Exception:
            processed_hashes = set()

    # Load and deduplicate input
    _hdr("LOADING DATASET")
    with open(augment_input, "r", encoding="utf-8") as f:
        raw = json.load(f)

    seen_hashes: set[str] = set()
    dataset: list[dict] = []
    for item in raw:
        h = hashlib.md5(
            json.dumps({k: item[k] for k in sorted(item)}, ensure_ascii=False).encode()
        ).hexdigest()
        if h not in seen_hashes:
            seen_hashes.add(h)
            dataset.append(item)

    _ok(f"Loaded {len(raw):,} examples → {len(dataset):,} unique after dedup")

    schema_counts: Counter = Counter()
    for ex in dataset:
        s = _classify_schema(ex["output"])
        schema_counts[s] += 1
        ex["_schema"] = s

    _sec("Schema distribution")
    for schema, cnt in schema_counts.most_common():
        _info(f"{schema:<20} {cnt:>5}")

    # Augmentation
    _hdr("AUGMENTATION PIPELINE")
    new_examples = [ex for ex in dataset if _source_hash(ex) not in processed_hashes]
    skipped_existing = len(dataset) - len(new_examples)
    if skipped_existing:
        _ok(f"Skipping {skipped_existing:,} already-processed examples")
    _sec(f"Generating up to 13 variants × {len(new_examples):,} new examples")

    augmented_data: list[dict] = []
    stats: dict[str, Any] = {
        "generated":         0,
        "output_augmented":  0,
        "output_skipped":    0,
        "validation_failed": 0,
        "instructions_used": Counter(),
    }
    failed_output_aug: list[tuple[int, str]] = []

    for idx, example in enumerate(new_examples):
        schema   = example["_schema"]
        features = _extract_features(example)
        variants = _generate_instruction_variants(example, features, 13)

        for instr in variants:
            stats["instructions_used"][instr] += 1
            do_aug     = (random.random() < 0.80) and (schema in ("dict-nodes", "list-nodes"))
            new_output = example["output"]

            if do_aug:
                aug_str = _augment_output(example["output"], schema)
                if aug_str is None:
                    stats["output_skipped"] += 1
                else:
                    valid, reason = _validate_augmented(example["output"], aug_str, schema)
                    if valid:
                        new_output = aug_str
                        stats["output_augmented"] += 1
                    else:
                        stats["validation_failed"] += 1
                        failed_output_aug.append((idx, reason))
            else:
                stats["output_skipped"] += 1

            augmented_data.append({
                "instruction": instr,
                "input":       example.get("input", ""),
                "output":      new_output,
            })

        stats["generated"] += len(variants)
        processed_hashes.add(_source_hash(example))

        if (idx + 1) % 50 == 0:
            _info(f"  processed {idx+1}/{len(new_examples)} examples → {len(augmented_data):,} new so far")

    if failed_output_aug:
        _warn(f"Output augmentation validation failures: {len(failed_output_aug)}")
        for i, reason in failed_output_aug[:5]:
            _info(f"  idx={i}: {reason}")
        if len(failed_output_aug) > 5:
            _info(f"  … and {len(failed_output_aug)-5} more")

    # Save
    _hdr("SAVING")
    combined = existing_data + augmented_data
    with open(augmented_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    size_mb = augmented_path.stat().st_size / (1024 ** 2)
    _ok(f"Wrote {len(combined):,} examples to {augmented_path.name}  ({size_mb:.2f} MB)"
        + (f" (+{len(augmented_data):,} new)" if augmented_data else ""))
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(list(processed_hashes), f)

    # Statistics
    _hdr("STATISTICS")
    total_in  = len(dataset)
    total_out = len(augmented_data)
    _sec("Volume")
    _info(f"{'Input unique examples':<35} {total_in:>7,}")
    _info(f"{'Output examples':<35} {total_out:>7,}")
    if total_in:
        _info(f"{'Expansion factor':<35} {total_out/total_in:>7.1f}×")

    unique_instr = len(stats["instructions_used"])
    _sec("Instruction diversity")
    _info(f"{'Unique instructions generated':<35} {unique_instr:>7,}")
    _info(f"{'Original instruction count':<35} {'4':>7}")
    if unique_instr:
        _info(f"{'Diversity multiplier':<35} {unique_instr/4:>7.1f}×")

    if total_out:
        _sec("Top 10 most frequent instructions")
        for instr, cnt in stats["instructions_used"].most_common(10):
            bar = "█" * int(cnt / total_out * 40)
            _info(f"  {cnt:>5}  {bar}  {instr[:65]}")

        _sec("Output augmentation")
        _info(f"{'Outputs augmented':<35} {stats['output_augmented']:>7,}  ({stats['output_augmented']/total_out*100:.1f}%)")
        _info(f"{'Outputs kept original':<35} {stats['output_skipped']:>7,}")
        _info(f"{'Validation failures':<35} {stats['validation_failed']:>7,}")

        fail_total  = stats["validation_failed"]
        success_pct = (total_out - fail_total) / total_out * 100
        _sec("Success rate")
        if success_pct >= 99:
            _ok(f"Augmentation success rate: {success_pct:.2f}%")
        elif success_pct >= 95:
            _warn(f"Augmentation success rate: {success_pct:.2f}%")
        else:
            _err(f"Augmentation success rate: {success_pct:.2f}%")

    after_count = before_count + len(augmented_data)
    diff        = after_count - before_count
    print(f"\n{B}{CY}{'═'*68}{R}")
    print(f"{B}{CY}  ENTRY COUNT SUMMARY{R}")
    print(f"{B}{CY}{'═'*68}{R}")
    print(f"  Before this run : {before_count:>10,}")
    print(f"  After this run  : {after_count:>10,}")
    print(f"  Difference      : {diff:>+10,}")
    print(f"{B}{CY}{'═'*68}{R}\n")


# =============================================================================
# STEP 6: ANALYSIS  (from analysis.py)
# Skipped if dataset_alpaca_father.json doesn't exist.
# =============================================================================

RESET  = "\033[0m"
BOLD   = "\033[1m"
RED    = "\033[91m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
CYAN   = "\033[96m"
BLUE   = "\033[94m"

def _header(title: str):
    width = 70
    pad   = (width - len(title) - 2) // 2
    print(f"\n{BOLD}{CYAN}{'═' * width}{RESET}")
    print(f"{BOLD}{CYAN}{'═' * pad} {title} {'═' * (width - pad - len(title) - 2)}{RESET}")
    print(f"{BOLD}{CYAN}{'═' * width}{RESET}")

def _section(title: str):
    print(f"\n{BOLD}{BLUE}── {title} {'─' * (60 - len(title))}{RESET}")

def _ok2(msg):    print(f"  {GREEN}✔  {msg}{RESET}")
def _warn2(msg):  print(f"  {YELLOW}⚠  {msg}{RESET}")
def _error2(msg): print(f"  {RED}✘  {msg}{RESET}")
def _info2(msg):  print(f"     {msg}")

def _pct(n, total): return f"{n:,} ({n/total*100:.1f}%)" if total else f"{n}"

def _percentile(sorted_list, p):
    if not sorted_list:
        return 0
    idx = int(len(sorted_list) * p / 100)
    return sorted_list[min(idx, len(sorted_list) - 1)]


def run_analysis() -> None:
    """STEP 6 — comprehensive analysis of dataset_alpaca_father.json."""
    print("=" * 50)
    print("STEP 6: ANALYSIS")
    print("=" * 50)

    if not augmented_path.exists():
        print(f"  {augmented_path.name} not found, skipping analysis.")
        print()
        return

    _header("1. JSON STRUCTURE VALIDATION")

    file_size_mb = augmented_path.stat().st_size / (1024 ** 2)
    _ok2(f"File found: {augmented_path.name}  ({file_size_mb:.2f} MB)")

    try:
        with open(augmented_path, "r", encoding="utf-8") as f:
            raw = f.read()
        data = json.loads(raw)
        _ok2("Top-level JSON parses without error")
    except json.JSONDecodeError as e:
        _error2(f"JSON parse failure: {e}")
        return

    if isinstance(data, list):
        _ok2(f"Root type: list  ({len(data):,} items)")
    else:
        _warn2(f"Root type is {type(data).__name__}, expected list")
        data = list(data.values()) if isinstance(data, dict) else []

    TOTAL = len(data)

    REQUIRED_FIELDS = {"instruction", "input", "output"}
    field_issues = []
    for idx, item in enumerate(data):
        missing = REQUIRED_FIELDS - set(item.keys())
        extra   = set(item.keys()) - REQUIRED_FIELDS
        if missing:
            field_issues.append((idx, "missing", missing))
        if extra:
            field_issues.append((idx, "extra", extra))

    if not field_issues:
        _ok2(f"All {TOTAL:,} examples have exactly the required fields: {sorted(REQUIRED_FIELDS)}")
    else:
        _warn2(f"{len(field_issues)} field anomalies detected")
        for idx, kind, fields in field_issues[:10]:
            _warn2(f"  idx={idx} {kind} fields: {fields}")
        if len(field_issues) > 10:
            _info2(f"  … and {len(field_issues)-10} more")

    type_issues = [(i, f, type(v).__name__)
                   for i, item in enumerate(data)
                   for f, v in item.items()
                   if not isinstance(v, str)]
    if not type_issues:
        _ok2("All field values are strings")
    else:
        _warn2(f"{len(type_issues)} non-string field values")
        for i, f, t in type_issues[:5]:
            _warn2(f"  idx={i} field='{f}' type={t}")

    _header("2. EXAMPLE COUNT & DISTRIBUTION")

    _section("Instruction distribution")
    inst_counter = Counter(item["instruction"] for item in data)
    for inst, count in inst_counter.most_common():
        bar = "█" * int(count / TOTAL * 40)
        print(f"  {_pct(count, TOTAL):>18}  {bar}  {inst}")

    _section("Input field usage")
    has_input = sum(1 for item in data if item.get("input", "").strip())
    no_input  = TOTAL - has_input
    _ok2(f"Non-empty input:  {_pct(has_input, TOTAL)}")
    _info2(f"Empty input:      {_pct(no_input, TOTAL)}")
    if no_input == TOTAL:
        _warn2("All examples have empty 'input' — dataset is instruction-only style")

    _section("Output length distribution")
    out_lens = sorted(len(item["output"]) for item in data)
    avg_len  = sum(out_lens) / TOTAL
    print(f"  {'Min':>12}: {out_lens[0]:>10,} chars")
    print(f"  {'p5':>12}: {_percentile(out_lens, 5):>10,} chars")
    print(f"  {'p25':>12}: {_percentile(out_lens, 25):>10,} chars")
    print(f"  {'Median':>12}: {_percentile(out_lens, 50):>10,} chars")
    print(f"  {'Mean':>12}: {avg_len:>10,.1f} chars")
    print(f"  {'p75':>12}: {_percentile(out_lens, 75):>10,} chars")
    print(f"  {'p95':>12}: {_percentile(out_lens, 95):>10,} chars")
    print(f"  {'Max':>12}: {out_lens[-1]:>10,} chars")

    buckets = [0, 500, 1_000, 5_000, 10_000, 50_000, 100_000, float("inf")]
    labels  = ["<500", "500–1k", "1k–5k", "5k–10k", "10k–50k", "50k–100k", ">100k"]
    bcounts = [0] * len(labels)
    for ln in out_lens:
        for i in range(len(buckets) - 1):
            if buckets[i] <= ln < buckets[i + 1]:
                bcounts[i] += 1
                break
    _section("Output length histogram")
    for label, count in zip(labels, bcounts):
        bar = "█" * int(count / TOTAL * 40)
        print(f"  {label:>12}  {_pct(count, TOTAL):>18}  {bar}")

    _header("3. DUPLICATE DETECTION")

    _section("Full-example duplicates (all fields identical)")
    hashes      = [_hash_item(item) for item in data]
    hash_counter = Counter(hashes)
    full_dups   = sum(c - 1 for c in hash_counter.values() if c > 1)
    dup_groups  = {h: c for h, c in hash_counter.items() if c > 1}

    if full_dups == 0:
        _ok2("No fully duplicate examples found")
    else:
        _error2(f"{_pct(full_dups, TOTAL)} fully duplicate examples across {len(dup_groups)} groups")
        for h in sorted(dup_groups, key=lambda x: -dup_groups[x])[:3]:
            idx  = hashes.index(h)
            inst = data[idx]["instruction"][:60]
            _info2(f"  Hash {h[:8]}…  ×{dup_groups[h]}  instruction='{inst}'")

    _section("Output-only duplicates (same output, different instruction)")
    out_hash_to_insts: dict[str, set] = defaultdict(set)
    for item in data:
        oh = hashlib.md5(item["output"].encode()).hexdigest()
        out_hash_to_insts[oh].add(item["instruction"])
    cross_insts = {h: insts for h, insts in out_hash_to_insts.items() if len(insts) > 1}
    if cross_insts:
        _warn2(f"{len(cross_insts)} outputs appear under multiple different instructions")
    else:
        _ok2("No outputs shared across different instructions")

    _section("Duplicate outputs per instruction")
    for inst in inst_counter:
        subset    = [item for item in data if item["instruction"] == inst]
        uniq      = len({item["output"] for item in subset})
        total_sub = len(subset)
        dup_sub   = total_sub - uniq
        if dup_sub == 0:
            _ok2(f"'{inst[:55]}': {total_sub:,} examples, 0 duplicates")
        else:
            rate = dup_sub / total_sub * 100
            _error2(f"'{inst[:55]}': {total_sub:,} examples, {dup_sub:,} dups ({rate:.1f}%), {uniq:,} unique")

    _header("4. OUTPUT JSON VALIDITY & SCHEMA ANALYSIS")

    _section("Output JSON parseability")
    valid_json_count   = 0
    invalid_json_items = []
    for idx, item in enumerate(data):
        try:
            json.loads(item["output"])
            valid_json_count += 1
        except json.JSONDecodeError as e:
            invalid_json_items.append((idx, str(e)[:80]))

    if invalid_json_items:
        _error2(f"{_pct(len(invalid_json_items), TOTAL)} outputs are NOT valid JSON")
        for idx, msg in invalid_json_items[:5]:
            _warn2(f"  idx={idx}: {msg}")
    else:
        _ok2(f"All {TOTAL:,} outputs are valid JSON")

    _section("Output JSON top-level keys")
    key_presence: Counter = Counter()
    for item in data:
        try:
            parsed = json.loads(item["output"])
            for k in parsed:
                key_presence[k] += 1
        except Exception:
            pass

    EXPECTED_OUTPUT_KEYS = {"export_version", "framed_nodes", "groups", "nodes", "update_lists"}
    for key, count in sorted(key_presence.items(), key=lambda x: -x[1]):
        flag       = "✔" if key in EXPECTED_OUTPUT_KEYS else "?"
        missing_in = TOTAL - count
        row = f"  {GREEN if flag == '✔' else YELLOW}{flag}{RESET}  {key:<25} present in {_pct(count, TOTAL)}"
        if missing_in:
            row += f"  {YELLOW}(absent in {missing_in:,}){RESET}"
        print(row)

    unexpected_keys = set(key_presence) - EXPECTED_OUTPUT_KEYS
    if unexpected_keys:
        _warn2(f"Unexpected top-level keys: {unexpected_keys}")

    _section("Export version distribution")
    version_counter: Counter = Counter()
    missing_version = 0
    for item in data:
        try:
            parsed = json.loads(item["output"])
            v = parsed.get("export_version")
            if v:
                version_counter[v] += 1
            else:
                missing_version += 1
        except Exception:
            missing_version += 1

    for version, count in version_counter.most_common():
        bar = "█" * int(count / TOTAL * 30)
        print(f"  v{version:<18} {_pct(count, TOTAL):>18}  {bar}")
    if missing_version:
        _warn2(f"Missing export_version: {_pct(missing_version, TOTAL)}")

    _section("Node count distribution")
    node_counts_list = []
    no_nodes_items   = []
    for idx, item in enumerate(data):
        try:
            parsed = json.loads(item["output"])
            nodes  = parsed.get("nodes", {})
            n = len(nodes)
            node_counts_list.append(n)
            if n == 0:
                no_nodes_items.append(idx)
        except Exception:
            pass

    if node_counts_list:
        nc_sorted = sorted(node_counts_list)
        print(f"  {'Min nodes':>14}: {nc_sorted[0]:>6}")
        print(f"  {'Median nodes':>14}: {_percentile(nc_sorted, 50):>6}")
        print(f"  {'Mean nodes':>14}: {sum(nc_sorted)/len(nc_sorted):>6.1f}")
        print(f"  {'p95 nodes':>14}: {_percentile(nc_sorted, 95):>6}")
        print(f"  {'Max nodes':>14}: {nc_sorted[-1]:>6}")
        if no_nodes_items:
            _warn2(f"Examples with 0 nodes: {len(no_nodes_items)}")
            for i in no_nodes_items[:5]:
                _info2(f"  idx={i}")

    _section("Top 20 node types (bl_idname)")
    node_type_counter: Counter = Counter()
    for item in data:
        try:
            parsed = json.loads(item["output"])
            for nd in parsed.get("nodes", {}).values():
                bl = nd.get("bl_idname", "unknown")
                node_type_counter[bl] += 1
        except Exception:
            pass

    total_nodes = sum(node_type_counter.values())
    print(f"  Total nodes across dataset : {total_nodes:,}")
    print(f"  Unique node types          : {len(node_type_counter):,}")
    print()
    for bl_id, count in node_type_counter.most_common(20):
        bar = "█" * int(count / total_nodes * 30) if total_nodes else ""
        print(f"  {bl_id:<35} {count:>7,}  {bar}")

    _section("Connection / link analysis (update_lists)")
    link_counts_list = []
    no_links_items   = []
    for idx, item in enumerate(data):
        try:
            parsed      = json.loads(item["output"])
            ul          = parsed.get("update_lists", [])
            total_links = sum(len(chain) for chain in ul) if ul else 0
            link_counts_list.append(total_links)
            if total_links == 0:
                no_links_items.append(idx)
        except Exception:
            pass

    if link_counts_list:
        lc_sorted = sorted(link_counts_list)
        print(f"  {'Min links':>14}: {lc_sorted[0]:>6}")
        print(f"  {'Median links':>14}: {_percentile(lc_sorted, 50):>6}")
        print(f"  {'Mean links':>14}: {sum(lc_sorted)/len(lc_sorted):>6.1f}")
        print(f"  {'Max links':>14}: {lc_sorted[-1]:>6}")
        if no_links_items:
            _warn2(f"Examples with 0 connections: {_pct(len(no_links_items), TOTAL)}")

    _header("5. INSTRUCTION & OUTPUT QUALITY")

    _section("Instruction quality")
    empty_instructions = [i for i, item in enumerate(data) if not item["instruction"].strip()]
    if empty_instructions:
        _error2(f"Empty instructions: {len(empty_instructions)}")
    else:
        _ok2("No empty instructions")

    too_short_inst = [i for i, item in enumerate(data) if 0 < len(item["instruction"].strip()) < 10]
    if too_short_inst:
        _warn2(f"Very short instructions (<10 chars): {len(too_short_inst)}")

    generic_instructions = sum(1 for item in data
                                if item["instruction"].lower().strip()
                                in {"generate a blender shader node graph.",
                                    "generate a blender node node graph.",
                                    "generate a blender compositor node graph.",
                                    "generate a blender geometry node graph."})
    _info2(f"Generic/template instructions  : {_pct(generic_instructions, TOTAL)}")
    _info2(f"  All instructions are short generic prompts — no natural language variety")
    _info2(f"  The dataset is essentially 'given prompt → generate graph JSON'")

    _section("Output quality signals")
    short_thresh = 300
    very_short = [(i, len(item["output"])) for i, item in enumerate(data)
                  if len(item["output"]) < short_thresh]
    if very_short:
        _warn2(f"Very short outputs (<{short_thresh} chars): {_pct(len(very_short), TOTAL)}")
        for i, ln in very_short[:3]:
            _info2(f"  idx={i} len={ln}  preview: {data[i]['output'][:80]}")
    else:
        _ok2(f"No outputs shorter than {short_thresh} chars")

    huge_thresh = 500_000
    huge = [(i, len(item["output"])) for i, item in enumerate(data)
            if len(item["output"]) > huge_thresh]
    if huge:
        _warn2(f"Very large outputs (>{huge_thresh//1000}k chars): {len(huge)}")
        for i, ln in huge[:3]:
            _info2(f"  idx={i} len={ln:,}")
    else:
        _ok2(f"No outputs exceeding {huge_thresh//1000}k chars")

    required_inner = {"nodes"}
    inner_missing  = []
    for idx, item in enumerate(data):
        try:
            parsed = json.loads(item["output"])
            miss   = required_inner - set(parsed.keys())
            if miss:
                inner_missing.append((idx, miss))
        except Exception:
            pass
    if inner_missing:
        _warn2(f"Outputs missing inner required keys: {len(inner_missing)}")
        for i, m in inner_missing[:5]:
            _info2(f"  idx={i} missing: {m}")
    else:
        _ok2("All parsed outputs contain a 'nodes' key")

    bl_missing_items = []
    for idx, item in enumerate(data):
        try:
            parsed = json.loads(item["output"])
            for node_name, node_data in parsed.get("nodes", {}).items():
                if "bl_idname" not in node_data:
                    bl_missing_items.append((idx, node_name))
        except Exception:
            pass
    if bl_missing_items:
        _warn2(f"Nodes missing 'bl_idname': {len(bl_missing_items)} across "
               f"{len({i for i,_ in bl_missing_items})} examples")
        for i, n in bl_missing_items[:5]:
            _info2(f"  idx={i} node='{n}'")
    else:
        _ok2("All nodes have 'bl_idname'")

    _header("6. POTENTIAL ISSUES & RECOMMENDATIONS")

    issues = []
    if full_dups > 0:
        issues.append(("CRITICAL", f"Massive duplication: {full_dups:,}/{TOTAL:,} examples ({full_dups/TOTAL*100:.1f}%) are exact duplicates. "
                                   f"Deduplication will reduce the dataset to ~{TOTAL-full_dups:,} unique examples."))
    if len(inst_counter) <= 4 and TOTAL > 1000:
        issues.append(("HIGH", f"Only {len(inst_counter)} distinct instructions for {TOTAL:,} examples. "
                                "Instructions provide no signal — the model must learn purely from output variation. "
                                "Consider enriching instructions with descriptive prompts."))
    if no_input == TOTAL:
        issues.append(("MEDIUM", "'input' field is empty for every example. "
                                  "This is valid for instruction-only datasets, but eliminates a useful conditioning channel."))
    if missing_version > 0:
        issues.append(("MEDIUM", f"{missing_version:,} examples ({missing_version/TOTAL*100:.1f}%) "
                                  "are missing the 'export_version' field in their output JSON."))
    if no_links_items:
        issues.append(("MEDIUM", f"{len(no_links_items):,} examples ({len(no_links_items)/TOTAL*100:.1f}%) "
                                  "have 0 connections (update_lists is empty). "
                                  "These may represent trivially simple or broken graphs."))
    if very_short:
        issues.append(("LOW", f"{len(very_short):,} outputs are shorter than {short_thresh} chars, "
                               "suggesting minimal/single-node graphs with limited training value."))
    if huge:
        issues.append(("LOW", f"{len(huge):,} outputs exceed {huge_thresh//1000}k chars. "
                               "Verify these are legitimate and not corrupted/merged entries."))
    if no_nodes_items:
        issues.append(("LOW", f"{len(no_nodes_items):,} examples have an empty 'nodes' dict — essentially blank graphs."))
    if invalid_json_items:
        issues.append(("CRITICAL", f"{len(invalid_json_items):,} outputs failed JSON parsing — unusable for training."))
    if bl_missing_items:
        issues.append(("LOW", f"{len({i for i,_ in bl_missing_items}):,} examples contain nodes without 'bl_idname'. "
                               "These nodes may cause errors if used in Blender directly."))

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    issues.sort(key=lambda x: severity_order[x[0]])

    colors = {"CRITICAL": RED, "HIGH": YELLOW, "MEDIUM": YELLOW, "LOW": BLUE}
    for sev, msg in issues:
        c     = colors[sev]
        label = f"{BOLD}{c}[{sev}]{RESET}"
        words = msg.split()
        line, lines_out = [], []
        for w in words:
            if sum(len(x)+1 for x in line) + len(w) > 72:
                lines_out.append(" ".join(line))
                line = [w]
            else:
                line.append(w)
        if line:
            lines_out.append(" ".join(line))
        print(f"\n  {label}")
        for ln in lines_out:
            print(f"         {ln}")

    if not issues:
        _ok2("No significant issues detected.")

    _header("7. SUMMARY")

    unique_examples  = TOTAL - full_dups
    unique_pct       = unique_examples / TOTAL * 100
    total_tokens_est = int(sum(out_lens) / 4)

    print(f"""
  Dataset file     : {augmented_path.name}
  File size        : {file_size_mb:.2f} MB
  Total examples   : {TOTAL:,}
  Unique examples  : {unique_examples:,}  ({unique_pct:.1f}% of total)
  Instruction types: {len(inst_counter)}
  Output format    : Blender node graph JSON
  Export versions  : {len(version_counter)} distinct versions
  Unique node types: {len(node_type_counter):,}
  Total nodes      : {total_nodes:,}  (avg {total_nodes/TOTAL:.1f}/example)
  Avg output len   : {avg_len:,.0f} chars  (~{avg_len//4:,.0f} tokens)
  Est. total tokens: ~{total_tokens_est:,}
  Issues flagged   : {len(issues)} ({sum(1 for s,_ in issues if s=='CRITICAL')} critical, \
{sum(1 for s,_ in issues if s=='HIGH')} high, \
{sum(1 for s,_ in issues if s=='MEDIUM')} medium, \
{sum(1 for s,_ in issues if s=='LOW')} low)
""")

    print(f"{BOLD}{CYAN}{'═' * 70}{RESET}\n")
    print(f"{'═' * 50}")
    print("ENTRY COUNT SUMMARY")
    print(f"{'═' * 50}")
    print(f"  Entries analyzed : {TOTAL:>10,}")
    print(f"  Changes made     : {'N/A (read-only)':>10}")
    print(f"{'═' * 50}")


# =============================================================================
# MAIN PIPELINE
# Must be wrapped in __main__ guard — multiprocessing on Windows uses "spawn"
# which re-imports this module in every worker process.  Without this guard,
# each worker would re-run Steps 0-1 and try to spawn its own Pool (infinite
# recursive loop).
# =============================================================================
if __name__ == "__main__":

    # Step -1: stage new raw/ files into joins/ for processing
    # Tracks which files have already been ingested via raw/.processed_manifest
    print("=" * 50)
    print("STEP -1: STAGE NEW RAW FILES")
    print("=" * 50)
    raw_dir = Path(_script_dir) / "raw"
    processed_manifest = raw_dir / ".processed_manifest"
    already_processed: set[str] = set()
    if processed_manifest.exists():
        already_processed = set(processed_manifest.read_text(encoding="utf-8").splitlines())
    new_staged = 0
    if raw_dir.exists():
        os.makedirs(JOINS_DIR, exist_ok=True)
        for raw_file in sorted(raw_dir.glob("*.json")):
            if raw_file.name not in already_processed:
                shutil.copy2(raw_file, os.path.join(JOINS_DIR, raw_file.name))
                already_processed.add(raw_file.name)
                new_staged += 1
        processed_manifest.write_text("\n".join(sorted(already_processed)), encoding="utf-8")
    print(f"  Staged {new_staged} new raw file(s) into joins/")
    print()

    # Step 0: collect files from joins/ → merge.jsonl
    run_joins()

    # Step 1: append merge.jsonl onto main JSONL
    run_merge()

    # Steps 2+3: parallel convert + filter → main_dataset_alpaca.json
    run_convert_filter()

    # Step 4: deduplicate → main_dataset_alpaca_deduped.json
    #   Skipped automatically if no duplicates are found.
    deduped_written = run_dedupe()

    # Step 5: augment → dataset_alpaca_father.json
    #   Uses deduped file if it was written, otherwise falls back to alpaca file.
    augment_input = deduped_path if deduped_written else alpaca_path
    if augment_input.exists():
        run_augment(augment_input)
    else:
        print(f"Augment input ({augment_input.name}) not found, skipping augmentation.")
        print()

    # Step 6: analysis of dataset_alpaca_father.json
    #   Skipped automatically if the file doesn't exist.
    run_analysis()
