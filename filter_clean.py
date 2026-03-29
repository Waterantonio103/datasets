"""
Filter dataset_alpaca_father.json to only valid Blender node graph examples.

Steps:
  1. Deduplicate (exact duplicates — all fields identical)
  2. Keep only outputs whose top-level JSON keys are a subset of the
     expected Blender node graph schema keys AND that contain a 'nodes' key.

Writes result to dataset_alpaca_clean.json and prints a report.
"""

import json
import hashlib
import sys
import io
from pathlib import Path
from collections import Counter

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

INPUT_PATH  = Path(__file__).parent / "dataset_alpaca_father.json"
OUTPUT_PATH = Path(__file__).parent / "dataset_alpaca_clean.json"

EXPECTED_KEYS = {"export_version", "framed_nodes", "groups", "nodes", "update_lists"}


def hash_item(item: dict) -> str:
    key = json.dumps({k: item[k] for k in sorted(item)}, ensure_ascii=False)
    return hashlib.md5(key.encode()).hexdigest()


def is_valid_node_graph(output_str: str) -> tuple[bool, str]:
    """Return (is_valid, reason). Valid = parses as JSON, has 'nodes', no unexpected keys."""
    try:
        parsed = json.loads(output_str)
    except json.JSONDecodeError as e:
        return False, f"invalid JSON: {e}"

    if not isinstance(parsed, dict):
        return False, "output is not a JSON object"

    keys = set(parsed.keys())

    unexpected = keys - EXPECTED_KEYS
    if unexpected:
        return False, f"unexpected keys: {sorted(unexpected)[:3]}"

    if "nodes" not in keys:
        return False, "missing 'nodes' key"

    return True, "ok"


def main():
    before_count = 0
    if OUTPUT_PATH.exists():
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                before_count = len(json.load(f))
        except Exception:
            before_count = 0

    print(f"Reading {INPUT_PATH.name} ...")
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = len(data)
    print(f"  Loaded {total:,} examples")

    # Step 1: Deduplicate
    seen: set[str] = set()
    unique: list[dict] = []
    for item in data:
        h = hash_item(item)
        if h not in seen:
            seen.add(h)
            unique.append(item)

    deduped_count = len(unique)
    dup_removed = total - deduped_count
    print(f"\n── Deduplication ──────────────────────────────")
    print(f"  Duplicates removed : {dup_removed:,}  ({dup_removed/total*100:.1f}%)")
    print(f"  Unique examples    : {deduped_count:,}")

    # Step 2: Filter to valid Blender node graph format
    clean: list[dict] = []
    rejection_reasons: Counter = Counter()

    for item in unique:
        valid, reason = is_valid_node_graph(item["output"])
        if valid:
            clean.append(item)
        else:
            # Bucket the reason for reporting
            if "unity_equivalent" in reason or "blender_to_unity" in reason:
                bucket = "Unity conversion docs"
            elif "node_trees" in reason:
                bucket = "Tree clipper format"
            elif "links" in reason and "links" in json.loads(item["output"]):
                bucket = "Old 'links' schema"
            elif "skillRef" in reason or "systemPrompt" in reason:
                bucket = "Claude Code skill definitions"
            elif "invalid JSON" in reason:
                bucket = "Invalid JSON"
            elif "missing 'nodes'" in reason:
                bucket = "Missing 'nodes' key"
            else:
                bucket = f"Other unexpected keys"
            rejection_reasons[bucket] += 1

    filtered_count = deduped_count - len(clean)
    print(f"\n── Schema filtering ───────────────────────────")
    print(f"  Rejected (wrong format): {filtered_count:,}  ({filtered_count/deduped_count*100:.1f}%)")
    for reason, count in rejection_reasons.most_common():
        print(f"    {count:>5,}x  {reason}")
    print(f"  Kept (valid format)    : {len(clean):,}  ({len(clean)/deduped_count*100:.1f}%)")

    print(f"\nWriting {OUTPUT_PATH.name} ...")
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(clean, f, ensure_ascii=False, indent=2)

    out_mb = OUTPUT_PATH.stat().st_size / (1024 ** 2)
    after_count = len(clean)
    diff = after_count - before_count

    print(f"  Saved {after_count:,} examples  ({out_mb:.2f} MB)")
    print("\nDone.")
    print()
    print("=" * 50)
    print("ENTRY COUNT SUMMARY")
    print("=" * 50)
    print(f"  Before this run : {before_count:>10,}")
    print(f"  After this run  : {after_count:>10,}")
    print(f"  Difference      : {diff:>+10,}")
    print("=" * 50)


if __name__ == "__main__":
    main()
