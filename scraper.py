"""
GitHub scraper for Blender node graph JSON files.

Strategy
--------
Uses the GitHub Code Search API with queries specifically targeting the
Blender node-graph export format (update_lists + export_version + bl_idname).
Falls back to repo-level searches for known Blender node graph repositories.

Run:
    python scraper.py --max-files 200 --out-dir raw/
    python scraper.py --max-files 500 --out-dir raw/  # more aggressive run
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Iterator

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
HEADERS = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
if GH_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GH_TOKEN}"

# These queries are ordered from most to least specific.
# Each must produce files that plausibly contain Blender node graph exports.
CODE_SEARCH_QUERIES = [
    # ── Tier 1: exact format signature ──────────────────────────────────────
    # Files that have all three canonical fields of the target export format
    '"update_lists" "export_version" "bl_idname" extension:json',
    '"update_lists" "framed_nodes" "bl_idname" extension:json',

    # ── Tier 2: node tree types with structured nodes ────────────────────────
    '"ShaderNodeTree" "update_lists" extension:json',
    '"GeometryNodeTree" "update_lists" extension:json',
    '"CompositorNodeTree" "update_lists" extension:json',
    '"SverchCustomTreeType" "update_lists" extension:json',

    # ── Tier 3: Blender node types in structured node dicts ──────────────────
    '"ShaderNodeBsdfPrincipled" "bl_idname" "export_version" extension:json',
    '"GeometryNodeMeshPrimitiveCube" "bl_idname" extension:json',
    '"ShaderNodeTexNoise" "bl_idname" "update_lists" extension:json',
    '"ShaderNodeTexVoronoi" "bl_idname" "update_lists" extension:json',
    '"ShaderNodeMixShader" "bl_idname" "update_lists" extension:json',
    '"GeometryNodeGroup" "update_lists" "bl_idname" extension:json',

    # ── Tier 4: procedural material / geometry node repos ───────────────────
    # These repos commonly ship JSON node presets
    '"NodeSocketShader" "bl_idname" extension:json',
    '"NodeSocketGeometry" "bl_idname" extension:json',
    '"ShaderNodeOutputMaterial" "export_version" extension:json',
]

# Known repositories with high-quality Blender node graphs.
# These are scraped directly via the repo trees API regardless of code search.
KNOWN_REPOS = [
    # Geometry / shader node libraries
    "bnzs/blender-node-presets",
    "ChrisBreedveld/the-grove",
    "nicktobey/blender-node-graph-exporter",
    "Obbut/blender-node-exporter",
    "natthawat-dev/blender_node_exporter",
    "diversityofsound/blender_gn_presets",
    "centipede3d/blender-node-library",
    "cgstudiomap/cgstudiomap",
    "ksons/gltf-blender-importer",
    "egtwobits/mesh-mesh-align-plus",
    "YuriyGuts/blender-node-arrange",
    "Pullusb/GP_tool_pack",

    # Addon repos that ship node setups as JSON
    "FeralAI/blender-assets",
    "ThomasParistech/blender_useful_scripts",
    "lucasposito/MotionBuilder",
    "snuq/VSEQF",
    "ubisoft/mixer",
    "zachEastin/blender-node-graph",

    # Procedural material / texture repos
    "PolyHaven/polyhaven-site",        # procedural material JSON specs
    "maximerenard/blender-materials",
    "mochiciel/blender_materials",
    "ProceduralTextures/material-database",
    "BearishSun/PBR-Materials",

    # Geometry nodes specifically
    "SimonStorelv/blender-geonodes-lib",
    "joshualeopold/blender-geometry-nodes",
    "williamchange/geometry-nodes-library",
    "higgsas/higgsas_geo_nodes_library",
    "erindale/erindale-toolkit",
    "higgsas/blender_geonode_presets",
    "iandanforth/blender-geonode-presets",

    # Compositor
    "spectralvectors/spectral-compositing-nodes",
    "LumineWollah/Projet-BCON-2025-groupe1",
]

# ──────────────────────────────────────────────────────────────────────────────
# Rate-limit-aware GitHub API helpers
# ──────────────────────────────────────────────────────────────────────────────

def _wait_for_rate_limit(response: requests.Response) -> None:
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    reset_at   = int(response.headers.get("X-RateLimit-Reset", 0))
    if remaining <= 2:
        wait = max(0, reset_at - int(time.time())) + 5
        print(f"  Rate limit almost exhausted — waiting {wait}s …")
        time.sleep(wait)


def gh_get(url: str, params: dict | None = None) -> dict | list | None:
    for attempt in range(4):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            _wait_for_rate_limit(r)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 403:
                retry = int(r.headers.get("Retry-After", 60))
                print(f"  403 — retrying after {retry}s")
                time.sleep(retry)
                continue
            if r.status_code == 422:
                return None  # search result set too large / query invalid
            if r.status_code in (404, 451):
                return None
            print(f"  HTTP {r.status_code} for {url} — skipping")
            return None
        except requests.RequestException as e:
            print(f"  Request error ({e}) — retry {attempt + 1}/4")
            time.sleep(5 * (attempt + 1))
    return None


def code_search_files(query: str, max_results: int = 100) -> Iterator[dict]:
    """Yield file metadata dicts from GitHub code search."""
    per_page = min(100, max_results)
    fetched = 0
    for page in range(1, 11):  # max 10 pages × 100 = 1000 results
        if fetched >= max_results:
            break
        data = gh_get(
            "https://api.github.com/search/code",
            params={"q": query, "per_page": per_page, "page": page},
        )
        if not data or "items" not in data:
            break
        items = data["items"]
        if not items:
            break
        for item in items:
            if fetched >= max_results:
                return
            yield item
            fetched += 1
        if len(items) < per_page:
            break
        time.sleep(1)  # be polite between pages


def repo_json_files(owner_repo: str) -> Iterator[dict]:
    """Yield all .json file metadata from a repo's default branch tree."""
    data = gh_get(f"https://api.github.com/repos/{owner_repo}")
    if not data:
        return
    branch = data.get("default_branch", "main")
    tree = gh_get(
        f"https://api.github.com/repos/{owner_repo}/git/trees/{branch}",
        params={"recursive": "1"},
    )
    if not tree or "tree" not in tree:
        return
    for entry in tree["tree"]:
        if entry.get("type") == "blob" and entry.get("path", "").lower().endswith(".json"):
            yield {
                "repository": {"full_name": owner_repo},
                "path": entry["path"],
                "url": f"https://raw.githubusercontent.com/{owner_repo}/{branch}/{entry['path']}",
                "_raw_url": True,
            }


def download_file_content(item: dict) -> str | None:
    """Download the raw content of a file item."""
    if item.get("_raw_url"):
        url = item["url"]
    else:
        url = item.get("url", "")
        # Convert API url to raw content url
        # https://api.github.com/repos/OWNER/REPO/contents/PATH
        # → https://raw.githubusercontent.com/OWNER/REPO/REF/PATH
        match = re.match(
            r"https://api\.github\.com/repos/([^/]+/[^/]+)/contents/(.+)",
            url,
        )
        if match:
            owner_repo, path = match.group(1), match.group(2)
            repo_data = item.get("repository", {})
            ref = item.get("ref", "HEAD")
            url = f"https://raw.githubusercontent.com/{owner_repo}/{ref}/{path}"
        else:
            # Fall back to API blob download
            pass

    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 200:
        return r.text
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Format validation — only save files that have a plausible Blender node graph
# ──────────────────────────────────────────────────────────────────────────────

BLENDER_FORMAT_SIGNALS = (
    "update_lists",
    "export_version",
    "framed_nodes",
    "bl_idname",
    "ShaderNodeTree",
    "GeometryNodeTree",
    "CompositorNodeTree",
)


def is_blender_node_file(content: str) -> bool:
    """Quick check before saving — must contain at least 2 Blender signals."""
    hits = sum(1 for sig in BLENDER_FORMAT_SIGNALS if sig in content)
    return hits >= 2


def content_hash(content: str) -> str:
    return hashlib.md5(content.encode()).hexdigest()


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-files", type=int, default=200,
                    help="Max files to download per code-search query")
    ap.add_argument("--out-dir", default="raw/",
                    help="Directory to save downloaded JSON files")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load hashes of already-downloaded content to avoid duplicates
    seen_hashes: set[str] = set()
    existing_files = list(out_dir.glob("*.json"))
    for f in existing_files:
        try:
            seen_hashes.add(content_hash(f.read_text(encoding="utf-8", errors="ignore")))
        except Exception:
            pass

    # Determine next file number
    nums = [int(f.stem[:5]) for f in existing_files if f.stem[:5].isdigit()]
    counter = max(nums, default=-1) + 1

    saved = 0
    skipped_format = 0
    skipped_dup = 0

    def try_save(item: dict, content: str) -> bool:
        nonlocal counter, saved, skipped_format, skipped_dup

        if not is_blender_node_file(content):
            skipped_format += 1
            return False

        h = content_hash(content)
        if h in seen_hashes:
            skipped_dup += 1
            return False
        seen_hashes.add(h)

        repo = item.get("repository", {}).get("full_name", "unknown_unknown")
        path = item.get("path", "file.json")
        # Sanitise filename parts
        owner, _, repo_name = repo.partition("/")
        stem = Path(path).stem
        stem = re.sub(r"[^a-zA-Z0-9_\- ]", "_", stem)[:50].strip("_ ")
        filename = f"{counter:05d}_{owner}_{repo_name}_{stem}.json"
        (out_dir / filename).write_text(content, encoding="utf-8")
        counter += 1
        saved += 1
        return True

    # ── Phase 1: targeted code search ─────────────────────────────────────────
    print(f"Phase 1: Code search ({len(CODE_SEARCH_QUERIES)} queries, "
          f"max {args.max_files} files each)")
    for i, query in enumerate(CODE_SEARCH_QUERIES, 1):
        print(f"  [{i}/{len(CODE_SEARCH_QUERIES)}] {query[:70]}")
        n_before = saved
        for item in code_search_files(query, max_results=args.max_files):
            content = download_file_content(item)
            if content:
                try_save(item, content)
            time.sleep(0.2)  # polite delay between file downloads
        print(f"    → saved {saved - n_before} new files  "
              f"(dup={skipped_dup}, no-signal={skipped_format})")
        time.sleep(2)  # pause between queries

    # ── Phase 2: known repos ──────────────────────────────────────────────────
    print(f"\nPhase 2: Known repos ({len(KNOWN_REPOS)} repos)")
    for repo in KNOWN_REPOS:
        n_before = saved
        for item in repo_json_files(repo):
            content = download_file_content(item)
            if content:
                try_save(item, content)
            time.sleep(0.1)
        delta = saved - n_before
        if delta:
            print(f"  {repo}: +{delta} files")
        time.sleep(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 50}")
    print("SCRAPE SUMMARY")
    print(f"{'=' * 50}")
    print(f"  Files saved      : {saved:>8,}")
    print(f"  Skipped (dup)    : {skipped_dup:>8,}")
    print(f"  Skipped (format) : {skipped_format:>8,}")
    print(f"  Output dir       : {out_dir.resolve()}")
    print(f"  Total files now  : {len(list(out_dir.glob('*.json'))):>8,}")
    print(f"{'=' * 50}")

    print()
    print("=" * 50)
    print("ENTRY COUNT SUMMARY")
    print("=" * 50)
    print(f"  Before this run : {len(existing_files):>10,}")
    print(f"  After this run  : {len(list(out_dir.glob('*.json'))):>10,}")
    print(f"  Difference      : {saved:>+10,}")
    print("=" * 50)


if __name__ == "__main__":
    main()
