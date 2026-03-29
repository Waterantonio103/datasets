# Scaling to 10,000+ Clean Examples

## Where we are now

| Stage | Count |
|---|---|
| Source JSONL lines | 179,842 |
| Unique source graph structures | ~222 |
| Clean examples (after dedup + filter) | ~2,911 |
| Augmentation multiplier | ~13× per graph |

**The ceiling is source variety, not volume.** The pipeline is healthy — you need
~750–1,000 unique graph structures to hit 10K examples at the current augmentation rate.

---

## Path 1 — Better GitHub scraping (scraper.py, done)

The old scraper pulled any JSON from Blender-adjacent repos with loose queries,
which caused contamination (Unity docs, skill definitions, VTK nodes, etc.)
and wasted the rate-limit budget.

The new `scraper.py` fixes this:
- **Tier 1 queries** target the exact format signature: `"update_lists" "export_version" "bl_idname"` — only files that are actually Blender node exports
- **Tier 2/3 queries** expand to all node tree types (Geometry, Compositor, Sverchok)
- **Pre-save validation** checks for ≥2 Blender signals before writing, cutting noise at the source
- **Known-repos phase** directly walks trees of curated high-signal repos

Expected yield: 300–600 new unique graphs per weekly run until GitHub's index is exhausted.

---

## Path 2 — Blender Extensions Marketplace

The official Blender Extensions site (https://extensions.blender.org) hosts hundreds of
free add-ons, many of which ship JSON node presets. You can:

1. Scrape the extensions API: `GET https://extensions.blender.org/api/v1/extensions/`
2. Download the `.zip` source packages for any add-on tagged `node_groups`, `materials`, `shaders`
3. Extract `.json` files from the zips — `logic.py` already handles zip extraction in the joins/ step

This is likely the single highest-yield untapped source. Blender's own add-on ecosystem
has hundreds of material/shader libraries.

**Implementation**: Add a `scrape_extensions.py` that:
- Hits the extensions API
- Filters by tag: `materials`, `node_groups`, `shader`, `geometry_nodes`
- Downloads zips into `joins/`
- `logic.py` handles the rest automatically

---

## Path 3 — Blender Market / Gumroad free assets

Many artists release free procedural material packs. These commonly include `.blend`
files that, once opened in Blender and exported with your exporter add-on, yield
dozens of unique node graphs each.

Manual effort required, but high signal:
- https://blendermarket.com/free  (search "node", "shader", "material")
- https://gumroad.com/discover?query=blender+nodes+free
- https://blendswap.com/category/materials (CC0 licensed)

Target: ~50 packs × ~20 nodes each = ~1,000 unique graphs.

---

## Path 4 — Polyhaven (open-licensed, very high quality)

Poly Haven (polyhaven.com) is fully CC0 and their Blender `.blend` files
are publicly available via their API:

```
GET https://api.polyhaven.com/assets?type=textures
```

Each texture asset has a Blender shader network. Their GitHub repo
(`PolyHaven/polyhaven-site`) contains node setup specs.
Target: 500–700 unique procedural material graphs.

---

## Path 5 — Augmentation improvement

Currently each graph gets ~13 instruction variants. Increasing this to 20–25
variants (adding more templates) would take 750 unique graphs → 15K–18K examples
without any new scraping.

Add instruction templates that vary along these axes:
- **Use case**: "for a realistic stone texture", "for stylised NPR rendering", etc.
- **Render engine**: "compatible with Eevee", "Cycles-optimised"
- **Complexity**: "simple", "moderate", "advanced", "production-ready"
- **Node type callout**: "using a Principled BSDF node", "with noise textures"
- **Question style**: "How would I...", "What nodes do I need for...", "Create a..."

---

## Path 6 — Synthetic generation (highest ceiling)

Use an LLM (Claude/GPT-4) to generate new valid node graph JSONs from
textual descriptions. Start with seed graphs from the clean dataset and
ask the model to:
- "Create a variation of this shader that adds subsurface scattering"
- "Extend this geometry node setup to support randomised scale"
- "Create a compositor setup that adds film grain"

The output can be validated with Blender's Python API before being added.
This approach has no ceiling.

---

## Realistic timeline to 10K

| Source | Estimated new unique graphs | Resulting clean examples |
|---|---|---|
| Current (after normalize) | 222 | ~2,911 |
| Improved GitHub scraper (3 runs) | +400 | ~8,000 |
| Blender Extensions scraper | +300 | ~12,000 |
| Polyhaven | +150 | ~14,000 |
| Augmentation template expansion | ×1.5 multiplier | ~21,000 |

The scraper + extensions route alone should get you to 10K within 3–4 weekly runs.
