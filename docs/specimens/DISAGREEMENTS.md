# Specimen DISAGREEMENTS log

Operator vs classifier vs schema. Records cases where the deterministic
classifier's verdict on a real-data evidence packet differs from what
the operator expected, and resolves which of the three is wrong:

- **classifier_wrong** — code has a bug; fix in `classifier.py`
- **schema_incomplete** — evidence vocabulary doesn't capture the
  distinction the operator was making; fix the schema (`*.evidence.json`
  shape) and update `derive_evidence.py` + `classifier.py` together
- **operator_wrong** — the operator's pre-judgment was the laundering
  shape; classifier is honest; resolution is a write-up explaining why
  the operator's intuition was off

Per the methodology: "the first real success is not agreement with the
operator. The first real success is a schema-grounded derived verdict
that disagrees with the operator and survives audit." Empty log = the
mechanization isn't yet paying for itself.

---

## D-001 — `!takedown` is render-layer per LABELS but hosting-layer in practice

**Packet:** `derived/derived-39516736-did-plc-ar7c4by46qjdydhdevvrndac-takedown.evidence.json`
**Specimen:** moderation.bsky.app emits `!takedown` on `did:plc:cthunuhvp2n3fgw7s7jdwp2n` at 2026-06-08T15:00:25Z.

**Classifier verdict:** `execution_gap_policy_present`
- `!takedown` is in the global `LABELS` map in `@atproto/api`
  (`defaultSetting: 'hide'`, `severity: 'alert'`, `flags: ['no-override', 'no-self']`)
- LabelObservation present, PolicyDocumentation status='documented', RenderObservation absent
- Classifier applies the rule "documented policy + no render witness = execution_gap_policy_present"

**Operator expectation:** the operator (labelwatch-claude during fixture
design) treated `!takedown` as a **hosting-layer constraint** — the
post is removed from the PDS by an admin action; the bsky client just
shows the absence (or a tombstone). The render-layer "blur" rule in
`LABELS` is downstream of, and predicated on, a removal that already
happened at the server. Treating `!takedown` as render-layer alone
loses that distinction.

**Diagnosis: schema_incomplete.**
The current `PolicyDocumentation` shape carries `policy_artifact +
extracted_rule + documented_expected_action` but has no field for
**which architectural layer the constraint operates at**. A label
whose primary effect is `app.bsky.feed.post` removal at the PDS
should not classify with the same `ConversionGap` value as a label
whose primary effect is client-side blur on a fully-present record.

**Patch required:**
1. Add `policy_layer` to `PolicyDocumentation`. Vocabulary candidates:
   `render` | `hosting` | `mixed`. (For `!takedown` it's `mixed`: the
   PDS removal is hosting-layer; the residual client render is
   render-layer.)
2. `derive_evidence.py` should populate `policy_layer` from a small
   table of known global labels. `!takedown`, `!hide`-on-account →
   `hosting`/`mixed`; `porn`, `sexual`, `nudity`, `graphic-media` →
   `render`. Unknown → `render` default with note.
3. `classifier.py` should emit a more specific gap value for
   `policy_layer=hosting`: e.g.
   `execution_gap_hosting_layer_constraint`. Render-layer remains
   `execution_gap_policy_present`.
4. Goldens for fixture 001 (`porn`) are unaffected (still render).
   No fixture for `!takedown` yet; D-001's resolution would create
   one as part of the patch.

**Audit result:** schema_incomplete (operator was right; classifier
honestly reported what the schema allowed it to see).

**Patch applied (this commit):**
1. Added `execution_surface` field to `PolicyDocumentation`. Vocabulary:
   `client_render | pds_hosting | mixed | unknown`. SOURCED FROM the
   policy artifact + a known-label semantics table in the deriver;
   describes WHERE the documented conversion acts; does NOT encode
   whether the conversion gap exists.
2. Added `HostingObservation` as a peer to `RenderObservation`.
   Surface-aware: `not_applicable` when the documented surface does
   not act on hosting; `absent` when it does but we have no
   hosting-side probe yet; `observed` when we do.
3. `derive_evidence.py` now populates `execution_surface` from
   `KNOWN_LABEL_SURFACE` (small hand-maintained table) and frames
   `RenderObservation` / `HostingObservation` to match the surface.
4. `classifier.py` `_classify_gap` now returns a struct
   `{name, surface}` and uses `_execution_witnessed_on_surface` to
   pick the right observation (Render for client_render; Hosting for
   pds_hosting; either for mixed).
5. Inadmissible claim set is now surface-aware: render-side claims
   fire when render_relevant + render absent; hosting-side claims
   (`no_individual_hosting_claim`, `no_population_hosting_claim`)
   fire when hosting_relevant + hosting absent.

**Patch verified against the original packet:**
- Re-derived `derived-NNNNN-...-takedown.evidence.json` shows
  `PolicyDocumentation.execution_surface = "pds_hosting"`,
  `RenderObservation.status = "not_applicable"`,
  `HostingObservation.status = "absent"`.
- Classifier output:
  `ConversionGap = {name: "execution_gap_policy_present", surface: "pds_hosting"}`.
- Inadmissible claim set now correctly includes
  `no_individual_hosting_claim` + `no_population_hosting_claim`
  instead of the render-side claims.

**Status:** patched-in-current-commit. Resolved.

**Forward note:** The patch does NOT yet ship specimen 003 as a
hand-authored fixture. The detection-lane `!takedown` packet
demonstrates the classifier behaves correctly under the patched
schema; a fixture-lane specimen 003 should follow once we have a
canonical hand-authored shape worth pinning.

---

## D-000 — (template, no entry)

When recording a new disagreement, copy this template:

```
## D-NNN — short title

**Packet:** path/to/packet.evidence.json
**Specimen:** description of the actual data row.

**Classifier verdict:** what classifier.py output.

**Operator expectation:** what the operator thought it should output, and why.

**Diagnosis:** classifier_wrong | schema_incomplete | operator_wrong (with one-sentence reasoning).

**Patch required:** concrete steps (changes to classifier.py / schema / fixtures / docs).

**Status:** open | patched-in-<commit> | wontfix (with reason).
```
