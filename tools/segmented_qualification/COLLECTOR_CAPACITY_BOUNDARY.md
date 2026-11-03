# Collector capacity refusal boundary

Decision owner: Codex /root/release_review; independent acceptance remains root-owned.
Input source: 598aa0edeca83fd9719a07f2f0028541889e9705. Result identity is the subsequent exact commit and campaign receipt, rather than a moving branch.

Observed qualification occurrence controls-17d2e3a3-d942-45e7-b934-b49a93993a52, reserve phase, installed source63c9e5c73af53f5541db210b510daccba7c4668d: stronger impossible policy yielded113attempts,21 per-source RuntimeError results and overall round=ran/exit0. Policy admission existed at label acceptance, but the collector misclassified its refusal and CLI lacked current-capacity admission before discovery/bootstrap work. This is not evidence that labels were accepted below the reserve. Paused count100 is measured; an exact contemporaneous post-control count is unavailable.

Proposition and scope: CLI admits current capacity before HTTP or operational store mutation. If that capacity changes later, the exact known reserve reasons raised by a trusted store operation become a whole-round refused result and exit1, including after earlier pages committed. Earlier accepted results, identities and sweep observations are retained; the affected attempt cursor is not advanced and is not marked source-unavailable. Best-effort ingest_paused gap/attempt cleanup and STATUS publication do not replace the primary reason if they fail. The existing allowed gap schema is reused; no storage/schema conversion introduced. STATUS is an explicitly best-effort diagnostic write after refusal, not permission to continue ingestion.

Classification has a narrow authority boundary: only an exact built-in RuntimeError message from a store call is converted into private collector control flow. Provider Refused, arbitrary RuntimeError text/substrings and transport failures retain their prior semantics. The private exception is not a new public exception protocol or general framework. Current shared_reserve_unavailable and its exact filesystem-not-covered variant are covered. Future store reason changes need explicit review.

Practical finite regression tests suffice for this bounded correction: real impossible-policy admission before transport; first-page and later-page reserve races; source cursor/failure invariants; claim/sweep/begin operation boundaries; discovery classification; cleanup failure preserving original reason; stdout/STATUS and nonzero CLI; and deterministic negative controls for unrelated/provider reason text. A general allocation-race model is outside scope: physical capacity can change between checks, metadata cleanup/publication can itself fail, and fixtures do not establish host/VM runtime behavior. Existing WAL ingest-first and midnight STORE_HOLD/exit75 semantics are retained and separately checked.

The final-scale export producer source20de9bc06d57fb322a279e19dea0d2bd8bba4f7b retains byte-identical components below. Collector/CLI changes do not alter these exported data-path implementations, but this correspondence is not itself a new performance qualification:

[
  {
    "path": "tools/segmented_qualification/recent_storage.py",
    "sha256": "28dc07f1d98f3f482a4ea251579b85911cbb8ed95c24011f4c645e432189af68"
  },
  {
    "path": "tools/segmented_qualification/recent_provider.py",
    "sha256": "c98722e64c3141e13bf9c729365a637991e1419ed5c08887860ad82b00ee5cb4"
  },
  {
    "path": "src/labelwatch/recent_observations.py",
    "sha256": "495c0058f123434ebb5893299589df5e4511c0076ce12e32253d4b3995a80f77"
  },
  {
    "path": "tools/segmented_qualification/tier.py",
    "sha256": "fd607c0b98e1009cace7fedff7dae909c768dcff92ebc9dffcd4f709c2f4357c"
  }
]
