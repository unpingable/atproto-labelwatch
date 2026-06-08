# Consumer-conversion census

> **Question this answers:** Are any production Bluesky clients converting
> third-party labelers into default visibility behavior **without explicit
> user adoption**?
>
> **Short answer (as of 2026-06-08, sampled corpus):** No. Every sampled
> client either hardcodes only `moderation.bsky.app` (`did:plc:ar7c4by46qjdydhdevvrndac`)
> or inherits the appview's defaults transparently. None hardcodes any
> third-party labeler DID as a default subscription.
>
> **Therefore:** the Bundle G opt-in-consumer-adoption machinery is fire
> code for a building not yet built — defensible, not urgent. The
> wildfire-perimeter framing in F-001/F-004 was inflated. The discipline
> still holds; the urgency does not.

## Method

For each client, search the public source for:

| pattern | what it would mean |
|---|---|
| `BSKY_LABELER_DID` / `moderation.bsky.app` / `did:plc:ar7c4by46qjdydhdevvrndac` | the official default labeler is referenced/hardcoded |
| other `did:plc:` string in a labeler-config context | a third-party labeler is hardcoded as default |
| `labelersPref` / `app.bsky.actor.defs#labelersPref` / `readLabelers` | user-chosen labelers are read from `app.bsky.actor.getPreferences` |
| `appLabelers` / `BskyAgent.configure({appLabelers})` | the SDK is configured with a specific labeler set |
| `queryLabels` / `com.atproto.label.queryLabels` | labels are pulled from labeler endpoints |
| `app.bsky.labeler.service` | the client reads labeler service records |

For each match, distinguish: **hardcoded default** vs **user-chosen** vs
**test-only**.

Sampled via `gh api search/code` (where available; rate-limited) and
`WebFetch` of raw files (where direct paths were guessable). Seven
clients/SDKs sampled. The Bluesky showcase lists ~47 client-shaped
projects; this is a focused-sample audit, not exhaustive.

## Findings table

| client | source | hardcoded official labeler | hardcoded third-party labeler | reads user `labelersPref` | applies hide/warn/badge | evidence path |
|---|---|---|---|---|---|---|
| `bluesky-social/social-app` (official iOS/Android/web) | public | YES — imports `BSKY_LABELER_DID` from `@atproto/api`; passes via `BskyAgent.configure({appLabelers: [BSKY_LABELER_DID]})` | **NO** | YES — `readLabelers(account.did)` from persistent storage; entries appended to `appLabelers` (excludes default to avoid double-subscribe) | YES — full moderation pipeline | `src/lib/constants.ts`, `src/state/session/moderation.ts`, `src/lib/moderation/useModerationCauseDescription.ts` |
| `bluesky-social/atproto` → `@atproto/api` (SDK) | public | YES — `export const BSKY_LABELER_DID = 'did:plc:ar7c4by46qjdydhdevvrndac'` is the ONLY content of `packages/api/src/const.ts` | **NO** | N/A (SDK; provides primitives, not a client) | YES — provides `decideLabelModeration`, `getModerationUI` etc. via `packages/api/src/moderation/` | `packages/api/src/const.ts`, `packages/api/src/moderation/const/labels.ts` (8-entry global LABELS map: `!hide`, `!warn`, `!no-unauthenticated`, `porn`, `sexual`, `nudity`, `graphic-media`, `gore`) |
| `mimonelu/klearsky` (web, Vue.js, popular) | public | YES — `OFFICIAL_LABELER_DID = "did:plc:ar7c4by46qjdydhdevvrndac"` in `src/consts/consts.json`; UI prevents un-subscribing from it; prepended if missing from `labelersPref` | **NO** | YES — reads `app.bsky.actor.defs#labelersPref` from `currentPreferences` | YES — subscribe/unsubscribe UI, full labeler settings popup | `src/consts/consts.json`, `src/composables/main-state/my-labeler.ts` |
| `mozzius/graysky` (iOS/Android, popular) | public | NO explicit hardcoding | **NO** | not visibly — no `labelersPref` or `appLabelers` references found in `apps/expo/src/lib/agent.tsx`; uses `new AtpAgent({service: "https://public.api.bsky.app"})` with no labeler configuration | inherits whatever the appview applies | `apps/expo/src/lib/agent.tsx` (sparse; no moderation directory under `src/lib/`) |
| `pdelfan/ouranos` (Next.js web) | public | no obvious config | **NO** | not visibly — no moderation directory or labeler config surfaced in `src/` | inherits SDK defaults | `src/` (no moderation/labeler subdirectory found) |
| `mary-ext/langit` → Skeetdeck (deck-style web) | public | NO — `app/api/moderation/` contains its own moderation primitives + a `GLOBAL_LABELS` map; labelers are passed in by callers | **NO** | not surfaced in service.ts (callers responsible) | YES — own moderation primitives (preference/blur/severity enums, `decideLabelModeration`, `getModerationUI`) | `app/api/moderation/index.ts`, `app/api/moderation/service.ts` |
| `ioriayane/Hagoromo` (Qt/C++ desktop) | public | delegated to `ConfigurableLabels` (not directly hardcoded in `labelerprovider.cpp`) | **NO** | retrieves via `m_labels.labelerDids()` — caller-configured | YES — `app/qtquick/moderation/labelerlistmodel.cpp` plus labeler list UI | `lib/tools/labelerprovider.cpp`, `lib/tools/configurablelabels.h`, `app/qtquick/moderation/` |

## What the table actually says

- **Universal finding (7/7 sampled):** No client hardcodes a
  non-`moderation.bsky.app` labeler as a default. Every sampled client
  either hardcodes ONLY `moderation.bsky.app` or hardcodes nothing
  (inheriting `@atproto/api`'s defaults or the appview's behavior).
- **Pattern split:** The clients that hardcode a labeler at all (3/7:
  social-app, @atproto/api SDK, Klearsky) all hardcode the SAME DID —
  `did:plc:ar7c4by46qjdydhdevvrndac`. The remaining 4/7 hardcode
  nothing (Graysky, Ouranos, Skeetdeck/langit, Hagoromo).
- **User adoption surface:** Clients that DO surface labeler
  subscription read `labelersPref` from `app.bsky.actor.getPreferences`.
  This matches Bluesky's stackable-moderation framing: built-in default
  is auto-applied; everything else requires explicit user opt-in.
- **Test-only carve-out:** social-app references `mod-authority.test`
  in `IS_TEST_USER` branches; production path is unaffected.
- **Cap:** social-app's `MAX_LABELERS = 20`; Klearsky's
  `LABELER_UPPER_LIMIT = 20`. User-chosen labeler set is bounded.

## What this falsifies

- **F-001's framing was inflated.** F-001 said reference-labeler status
  doesn't imply default-client conversion. That's true and worth
  recording. The census confirms a stronger claim: **no labeler other
  than `moderation.bsky.app` enjoys default-client conversion in any
  sampled production client.** The "reference vs unknown classifier"
  distinction was at most an operator-side cataloging confusion; it
  was never close to becoming a real conversion-path collapse.
- **F-004's "wildfire perimeter" framing was inflated.** Third-party
  service records exist; opt-in subscription mechanisms exist;
  Bluesky's stackable-moderation framework explicitly anticipates
  user-added labelers. But the census finds no client that BYPASSES
  the user-opt-in step. The opt-in machinery in Bundle C/G is
  defensible (it correctly models what the protocol allows), but
  there is no current real-world case it prevents.
- **Bundle G's `consumer_scope=opt_in_consumer_observed` machinery
  is fire code for a building not yet built.** No production
  consumer observed to use this path; the only "real" instance is
  Driftwatch's synthetic policy we wrote ourselves.

## What this does NOT falsify

- **The discipline itself.** The classifier's refusal of laundering
  is correct under ANY consumer landscape. If a third-party client
  EVER starts defaulting to a non-mod.bsky labeler, the existing
  schema represents that case honestly (it would be a `global_platform`
  finding for that consumer specifically, but the consumer scope
  would still be the named-client, not "Bluesky as a whole").
- **Bundle B/C/D.5/E/F structural finds.** F-002 (the fictional
  GLOBAL_LABELS), F-005 (the ingestion gap), F-006 (undeclared
  emission), the surface/scope/basis distinctions — these stand
  independently of whether opt-in adoption is wildfire or fire code.
- **The general possibility.** A future client release could add
  hardcoded third-party labelers tomorrow. This census is a snapshot,
  not a stationary truth.

## Sampling gaps and honest caveats

- ~40 of ~47 clients in the Bluesky showcase were NOT sampled.
  CLI/TUI/CMS clients are unlikely to have moderation pipelines
  (no UI to blur), but mobile clients (Greenland, Seiun, Ozone Android,
  Sora, Skywalker) and additional web clients (deck.blue, Tokimeki,
  SkyDeck, Bluejeans, etc.) were skipped for time. The sampled clients
  cover: official app, the SDK, the most-starred mobile (Graysky), the
  most-starred web alternatives (Klearsky, Ouranos), the deck-shaped
  alternative framework (langit), and one non-web/non-SDK runtime
  (Hagoromo Qt).
- Closed-source clients (Bluejeans, Connectsky, Helico, Skeets, Sora,
  Subium, Sunrise, Tokimeki, The Blue, Yup, etc.) are unobservable
  through this method. A future client could hardcode third-party
  labelers and we'd only see it via behavioral probes.
- The census measures **declarative source-code intent**. It does
  NOT measure runtime behavior; a client could fetch a remote
  config that adds labelers at startup. None observed in sampled
  source.
- The census measures **defaults**. User-installed labelers are
  excluded by design — those are exactly the opt-in surface the
  protocol intends.

## Next moves the census enables

1. **De-emphasize the wildfire framing in F-001/F-004.** Update those
   findings' status to note the census result: the opt-in machinery
   is correct but its real-world urgency is low.
2. **Quietly retain Bundle G machinery.** Don't tear it out; it
   correctly models a possibility the protocol allows, and the
   Driftwatch opt-in remains a real specimen demonstrating the
   distinction. But stop motivating new work with "what if a client
   does X" until a real client does X.
3. **Re-probe periodically.** If a client release adds hardcoded
   third-party labelers (or if the appview starts shipping additional
   defaults), the census needs a refresh. Cheap to re-run; ~1 hour
   for the sampled clients.
4. **Consider behavioral probes for closed-source clients.** Subscribe
   a test account, load the client, observe whether labels from a
   known third-party labeler render. Outside the goblin-math scope
   but the only way to extend the census to closed clients.

## Provenance

- **Sampled:** 2026-06-08
- **Source-of-truth checkpoint:** `bluesky-social/atproto` main branch,
  `BSKY_LABELER_DID` constant value at the time of fetch was
  `did:plc:ar7c4by46qjdydhdevvrndac`.
- **Method:** `gh api search/code` + raw-file WebFetch.
- **Reproduction:** see grep targets in the Method section; each
  finding cites the file path that backs it.
