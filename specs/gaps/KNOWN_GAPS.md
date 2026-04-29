# Known Gaps

Protocol-level limitations that constrain what Labelwatch can observe.
These are not bugs — they're boundaries of what the ATProto ecosystem
currently makes visible.

---

## Labeler subscriber counts are invisible

**Impact**: We cannot measure labeler *reach* — how many users have
subscribed to a given labeler and are therefore affected by its decisions.

**Why**: When a user subscribes to a labeler, the subscription is stored as
a preference record in the *user's* repo (`app.bsky.labeler.service`), not
in any public index. There is no `getSubscriberCount` endpoint, no way to
enumerate subscribers, and no aggregate metric exposed by the AppView or
relay infrastructure.

**What we can see instead**:
- Label volume (how actively the labeler is labeling)
- Target breadth (how many distinct subjects it labels)
- Whether the labeler has published its declaration record
- List memberships (e.g. labeler-lists.bsky.social)

**Why this matters**: A labeler with 100k subscribers and one with 3
subscribers look identical from the outside. Volume and behavior are
observable; reach is not. This means we can detect *behavioral* anomalies
(rate spikes, flip-flops, going dark) but cannot assess *impact* — whether
an anomaly affected 3 people or 300,000.

This is a meaningful gap for governance. A labeler that flip-flops on a
label value is a curiosity if 3 people subscribed; it's an incident if
300,000 did. Without subscriber counts, we can't distinguish.

**Status**: Protocol limitation. No current ATProto API exposes this data.
A feature request to Bluesky may be warranted — subscriber counts (even
coarse buckets: <100, <1k, <10k, 10k+) would materially improve labeler
accountability without compromising user privacy.

**Filed**: 2026-03-16
