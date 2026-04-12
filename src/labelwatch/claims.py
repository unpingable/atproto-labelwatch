"""Claim log: public transparency page for analytical findings and their status.

Renders investigation results, methodology notes, and admissibility
assessments as a static page. No DB dependency.
"""

from .report import _layout


def render_claims_html() -> str:
    """Return the full claims log page HTML."""
    body = """
<div class="hero">
  <p class="hero-pitch">Claim Log</p>
  <p class="small">Analytical findings, their methodology, and their current status.
  Labelwatch publishes its reasoning, not just its conclusions.</p>
</div>

<div class="about-prose">

<h2>Hosting Locus Investigation</h2>
<p class="small" style="opacity:0.7">Investigated 2026-04-10. Status: <strong>conclusion revised</strong>.</p>

<p>The original hosting-locus hypothesis asked: <em>do labeled targets cluster on
specific PDS hosts in ways that reveal coordinated or concentrated adversarial
behavior?</em></p>

<p>The pipeline was honest; the interpretation was wrong. It was measuring
something real, just not the thing it was built to surface.</p>

<h3>What we tested</h3>

<p>Labelwatch seeds driftwatch&rsquo;s resolver with labeled-target DIDs so their
PDS hosts can be resolved. We compared the host distribution of these
&ldquo;seed&rdquo; DIDs (accounts that have been labeled) against &ldquo;live&rdquo;
DIDs (accounts observed posting in real time) to look for hosting-locus
signals.</p>

<h3>Finding 1: Head divergence is an age confounder</h3>
<p><span class="badge badge-stable">Explained</span></p>

<p>The top of the distribution showed a dramatic divergence: two newer Bluesky PDS
shards (jellybaby, stropharia) held ~131,000 live accounts each with near-zero
seed presence, while older &ldquo;mushroom&rdquo; shards (lionsmane, amanita, oyster, etc.)
held ~2,000 each with ~60% seed ratio.</p>

<p>This is explained by Bluesky&rsquo;s PDS shard rotation. New accounts go to the
newest shards. Labels take time to accumulate. Therefore older shards are
seed-heavy (more labeled accounts) and newer shards are live-heavy (more
currently-active posters). The divergence reflects account age, not any
property of the hosts themselves.</p>

<h3>Finding 2: Stale pds_host hypothesis falsified</h3>
<p><span class="badge badge-stable">Tested, 100/100</span></p>

<p>We tested whether the stored <code>pds_host</code> field might be stale &mdash;
that is, whether seed DIDs had migrated to different PDS hosts since resolution,
making the comparison invalid.</p>

<p>100 seed DIDs from the top-20 mushroom-head hosts were re-resolved fresh
against <code>plc.directory</code> using the same extraction logic as the
driftwatch resolver. All 100 matched their stored host exactly. The stale-field
explanation is killed for the mushroom-head population.</p>

<p class="small"><strong>Scope limitation:</strong> This check was bounded to the
mushroom-head (top-20 seed hosts, all <code>did:plc</code>, all
<code>*.host.bsky.network</code> shards). It does not certify
<code>pds_host</code> freshness for the long tail, for <code>did:web</code>,
or for any other population.</p>

<h3>Finding 3: Seed:live ratio measures labeler coverage, not host behavior</h3>
<p><span class="badge badge-churn">Conclusion revised</span></p>

<p>The long-tail analysis found hosts with 100% seed ratios &mdash; every account
labeled, zero observed posting. The leading example was
<code>skystack.xyz</code>: 276 accounts, all carrying a <code>substack</code>
label from a single labeler (<code>did:plc:uxjwly6emtgik7juvxxdpl3c</code>,
29,620 label events). The same labeler had enumerated every account on the
host with a content-type label.</p>

<p>The 100% seed ratio was not a behavioral signal. It was a <strong>labeler
coverage artifact</strong>: one labeler chose to enumerate all accounts on a
specific PDS, which is a governance decision about labeler tactics, not
evidence of adversarial account clustering.</p>

<p>The same pattern likely holds for other high-seed-ratio hosts:
<code>pds.1440.news</code> (100%, 24 accounts), <code>northsky.social</code>
(83%), <code>bsky.bestofmodels.blog</code> (72.7%), and
<code>atproto.brid.gy</code> (49.8%, the Bridgy Fed fediverse bridge).</p>

<p><strong>The pipeline is measuring something real, just not the thing it was
built to surface.</strong> The seed:live ratio is a proxy for labeler coverage
density &mdash; which labelers chose to enumerate which PDSs &mdash; not for
behavioral concentration of adversarial accounts on hosts.</p>

<h3>Finding 4: Prior pds.rip claim not supported</h3>
<p><span class="badge badge-burst">Unresolved</span></p>

<p>A prior internal note claimed that a <code>pds.rip</code> cluster was
&ldquo;the first real coordinated inauthentic behavior pattern via PDS
data&rdquo; and &ldquo;validated the hosting-locus thesis.&rdquo;</p>

<p>This claim has no support in persisted state currently examined. Driftwatch
sees 32 live accounts on <code>pds.rip</code> with zero seed presence. Labelwatch
has zero label events for those 32 DIDs, zero alerts referencing
<code>pds.rip</code>, and no hosting-locus findings table. The only persistent
reference is a provider registry suffix-match rule classifying
<code>pds.rip</code> as <code>known_alt</code>.</p>

<p>The original observation may have been a transient CLI output that was never
persisted. &ldquo;Validated&rdquo; is not supportable for a one-off result that
left no persistent trace. This is a narrow finding about one specific prior
claim, not a general assessment of data reliability.</p>

<h3>What this means going forward</h3>

<ul>
<li><strong>Do not</strong> publish seed:live ratios as &ldquo;labeled-target
concentration on hosts&rdquo; findings. That framing is wrong.</li>
<li>A reformulated analysis would need to control for labeler coverage: require
labels from multiple independent labelers, or restrict to behavior-based
label classes (spam, abuse, impersonation) rather than content-type or
enumeration labels.</li>
<li>The current data <em>can</em> answer a different, arguably more interesting
question: which labelers concentrate their output on which PDSs? That is a
governance signal about labeler behavior, which is what Labelwatch is
actually for.</li>
<li>The <a href="/about">about page</a> explains the broader thesis.</li>
</ul>

<h3>Provider registry gaps</h3>

<p>The investigation also found that Labelwatch&rsquo;s provider registry is
sparse for long-tail hosts. Of 7 hosts with elevated seed:live ratios, only 2
(<code>atproto.brid.gy</code>, <code>pds.rip</code>) had provider registry
entries. <code>skystack.xyz</code>, <code>pds.1440.news</code>,
<code>northsky.social</code>, and <code>bsky.bestofmodels.blog</code> were
unclassified. Any downstream analysis relying on provider classification would
miss them.</p>

</div>

<p class="small" style="margin-top:2rem;"><a href="/">&larr; Back to dashboard</a></p>
"""
    return _layout(
        "Claim Log \u2014 Labelwatch",
        body,
        canonical="https://labelwatch.neutral.zone/claims",
        description=(
            "Analytical findings, their methodology, and their current status. "
            "Labelwatch publishes its reasoning, not just its conclusions."
        ),
    )
