"""About page: Labelers Are a Claims Layer.

Static prose page explaining why Labelwatch exists and what labelers
actually are in the ATProto stack. No DB dependency.
"""

from .report import _layout


def render_about_html() -> str:
    """Return the full about page HTML."""
    body = """
<div class="hero">
  <p class="hero-pitch">Labelers Are a Claims Layer</p>
</div>

<div class="about-prose">

<p>Labelers matter. They are not fake. They are not ornamental. They can shape
how accounts and posts are seen, filtered, avoided, or trusted. They are also
not sovereign.</p>

<p>That distinction is where most of the confusion lives.</p>

<p>In the current stack, moderation is explicitly split across multiple layers:
network takedowns, labels from moderation services, and user controls such as
mutes and blocks. Labels are one layer in that system, not the system itself.
(<a href="https://docs.bsky.app/docs/advanced-guides/moderation">Bluesky: Labels and moderation</a>)</p>

<p>That matters because labelers are easy to overread. A label can shape whether
content is shown, warned, filtered, or avoided. It can change how an account
is perceived. It can travel as moderation metadata rather than open argument.
But a label is not a hard policy boundary, not a sandbox, not due process, and
not a final adjudication. It is a claim attached to a subject and distributed
through application surfaces.</p>

<h2>What a label actually does</h2>

<p>A label does not need to ban you to matter.</p>

<p>It only needs to make you look risky, suspect, low-trust, or not worth the
trouble. That is enough to change behavior downstream. People hesitate. Clients
filter. Other services infer. Reputation shifts.</p>

<p>So no, this is not "just metadata."</p>

<p>It is a socially consequential claims layer. That is precisely why it needs
to be described correctly.</p>

<h2>The asymmetry</h2>

<p>The public argument around labelers often treats them as though they are the
main moderation authority in a decentralized system. They are not.</p>

<p>Independent labelers mostly operate on public-surface data and reports. The
firehose exists to aggregate public data updates across the network, and
independent developers use it to build tools including labeling services.
(<a href="https://docs.bsky.app/blog/jetstream">Bluesky: Introducing Jetstream</a>)
That makes labelers useful for visible spam, harassment, content
classification, and community-specific norms. It does not put them in
possession of the richer private telemetry that often underwrites stronger
anti-abuse decisions.</p>

<p>That richer telemetry lives elsewhere. The PDS is where account creation,
authentication, and anti-abuse checks happen. Bluesky's own account-management
work makes that explicit: the PDS now handles the full account creation flow,
including anti-abuse checks.
(<a href="https://docs.bsky.app/blog/account-management">Bluesky: Network Account Management</a>)
In other words, the most decisive correlates sit upstream, at the account and
operator layer, not in the public claims layer where independent labelers
live.</p>

<p>The asymmetry is not limited to detection. It also affects process. Bluesky's
roadmap notes that independent moderators still have no private mechanism to
communicate with affected accounts across providers, and that reports and
appeals are one-way. So the layer that can visibly shape reputation is also a
layer with thin cross-provider process and weak channels for explanation or
redress.
(<a href="https://docs.bsky.app/blog/page/2">Bluesky: Blog</a>)</p>

<p>So the system produces an awkward split:</p>

<ul>
<li><strong>Operators hold the stronger evidence</strong></li>
<li><strong>Labelers carry the more visible claims</strong></li>
</ul>

<p>Visible consequences. Invisible premises.</p>

<p>That is not a bug in one service. It is a structural feature of the
stack.</p>

<h2>The danger zone</h2>

<p>A weak moderation layer can still be a powerful reputation layer.</p>

<p>That is the danger.</p>

<p>A system like this can be too weak to stop determined bad actors and still be
strong enough to shape perception. It can be too procedurally thin to deserve
being treated as truth and still be consequential enough to alter who gets
trusted, avoided, filtered, or stigmatized. And because labels are
comparatively cheap to deploy, this force can proliferate quickly.</p>

<p>Cheap enough to proliferate.<br/>
Weak enough to evade real accountability.<br/>
Strong enough to alter social reality.</p>

<p>This is the epistemic warfare window.</p>

<p>Not because every labeler is malicious. Most are not. The problem is that
the architecture allows claims to travel farther than their grounding, and
consequences to arrive before explanation, appeal, or context.</p>

<h2>Claims, not verdicts</h2>

<p>The right way to think about labelers is not as law. It is as claims.</p>

<p>A label may be careful. It may be sloppy. It may be fair, factional, stale,
useful, opportunistic, or all of the above at different times. Nothing about
machine-readable moderation metadata automatically upgrades it into truth.</p>

<p>Labelers are lenses, not law.</p>

<p>That does not make them unimportant. It makes them dangerous to overread.
The mistake is not taking labelers seriously. The mistake is taking them as
sovereign.</p>

<h2>Why "more metadata" is not a clean fix</h2>

<p>There is a real design problem here. Independent labelers are asked to
participate in abuse detection while lacking some of the operator-side signals
that would make that work less error-prone.</p>

<p>But the obvious fix &mdash; expose more anti-abuse correlates &mdash; is where things
get ugly.</p>

<p>Signals useful for adjudication are not the same thing as signals safe to
expose as public account metadata. If you promote backend anti-abuse heuristics
into profile-adjacent social facts, you risk building a new caste system of
rough trust markers, portable stigma, and socially sticky suspicion.</p>

<p>That is not transparency. That is frontendized suspicion.</p>

<p>So yes, labelers may need more structured operator-provided signals. No, that
does not mean everything useful for abuse detection belongs on the public
surface. The adjudication layer and the profile layer are not the same thing.</p>

<h2>Why observatories matter</h2>

<p>The relay layer does not solve this either. The public firehose is valuable,
but it is still a public-data layer, and relays are now explicitly non-archival
rather than full mirrors of every repository in the network.
(<a href="https://docs.bsky.app/blog/relay-sync-updates">Bluesky: Relay Updates for Sync v1.1</a>)
That means long-horizon evidence work does not fall out of the protocol for
free. Someone has to capture it, retain it, compare it, and make sense of it
over time.</p>

<p>Most of the interesting data here is not event-shaped. It appears over
time.</p>

<p>The important questions are usually not what label was applied today, who got
into one fight, or which screenshot went viral. They are things like:</p>

<p>A labeler that goes silent, turns flaky, or narrows scope without notice
changes the practical moderation commons whether anyone announces it or
not.</p>

<ul>
<li>Which labelers go dark</li>
<li>Which become flaky</li>
<li>Which widen or narrow their scope</li>
<li>Which collide repeatedly with others</li>
<li>Which cluster around particular parts of the network</li>
<li>Which continue projecting authority while their own operational footing
    degrades</li>
</ul>

<p>That kind of truth is longitudinal. It has to be earned through
persistence.</p>

<p>A snapshot gives you discourse. Time gives you behavior.</p>

<p>And in a system where the public memory layer is thin, non-archival, or
procedurally awkward, someone has to do the annoying work of remembering.</p>

<h2>Why Labelwatch exists</h2>

<p>Labelwatch is not here to become the authority that settles what every label
means.</p>

<p>It is here to make the claims layer legible over time: who is labeling, how
often, with what stability, from what locus, under what health conditions, and
with what observable changes. If labelers are going to shape perception, then
their own behavior should be observable too. If they are going to emit standing
claims about others, then they should not be exempt from standing observation
themselves.</p>

<p>In a stack where visible consequences often rest on invisible premises,
memory is not ornamental. It is the beginning of accountability.</p>

<h2>The blunt version</h2>

<p>Labelers are not a gimmick.</p>

<p>They are a cheap, non-sovereign moderation and reputation layer with enough
force to matter and too little grounding to be mistaken for truth.</p>

<p>Treat them as claims. Track them like institutions. Do not confuse them for a
control plane.</p>

<p>If the protocol wants labelers to remain plural rather than slide into a
distributed insinuation layer, then the answer is not to pretend they are
sovereign. It is to treat them as what they are: a non-sovereign moderation and
reputation layer that needs legibility, provenance, and sustained observation
precisely because it does not carry final authority.</p>

<p>That is the claim. And that is why this site exists.</p>

</div>

<p class="small" style="margin-top:2rem;"><a href="/">&larr; Back to dashboard</a></p>
"""
    return _layout(
        "About \u2014 Labelers Are a Claims Layer",
        body,
        canonical="https://labelwatch.neutral.zone/about",
        description=(
            "Labelers are a cheap, non-sovereign moderation and reputation layer "
            "with enough force to matter and too little grounding to be mistaken "
            "for truth. Labelwatch exists to make that layer legible over time."
        ),
    )
