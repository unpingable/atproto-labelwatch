# Missing-state refusal and initial collection timer

Root approved these narrow corrections on November 3. The consumers are the
existing recent collector/store, explicit `recent_init`, and substitute
watcher's L2 evaluator and transition flow. No production action is included.

An uninitialized or emptied store must refuse ordinary writers before creating
a lock or SQLite state. Keep the existing writer lock for initialized stores.
Use a noncreating SQLite open for existing `state.sqlite`; only explicit store
creation may create it. Every lock helper caller operates on existing state,
including daily maintenance and archives; no initializer uses the helper.
Do not remove existing locks, tolerate partial state,
or claim safe concurrent store replacement. Test the real collector, lock,
SQLite connection, fresh init, and concurrent writer fencing.

A collection timer with no last trigger needs bounded initial scheduling
evidence. Read its active-entry and next-due timestamps. A loaded, active timer
with a valid start and first due no more than 600 seconds after start may wait
silently for at most 600 seconds from active entry; a moving next-due must not
extend that fixed deadline. After 600 seconds report `collection-not-scheduled`.
A pending initial
timer is `skip`, including its retained incident observation, so restarting
cannot resolve an existing condition or label it absent. Only a
fresh actual trigger resolves a scheduling incident. Missing or inconsistent
timestamps are unknown; two failed observed invocations still notify first.

Alternatives considered: checking only at collector entry misses other writer
paths; removing lock files after refusal risks splitting the lock domain;
using SQLite create mode can recreate schema-less state. A grace measured from
each watcher evaluation or a moving next-due can suppress notices indefinitely.
The chosen shared guards and anchored bounded wait address those mechanisms.

Discovery policy remains unchanged: latest successful attempt and last full
scan must each be at most 600 seconds old. The observed 612-row, seven-page
cycle takes about 14 minutes at ordinary cadence, so accepted intermediate
pages can coexist with discovery.current=false. Document this truthful bound;
changing cadence or currentness policy is separate work.

Original production epoch, 128 gap rows, overflow, notices and acceptance
outcomes remain evidence. Small real-store and finite watcher tests establish
only these bounded local transitions; practical tests suffice without a model.
Root owns integration, deployment, tracking and independent acceptance.
