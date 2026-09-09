# Fixture request timestamp correspondence

VM006 historical cleanup failed before unit installation: both request evaluation
times were emitted with `+00:00`, while native NQ's typed Chrono serialization
returned `Z`. The receipt was ESTABLISHED; the two strings represented the same
instants. Original VM evidence remains unchanged, not retroactively accepted.

The app capture producer now emits UTC RFC3339 in Chrono AutoSi spelling:
whole seconds, three fractional digits for exact milliseconds, otherwise six
for Python microseconds. Offset conversion preserves the instant. Naive inputs
and string precision finer than microseconds are refused, never truncated.
Raw observation bytes and strict consumer request equality remain unchanged.
This affects shared pre/post evaluation time and both cleanup evaluation times;
every cleanup-dependent ordinary/controller case uses that same capture path.

`test_m3_capture_request_time.py` covers exact precision, positive/negative offset
date crossings, invalid/naive/finer input refusal, and optional real retained
VM006→pinned native NQ qualification. That native control reproduces the original
mismatch, changes only producer timestamp spelling, and obtains the exact same
receipt. It does not establish current freshness or perform cleanup.

Only the Python capture script and its tests/docs change. The compiled admission
resolver source is unchanged; existing driver package applicability is limited
to those identical compiled bytes. Updated app-source/fixture pins and fresh
integration qualification are still required. No VM or production acceptance
is supplied by this note.
