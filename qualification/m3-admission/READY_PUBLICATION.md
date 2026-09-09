# Complete writer-readiness publication

VM008 failed when the shared fixture reader opened a visible readiness filename
before the application's writer had finished writing it. Both retained records
were complete later. Original VM evidence remains a failed qualification, not
retroactively accepted.

`wait_before_writer_start`, used by actual main and discovery entrypoints, now
writes a newly allocated `.ready-pending-*` file in the enrolled directory,
flushes and fsyncs its contents, atomically creates the final hard link without
replacement, removes only its own pending name, and fsyncs the directory before
return. Existing final names, including malformed files or symlinks, are never
overwritten. No rename fallback or repeated maintenance effect is introduced.
Loss before publication can leave a private nonmatching file; this is not ready.
After publication readers can see complete synced contents; the writer's return
additionally establishes successful directory sync. Reader visibility alone is
not a claim that the publisher has returned or that host power-loss was tested.

The parent remains enrolled/protected from unrelated pathname changes. Same-dir
hard-link support is required; unsupported publication refuses. This is a bounded
Linux application readiness operation, not a general filesystem framework.

Strict readers remain unchanged: ordinary and controller `Case.start_writers`,
fixture enrollment/capture, maintenance verification, and factual observation.
They still bind exact PID/start, role, hold and verification identity. Malformed
final records are not retried as though publication were incomplete. Private
filenames do not match any `*.ready.json` reader.

Deterministic tests cover partial private writes, exact complete publication,
simultaneous final-name collision, existing valid/malformed/symlink refusal,
file-sync failure and sync ordering. Existing real two-writer and native factual
qualification controls must also pass. New source/installed-wheel/VM qualification
is required; prior70d and VM008 evidence retain their original subjects.
