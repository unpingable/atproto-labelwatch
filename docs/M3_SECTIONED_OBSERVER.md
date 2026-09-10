# M3 sectioned operational observer

Status: local implementation candidate. It has not contacted production, does
not authorize an occurrence, and does not authorize provisioning or
maintenance. B007 and B008 remain terminal consumed occurrences.

This source-owned observer replaces the campaign-local monolithic observation
shape which let optional configuration-census refusal prevent retention of
capacity facts. The required capacity and storage-topology section now takes
and closes two cuts before any optional section runs. Optional release,
configuration, unit-file, process/descriptor, SQLite-header, and later
corroboration sections each report `COMPLETE`, `REFUSED`, or `NOT_OBSERVED`
without changing the closed required disposition.

The required cuts report `lstat` identity, logical and allocated bytes for the
enrolled database, WAL, SHM, their parent, backup destination, and release
root; `statvfs` capacity; the exact mount source, point, root, filesystem type,
and major:minor identity; and bounded sysfs device ancestry. A missing locally
observable device locator, parent layer, or UUID remains explicit. The
database, parent, backup destination, and release root must exist. WAL and SHM
absence is an observation, not proof that no concurrent writer exists.

## Read and output boundaries

Defaults are part of the candidate contract:

| Section | Entry bound | Content-read bound | Other bounds |
| --- | ---: | ---: | --- |
| each required cut | enrolled paths/devices only | 2 MiB | 15 s; two cuts before optional work |
| release | 8,192 admitted; at most one bounded refusal probe | 256 MiB aggregate, 64 MiB/file | 512 KiB path bytes; 3 MiB section output; 60 s |
| configuration | 64 | 16 MiB aggregate, 4 MiB/file | 64 KiB path bytes; 256 KiB output; 20 s |
| unit files | 128 | 16 MiB aggregate, 4 MiB/file | 128 KiB path bytes; 512 KiB output; 20 s |
| processes | 1,024 | 128 MiB aggregate | 20,000 descriptors; 1,024 matches; 32 KiB argv and 1 MiB maps/process; 512 KiB output; 20 s |
| SQLite header | one exact file | exactly 100 bytes | header-only claim |
| post-optional cut | enrolled paths/devices only | 2 MiB | corroboration only; cannot reopen required section |

The maximum declared content-read load is therefore 442,499,172 bytes: three
2 MiB required/corroboration cuts, 256 MiB release, 16 MiB configuration, 16
MiB unit files, 128 MiB process data, and 100 header bytes. This is a ceiling,
not an assertion that all bytes will be read. Metadata operations are bounded
separately by the table's entry counts and the enrolled device ancestry.

Hashed file contents are read under `O_NOFOLLOW`; the result retains the digest,
byte count, and identity but never the raw bytes. Directory and non-regular
entries are metadata-only. Symlinks encountered during an optional tree census
are recorded and not followed. A symlink at an enrolled required path refuses
the required section. No one-byte content probe crosses a content ceiling; if
EOF cannot be established within the remaining allowance, the section refuses
at the ceiling. Release traversal admits at most 8,192 entries and observes at
most one additional directory-entry probe solely to establish refusal; it does
not eagerly enumerate a larger directory. Process argv, maps, and fdinfo are hashed or interpreted
only for bounded database-descriptor evidence; raw values and descriptor
targets are not returned.

Overall JSON is capped at 4 MiB. Optional detail is explicitly omitted before
required facts or section dispositions. A configured limit too small to retain
required facts yields a compact refusal rather than oversized output.
Unexpected filesystem errors are confined to the affected optional section;
they cannot discard a completed required cut or prevent later independent
sections from reporting their own dispositions.

The module contains no SQLite API, subprocess execution, systemd action,
network operation, retry, file replacement, deletion, provisioning, backup, or
maintenance operation. The only CLI output is JSON on stdout.

## Invocation shape

The source module is self-contained and may be executed with Python 3.10 or
newer after its exact bytes and arguments are independently reviewed:

```text
python3 sectioned_observer.py \
  --database <exact-enrolled-database> \
  --backup-destination <exact-enrolled-backup-directory> \
  --release-root <exact-enrolled-release-root> \
  --configuration-file <exact-file> ... \
  --unit-file <exact-file> ...
```

A later occurrence proposal must bind the exact source hash, exact argument
list, interpreter, host identity, transport, timeouts, output retention, and
all content reads described above. It must grant the necessary bounded content
reads expressly. This document and its tests create no B009 or successor
authority.
