# Labelwatch retirement manifest — verified, awaiting owner approval

**No deletion authorized.** Scope is exactly the ten paths below on `192.46.223.21`. No directories or other files are included.

| Exact path | Allocated bytes |
|---|---:|
| `/var/lib/labelwatch/labelwatch.db` | 0 |
| `/var/lib/labelwatch/labelwatch_state.db` | 0 |
| `/var/lib/labelwatch/generations/generation-2/labelwatch.db` | 45,054,324,736 |
| `/var/lib/labelwatch/generations/generation-2/labelwatch.db-wal` | 14,606,336 |
| `/var/lib/labelwatch/generations/generation-2/labelwatch.db-shm` | 32,768 |
| `/var/lib/labelwatch/generations/generation-2/labelwatch_state.db` | 293,519,360 |
| `/var/lib/labelwatch/generations/generation-2/build-result.json` | 4,096 |
| `/var/lib/labelwatch/generations/generation-2/.labelwatch-generation` | 4,096 |
| `/var/lib/labelwatch/cold/catalog-cc5ab1c4abdbe25a/frontdoor-cold-catalog.sqlite` | 5,529,505,792 |
| `/var/lib/labelwatch/cold/catalog-cc5ab1c4abdbe25a/frontdoor-cold-catalog.sqlite.manifest.json` | 77,824 |

Expected recovery: **50,892,075,008 bytes (47.397 GiB)**. Actual root free space will be measured before/after removal; concurrent tenant changes will not be attributed to this operation. At 05:05Z root had **30,222,680,064 bytes** free; this removal projects **81,114,755,072 bytes (75.544 GiB)** free before concurrent changes. The existing minimum-free-space requirement remains **32 GiB**.

## Custody and historical access

Generation archive: `/tank/nfs/labelwatch-archive/2026-10-06/generation-2-final/` — **verified and sealed** at 2026-10-10T05:21Z. All six file hashes and five receipt hashes match; local database/WAL recovery passed SQLite `quick_check` and counted **45,137,618 events**, from **2026-08-15 to 2026-10-06**. Previous transfer/setup failures remain recorded; no large transfer was repeated.

Cold archive: `/tank/nfs/labelwatch-archive/deltas/delta-20261003-001/` — **verified**. Production, archive and restored-copy hashes match; the local catalog passes SQLite `quick_check`.

The NAS is mounted from `192.168.69.10:/tank/nfs`; production uses `/dev/sda` on a different host. Recovery uses copies on crow `/data`, independently of production root. Historical access remains offline SQLite access: copy `labelwatch.db` and its matching `labelwatch.db-wal` together to a local filesystem, then query that copy. State and cold-catalog databases can be copied and queried separately. Never open the archived WAL database directly on NFS. Existing historical gaps and generation-build limitations remain recorded.

Full hashes, exact inode/size identities and recovery receipts are in [RETIREMENT-TARGETS-20261010.json](RETIREMENT-TARGETS-20261010.json), SHA256 `eb8fe7ba75692a13e443347ad2787d50d4f798dde36585c5a38c52f5ce376ff0`. The separately restored state database also passed `quick_check`. Archive custodian: repository owner `jbeck` through the existing crow/NAS access.

## After approval

Keep production held; verify the exact targets still match the manifest and have no live consumers. Fence obsolete services/timers using the existing cutover procedure. Remove the two named symlinks and eight named regular files only. Measure reclaimed space, recheck the existing 32 GiB reserve and resume the qualified replacement deployment path under the existing authority.

Publication files, logs, incident evidence, releases, configuration, other tenants and all archive copies remain untouched by this removal. Restoring historical data onto production later requires sufficient free space; offline access on crow remains available.

This manifest does not authorize deletion. The owner must approve its final committed revision after custody verification.
