# Labelwatch segmented-storage decision — reopened October5 horizon lane

`REJECT_SEGMENTED_SQLITE_PARQUET_DUCKDB`

Exercised source `6a88178f8d30fed13c582acccd532128c6b306ce`;
root run `28e39263-168f-46e7-8f22-25a7f29fc684`.

| Property | Decision evidence |
|---|---|
| Shared event identity/custody | QUALIFIED; unchanged from independently accepted 1fdfd028 / publication 6a88178 |
| Long-history new-segment admission | FAIL: adding one owner requires a fresh catalog hashing all old/new Parquet |
| Real committed-owner correspondence | Independent two-owner case: stale catalog refuses; fresh admission hashes both files and returns count 2 |
| 40+7-day capacity / full 40-day queries | NOT_RUN after concrete required-gate failure; no acceptance inferred |
| Prior rollover/reclamation/coverage/crash regressions | Preserved, unchanged implementation; not repeated |

The disqualifying property is **new-segment admission cost tied to all historical
payload bytes**. The requested gate explicitly disallows this dependency.
At8,192 old identities, one admission invokes8,193 full file hashes; independent
qualification confirms the same requirement in the actual custody/TierSession
path with two real committed owners. Cached queries avoid full hashes but cannot
admit newly committed history without reconstruction.

This rejects the current catalog admission design. It does not reject the
storage engines or prove incremental repair impossible. It does not reopen
engine comparison, change the qualified custody contract, or authorize production.

[Qualification](LABELWATCH-SEGMENTED-HORIZON-QUALIFICATION.md) records measurements,
provenance and limits. [Campaign report](LABELWATCH-SEGMENTED-CAMPAIGN-REPORT.md)
records exact independent acceptance, published source and custody pointers.

Existing [#7](https://github.com/unpingable/atproto-labelwatch/issues/7) owns the
bounded follow-on: adapt the existing catalog for incremental new-owner admission
and indexed/range recovery under the earned custody/completeness contract; then
complete the same three remaining gates. Do not rerun unchanged acceptance or
launch another architecture comparison. Production mutations **NONE**.

Earlier lifetime/custody rejection at 872693a remains historical evidence.
Its three custody defects were superseded by the separate qualified shared repair;
this reopened rejection is solely the still-failing catalog admission gate.

Independent acceptance: `4bf47479-ebb8-4661-85d7-ee53f6c1044e`, disposition
`ACCEPTED_CONCRETE_GATE3_REJECTION`, SHA256
`dcd7302bd8a6bf55a76ab55cf03ebe1e65b56e225b05148d046afc4874628693`.
Exact private record: `ACCEPTANCE-4bf47479-ebb8-4661-85d7-ee53f6c1044e.json`
in the owning existing horizon campaign. Review source equals exercised `6a88178`;
final publication changes documents only.
