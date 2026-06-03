# Retired reference labelers

**Status**: ledger.
**Doctrine**: [authority-failure-modes.md § Reference capture](authority-failure-modes.md).

This file records labelers that were previously in the reference set and have been demoted. The point of recording the demotion explicitly is so it does not look like silent curation — retiring a reference is editorial, and editorial decisions should leave a trail.

A reference labeler is a calibration witness, not a memorial plaque. When a witness goes quiet, the reference set retires it.

Mechanics:
- Retired DIDs live in `Config.retired_reference_dids`.
- Discovery actively demotes them: `is_reference=0`, `labeler_class='third_party'`.
- The labeler stays tracked and observed; it just no longer carries reference status.

## Ledger

### labeler.hailey.at — `did:plc:saslbwamakedc4h6c5bmshvz`

- **Retired**: 2026-06-03
- **Reason**: sustained `endpoint_unreachable`; `events_7d = 0`, `events_30d = 0` at retirement; regime `inactive`.
- **Replacement**: `label.haus` (`did:plc:6ebfnuunfngxfw3rth3ewojw`) — stable regime, ~262k 7d events, low auditability risk, broad ecosystem footprint.
- **Notes**: Historically a high-volume judgment reference; included in the `flaky_reference_dids` set from at least early 2026 to preserve tracking while suppressing system-wide CRITICAL on its silences. With sustained silence and no recovery signal, the appropriate move is retirement, not continued advisory routing.
