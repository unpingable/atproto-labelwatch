# Doc filename convention

**Status**: ratified.
**Last updated**: 2026-06-03.

## The rule

> Top-level project contract files MAY use `UPPER_SNAKE.md`. Everything else uses `lower-kebab.md`.

That is the entire rule. The reason it gets a file of its own is that "tiny inconsistency now → repo folklore → CI check with a grudge six months later" is a known failure mode for naming conventions.

## What counts as a "top-level project contract file"

Files that consumers expect to find at the repo root, in their canonical-cased form, because tooling or convention looks there:

```
README.md
CHANGELOG.md
CONTRIBUTING.md
LICENSE (or LICENSE-*)
DOC_FILE_NAMES.md   — this file
```

Project-specific contract files that act like a README for a major axis are also fine in UPPER_SNAKE at the repo root, since they pair with `README.md` as orientation:

```
ARCHITECTURE.md
CLAUDE.md
NEXT.md
NON_GOALS.md
PROVENANCE.md
```

The list is not extensible by accretion. If a new file is contract-shaped, name it explicitly; if it is a doc, it goes in `docs/` and uses kebab.

## Everything else uses kebab

Anywhere below the root — `docs/`, `specs/`, `working/`, `examples/`, etc. — files use `lower-kebab.md`:

```
docs/labelers-as-testimony.md
docs/authority-failure-modes.md
docs/findings/2026-04-22-tier-threshold-validation.md
specs/gaps/gap-spec-derive-workload-isolation.md
```

## Hard rules

- **Never create both `DOC_FILE_NAMES.md` and `doc-file-names.md`.** If you find both, choose the canonical path (UPPER_SNAKE if top-level, kebab otherwise) and remove the other.
- **Case-only renames need a tmp hop on case-insensitive filesystems.** Use `git mv FOO.md tmp.md && git mv tmp.md foo.md`. Filesystems are petty about this.
- **Updating links is part of the rename.** Grep for the old filename before committing.

## Existing drift

Files in `docs/architecture/` and `specs/core/` predate this convention and currently use `UPPER_SNAKE.md`. They will be normalized opportunistically — when a doc is being edited substantively, the rename pairs cheaply. **Do not do a mass rename pass; that is a folklore-generating event in itself.**

## Scope

This convention is filed in labelwatch but is intended workbench-wide (labelwatch, driftwatch, reference-labeler, and adjacent observatories). If it earns its keep here, it gets promoted upward; until then, sibling repos may adopt it by copying this file.
