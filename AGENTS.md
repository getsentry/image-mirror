# AGENTS.md

`main.py` mirrors the images pinned in its `IMAGES` tuple from Docker Hub to
`ghcr.io/getsentry/image-mirror-*`. See [README.md](README.md) for naming and
the add-an-image workflow.

## Commands

- `python3 main.py update [--dry-run]`: re-resolve every entry's digests and
  rewrite `IMAGES` in place.
- `python3 main.py sync [--dry-run]`: push missing digests to ghcr.io. Needs
  `docker` and a ghcr.io login. CI ([`.github/workflows/main.yml`](.github/workflows/main.yml))
  runs it.
- `pre-commit run --files <files>`: lint and format.

## Invariants

- `update` finds `IMAGES` with a regex. Keep `IMAGES = (` and its closing `)`
  at column 0 with only indented, non-blank lines between them. Otherwise
  `update` exits with an error.
- Pin digests only for architectures in `ARCHS`.
