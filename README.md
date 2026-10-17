image-mirror
============

Sentry development pulls images from several Docker registries, and Docker Hub
is the flakiest. This repo mirrors pinned Docker Hub images into GitHub's
registry (ghcr.io) so you have one fewer place to pull from.

## Use a mirrored image

Each entry in `IMAGES` in [`main.py`](main.py) is published as:

```
ghcr.io/getsentry/image-mirror-<source with "/" replaced by "-">:<tag>
```

For example, `library/postgres` tag `14-alpine` is
`ghcr.io/getsentry/image-mirror-library-postgres:14-alpine`.

Only the pinned `amd64` and `arm64` digests are mirrored.

## Add or update an image

1. Add an `Image(registry=..., source=..., tag=...)` entry to `IMAGES` in
   `main.py`.
2. Run `python3 main.py update` to fill in the digests and sort the list.
3. Open a pull request.

`update` re-resolves the digests of every entry, not only the new one. Entries
with mutable tags, such as `latest` or `14`, pick up new digests. Run
`python3 main.py update --dry-run` to print the rewritten file without saving
it.

CI runs `python3 main.py sync --dry-run` on pull requests to list the images it
would push. After merge, CI runs `sync` on `main` and pushes each image that is
missing one of its pinned digests on ghcr.io.

## License

See [LICENSE](LICENSE).
