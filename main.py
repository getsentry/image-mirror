from __future__ import annotations

import argparse
import ast
import functools
import json
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from typing import Mapping
from typing import NamedTuple

ARCHS = frozenset(('amd64', 'arm64'))

LIST = 'application/vnd.docker.distribution.manifest.list.v2+json'
SINGLE = 'application/vnd.docker.distribution.manifest.v2+json'


def _parse_auth_header(s: str) -> dict[str, str]:
    bearer = 'Bearer '
    assert s.startswith(bearer)
    s = s[len(bearer):]

    ret = {}
    for part in s.split(','):
        k, _, v = part.partition('=')
        v = ast.literal_eval(v)
        ret[k] = v
    return ret


@functools.lru_cache(maxsize=None)
def _auth_challenge(registry: str) -> tuple[str, Mapping[str, str]]:
    try:
        urllib.request.urlopen(f'https://{registry}/v2/', timeout=5)
    except urllib.error.HTTPError as e:
        if e.code != 401 or 'www-authenticate' not in e.headers:
            raise

        auth = _parse_auth_header(e.headers['www-authenticate'])
    else:
        raise AssertionError(f'expected auth challenge: {registry}')

    realm = auth.pop('realm')
    auth.setdefault('scope', 'repository:user/image:pull')

    return realm, auth


def _digests(registry: str, image: str, tag: str) -> list[tuple[str, str]]:
    realm, auth = _auth_challenge(registry)
    auth = {k: v.replace('user/image', image) for k, v in auth.items()}

    auth_url = f'{realm}?{urllib.parse.urlencode(auth)}'
    token = json.load(urllib.request.urlopen(auth_url))['token']

    req = urllib.request.Request(
        f'https://{registry}/v2/{image}/manifests/{tag}',
        headers={
            'Authorization': f'Bearer {token}',
            # annoyingly, even if we only "Accept" the list, docker.io will
            # send us a single manifest
            'Accept': f'{LIST}, {SINGLE};q=.9',
        },
    )
    resp = urllib.request.urlopen(req)
    ret = json.load(resp)
    if resp.headers['Content-Type'] == LIST:
        return [
            (manifest['platform']['architecture'], manifest['digest'])
            for manifest in ret['manifests']
        ]
    elif resp.headers['Content-Type'] == SINGLE:
        blob = ret['config']['digest']
        req = urllib.request.Request(
            f'https://{registry}/v2/{image}/blobs/{blob}',
            headers={'Authorization': f'Bearer {token}'},
        )
        ret = json.load(urllib.request.urlopen(req))
        return [(ret['architecture'], resp.headers['Docker-Content-Digest'])]
    else:
        raise NotImplementedError(resp.headers['Content-Type'])


class Image(NamedTuple):
    registry: str
    source: str
    tag: str
    digests: tuple[str, ...] = ()

    @property
    def display(self) -> str:
        return f'{self.registry}/{self.source}:{self.tag}'

    def update(self) -> Image:
        digests = tuple(
            digest
            for arch, digest in _digests(self.registry, self.source, self.tag)
            if arch in ARCHS
        )
        return self._replace(digests=digests)


IMAGES = (
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='21.6.1.6734-testing-arm',
        digests=(
            'sha256:9a4516444fef9e0f11ee6b2de716d3b97b36bf05d9cc2d44c4596cfb0584dea6',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='confluentinc/cp-kafka',
        tag='6.2.0',
        digests=(
            'sha256:97f572d93c6b2d388c5dadd644a90990ec29e42e5652c550c84d1a9be9d6dcbd',  # noqa: E501
        ),
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=('update', 'sync'))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if args.command == 'update':
        imgs = []
        for img in IMAGES:
            print(f'updating {img.display}...')
            imgs.append(img.update())
        imgs.sort()

        lines = ['IMAGES = (']
        for img in imgs:
            lines.append('    Image(')
            for field in img._fields:
                if field != 'digests':
                    lines.append(f'        {field}={getattr(img, field)!r},')
            lines.append('        digests=(')
            for digest in img.digests:
                lines.append(f'            {digest!r},')
            lines.append('        ),')
            lines.append('    ),')
        lines.append(')')

        lines = [s if len(s) < 80 else f'{s}  # noqa: E501' for s in lines]

        with open(__file__) as f:
            src = f.read()

        src = re.sub(r'IMGS = \(\n( +.+\n)+\)', '\n'.join(lines), src)

        if args.dry_run:
            print(src)
        else:
            with open(__file__, 'w') as f:
                f.write(src)
    elif args.command == 'sync':
        for img in IMAGES:
            dest_img = f'getsentry/image-mirror-{img.source.replace("/", "-")}'

            try:
                target_digest_info = _digests('ghcr.io', dest_img, img.tag)
            except urllib.error.HTTPError as e:
                if e.code != 403:
                    raise
                else:
                    target_digest_info = []

            target_digests = [digest for _, digest in target_digest_info]
            todo = sorted(frozenset(img.digests) - frozenset(target_digests))
            if not todo:
                continue
            elif args.dry_run:
                print(f'would sync {img.display}:')
                for digest in todo:
                    print(f'- {digest}')
                continue
            else:
                print(f'syncing {img.display}...')

            for digest in todo:
                src = f'{img.registry}/{img.source}@{digest}'
                dest = f'ghcr.io/{dest_img}:{img.tag}'

                subprocess.check_call(('docker', 'pull', src))
                subprocess.check_call(('docker', 'tag', src, dest))
                subprocess.check_call(('docker', 'push', dest))
    else:
        raise NotImplementedError(args.command)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
