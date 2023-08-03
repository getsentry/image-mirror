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
        source='altinity/clickhouse-server',
        tag='21.8.13.1.altinitystable',
        digests=(
            'sha256:125d2ea49c298515c46784d202a2bd4dde05157c85a76517afc2567f262ab335',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='checkr/flagr',
        tag='latest',
        digests=(
            'sha256:407d7099d6ce7e3632b6d00682a43028d75d3b088600797a833607bd629d1ed5',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='confluentinc/cp-kafka',
        tag='5.5.13-1-ubi8',
        digests=(
            'sha256:5da5ef4d763574d42d81be5bba1df192a30da8e34ec33daaa54bd1046f19e36a',  # noqa: E501
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
    Image(
        registry='registry-1.docker.io',
        source='confluentinc/cp-zookeeper',
        tag='5.5.13-1-ubi8',
        digests=(
            'sha256:ad410ca75d779512abc7255a8360813c5860bddbf438497937c2a37d9618ce95',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='confluentinc/cp-zookeeper',
        tag='6.2.0',
        digests=(
            'sha256:9a69c03fd1757c3154e4f64450d0d27a6decb0dc3a1e401e8fc38e5cea881847',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/memcached',
        tag='1.5-alpine',
        digests=(
            'sha256:48cb7207e3d34871893fa1628f3a4984375153e9942facf82e25935b0a633c8a',  # noqa: E501
            'sha256:fab6966ea6418a38663d63aa904b4de729cdf51cd90c22a70ea4d234cb4b37a4',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='14',
        digests=(
            'sha256:55247e19106a3998b54d8f06d0c5070285a7f47b20f34a56c23ed9a75573d0e5',  # noqa: E501
            'sha256:1050153f46ad3fa89c27f3002e6ebf15fef0ceedf1d21f49a982e1364a6c2e3d',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='14-alpine',
        digests=(
            'sha256:4a0c9e5e23520c3403abf81b4f74b8f820f5f5e64654f51f5e4c92ece160a519',  # noqa: E501
            'sha256:5a7565060898c9fd7629bb415697093d34a51ed381f5e7f23e2398fdfa4389ac',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='9.6',
        digests=(
            'sha256:15055f7b681334cbf0212b58e510148b1b23973639e3904260fb41fa0761a103',  # noqa: E501
            'sha256:decbf20be3383f2ba0cfcf67addd5b635d442b4739132e666ed407b6f98abfc6',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='9.6-alpine',
        digests=(
            'sha256:84e6f6c787244669a874be441f44a64256a7f1d08d49505bd03cfc3c687b6cfd',  # noqa: E501
            'sha256:2cde527ea258b21a2966bd18a604b320bc89b47f378ca75cb57596c5a2d4f2c5',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/rabbitmq',
        tag='3-management',
        digests=(
            'sha256:74470f3aa108cc2dc2aaaa7c5cc5ec3d7b282d30e4037185fe2720a85b42a116',  # noqa: E501
            'sha256:c80c4d52e1f8bb8327dbee806894db94f2a2fbc31e9ecace5da1dce67ec8a5ad',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/redis',
        tag='5.0-alpine',
        digests=(
            'sha256:1b24e5253e866e60e320446bd588407df499936bdc7d89fa52cd2772a4e3a162',  # noqa: E501
            'sha256:3752d9ab7e7abb59bc2a7c08323812af104251861a0037925883dd7af8ca2602',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/redis',
        tag='7.0.8-bullseye',
        digests=(
            'sha256:87583c95fd2253658fdd12e765addbd2126879af86a90b34efc09457486b21b1',  # noqa: E501
            'sha256:2577ec9ba2a7a6f10a686b8e2cd354ee4e1a05688374cdc566c1427516d47c8f',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='yandex/clickhouse-server',
        tag='20.3.9.70',
        digests=(
            'sha256:932ef73994dd4b6507a55a288c5ee065aae8e77e61ee569512a76a65eddbe2c3',  # noqa: E501
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

        src = re.sub(r'IMAGES = \(\n( +.+\n)+\)', '\n'.join(lines), src)

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
                if e.code not in {403, 404}:
                    raise
                else:
                    target_digest_info = []

            target_digests = [digest for _, digest in target_digest_info]
            todo = sorted(frozenset(img.digests) - frozenset(target_digests))
            if not todo:
                continue
            elif args.dry_run:
                print(f'would sync {img.display}...')
                continue
            else:
                print(f'syncing {img.display}...')

            manifest = f'ghcr.io/{dest_img}:{img.tag}'
            for i, digest in enumerate(img.digests):
                src = f'{img.registry}/{img.source}@{digest}'
                dest = f'{manifest}-digest{i}'

                subprocess.check_call(('docker', 'pull', '--quiet', src))
                subprocess.check_call(('docker', 'tag', src, dest))
                subprocess.check_call(('docker', 'push', '--quiet', dest))

            subprocess.check_call((
                'docker', 'manifest', 'create', manifest,
                *(f'{manifest}-digest{i}' for i in range(len(img.digests))),
            ))
            subprocess.check_call(('docker', 'manifest', 'push', manifest))
    else:
        raise NotImplementedError(args.command)

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
