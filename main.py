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

ARCHS = frozenset(('amd64', 'arm64', 'arm64/v8'))

LIST = 'application/vnd.docker.distribution.manifest.list.v2+json'
SINGLE = 'application/vnd.docker.distribution.manifest.v2+json'
INDEX = 'application/vnd.oci.image.index.v1+json'


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
            'Accept': f'{LIST}, {INDEX}, {SINGLE};q=.9',
        },
    )
    resp = urllib.request.urlopen(req)
    ret = json.load(resp)
    if resp.headers['Content-Type'] in {LIST, INDEX}:
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
        source='altinity/clickhouse-server',
        tag='22.3.15.34.altinitystable',
        digests=(
            'sha256:5a67ec149acc13e3d87ed1e3b94b4ada6f0acdc75145724959bbd8c0a6f18410',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='22.8.15.25.altinitystable',
        digests=(
            'sha256:5a67ec149acc13e3d87ed1e3b94b4ada6f0acdc75145724959bbd8c0a6f18410',  # noqa: E501
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
        tag='6.2.0',
        digests=(
            'sha256:97f572d93c6b2d388c5dadd644a90990ec29e42e5652c550c84d1a9be9d6dcbd',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='confluentinc/cp-kafka',
        tag='7.5.0',
        digests=(
            'sha256:69022c46b7f4166ecf21689ab4c20d030b0a62f2d744c20633abfc7c0040fa80',  # noqa: E501
            'sha256:ba503c5f09291265b253f2c299573d96433b05b930c2732f5c13b82056c824dd',  # noqa: E501
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
            'sha256:8769dc9a6cc47201df7112475f22c400cd734880b51511efbd2b581f19ebb59a',  # noqa: E501
            'sha256:2c301a800817b23763b976b80e7c3579284afcc9d9ff6f968ecb524da48383a1',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='14-alpine',
        digests=(
            'sha256:8dc41c1f358669e2006559bc817cd7c1daaa328d1ee8370ac209167f3b6a894f',  # noqa: E501
            'sha256:583063ce031a46cdff262cdd88f22c15a829ce5814957946ccfdf20ef66d2de6',  # noqa: E501
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
            'sha256:e2de39b422da7d4b71b956f786b0231493b5c52cd2879b3bba1993a2037d3498',  # noqa: E501
            'sha256:6a8a10b97d1902b2a5edc1179640329767609d3fc21a2893e0ad96e778de1452',  # noqa: E501
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
        source='redpandadata/redpanda',
        tag='v22.3.23',
        digests=(
            'sha256:5bb4da6e91eeaeecc693289bcc5fa91c46dc68b3b128e878bb7d2a221ad65c3b',  # noqa: E501
            'sha256:22fbd63c5b7480c584fe6f3408e92cad01e2d1c2b47128e680146ce9a2500d52',  # noqa: E501
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
