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
        tag='22.8.15.25.altinitystable',
        digests=(
            'sha256:99d52fc10915136234a3b902b16d53f0ee80cd4f9149064acc30c89e54d06cf9',  # noqa: E501
            'sha256:2935d3849fcdf3c9a24883522630758a6f017c7b48a252f6e54e8fc188ccd0cc',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='24.8.11.51285.altinitystable',
        digests=(
            'sha256:65b4fed146dd9fa4fc7b0eff17adbeb3c1eb3e5d3c37fc2f635913eafc22b7a5',  # noqa: E501
            'sha256:9730299fc9d9728e5e23a266e54a65d267acd565301fbb3d21092f2c6c3f0ea3',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='24.8.14.10459.altinitystable',
        digests=(
            'sha256:931fa5676481358dab1b518040f31001aca088cd44deb4c901196d28097131fc',  # noqa: E501
            'sha256:6468b5dd3f6fcf768850e54f945e51567852512587737ed003588e6ff6406926',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='25.3.6.10034.altinitystable',
        digests=(
            'sha256:61139339f342ffec40111497eaf3ccb124c724c2c6dc6a1cf9ed9897aa8e0293',  # noqa: E501
            'sha256:779e5d1b07e776652c8ae96af3596bc11021ec155d0e3aa6289fd739b4f85ae1',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='25.3.8.10041.altinitystable',
        digests=(
            'sha256:ae31c277673ca04facee2a2812f57ec097a53028c2eef3810968faabe752f8c3',  # noqa: E501
            'sha256:fe43c34d28a7f1d9b539de2cec158bc5dac3c3bb6f6e2dda0963728f327db06e',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='25.8.16.10001.altinitystable',
        digests=(
            'sha256:27341ab1d7988babfa011933d94c8531b2cb6b1cace7b6813ded7470b4b6e074',  # noqa: E501
            'sha256:d880b9caff968d7530161670a5665decb0b5cf1dd40f75efe80a80c8145f68d5',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='altinity/clickhouse-server',
        tag='26.3.33.10001.altinitystable',
        digests=(
            'sha256:df474c31b4c091e2c64d358b9a48676b79c9f2cdb01235ed93573a564b037837',  # noqa: E501
            'sha256:b948d7258c378dd0d01fb9a5f98146eb80f3b7cd962c36b29ccb0d38782ded5d',  # noqa: E501
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
        source='clickhouse/clickhouse-server',
        tag='26.8.1.2041',
        digests=(
            'sha256:24292a4b0041cbdefb3ac8dc071f250badacb2a1959c8606dc2c916e72a3188f',  # noqa: E501
            'sha256:70edee918872c93de512765edafa68758dd93614d77ff9e7c64e6585c4300562',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='clickhouse/clickhouse-server',
        tag='26.8.10.6',
        digests=(
            'sha256:ef0af643f169121268267125a84b0d67bf390b921b3efee62e8d772714eb2f16',  # noqa: E501
            'sha256:4842aee4da0c9679ab7558de0685dbec667c49a0b55999378fca7cb87e49b4fd',  # noqa: E501
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
        source='confluentinc/cp-kafka',
        tag='7.9.0',
        digests=(
            'sha256:e6b87a4a8ca07aadba9c04d86515a340f67cd11ca6160c9b07205f3d88dfb5f1',  # noqa: E501
            'sha256:0ec55a5b2d80222b7ca87d3d0716347151b81fa385d73760ac73a079583e328c',  # noqa: E501
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
        source='google/cloud-sdk',
        tag='588.0.0',
        digests=(
            'sha256:f36908a982a2d59b7b9d496bcd11371e58956ff55b9748dc08b5b2d20303eac0',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/alpine',
        tag='3.16.2',
        digests=(
            'sha256:1304f174557314a7ed9eddb4eab12fed12cb0cd9809e4c28f29af86979a3c870',  # noqa: E501
            'sha256:922df7f9352943c7447dbc07c53563e6971fce3100d2ef2b8368b1ba9aac605d',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/alpine',
        tag='3.22.1',
        digests=(
            'sha256:eafc1edb577d2e9b458664a15f23ea1c370214193226069eb22921169fc7e43f',  # noqa: E501
            'sha256:4562b419adf48c5f3c763995d6014c123b3ce1d2e0ef2613b189779caa787192',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/debian',
        tag='12.15-slim',
        digests=(
            'sha256:a4672c0cb26fbdde88e38fa2dfb6c681942306680e41e4378b28770b6e79ee91',  # noqa: E501
            'sha256:a1b86db52ce3daef089e45aabe36dfec4091f82464c25c1fdcf03de197cbe82a',  # noqa: E501
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
        source='library/node',
        tag='22.23.3-alpine',
        digests=(
            'sha256:2c752226d477b4a886378baa95b9af252be59301b725fdb0b7e15208131505a8',  # noqa: E501
            'sha256:85cdd100016a2e09927c0776bb7ebebcfefce5cd0af49893ada91cbc4d78ee22',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='14',
        digests=(
            'sha256:b7bc018398a6dcf4055b5d00ab3cbf08785953a2757a1822b38e60badd35ad95',  # noqa: E501
            'sha256:50d1e1b3c0b3e951d53be779310d537014b8f20eb09b29c54c5dc2f2940e4789',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='14-alpine',
        digests=(
            'sha256:b66f1f819fe87de88ddfaff72035634913de17b066ab50f6cc0e765c168e4370',  # noqa: E501
            'sha256:b59c0f24d10e0fa0dec3d342e33334ddeb8a3c0beb541c336164af2adfbe31dc',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='15.19',
        digests=(
            'sha256:a5f9ead8ed7cb25abc36bea51fb9bb2be8d5579ed4ef3d28876027e438391ff0',  # noqa: E501
            'sha256:7edb00ef081f792e6b08ff2990bb72de1b842581de7cfe516d63c9ce293b26ab',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='17',
        digests=(
            'sha256:c45202f004c3a3188d1735ccb0967a0c6b1169a9d08e732c854d08f8fe456343',  # noqa: E501
            'sha256:485356b847a5c89efa454098485b07b9ed663096446361ec0fa997974666500f',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='17-alpine',
        digests=(
            'sha256:5a6fcbc5d93831991d2386fa634509b3c49a1ac5ffb70c13c2322840f821d7e7',  # noqa: E501
            'sha256:8e84d8ceac078763474c89cac6d2032899c050ce9f8b9c259b88e1445cd14445',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='18',
        digests=(
            'sha256:41da01536bc3ae26308cefb0c57235e7488001360bdb15191eb0b7955b570299',  # noqa: E501
            'sha256:f6902dfddea256a7bf788aefabbf10e5305ea348693caa4c741795559044001a',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/postgres',
        tag='18-alpine',
        digests=(
            'sha256:15b46a9c5a6b361eb4c0ce8d689365bf49fbf6802e615dce4e5e2326b3213e15',  # noqa: E501
            'sha256:563d9a314daa3a9f8e3249e217514210a747970c36d50d83ae5e9dc6749fe354',  # noqa: E501
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
        source='library/python',
        tag='3.12.9-bookworm',
        digests=(
            'sha256:400676191e9a6e946575345cfaf2100f874a40220818d20b5edf8afa4b39a993',  # noqa: E501
            'sha256:1a52f50baf2ca1704a4a383acfea5de12053613318da621abbc03daed292fe98',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/python',
        tag='3.13.12-slim-trixie',
        digests=(
            'sha256:f1fbe55d40a15f6b118d8688b2c762bb57309b96c30d561435e91a13fd97780c',  # noqa: E501
            'sha256:38c55d91fcc578329971016dbe8dc059f37289984ed1c5835c05b36b3e1b6abb',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/python',
        tag='3.13.15-slim-trixie',
        digests=(
            'sha256:37134a49d21d2120e4c4d73bb76f8a4ab9aef31f096f7ec2ead48c2feead4332',  # noqa: E501
            'sha256:e2a5fce94bd761967528a12f16d707c2613e1522f3f2d77fa45766f45962547f',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/python',
        tag='3.14.4-slim-bookworm',
        digests=(
            'sha256:db5942d111df72110e7a67da3fc5159e83ac85cd24808b91bdc4769e166ed1b7',  # noqa: E501
            'sha256:8cc758368ddbbc2ac75f93c734e34e3b1bbb3751f0b56f5a1e1e0acc0c55d46f',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/rabbitmq',
        tag='3-management',
        digests=(
            'sha256:9cfb7e92ae7d296aec4d1ae799e431209f7ed57d55f9c929d95667d0ccf1c920',  # noqa: E501
            'sha256:8689ddfceca1ff1ecfeaa6619cb9cfc73570cf5fa022d3d904cdb065da1f925d',  # noqa: E501
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
        source='library/redis',
        tag='7.4.11-alpine',
        digests=(
            'sha256:ca0acbb137c1dc3339c8b147a58fd6f42775d4599327b50e7b116c23de501af2',  # noqa: E501
            'sha256:1f09a89a207d794a8c61d9edfc26e7c58427de10ccef7c5d18d638df79a63b85',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/rust',
        tag='1.96.1-bookworm',
        digests=(
            'sha256:d99f7b31f49909348dc59b51f3c95d1efded1701ffb222f095aaab7de3c4abd8',  # noqa: E501
            'sha256:809725748b728a8e1f8621a3c76e49fba8780c16d99ceda20abdb44d32665c30',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='library/ubuntu',
        tag='noble-20260917',
        digests=(
            'sha256:f610ab94648195aa356059f5b41d6085c9d4d903c072430cdd1af7bdb646106b',  # noqa: E501
            'sha256:08571ca13e00ca07a2a84eab83a959b4242e22cceb16486a11bef1428c9e93a7',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='moby/buildkit',
        tag='v0.33.1',
        digests=(
            'sha256:98cc6a3fc46220d00f8224ae483f3274fc874e9be8d7dd1e2e2c5481209228b5',  # noqa: E501
            'sha256:3ad6bb9bc8c78c0069d03247adb9a59b3b43d68e55353e876e558b888c6c1768',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='nvidia/cuda',
        tag='12.8.0-base-ubuntu24.04',
        digests=(
            'sha256:42bb05f545fbb2583bada43c553e04c3f2ef711b755ff18bab8b29bfa9d8d3c6',  # noqa: E501
            'sha256:c7890174d7180c01bac52d3f69e052b10e1332efbd3e322a11bb83400c990e2d',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='pgvector/pgvector',
        tag='0.8.7-pg14-bookworm',
        digests=(
            'sha256:1f9ebec0314d93fd6c612543e84de327f80bf3d643cfb99d6279be70eb2d949e',  # noqa: E501
            'sha256:3c278e3fe2cb5595e78fa4e5a49ea44184f8a74dc92d08ed83eaa09fd5630402',  # noqa: E501
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
        source='tonistiigi/binfmt',
        tag='qemu-v10.2.3-68',
        digests=(
            'sha256:465d3fdd28d0f2b871ba4b4ec98bd183292e96167f00d9fd40bd249f8632d705',  # noqa: E501
            'sha256:b4c6a09270133b3c5b4dff94f83067df4dd27eced195fc6a1dbad102999e24dd',  # noqa: E501
        ),
    ),
    Image(
        registry='registry-1.docker.io',
        source='tufin/oasdiff',
        tag='v1.33.0',
        digests=(
            'sha256:8a1e0be7c661f2c103020389cb022e0e76ca91f931e3eacf51d30512b724ab0e',  # noqa: E501
            'sha256:2ddfc622e73d098603de1a54c6513ac2d217ae9077b5047cd29520bab8bf0092',  # noqa: E501
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
