"""Real Python/credential child processes; the cloud signer alone is replaced.

Go's barrier tests separately exercise the real CLI rotation/checkpoint lock.
This verifies Python never persists/reuses an obsolete STS or bearer across
workers and that the SDK callback is invoked again after the refresh deadline.
"""

import json
import multiprocessing
import sys
from datetime import datetime, timedelta, timezone

from maxc_cli.bearer_auth import BearerAuthConfig


def worker(config, pipe):
    from types import SimpleNamespace
    from unittest.mock import patch

    from maxc_cli.bearer_auth import create_bearer_account

    issued = []

    def client(**kwargs):
        token = kwargs["account"].sts_token

        def sign(*args):
            issued.append(token)
            return "bearer-" + token

        return SimpleNamespace(get_project=lambda: SimpleNamespace(generate_auth_token=sign))

    try:
        with patch("odps.ODPS", client):
            account = create_bearer_account(BearerAuthConfig.from_mapping(config))
            pipe.send(("ready", account.token))
            assert pipe.recv() == "refresh"
            account._expire_time = 0
            account.reload()
            pipe.send(("done", account.token, issued))
    except Exception as error:
        pipe.send(("error", type(error).__name__))
    finally:
        pipe.close()


def test_six_processes_refresh_without_shared_bearer_files(tmp_path):
    helper = tmp_path / "helper"
    helper.write_text(
        "#!"
        + sys.executable
        + '\nimport sys\nprint(open(sys.argv[sys.argv.index("--config-path")+1]).read())\n'
    )
    helper.chmod(0o700)
    credentials = tmp_path / "credentials.json"

    def write(token):
        credentials.write_text(
            json.dumps(
                dict(
                    Version=1,
                    Type="sts",
                    AccessKeyId="key",
                    AccessKeySecret="secret",
                    SecurityToken=token,
                    Expiration=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                )
            )
        )

    write("old")
    config = dict(
        cli=str(helper),
        profile="test",
        config_path=str(credentials),
        issuer_region="cn-beijing",
        issuer_project="issuer",
        policy={
            "Version": "1",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["odps:Read"],
                    "Resource": "acs:odps:*:projects/p/instances/*",
                }
            ],
        },
    )
    ctx = multiprocessing.get_context("spawn")
    children = []
    try:
        for _ in range(6):
            parent, child = ctx.Pipe()
            process = ctx.Process(target=worker, args=(config, child))
            process.start()
            child.close()
            children.append((process, parent))
        for _, pipe in children:
            assert pipe.poll(30)
            assert pipe.recv() == ("ready", "bearer-old")
        # All workers have cached the old token before rotation: no sleeps/races.
        write("new")
        for _, pipe in children:
            pipe.send("refresh")
        for process, pipe in children:
            assert pipe.poll(30)
            assert pipe.recv() == ("done", "bearer-new", ["old", "new"])
            process.join(10)
            assert process.exitcode == 0
        assert sorted(x.name for x in tmp_path.iterdir()) == ["credentials.json", "helper"]
    finally:
        for process, pipe in children:
            if process.is_alive():
                process.terminate()
            process.join(10)
            pipe.close()
