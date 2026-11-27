"""Exercise the released Alibaba Cloud CLI installer against a staged MaxC channel."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.request
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

OSS = "https://maxcompute-repo.oss-cn-hangzhou.aliyuncs.com/maxc-cli"
ALIYUN_VERSION = "3.4.11"
ALIYUN_ASSETS = {
    "linux-amd64": ("linux", "tgz", "a7e3df497db14c10d4d7587795e9fa7849b0c51dfce02908b9de5a41fe717d5c"),
    "linux-arm64": ("linux", "tgz", "b0b6f6f7545f9472b697fc27915c8956e5a3320a6160729dcc5b4eff21435ae8"),
    "darwin-amd64": ("macosx", "tgz", "4add3122147ff31aa3801efced5b1df7a9c4b2397dc361c90c8fc8804f71337a"),
    "darwin-arm64": ("macosx", "tgz", "2783fdd41c7f4f23174ef5ed08fe24c7f57d1ada12ffc4ee45d3b244e365a279"),
    "windows-amd64": ("windows", "zip", "e1feb461f68e200255ca0e96081947060f81526a45233b4ded030f8b43b2f188"),
}
TARBALL = "maxc.tar.gz"
CHECKSUM = TARBALL + ".sha256"
LATEST = "/versions/latest"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, path):
    with urllib.request.urlopen(url, timeout=45) as response, path.open("wb") as target:
        shutil.copyfileobj(response, target)


def aliyun_executable(directory, target):
    system, extension, expected = ALIYUN_ASSETS[target]
    architecture = target.rsplit("-", 1)[1]
    name = f"aliyun-cli-{system}-{ALIYUN_VERSION}-{architecture}.{extension}"
    archive = directory / name
    url = f"https://github.com/aliyun/aliyun-cli/releases/download/v{ALIYUN_VERSION}/{name}"
    download(url, archive)
    require(sha256(archive) == expected, "Alibaba Cloud CLI archive checksum differs from the pinned release")

    executable = directory / ("aliyun.exe" if system == "windows" else "aliyun")
    if extension == "zip":
        with zipfile.ZipFile(archive) as package:
            member, = (name for name in package.namelist() if Path(name).name == executable.name)
            executable.write_bytes(package.read(member))
    else:
        with tarfile.open(archive) as package:
            member, = (item for item in package if item.isfile() and Path(item.name).name == executable.name)
            with package.extractfile(member) as source, executable.open("wb") as output:
                shutil.copyfileobj(source, output)
    executable.chmod(0o700)
    return executable


def isolated_env(home):
    keys = {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "TMPDIR", "PATHEXT", "LANG"}
    env = {key: value for key, value in os.environ.items() if key.upper() in keys}
    env.update({"HOME": str(home), "USERPROFILE": str(home), "NO_PROXY": "127.0.0.1,localhost"})
    return env


def run_upgrade(executable, root, target, baseline, candidate, old_dir, new_dir):
    for directory in (old_dir, new_dir):
        expected = (directory / CHECKSUM).read_text(encoding="ascii").split()[0]
        require(sha256(directory / TARBALL) == expected, f"Bundle checksum does not match in {directory.name}")

    old_path = f"/{baseline}/{target}/{TARBALL}"
    new_path = f"/{candidate}/{target}/{TARBALL}"
    files = {
        old_path: old_dir / TARBALL,
        old_path + ".sha256": old_dir / CHECKSUM,
        new_path: new_dir / TARBALL,
        new_path + ".sha256": new_dir / CHECKSUM,
    }
    requests = []
    latest = baseline
    bad_checksum = False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            requests.append(self.path)
            if self.path == LATEST:
                body = latest.encode("ascii")
            elif self.path == new_path + ".sha256" and bad_checksum:
                body = b"0" * 64
            elif self.path in files:
                path = files[self.path]
                self.send_response(200)
                self.send_header("Content-Length", str(path.stat().st_size))
                self.end_headers()
                with path.open("rb") as stream:
                    shutil.copyfileobj(stream, self.wfile)
                return
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            shared = root / "shared config.yaml"
            shared.write_text("default_project: release_e2e_shared\n", encoding="utf-8")

            def invoke(home, *args, config=None):
                env = isolated_env(home)
                env["ALIBABA_CLOUD_MAXC_DOWNLOAD_BASE_URL"] = f"http://127.0.0.1:{server.server_port}"
                env["ALIBABA_CLOUD_CLI_TELEMETRY_DISABLE"] = "1"
                if config is not None:
                    env["MAXC_CONFIG"] = str(config)
                result = subprocess.run(
                    [str(executable), "maxc", *args], cwd=home, env=env,
                    capture_output=True, text=True, encoding="utf-8", timeout=90,
                )
                require(result.returncode == 0, f"aliyun maxc {' '.join(args)} exited {result.returncode}: {result.stderr}")
                return result

            def check_version(home, version):
                result = invoke(home, "--version")
                require(result.stdout.strip().endswith(f"maxc {version}"), f"Expected MaxC {version}: {result.stdout}")
                return result

            def check_context(home, project, config=None):
                envelope = json.loads(invoke(home, "agent", "context", "--json", config=config).stdout)
                require(envelope["status"] == "success", "agent context failed")
                require(envelope["data"]["context"]["project"] == project, "The installed CLI selected the wrong project")
                if config is not None:
                    sources = [Path(name).resolve() for name in envelope["metadata"]["config_sources"]]
                    require(sources == [config], "The installed CLI did not select MAXC_CONFIG")

            existing = root / "existing"
            preserved = existing / ".maxc" / "config.yaml"
            preserved.parent.mkdir(parents=True)
            saved_config = b"default_project: release_e2e_existing\ntelemetry:\n  enabled: false\n"
            preserved.write_bytes(saved_config)
            check_version(existing, baseline)
            require(old_path in requests, "The baseline was not installed through the downloader")
            check_context(existing, "release_e2e_existing")

            stamp = existing / ".aliyun" / "maxc" / ".version_check"

            def expire_check():
                stamp.touch()
                old = time.time() - 2 * 86400
                os.utime(stamp, (old, old))

            latest = candidate
            bad_checksum = True
            expire_check()
            result = check_version(existing, baseline)
            require("sha256 mismatch" in result.stderr and new_path in requests, "The invalid upgrade was not rejected")
            cached_requests = len(requests)
            check_version(existing, baseline)
            require(len(requests) == cached_requests, "A cached user immediately retried an invalid upgrade")

            bad_checksum = False
            expire_check()
            before = requests.count(new_path)
            check_version(existing, candidate)
            require(requests.count(new_path) == before + 1, "The candidate was not downloaded on upgrade")
            check_context(existing, "release_e2e_existing")
            check_context(existing, "release_e2e_shared", shared)
            require(preserved.read_bytes() == saved_config, "The upgrade modified the existing MaxC config")

            fresh = root / "fresh"
            fresh.mkdir()
            before = requests.count(new_path)
            check_version(fresh, candidate)
            require(requests.count(new_path) == before + 1, "The candidate was not downloaded on a fresh install")
            check_context(fresh, "release_e2e_shared", shared)
            help_text = invoke(fresh, "auth", "whoami", "--help").stdout
            require("--config" in help_text and "MAXC_CONFIG" in help_text, "Config help is missing from whoami")
        finally:
            server.shutdown()
            thread.join()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True, help="Candidate tarball and checksum directory")
    parser.add_argument("--version", required=True)
    parser.add_argument("--platform", choices=ALIYUN_ASSETS, required=True)
    parser.add_argument("--baseline-version", help="Defaults to the public latest version at test start")
    parser.add_argument("--baseline-dir", type=Path, help="Use a previously downloaded official baseline pair")
    parser.add_argument("--aliyun", type=Path, help="Use an installed Alibaba Cloud CLI executable")
    args = parser.parse_args()

    baseline = args.baseline_version
    if baseline is None:
        with urllib.request.urlopen(OSS + LATEST, timeout=20) as response:
            baseline = response.read(64).decode("ascii").strip()
    for version in (baseline, args.version):
        require(re.fullmatch(r"\d+\.\d+\.\d+", version), "Expected a stable MaxC version X.Y.Z")
    require(tuple(map(int, args.version.split("."))) > tuple(map(int, baseline.split("."))), "Upgrade candidate must be newer than the baseline")

    with tempfile.TemporaryDirectory(prefix="maxc-upgrade-e2e-") as directory:
        root = Path(directory).resolve()
        executable = args.aliyun.resolve() if args.aliyun else aliyun_executable(root, args.platform)
        old_dir = args.baseline_dir.resolve() if args.baseline_dir else root / "baseline"
        if args.baseline_dir is None:
            old_dir.mkdir()
            for name in (TARBALL, CHECKSUM):
                download(f"{OSS}/{baseline}/{args.platform}/{name}", old_dir / name)
        run_upgrade(executable, root, args.platform, baseline, args.version, old_dir, args.bundle_dir.resolve())

    print(json.dumps({
        "platform": args.platform, "host_arch": platform.machine(), "baseline": baseline,
        "candidate": args.version, "bundle_sha256": sha256(args.bundle_dir / TARBALL),
        "cases": ["baseline_install", "bad_checksum_preserves_baseline", "automatic_upgrade", "fresh_install", "config_preserved"],
        "status": "passed",
    }))


if __name__ == "__main__":
    main()
