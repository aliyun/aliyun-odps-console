# MaxC release acceptance

The **Release maxc binaries** workflow gates publication on two consumer checks against the same source commit and build artifacts.

| Required CI job | Consumer behavior |
| --- | --- |
| **All commands from the release CLI entry points** | Run the actual Linux bundle and a clean wheel install as subprocesses. Read the command inventory from each executable's `agent manifest`. Exercise every advertised command and verify its response or effect. A missing case or failed assertion blocks publication. |
| **Install and upgrade** | On Linux amd64/arm64, macOS amd64/arm64, and Windows amd64, run the released Alibaba Cloud CLI against a temporary channel. Install the public baseline, verify a checksum failure leaves it usable, upgrade from the staged candidate, and verify a clean install and configuration selection. |

The command suite runs SQL and Tunnel transfers against a fresh, loopback MaxCompute Emulator. Catalog, task diagnostics, and MCP calls use loopback protocol fixtures. The suite uses temporary homes, synthetic data and credentials, and disposable files for the write, cancel, and cleanup commands. For changes to authentication or a live service integration, record a supplemental run against an approved test environment with the candidate executable.

The upgrade suite pins the published Alibaba Cloud CLI and verifies its download checksum. The test channel serves the current public MaxC baseline and the exact candidate bundle for the runner. In a formal release the macOS job tests the signed and notarized artifact. All five platform checks must pass before the workflow can upload or promote the candidate. The publish job rechecks the public baseline before uploading, so a changed baseline requires a fresh acceptance run.

## Review and release

1. Run a build-only candidate workflow on the reviewed source commit. The default `skip_publish=true` leaves all public release channels at their current version. Pull requests that change the release workflow or acceptance drivers run this mode automatically.
2. Review the two command receipts in `maxc-command-e2e` and the five `maxc-upgrade-e2e-*` artifacts. Each command receipt records the candidate version and every command that ran. `passed` must equal `advertised`, and `failed`, `missing`, and `unexpected` must be empty. Each upgrade receipt includes the platform, versions and candidate SHA-256.
3. After approval, dispatch the formal release from the approved source ref with `skip_publish=false`. This run repeats the gates on the artifacts it will upload, then promotes `versions/latest`. Read back the public version and checksums, and use a fresh and an existing consumer installation to confirm that the public channel serves the verified version.

The `maxc-python-release-dist` artifact contains the Python distributions from the workflow that tested the wheel. `scripts/release.sh --upload` also runs the full command suite against its own freshly installed wheel before invoking Twine.

## Run locally

Run the full command check against a release binary or an installed wheel. The driver starts and stops its own emulator and needs Python 3.9+, Docker and OpenSSL.

```bash
python scripts/regression_test.py --maxc /path/to/installed/maxc
```

Run the upgrade check with the bundle directory containing `maxc.tar.gz` and `maxc.tar.gz.sha256`. Use the platform of the Alibaba Cloud CLI executable. The driver reads the public baseline on startup unless `--baseline-version` is provided.

```bash
python scripts/release_e2e/upgrade.py \
  --bundle-dir dist/release --version 0.9.11 --platform darwin-arm64
```

When a command is added or changed, add a scenario to `scripts/regression_test.py` with observable assertions at the CLI boundary. Keep any new service fixture in `scripts/release_e2e/services.py`.
