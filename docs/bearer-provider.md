# RFC: opt-in ODPS bearer provider backed by CLI OAuth


## Status and decision requested

Draft RFC with an opt-in reference implementation in this PR; not enabled by default. This
proposes reuse of official signing and authentication APIs; it does not claim
that cross-region bearer use is a documented service contract or that the
foreign-region direct OAuth STS principal-resolution issue is fixed.

Please review the provider interface separately from the service guarantees
listed below. Accepting client code or passing mock tests cannot establish those
guarantees. The existing authentication modes remain available and unchanged.

## Evidence levels

- **Official mechanism:** [PyODPS Project.generate_auth_token](https://github.com/aliyun/aliyun-odps-python-sdk/blob/dd77b530250f5b62d89424aa445145e459e82f85/odps/models/project.py#L436)
  submits policy and expiry to `sign_bearer_token`. The [official authentication guide](https://github.com/aliyun/aliyun-odps-java-sdk/blob/1c11317c18ad63ac84030867f5a8dd2def48122f/docs/docs/getting-started/authentication.md#bearer-token)
  describes short-lived bearer authentication and Policy. Its Java-only issuance
  statement conflicts with the cited Python implementation and our successful use.
- **Empirical combination:** one independently authorized official-cli OAuth
  identity, Beijing issuer, Beijing/US East/Singapore data endpoints; six CLI
  processes, bounded real-table queries, renewal, job recovery and Tunnel downloads.
  This is repeatable evidence, not a promise for other regions, accounts or projects.
- **Unconfirmed contract:** we found no explicit public cross-region guarantee
  for this OAuth STS → ODPS bearer combination. Policy enforcement and revocation
  semantics have not received a complete negative-test acceptance.

## Questions requiring service-owner confirmation

| ID | Question | Why it matters |
| --- | --- | --- |
| Q1 | Is a bearer signed through a Beijing project supported at overseas ODPS endpoints? What issuer/target region, project, tenant and China/international-site combinations are supported? | Do not infer a universal audience from three successful regions or a wildcard in a resource string. |
| Q2 | May official-cli OAuth STS sign bearer tokens? Which RAM/MaxCompute privileges, project membership and principal types are required? Is this a supported long-term public issuance interface? | Avoid depending on an incidental permission or identity-mapping behavior. |
| Q3 | How are effective permissions computed from signer identity, STS/session constraints, bearer Policy and target-project ACL/roles? Which action/resource forms and schema rules apply to SQL and Tunnel? Are out-of-policy and explicit-Deny cases rejected consistently? | No broader privilege is a required security property, not something successful queries alone prove. |
| Q4 | After OAuth revocation, signer removal or permission reduction, or STS expiry, when does an existing bearer stop working? Is individual token revocation supported? | Define the residual access window; do not assume immediate revocation or continued validity. |
| Q5 | What lifetimes, clock-skew allowances, issuance quotas, throttling signals and retry rules are supported? Is one token per worker an intended usage pattern? | Bound renewal load and failure handling. Client lifetime limits are not a service guarantee. |
| Q6 | Are native SQL/results, TableTunnel, Catalog and other SDK endpoints intended to accept this token, with the same authorization semantics? Which operations are excluded? | Successful reads do not cover every MaxC operation. |
| Q7 | How is the original OAuth/RAM principal represented in target-region audit records? Can the STS/bearer mapping difference be explained and supported without masking an authorization defect? | Establish attributable, correct identity rather than merely successful execution. |
| Q8 | What availability/support contract covers signing and cross-region validation? Is another issuer a supported recovery option? | The selected signing region becomes a renewal dependency for every target region. |

## Risks and acceptance gates

- **Permission scope:** the client requires an explicit policy but does not prove
  least privilege or prohibit all broad policies. Before production acceptance,
  test allowed versus out-of-policy resources/actions, schema boundaries, explicit
  Deny and signer-permission limits in an authorized disposable environment.
- **Revocation:** confirm Q4, then test revocation/permission reduction only with
  separately authorized test identities/resources. Do not change an existing
  user's permissions to perform acceptance.
- **Availability:** an issuer outage can prevent renewal even while target data
  regions are healthy. No automatic issuer, identity or AK fallback is implemented.
- **Credential exposure:** HTTPS, memory-only bearer storage and log suppression
  reduce exposure. A leaked bearer remains usable to the extent and duration the
  service accepts it; these client controls do not establish service-side scope.
- **Long-running operation:** forced local expiry with real rotation is covered;
  natural-expiry soak, sustained throttling, long outages and revoked grants are
  not fully accepted. Add bounded-load and recovery validation after the service
  contract is clarified, without automatic SQL resubmission.

Keep this feature explicit opt-in and under RFC review pending service-owner
answers and the relevant tests. Do not present it as the general overseas STS fix
or replace a working production authentication configuration on this evidence.

## Independent submissions

Go OAuth concurrency/persistence and Python jobs.lock are standalone bug fixes;
neither depends on any cross-region bearer guarantee. The Go credential-process
interface is a separate feature depending on the OAuth fix and can be reviewed
independently of this RFC. This Python provider needs that interface at runtime
only when opted in. Native `mc` and wrapper decoupling are not prerequisites.

## Proposed client contract and implementation sequence

Reuse an explicit Alibaba Cloud CLI OAuth profile. Python never implements OAuth,
reads its refresh token, or rewrites its configuration. The credential process owns
rotation and durable locking. Sign bearer in an explicit issuer region/project;
route each data request independently. No fallback to long-term AK or MCP.

1. Go: add a versioned OAuth-only credential process command on top of the existing
   concurrency/checkpoint fix. Return short-lived STS only, with expiry.
2. Python: add opt-in bearer configuration and official PyODPS callback integration;
   bounded policies, no persistent bearer cache, sanitized failures and logs.
3. Request routing and durable job context: region/project per command, endpoint
   derivation, old jobs recoverable without changing defaults; preserve legacy auth.
4. Deterministic concurrency/failure/compatibility tests, then isolated real table
   SELECT and Tunnel read tests. Force expiration only in the isolated OAuth grant.

This draft includes the provider, routing, recovery and regression tests for review.
It does not install or activate the feature. A separate baseline cursor-test
assertion correction changes no production behavior.

## Acceptance boundaries

Beijing-issued tokens have worked in three regions, but this is empirical, not an
unconditional service support guarantee. Overseas OAuth STS direct authentication
is still unresolved. Not exceeding the signer’s effective permissions is a
required security boundary; its complete intersection and revocation semantics
remain subject to Q3/Q4 and negative testing.
A successful constant query is not proof of table SELECT or Tunnel capability.

## Configuration

Opt in explicitly; do not replace existing authentication configuration. Example
for a new isolated YAML file (replace example project/table and paths):

```yaml
auth:
  provider: bearer
  bearer:
    cli: /path/to/aliyun-with-oauth-credential
    profile: isolated-oauth
    config_path: /absolute/isolated/aliyun-config.json
    issuer_region: cn-beijing
    issuer_project: example_project
    lifetime_hours: 1
    timeout_seconds: 90
    policy:
      Version: "1"
      Statement:
        - Effect: Allow
          Action: ["odps:CreateInstance"]
          Resource: "acs:odps:*:projects/example_project"
        - Effect: Allow
          Action: ["odps:Read"]
          Resource: "acs:odps:*:projects/example_project/instances/*"
        - Effect: Allow
          Action: ["odps:Describe", "odps:Select"]
          Resource: "acs:odps:*:projects/example_project/tables/example_table"
```

PyODPS >= 0.13.2 is required only for this provider. Explicit policy controls its
scope; the client does not infer or broaden grants. OAuth stays in the CLI, and
bearer tokens stay in process memory. Refresh failures preserve the retry deadline;
there is no automatic SQL resubmission or identity fallback. The optional typed
Catalog client is not constructed with empty AK fields for this provider.

The Go prerequisite is a proposed `aliyun configure oauth-credential` command,
which is implemented locally but not submitted or released. It is not included
in [the independent OAuth refresh fix](https://github.com/aliyun/aliyun-cli/pull/1483).
The command contract proposed here is one JSON object on stdout with Version=1,
Type=sts, AccessKeyId, AccessKeySecret, SecurityToken and RFC3339 Expiration;
errors must not print credentials. OAuth access/refresh tokens are never returned.
Python tests use a synthetic credential process; live use needs the unreleased
Go interface. Maintainer feedback on using an existing official equivalent is welcome.
Older CLI versions fail clearly when the new credential command is unavailable.

## Request routing and recovery

```
maxc --config /absolute/isolated.yaml query 'SELECT ...' --region us-east-1 --project example_project --json
maxc --config /absolute/isolated.yaml job status JOB_ID --json
maxc --config /absolute/isolated.yaml job result JOB_ID --json
```

Data commands accept `--region` and optional `--endpoint` after the command.
An explicit region derives a fresh endpoint and clears stale tunnel/catalog
endpoint defaults. Auth-login flags retain their previous meaning.

Bearer job records persist only routing and a fingerprint of the CLI credential
source, never token material. Follow-up CLI commands recover the original route
before creating the backend. Conflicting targets or a changed credential source
are rejected. Token rotation does not change the source fingerprint. Legacy jobs
without a regional record require explicit original region/project in bearer mode;
legacy authentication modes keep their existing recovery behavior. Failed local
bookkeeping never authorizes resubmission. Bearer metadata caches are partitioned
by routing/source; existing caches and jobs are not migrated or rewritten.

## Refresh and compatibility verification

- Six spawned Python workers hold the old bearer behind a barrier, then consume
  rotated credential-process output and each reissue once. The cloud signer is
  replaced only in this offline test; actual Go concurrency is covered by its
  separate six-process rotating-endpoint test.
- Deadline failure restores the SDK refresh timestamp. Signer snapshots cannot
  switch to ODPS_STS_* environment credentials on a forced SDK reload.
- Request configuration is snapshotted for signing; changing a caller's config
  object cannot move an existing account's issuer. Executable upgrades do not
  invalidate job source identity (the profile and resolved config path define it).
- A timed-out query retains its job ID and regional context; a restarted app
  reads the result without submitting another query. Unknown legacy regional
  context requires explicit routing. Legacy auth configurations still round-trip.
- The bearer provider suppresses upstream token/header/body debug logs and does
  not mint Logview capability URLs. Transport warnings remain available.
- Go `./config` tests passed with its metadata fixture present. Python default
  offline suite: 1699 passed, 5 skipped, 138 integration/e2e/eval tests deselected.
  A stale existing cursor-message assertion was reproduced on baseline 709a6df
  and corrected separately; production cursor behavior was not changed.

The initial Python 3.9/LibreSSL environment failed an existing TLS trust-store
test. A fresh Homebrew Python 3.13 virtual environment passed the authentication
suite and the full default suite. No installed interpreter or CLI was modified.

## Isolated live acceptance (2026-10-22)

The actual CLI commands (not an injected backend) passed against cn-beijing,
us-east-1 and ap-southeast-1, using one separately authorized official-cli OAuth
profile. Six independent query processes encountered forced OAuth/STS cache expiry,
refreshed through the real OAuth service, and submitted bounded COUNT(DISTINCT)
queries on small, metadata-verified dimension tables (partition filters where
applicable). Waiting one second returned pending for some queries and preserved
usable job IDs. A second forced expiry followed by six fresh `job result` processes
recovered all results using only saved job IDs, without repeating SQL or passing
region/project again. Refresh tokens rotated and the new STS expiry was valid.

`data download` used native TableTunnel successfully in all three regions: two
rows, one selected column, private local CSVs (0600). Singapore's table contains
TIMESTAMP_NTZ, which requires the optional pandas dependency in PyODPS even when
another column is selected. Installing pandas only in the temporary venv resolved
that dependency error. The initial Singapore test also selected an incorrect
column; it was corrected from metadata before the final six-process query wave.
Neither issue was an authentication failure.

Example recovery and read-only Tunnel checks (use already verified resources):

```
maxc --config /absolute/isolated.yaml query 'SELECT COUNT(DISTINCT known_column) FROM known_table WHERE pt=...' --region us-east-1 --project example_project --wait 1 --json
maxc --config /absolute/isolated.yaml job result RETURNED_JOB_ID --max-rows 1 --json
maxc --config /absolute/isolated.yaml data download known_table --region us-east-1 --project example_project --partition pt=... --columns known_column --limit 2 --output /private/new-file.csv --json
```

Do not force expiry in an existing production OAuth configuration. This acceptance
used an independently authorized test grant; rotating a copied refresh token would
affect the original grant. No credentials, business rows or Logview URLs are stored
in this repository. The temporary environment used Homebrew Python 3.13, PyODPS
0.13.2 and pandas; it was not installed into the user's CLI or MCP environment.

Still outside acceptance: natural-expiry soak, arbitrary projects/schemas,
revocation/long-outage recovery, uploads and other writes. Foreign-region direct
OAuth STS remains affected by the server identity-mapping issue. Native bearer is
an empirically verified alternate path, not a service-side fix or a universal
cross-region support guarantee.

## Security and compatibility requirements implemented for review

- Suppress credential-bearing SDK retry-signing and token/body logs.
- Require HTTPS before acquiring credentials and again when signing SDK-routed
  requests, including discovered Tunnel endpoints.
- Normalize provider names consistently through serialization and job recovery.
- Explicit request connection parameters take precedence over legacy environment
  defaults; invocations without new routing flags retain existing behavior.
- Keep the issuer STS account from silently reloading ambient credentials.
- Preserve retry eligibility when bearer refresh fails; never resubmit SQL to
  recover an authentication failure.

## Dependencies and remaining limits

This RFC builds on [jobs.lock PR #51](https://github.com/aliyun/aliyun-odps-console/pull/51).
That fix is independently useful and does not depend on this proposal. The Go
credential interface is a separate, unreleased runtime prerequisite only for
opted-in bearer users. No native mc or wrapper-refactor code is included here.

The job fingerprint binds a profile name and resolved config file, not a verified
RAM UserId. Reauthorizing the same profile as a different identity is not detected
by this fingerprint. Local cached results are not a fresh permission check.

Remote OAuth rotation and local persistence cannot be atomic: a lost response or
crash before durable receipt can still require reauthorization. The signing region
is a renewal availability dependency; there is no automatic issuer or AK fallback.

Prototype default-suite validation: 1716 passed, 5 skipped, 138 deselected
(integration/e2e/eval excluded). This is client regression coverage, not proof of
service-side permission, revocation or cross-region guarantees. Maintainers should
confirm Q1–Q8 before recommending this path as a general production solution.
