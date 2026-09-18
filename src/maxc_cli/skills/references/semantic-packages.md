# Remote semantic packages

Use `{{cli}} semantic` for account-scoped Catalog packages. `meta semantic`
manages local annotations and never silently synchronizes with a remote package.
Read the live help/manifest before choosing arguments.

## Discover and read

Use the verified main-account ID as `--namespace`; a project's tenant ID is not
an interchangeable identifier. The active authentication and Catalog endpoint
select the caller and region. Follow `nextPageToken` with `--page-token` until
the intended exact name is found. Authorization remains server-enforced.

```bash
{{cli}} semantic list --namespace <account_id> --user-agent "$UA" --json
{{cli}} semantic get <name> --namespace <account_id> --source PUBLISHED --revision <revision> --user-agent "$UA" --json
```

`get` returns complete editable sections and version metadata; a missing
USER_DRAFT returns a first-draft template from the object's data scope. A missing
Published or Suggestions version fails. `revisions` enumerates at most the most
recent 20 publication summaries. Reading an older immutable revision does not
promise that DataBridge can search/analyze that revision.

## Create and edit

Create takes a JSON object with `description`, `tags` and required
`dataScope.dataReferences` (1–20 project/schema/table references). It creates the
package object, without publishing or manufacturing a Draft. Inspect the exact
file/name/account before a user-authorized `create ... --yes`.

1. Export USER_DRAFT to a new file. A new package yields `revisionId: null`.
2. Edit only `object` and `content`. Keep identity, endpoint, source and revision
   unchanged. Preserve all unrelated entries and nested fields of each supplied
   section. An omitted top-level section stays unchanged; an explicit empty
   array clears it. `dataReferences` must remain nonempty. To import other
   content, copy only reviewed sections into a fresh target USER_DRAFT export.
3. Run `diff`, review its complete `changes`, and record `planDigest`. The digest
   binds this file to the observed target and revision. Metadata/dataScope
   updates have no server-side CAS; a preflight check cannot remove that race.
4. After authorization for that exact plan, run `apply` with its digest and
   `--yes`. Success includes the new revision and verified content SHA-256.
   On conflict, export again, reconcile the changes, and review a new diff.

```bash
{{cli}} semantic export <name> --namespace <account_id> --output draft.json --user-agent "$UA" --json
{{cli}} semantic diff <name> --namespace <account_id> --file draft.json --user-agent "$UA" --json
{{cli}} semantic apply <name> --namespace <account_id> --file draft.json --plan-digest <reviewed_digest> --yes --user-agent "$UA" --json
```

The portable `maxc.semantic/v1` document strips server-generated `tableId` from
physical references; identity stays in its separate envelope. Existing unknown
nested fields are preserved. Unknown top-level content sections fail closed.
Local content hashes describe normalized editable JSON, not server section-hash
canonicalization. Exports use owner-only files, atomic publication and explicit
`--overwrite`; symlink/reparse paths are rejected. Prefer a real existing
working directory over a symlink such as macOS `/tmp`.

## Publish and delete

Publication is separate from saving Draft. Review physical table/field/type
references, metric definitions, joins, SQL validity and trusted-query provenance
before proposing publication. Server acceptance checks structure/internal
references and is not evidence of business correctness or executed SQL. Keep
unreviewed query provenance untrusted. Suggestions must be reviewed into a
USER_DRAFT rather than directly published.

After the user authorizes the exact Draft revision, publish with both its
`--expected-spec-id` and `--expected-revision`, plus `--yes`. The command verifies
the resulting immutable Published content. Preserve the receipt. If the response
is `SEMANTIC_WRITE_UNCERTAIN`, inspect that object and publication history before
any retry: the server has no publish idempotency key.

Delete requires a fresh `specId`, exact-target authorization and
`--expected-spec-id ... --yes`. It is deletion, not archival. The server identity
guard protects a subsequently recreated object with the same name.

## Completion

A successful edit requires readback at the returned Draft revision. A successful
publish requires matching immutable content and source revision. For cross-client
acceptance, read the same `specId` and Published revision through DataAgent or the
console. Command success alone does not prove a deployment, permanent retention,
or historical-version search. DataScan suggestion generation and service-side
validate/compile/approval APIs are outside this first command surface.
