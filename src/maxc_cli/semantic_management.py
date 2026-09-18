"""Remote semantic workflows: exact snapshots, reviewed plans, mutation readback."""

from .exceptions import ValidationError
from .semantic import (
    SECTIONS,
    SemanticError,
    change_plan,
    digest,
    identifier,
    namespace_id,
    read_json,
    slot,
    snapshot,
    writable_object,
    write_json,
)


class SemanticManager:
    def __init__(self, backend, namespace):
        self.backend = backend
        self.namespace = namespace_id(namespace)
        self.warnings = []

    def request(self, method, name=None, **kwargs):
        return self.backend.semantic_request(self.namespace, method, name, **kwargs)

    def read(self, name, *, source="USER_DRAFT", revision=None):
        identifier(name, name=True)
        if revision:
            if source != "PUBLISHED":
                raise ValidationError("--revision requires --source PUBLISHED.")
            identifier(revision)
            spec = self.request("GET", name, suffix="/publishedRevisions/" + revision)
        else:
            params = {"sections": ",".join(SECTIONS)}
            if source == "PUBLISHED":
                params["version"] = "published"
            elif source == "SYSTEM_SUGGESTIONS":
                params.update(version="draft", source=source)
            # Management view supports an absent USER_DRAFT; a selected draft
            # GET instead returns RevisionConflict before we can build a template.
            spec = self.request("GET", name, params=params)
        result = snapshot(spec, endpoint=self.backend.semantic_endpoint(), namespace=self.namespace, name=name, source=source)
        if revision and result["revisionId"] != revision:
            raise SemanticError("SEMANTIC_INVALID_RESPONSE", "Server returned a different published revision.")
        return result

    @staticmethod
    def confirm(yes):
        if not yes:
            raise ValidationError("This mutation requires --yes for the exact reviewed target and content.")

    def execute(self, operation, *, name=None, **options):
        if name is not None:
            identifier(name, name=True)
        if operation == "list":
            params = {"pageSize": options.get("page_size", 20)}
            if not 1 <= params["pageSize"] <= 100:
                raise ValidationError("--page-size must be between 1 and 100.")
            for flag, key in (("page_token", "pageToken"), ("by_table", "byTable"), ("by_tag", "byTag")):
                if options.get(flag):
                    params[key] = options[flag]
            return self.request("GET", params=params)
        if operation == "revisions":
            self.warnings.append("The server lists at most the latest 20 published revision summaries; this is not a complete audit log.")
            return self.request("GET", name, suffix="/publishedRevisions")
        if operation in ("get", "export"):
            source = options.get("source", "USER_DRAFT")
            if source not in ("USER_DRAFT", "SYSTEM_SUGGESTIONS", "PUBLISHED"):
                raise ValidationError("Unsupported semantic source.")
            document = self.read(name, source=source, revision=options.get("revision"))
            if operation == "export":
                receipt = write_json(options["output"], document, overwrite=options.get("overwrite", False))
                return {**receipt, "identity": document["identity"], "source": source, "revisionId": document["revisionId"]}
            return document
        if operation == "create":
            self.confirm(options.get("yes"))
            definition = read_json(options["file"])
            obj = writable_object(definition)
            if "dataScope" not in obj:
                raise ValidationError("Create requires dataScope.dataReferences in --file.")
            created = self.request("POST", name=None, body={"specName": name, **obj})
            if created.get("specName") != name or not created.get("specId"):
                raise SemanticError("SEMANTIC_WRITE_UNCERTAIN", "Create response is incomplete; reconcile the named object before retrying.")
            try:
                observed = self.read(name)
                if observed["identity"]["specId"] != created["specId"] or any(observed["object"].get(k) != v for k, v in obj.items()):
                    raise ValueError("create readback mismatch")
            except Exception as exc:
                raise SemanticError("SEMANTIC_WRITE_UNCERTAIN", "Create was accepted but readback could not verify it. Reconcile the named object before retrying.", context={"specId": created["specId"]}) from exc
            return created
        if operation in ("diff", "apply"):
            document = read_json(options["file"])
            if operation == "apply":
                self.confirm(options.get("yes"))
                if not options.get("plan_digest"):
                    raise ValidationError("Apply requires --plan-digest from a reviewed semantic diff.")
            current = self.read(name)
            plan = change_plan(document, current)
            if any(path in plan["updateMask"] for path in ("description", "tags", "dataScope.dataReferences")):
                self.warnings.append("Metadata/dataScope has no server-side CAS. This plan checks the latest observation but cannot prevent a concurrent metadata write.")
            if operation == "diff":
                return plan
            if options["plan_digest"] != plan["planDigest"]:
                raise SemanticError("SEMANTIC_PLAN_CHANGED", "The reviewed plan no longer matches this file and remote snapshot. Review a new diff.")
            if not plan["updateMask"]:
                return {"applied": False, "reason": "no changes", "revisionId": current["revisionId"]}
            params = {"updateMask": ",".join(plan["updateMask"])}
            if plan["hasContentChanges"] and current["revisionId"]:
                params["expectedDraftRevisionId"] = current["revisionId"]
            response = self.request("PATCH", name, body=plan["body"], params=params)
            written_revision = None
            try:
                written_revision = slot(response, "USER_DRAFT").get("revisionId")
                observed = self.read(name)
                expected = {**current["content"], **plan["body"].get("draft", {}).get("userDraft", {}).get("content", {})}
                # A cleared optional section may be omitted by the server.
                actual = observed["content"]
                content_matches = all(actual.get(k, []) == v if v == [] else actual.get(k) == v for k, v in expected.items())
                object_expected = {**current["object"], **{k: v for k, v in plan["body"].items() if k != "draft"}}
                if (observed["identity"] != current["identity"] or not content_matches
                        or observed["object"] != object_expected
                        or (plan["hasContentChanges"] and (not written_revision or observed["revisionId"] != written_revision))):
                    raise ValueError("readback mismatch")
            except Exception as exc:
                raise SemanticError("SEMANTIC_WRITE_UNCERTAIN", "Patch was accepted but readback could not verify the exact result. Do not repeat it automatically.", context={"specId": current["identity"]["specId"], "revisionId": written_revision}) from exc
            return {"applied": True, "identity": observed["identity"], "revisionId": observed["revisionId"], "planDigest": plan["planDigest"], "contentSha256": digest(observed["content"])}
        if operation == "publish":
            self.confirm(options.get("yes"))
            expected_revision = identifier(options["expected_revision"])
            expected_id = identifier(options["expected_spec_id"])
            draft = self.read(name)
            if draft["revisionId"] != expected_revision or draft["identity"]["specId"] != expected_id:
                raise SemanticError("SEMANTIC_REVISION_CONFLICT", "Publish target changed since review.")
            self.warnings.append("Publication checks structure and internal references, not business correctness, physical schema, or SQL execution. Historical revision search is not guaranteed.")
            response = self.request("POST", name, suffix=":publish", body={"source": "USER_DRAFT", "expectedRevisionId": expected_revision})
            published_revision = None
            try:
                published_revision = slot(response, "PUBLISHED").get("revisionId")
                if not published_revision or response.get("specId") != expected_id:
                    raise ValueError("missing receipt")
                published = self.read(name, source="PUBLISHED", revision=published_revision)
                if published["identity"] != draft["identity"] or published["content"] != draft["content"]:
                    raise ValueError("content mismatch")
                if slot(response, "PUBLISHED").get("sourceRevisionId") != expected_revision:
                    raise ValueError("source mismatch")
            except Exception as exc:
                raise SemanticError("SEMANTIC_WRITE_UNCERTAIN", "Publish was accepted but its immutable version could not be verified. Reconcile publication history before any retry.", context={"specId": expected_id, "publishedRevisionId": published_revision}) from exc
            untrusted = bool(slot(response, "PUBLISHED").get("containsUntrustedVqs"))
            if untrusted:
                self.warnings.append("PUBLISHED_CONTAINS_UNTRUSTED_VERIFIED_QUERY: publication did not establish verified-query trust.")
            return {"containsUntrustedVqs": untrusted, "identity": published["identity"], "publishedRevisionId": published_revision, "sourceRevisionId": expected_revision, "contentSha256": digest(published["content"])}
        if operation == "delete":
            self.confirm(options.get("yes"))
            expected_id = identifier(options["expected_spec_id"])
            # Server identity guard is authoritative even across delete/recreate races.
            return self.request("DELETE", name, params={"expectedSemanticSpecId": expected_id})
        raise ValidationError("Unknown semantic operation.")
