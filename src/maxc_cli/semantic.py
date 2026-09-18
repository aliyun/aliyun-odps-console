"""SemanticSpec portable documents and guarded change plans (no cloud I/O)."""

import copy
import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any

from .exceptions import MaxCError, ValidationError

FORMAT = "maxc.semantic/v1"
SECTIONS = (
    "dataReferences", "semanticModel", "instructions", "metricDefinitions",
    "verifiedQueries", "glossary", "benchmarks",
)
OBJECT_FIELDS = ("description", "tags", "dataScope")
MAX_FILE_BYTES = 16 * 1024 * 1024


class SemanticError(MaxCError):
    """Stable semantic errors never include raw transport messages or credentials."""

    recoverable = False

    def __init__(self, code: str, message: str, *, context=None):
        super().__init__(message, context=context, suggestion=(
            "Read the exact object and revisions before retrying. After an uncertain "
            "write, reconcile the remote state; do not automatically repeat the mutation."
        ))
        self.error_code = code


def identifier(value: str, *, name: bool = False) -> str:
    pattern = r"[A-Za-z][A-Za-z0-9_]{0,127}" if name else r"[A-Za-z0-9_-]{1,256}"
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValidationError("Invalid semantic object name or revision identifier.")
    return value


def namespace_id(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{1,32}", value):
        raise ValidationError("--namespace must be the verified main-account ID, not a project tenant ID.")
    return value


def digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def safe_path(value: str) -> Path:
    path = Path(os.path.abspath(os.path.expanduser(value)))
    for component in (path, *path.parents):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValidationError("Semantic files must not traverse symlinks or reparse points.")
    return path


def read_json(value: str) -> dict:
    path = safe_path(value)
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ValidationError("Semantic input must be a regular JSON file.")
            raw = stream.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise ValidationError("Semantic input exceeds the 16 MiB file limit.")
        def unique(pairs):
            obj = {}
            for key, item in pairs:
                if key in obj:
                    raise ValueError("duplicate key")
                obj[key] = item
            return obj
        def invalid_constant(_):
            raise ValueError("non-finite number")
        result = json.loads(raw, object_pairs_hook=unique, parse_constant=invalid_constant)
        if not isinstance(result, dict):
            raise ValueError("not an object")
        return result
    except (OSError, ValueError, UnicodeError, RecursionError) as exc:
        raise ValidationError("Cannot read a valid semantic JSON object (duplicate keys and NaN are rejected).") from exc


def write_json(value: str, document: dict, *, overwrite: bool = False) -> dict:
    path = safe_path(value)
    if not path.parent.is_dir() or (path.exists() and not path.is_file()):
        raise ValidationError("Choose a regular output file in an existing directory.")
    if path.exists() and not overwrite:
        raise ValidationError("Output already exists; choose another path or explicitly use --overwrite.")
    raw = (json.dumps(document, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    if len(raw) > MAX_FILE_BYTES:
        raise ValidationError("Export exceeds the 16 MiB portable file limit.")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".semantic-", delete=False) as stream:
            temporary = Path(stream.name)
            os.chmod(temporary, 0o600)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        safe_path(str(path))
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
        return {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    except OSError as exc:
        raise ValidationError("Could not atomically publish semantic export; the existing file was preserved.") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def writable_references(refs: Any) -> list:
    if not isinstance(refs, list) or not 1 <= len(refs) <= 20:
        raise ValidationError("dataReferences must contain 1 to 20 tables.")
    cleaned = copy.deepcopy(refs)
    seen = set()
    for ref in cleaned:
        if not isinstance(ref, dict) or not all(isinstance(ref.get(k), str) and ref[k] for k in ("project", "table")):
            raise ValidationError("Every dataReference requires project and table strings.")
        if "schema" in ref and not isinstance(ref["schema"], str):
            raise ValidationError("dataReference.schema must be a string.")
        ref.pop("tableId", None)  # server-generated, never echo into a client write
        key = (ref["project"], ref.get("schema", ""), ref["table"])
        if key in seen:
            raise ValidationError("Duplicate dataReference.")
        seen.add(key)
    return cleaned


def writable_content(content: Any) -> dict:
    if not isinstance(content, dict) or set(content) - set(SECTIONS):
        raise ValidationError("content must contain only supported SemanticSpec sections.")
    result = copy.deepcopy(content)
    for key, item in result.items():
        expected = dict if key in ("semanticModel", "instructions", "benchmarks") else list
        if not isinstance(item, expected):
            raise ValidationError(f"Invalid container type for section {key}.")
    if "dataReferences" in result:
        result["dataReferences"] = writable_references(result["dataReferences"])
    return result


def writable_object(obj: Any) -> dict:
    if not isinstance(obj, dict) or set(obj) - set(OBJECT_FIELDS):
        raise ValidationError("object may contain only description, tags, and dataScope.")
    result = copy.deepcopy(obj)
    if "description" in result and not isinstance(result["description"], str):
        raise ValidationError("description must be a string.")
    if "tags" in result and (not isinstance(result["tags"], list) or not all(isinstance(t, str) for t in result["tags"])):
        raise ValidationError("tags must be an array of strings.")
    if "dataScope" in result:
        scope = result["dataScope"]
        if not isinstance(scope, dict) or set(scope) != {"dataReferences"}:
            raise ValidationError("dataScope requires dataReferences only.")
        scope["dataReferences"] = writable_references(scope["dataReferences"])
    return result


def slot(spec: dict, source: str) -> dict:
    if not isinstance(spec, dict) or (spec.get("draft") is not None and not isinstance(spec["draft"], dict)):
        raise SemanticError("SEMANTIC_INVALID_RESPONSE", "Invalid version response.")
    if source == "PUBLISHED":
        result = spec.get("published") or {}
    else:
        result = (spec.get("draft") or {}).get("userDraft" if source == "USER_DRAFT" else "systemSuggestions") or {}
    if not isinstance(result, dict):
        raise SemanticError("SEMANTIC_INVALID_RESPONSE", "Invalid version response.")
    return result


def snapshot(spec: dict, *, endpoint: str, namespace: str, name: str, source: str) -> dict:
    spec_id = spec.get("specId")
    if not isinstance(spec_id, str) or not spec_id or spec.get("specName") != name:
        raise SemanticError("SEMANTIC_INVALID_RESPONSE", "Response does not identify the requested object.")
    selected = slot(spec, source)
    revision = selected.get("revisionId")
    if selected and not revision:
        raise SemanticError("SEMANTIC_INVALID_RESPONSE", "Version metadata is missing its revision identity.")
    if revision:
        identifier(revision)
        if not isinstance(selected.get("content"), dict) or not selected["content"].get("dataReferences"):
            raise SemanticError("SEMANTIC_INCOMPLETE_CONTENT", "Full versioned content was not returned; refusing to export or plan from a summary.")
        content = writable_content(selected["content"])
    elif source == "USER_DRAFT":
        content = {"dataReferences": writable_references(spec.get("dataScope", {}).get("dataReferences"))}
    else:
        raise SemanticError("SEMANTIC_NOT_READY", "The selected version is not available.")
    return {
        "format": FORMAT,
        "identity": {"endpoint": endpoint, "namespace": namespace, "specName": name, "specId": spec_id},
        "source": source, "revisionId": revision,
        "object": writable_object({k: spec[k] for k in OBJECT_FIELDS if k in spec}),
        "content": content,
    }


def change_plan(document: dict, current: dict) -> dict:
    allowed = {"format", "identity", "source", "revisionId", "object", "content"}
    if set(document) != allowed or document.get("format") != FORMAT:
        raise ValidationError("Use an unmodified maxc.semantic/v1 export envelope; edit object/content only.")
    if document["source"] != "USER_DRAFT":
        raise ValidationError("Apply requires a USER_DRAFT export. Import reviewed published content into a fresh draft export explicitly.")
    if document["identity"] != current["identity"] or document["revisionId"] != current["revisionId"]:
        raise SemanticError("SEMANTIC_REVISION_CONFLICT", "Object identity, endpoint, namespace, or draft revision changed. Export again and review a new diff.")
    desired_content = writable_content(document["content"])
    desired_object = writable_object(document["object"])
    content = {k: v for k, v in desired_content.items() if current["content"].get(k) != v}
    # Absence of a draft requires an explicit dataReferences write even when the
    # template already contains the same scope. Omitted sections stay unchanged.
    if current["revisionId"] is None:
        if not desired_content.get("dataReferences"):
            raise ValidationError("The first draft requires dataReferences.")
        content = desired_content
    obj = {k: v for k, v in desired_object.items() if current["object"].get(k) != v}
    mask = ["dataScope.dataReferences" if k == "dataScope" else k for k in obj]
    mask += ["draft.userDraft.content." + k for k in content]
    body = dict(obj)
    if content:
        body["draft"] = {"userDraft": {"content": content}}
    changes = [{"path": "object." + k, "before": current["object"].get(k), "after": v} for k, v in obj.items()]
    changes += [{"path": "content." + k, "before": current["content"].get(k), "after": v} for k, v in content.items()]
    plan = {"identity": current["identity"], "expectedDraftRevisionId": current["revisionId"],
            "updateMask": sorted(mask), "body": body, "changes": changes,
            "metadataCAS": False, "hasContentChanges": bool(content)}
    # Bind the entire observed object/content, including preserved fields.
    plan["planDigest"] = digest({"plan": plan, "observed": current})
    return plan
