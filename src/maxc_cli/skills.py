from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .config import MaxCConfig
from .exceptions import NotFoundError, ValidationError


@dataclass(slots=True)
class SkillDefinition:
    skill_id: str
    name: str
    version: str
    description: str
    input_schema: dict[str, Any]
    guards: dict[str, Any]
    implementation: dict[str, Any]
    path: Path

    @classmethod
    def from_file(cls, path: Path) -> "SkillDefinition":
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        skill_payload = payload.get("skill", {})
        if not isinstance(skill_payload, dict):
            raise ValidationError(f"Skill 文件格式错误: {path}")
        return cls(
            skill_id=str(skill_payload["id"]),
            name=str(skill_payload.get("name", skill_payload["id"])),
            version=str(skill_payload.get("version", "0.1.0")),
            description=str(skill_payload.get("description", "")).strip(),
            input_schema=dict(skill_payload.get("input", {}).get("schema", {})),
            guards=dict(skill_payload.get("guards", {})),
            implementation=dict(skill_payload.get("implementation", {})),
            path=path,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "id": self.skill_id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "path": str(self.path),
        }

    def resolve_input(self, raw_input: dict[str, Any], config: MaxCConfig) -> dict[str, Any]:
        resolved = dict(raw_input)
        for field_name, field_schema in self.input_schema.items():
            if field_name in resolved:
                continue
            if not isinstance(field_schema, dict):
                continue
            default = field_schema.get("default")
            if default == "${default_project}":
                resolved[field_name] = config.default_project
            elif default is not None:
                resolved[field_name] = default

        missing = []
        for field_name, field_schema in self.input_schema.items():
            if isinstance(field_schema, dict) and field_schema.get("required") and field_name not in resolved:
                missing.append(field_name)
        if missing:
            raise ValidationError(
                f"Skill {self.skill_id} 缺少必填输入: {', '.join(missing)}。"
            )
        return resolved


class SkillRegistry:
    def __init__(self, skill_dirs: list[Path]) -> None:
        self.skill_dirs = skill_dirs
        self._skills = self._load_skills()

    def _load_skills(self) -> dict[str, SkillDefinition]:
        skills: dict[str, SkillDefinition] = {}
        for skill_dir in self.skill_dirs:
            if not skill_dir.exists():
                continue
            for path in sorted(skill_dir.glob("*.y*ml")):
                definition = SkillDefinition.from_file(path)
                skills[definition.skill_id] = definition
        return skills

    def list(self) -> list[SkillDefinition]:
        return sorted(self._skills.values(), key=lambda item: item.skill_id)

    def get(self, skill_id: str) -> SkillDefinition:
        try:
            return self._skills[skill_id]
        except KeyError as exc:
            raise NotFoundError(
                f"未找到 Skill: {skill_id}",
                suggestion="请执行 maxc skill list 查看当前已安装 Skill。",
            ) from exc
