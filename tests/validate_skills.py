#!/usr/bin/env python3
"""Validate every skills/*/SKILL.md (stdlib only, no install needed).

Checks:
  * YAML frontmatter is present and delimited by '---' lines.
  * `name` exists, is kebab-case, and matches the skill's directory name.
  * `description` exists and is non-empty (folded `>` / `|` blocks supported).
  * Every relative Markdown link inside the skill resolves to a real file.
    Links that leave the skill's own directory (e.g. ../dsql/SKILL.md, a sibling
    skill shipped by another plugin) are intentionally not checked.
  * Every evals/*.json file parses and names this skill.

Exit code is non-zero on any failure, so CI fails.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILLS = ROOT / "skills"
LINK = re.compile(r"\[[^\]]*\]\(([^)\s#]+)(?:#[^)]*)?\)")
NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


def frontmatter(text: str) -> dict[str, str] | None:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return None
    try:
        end = next(i for i, ln in enumerate(lines[1:], 1) if ln.strip() == "---")
    except StopIteration:
        return None
    fields: dict[str, str] = {}
    key = None
    for ln in lines[1:end]:
        m = re.match(r"^([A-Za-z_][\w-]*):\s*(.*)$", ln)
        if m:
            key, val = m.group(1), m.group(2).strip()
            fields[key] = "" if val in (">", "|", ">-", "|-") else val.strip("\"'")
        elif key and ln.startswith((" ", "\t")):
            fields[key] = (fields[key] + " " + ln.strip()).strip()
    return fields


def check_skill(skill_dir: Path) -> list[str]:
    errs: list[str] = []
    rel = skill_dir.relative_to(ROOT)
    skill_md = skill_dir / "SKILL.md"
    if not skill_md.is_file():
        return [f"{rel}: missing SKILL.md"]

    fm = frontmatter(skill_md.read_text(encoding="utf-8"))
    if fm is None:
        return [f"{rel}/SKILL.md: missing or unterminated YAML frontmatter"]
    name = fm.get("name", "")
    if not NAME.match(name):
        errs.append(f"{rel}/SKILL.md: name {name!r} is not kebab-case")
    if name != skill_dir.name:
        errs.append(f"{rel}/SKILL.md: name {name!r} != directory {skill_dir.name!r}")
    if not fm.get("description"):
        errs.append(f"{rel}/SKILL.md: empty or missing description")

    for md in sorted(skill_dir.rglob("*.md")):
        for target in LINK.findall(md.read_text(encoding="utf-8")):
            if re.match(r"^[a-z][a-z0-9+.-]*:", target):  # http:, https:, mailto:
                continue
            resolved = (md.parent / target).resolve()
            if not resolved.is_relative_to(skill_dir.resolve()):
                continue  # cross-skill / cross-plugin reference
            if not resolved.exists():
                errs.append(f"{md.relative_to(ROOT)}: broken link -> {target}")

    for ev in sorted(skill_dir.glob("evals/*.json")):
        try:
            data = json.loads(ev.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            errs.append(f"{ev.relative_to(ROOT)}: invalid JSON ({e})")
            continue
        sn = data.get("skill_name") if isinstance(data, dict) else None
        if sn is not None and sn != skill_dir.name:
            errs.append(f"{ev.relative_to(ROOT)}: skill_name {sn!r} != {skill_dir.name!r}")
    return errs


def main() -> int:
    dirs = sorted(p for p in SKILLS.iterdir() if p.is_dir())
    if not dirs:
        print("no skills found", file=sys.stderr)
        return 1
    errors = [e for d in dirs for e in check_skill(d)]
    for e in errors:
        print(f"::error::{e}" if os.environ.get("GITHUB_ACTIONS") else e)
    print(f"checked {len(dirs)} skills: {len(errors)} problem(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
