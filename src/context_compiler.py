"""RLM-style lightweight context compiler for large codebases."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{3,}")
_STOPWORDS = {
    "this", "that", "with", "from", "have", "will", "should", "could", "would",
    "about", "there", "their", "your", "after", "before", "while", "where",
    "when", "what", "which", "into", "between", "also", "want", "need", "please",
    "step", "plan", "context", "token", "tokens", "layer", "rlm", "codebase",
}


@dataclass(frozen=True)
class MatchSnippet:
    path: str
    line_no: int
    line: str


def _extract_keywords(text: str, limit: int = 8) -> list[str]:
    seen: set[str] = set()
    words: list[str] = []
    for match in _WORD_RE.finditer(text or ""):
        word = match.group(0).lower()
        if word in _STOPWORDS or word in seen:
            continue
        seen.add(word)
        words.append(word)
        if len(words) >= limit:
            break
    return words


def _iter_candidate_files(repo_root: Path) -> list[Path]:
    candidates: list[Path] = []
    for rel in ("src", "tests"):
        base = repo_root / rel
        if not base.exists():
            continue
        candidates.extend(sorted(base.rglob("*.py")))
    return candidates


def _find_snippets(repo_root: Path, keywords: list[str], max_matches: int = 8) -> list[MatchSnippet]:
    if not keywords:
        return []
    snippets: list[MatchSnippet] = []
    for path in _iter_candidate_files(repo_root):
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for idx, line in enumerate(lines, start=1):
            line_l = line.lower()
            if any(keyword in line_l for keyword in keywords):
                snippets.append(
                    MatchSnippet(
                        path=str(path.relative_to(repo_root)),
                        line_no=idx,
                        line=line.strip()[:200],
                    )
                )
                break
        if len(snippets) >= max_matches:
            break
    return snippets


def build_context(raw_prompt: str, repo_root: Path, max_chars: int = 1600) -> str:
    """Compile a compact context pack based on prompt keywords."""
    keywords = _extract_keywords(raw_prompt)
    snippets = _find_snippets(repo_root, keywords)

    lines = [
        "<context_compiler>",
        "mode: heuristic",
        f"keywords: {', '.join(keywords) if keywords else 'none'}",
        "matches:",
    ]
    if snippets:
        for item in snippets:
            lines.append(f"- {item.path}:{item.line_no} | {item.line}")
    else:
        lines.append("- none")
    lines.append("</context_compiler>")

    block = "\n".join(lines)
    if len(block) > max_chars:
        block = block[: max(0, max_chars - 20)].rstrip() + "\n... [truncated]"
    return block
