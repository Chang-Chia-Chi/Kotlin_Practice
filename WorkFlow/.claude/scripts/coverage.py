#!/usr/bin/env python3
"""
JaCoCo HTML Coverage Checker — for AI agent / CI integration.

Parses a JaCoCo index.html report, extracts per-package and total coverage,
and checks against configurable thresholds. Exit code 1 on failure.

Usage:
    python check_coverage.py <report.html> [--min-instruction 80] [--min-branch 70]
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from bs4 import BeautifulSoup, Tag


@dataclass
class CoverageRow:
    element: str
    instr_missed: int
    instr_total: int
    instr_cov: float  # percentage
    branch_missed: int
    branch_total: int
    branch_cov: float
    cxty_missed: int
    cxty_total: int
    lines_missed: int
    lines_total: int
    methods_missed: int
    methods_total: int
    classes_missed: int
    classes_total: int


def parse_bar_cell(td: Tag) -> tuple[int, int]:
    """Parse a 'Missed X of Y' or bar-image cell into (missed, total)."""
    text = td.get_text(strip=True)
    # Footer style: "613 of 2,362"
    if "of" in text:
        parts = text.split("of")
        missed = int(parts[0].strip().replace(",", ""))
        total = int(parts[1].strip().replace(",", ""))
        return missed, total
    # Body style: bar images with title="N" for missed (red) and covered (green)
    imgs = td.find_all("img")
    missed = 0
    covered = 0
    for img in imgs:
        val = int(img.get("title", "0"))
        src = img.get("src", "")
        if "redbar" in src:
            missed = val
        elif "greenbar" in src:
            covered = val
    return missed, missed + covered


def parse_pct(td: Tag) -> float:
    """Parse a percentage cell like '74%' into 74.0."""
    text = td.get_text(strip=True).replace("%", "").replace("n/a", "0")
    return float(text) if text else 0.0


def parse_int(td: Tag) -> int:
    text = td.get_text(strip=True).replace(",", "")
    return int(text) if text else 0


def parse_counter_pair(tds: list[Tag], offset: int) -> tuple[int, int]:
    """Parse a (missed, total) pair from two adjacent ctr1/ctr2 cells."""
    missed = parse_int(tds[offset])
    total = parse_int(tds[offset + 1])
    return missed, total


def parse_report(html: str) -> tuple[list[CoverageRow], CoverageRow | None]:
    """Return (package_rows, total_row) from JaCoCo HTML."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="coveragetable")
    if not table:
        raise ValueError("No coverage table found in HTML")

    def row_to_coverage(tr: Tag) -> CoverageRow:
        tds = tr.find_all("td")
        element = tds[0].get_text(strip=True)
        instr_missed, instr_total = parse_bar_cell(tds[1])
        instr_cov = parse_pct(tds[2])
        branch_missed, branch_total = parse_bar_cell(tds[3])
        branch_cov = parse_pct(tds[4])
        cxty_missed, cxty_total = parse_counter_pair(tds, 5)
        lines_missed, lines_total = parse_counter_pair(tds, 7)
        methods_missed, methods_total = parse_counter_pair(tds, 9)
        classes_missed, classes_total = parse_counter_pair(tds, 11)
        return CoverageRow(
            element=element,
            instr_missed=instr_missed, instr_total=instr_total, instr_cov=instr_cov,
            branch_missed=branch_missed, branch_total=branch_total, branch_cov=branch_cov,
            cxty_missed=cxty_missed, cxty_total=cxty_total,
            lines_missed=lines_missed, lines_total=lines_total,
            methods_missed=methods_missed, methods_total=methods_total,
            classes_missed=classes_missed, classes_total=classes_total,
        )

    rows: list[CoverageRow] = []
    for tr in table.find("tbody").find_all("tr"):
        rows.append(row_to_coverage(tr))

    total_row = None
    tfoot = table.find("tfoot")
    if tfoot:
        total_row = row_to_coverage(tfoot.find("tr"))

    return rows, total_row


def check_thresholds(
    rows: list[CoverageRow],
    total: CoverageRow | None,
    min_instr: float,
    min_branch: float,
) -> list[str]:
    """Return list of violation messages."""
    violations = []
    all_rows = rows + ([total] if total else [])
    for r in all_rows:
        if r.instr_cov < min_instr:
            violations.append(
                f"[FAIL] {r.element}: instruction coverage {r.instr_cov:.0f}% < {min_instr:.0f}%"
            )
        if r.branch_total > 0 and r.branch_cov < min_branch:
            violations.append(
                f"[FAIL] {r.element}: branch coverage {r.branch_cov:.0f}% < {min_branch:.0f}%"
            )
    return violations


def print_report(rows: list[CoverageRow], total: CoverageRow | None) -> None:
    header = f"{'Package':<40} {'Instr':>7} {'Branch':>7} {'Lines':>7} {'Methods':>7}"
    print(header)
    print("─" * len(header))
    for r in rows:
        print(
            f"{r.element:<40} {r.instr_cov:>6.0f}% {r.branch_cov:>6.0f}% "
            f"{_pct(r.lines_missed, r.lines_total):>6.0f}% "
            f"{_pct(r.methods_missed, r.methods_total):>6.0f}%"
        )
    if total:
        print("─" * len(header))
        print(
            f"{'TOTAL':<40} {total.instr_cov:>6.0f}% {total.branch_cov:>6.0f}% "
            f"{_pct(total.lines_missed, total.lines_total):>6.0f}% "
            f"{_pct(total.methods_missed, total.methods_total):>6.0f}%"
        )


def _pct(missed: int, total: int) -> float:
    return ((total - missed) / total * 100) if total > 0 else 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Check JaCoCo HTML coverage report")
    parser.add_argument("report", help="Path to JaCoCo index.html")
    parser.add_argument("--min-instruction", type=float, default=80.0,
                        help="Min instruction coverage %% (default: 80)")
    parser.add_argument("--min-branch", type=float, default=70.0,
                        help="Min branch coverage %% (default: 70)")
    args = parser.parse_args()

    html = Path(args.report).read_text(encoding="utf-8")
    rows, total = parse_report(html)

    print_report(rows, total)
    print()

    violations = check_thresholds(rows, total, args.min_instruction, args.min_branch)
    if violations:
        print(f"{'='*60}")
        print(f"  {len(violations)} threshold violation(s) detected:")
        print(f"{'='*60}")
        for v in violations:
            print(f"  {v}")
        print()
        sys.exit(1)
    else:
        print("✓ All packages meet coverage thresholds.")


if __name__ == "__main__":
    main()