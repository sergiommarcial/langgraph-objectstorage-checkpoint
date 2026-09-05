from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_JSON = REPO_ROOT / ".benchmarks" / "latest.json"
README_PATH = REPO_ROOT / "README.md"
HTML_PATH = Path(__file__).resolve().parent / "report.html"
START_MARKER = "<!-- BENCHMARK-RESULTS:START -->"
END_MARKER = "<!-- BENCHMARK-RESULTS:END -->"

_DIMENSION_KEYS = ("envelope", "payload_size", "history_size")


def _row(benchmark: dict) -> dict:
    info = benchmark["extra_info"]
    stats = benchmark["stats"]
    dimension = (
        ", ".join(f"{key}={info[key]}" for key in _DIMENSION_KEYS if key in info)
        or "-"
    )
    return {
        "operation": info.get("operation", "?"),
        "backend": info.get("backend", "?"),
        "dimension": dimension,
        "mean_us": stats["mean"] * 1_000_000,
        "stddev_us": stats["stddev"] * 1_000_000,
        "ops": stats["ops"],
    }


def load_rows(json_path: Path) -> tuple[list[dict], str]:
    payload = json.loads(json_path.read_text())
    rows = [_row(b) for b in payload["benchmarks"]]
    measured_on = payload["datetime"][:10]
    return rows, measured_on


def render_markdown(rows: list[dict], measured_on: str) -> str:
    lines = [
        "| Operation | Backend | Dimension | Mean (µs) | StdDev (µs) | Ops/sec |",
        "|---|---|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row['operation']} | {row['backend']} | {row['dimension']} "
            f"| {row['mean_us']:.2f} | {row['stddev_us']:.2f} | {row['ops']:.1f} |"
        )
    caveat = (
        f"_Measured on {measured_on}, on maintainer hardware against local "
        "disk and an in-process moto S3 emulator -- a relative comparison "
        "across operations/backends/envelopes, not an absolute production "
        "guarantee._"
    )
    return "\n".join(lines) + "\n\n" + caveat


def update_readme(markdown_table: str) -> None:
    text = README_PATH.read_text()
    start = text.find(START_MARKER)
    end = text.find(END_MARKER)
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"README.md is missing {START_MARKER}/{END_MARKER} markers")
    new_text = (
        text[: start + len(START_MARKER)] + "\n" + markdown_table + "\n" + text[end:]
    )
    README_PATH.write_text(new_text)


def render_html(rows: list[dict]) -> str:
    max_ops = max((row["ops"] for row in rows), default=1.0)
    bars = []
    for row in rows:
        width = (row["ops"] / max_ops) * 100 if max_ops else 0.0
        label = f"{row['operation']} / {row['backend']} / {row['dimension']}"
        bars.append(
            '<div class="row">'
            f'<span class="label">{label}</span>'
            '<svg width="400" height="16">'
            f'<rect width="{width:.1f}%" height="16"></rect>'
            "</svg>"
            f'<span class="value">{row["ops"]:.0f} ops/sec</span>'
            "</div>"
        )
    return (
        '<!doctype html><html><head><meta charset="utf-8">'
        "<title>ObjectStorageSaver benchmark report</title>"
        "<style>"
        "body{font-family:monospace;background:#fff;color:#111;padding:2rem}"
        ".row{display:flex;align-items:center;gap:0.5rem;margin:0.25rem 0}"
        ".label{width:340px}"
        "svg rect{fill:#2563eb}"
        "</style></head><body>"
        "<h1>ObjectStorageSaver benchmark report</h1>"
        + "".join(bars)
        + "</body></html>"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", type=Path, default=DEFAULT_JSON)
    args = parser.parse_args()

    rows, measured_on = load_rows(args.json)
    update_readme(render_markdown(rows, measured_on))
    HTML_PATH.write_text(render_html(rows))
    print(f"Updated {README_PATH} and wrote {HTML_PATH}")


if __name__ == "__main__":
    main()
