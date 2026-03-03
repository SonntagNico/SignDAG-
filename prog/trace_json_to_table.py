#!/usr/bin/env python3
"""Convert dag_implications trace JSON output into CSV and PDF tables."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List


def sign_symbol(sign_prediction: str) -> str:
    if sign_prediction == "positive":
        return "+"
    if sign_prediction == "negative":
        return "-"
    if sign_prediction == "indeterminate":
        return "?"
    if sign_prediction == "independent":
        return "0"
    return ""


def query_key(query: Dict[str, object]) -> str:
    x = str(query.get("x", ""))
    y = str(query.get("y", ""))
    given = sorted([str(v) for v in query.get("given", [])])
    g = ",".join(given) if given else "{}"
    return f"{x},{y}|{g}"


def flatten_trace_payload(payload: Dict) -> tuple[list[str], list[Dict[str, str]], list[str], list[Dict[str, str]]]:
    summary_fields = [
        "model",
        "prediction",
        "overall_sign",
        "overall_symbol",
        "d_connected",
        "active_path_count",
    ]
    path_fields = [
        "model",
        "prediction",
        "path_index",
        "path",
        "path_sign",
        "path_symbol",
        "negative_edge_count",
        "controlled_collider_count",
        "parity_total",
        "negative_edges",
        "controlled_colliders",
    ]

    summary_rows: List[Dict[str, str]] = []
    path_rows: List[Dict[str, str]] = []

    for model in payload.get("models", []):
        model_name = str(model.get("name", ""))
        for trace in model.get("traces", []):
            pred = query_key(trace.get("query", {}))
            overall = str(trace.get("overall_sign_prediction", ""))
            summary_rows.append(
                {
                    "model": model_name,
                    "prediction": pred,
                    "overall_sign": overall,
                    "overall_symbol": sign_symbol(overall),
                    "d_connected": str(trace.get("d_connected", "")),
                    "active_path_count": str(trace.get("active_path_count", "")),
                }
            )
            for idx, p in enumerate(trace.get("active_paths", []), start=1):
                neg_edges = [f"{e['from']}->{e['to']}" for e in p.get("negative_edges", [])]
                cols = [str(c) for c in p.get("controlled_colliders", [])]
                path_sign = str(p.get("path_sign", ""))
                path_rows.append(
                    {
                        "model": model_name,
                        "prediction": pred,
                        "path_index": str(idx),
                        "path": str(p.get("path_str", "")),
                        "path_sign": path_sign,
                        "path_symbol": sign_symbol(path_sign),
                        "negative_edge_count": str(p.get("negative_edge_count", "")),
                        "controlled_collider_count": str(p.get("controlled_collider_count", "")),
                        "parity_total": str(p.get("parity_total", "")),
                        "negative_edges": ", ".join(neg_edges),
                        "controlled_colliders": ", ".join(cols),
                    }
                )

    summary_rows.sort(key=lambda r: (r["prediction"], r["model"]))
    path_rows.sort(key=lambda r: (r["prediction"], r["model"], int(r["path_index"])))
    return summary_fields, summary_rows, path_fields, path_rows


def write_csv(fields: List[str], rows: List[Dict[str, str]], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def escape_pdf_text(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def make_text_table_lines(fields: List[str], rows: List[Dict[str, str]]) -> List[str]:
    widths = {f: len(f) for f in fields}
    for r in rows:
        for f in fields:
            widths[f] = max(widths[f], len(r.get(f, "")))

    def fmt_row(vals: Dict[str, str]) -> str:
        return " | ".join(vals.get(f, "").ljust(widths[f]) for f in fields)

    lines = [fmt_row({f: f for f in fields}), "-+-".join("-" * widths[f] for f in fields)]
    lines.extend(fmt_row(r) for r in rows)
    return lines


def write_pdf_from_lines(lines: List[str], out_path: Path) -> None:
    page_width = 842
    page_height = 595
    line_height = 10
    top_margin = 560
    page_lines = 52

    pages = [lines[i : i + page_lines] for i in range(0, len(lines), page_lines)]
    objects: List[bytes] = []

    def add_obj(content: bytes) -> int:
        objects.append(content)
        return len(objects)

    font_id = add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")
    pages_id = add_obj(b"<< /Type /Pages /Kids [] /Count 0 >>")
    page_ids: List[int] = []

    for page in pages:
        text_lines = ["BT", "/F1 8 Tf", f"{line_height} TL", f"24 {top_margin} Td"]
        for i, line in enumerate(page):
            text_lines.append(f"({escape_pdf_text(line)}) Tj")
            if i != len(page) - 1:
                text_lines.append("T*")
        text_lines.append("ET")
        stream_data = ("\n".join(text_lines) + "\n").encode("latin-1", errors="replace")
        contents_id = add_obj(
            f"<< /Length {len(stream_data)} >>\nstream\n".encode("ascii") + stream_data + b"endstream"
        )
        page_id = add_obj(
            (
                f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {contents_id} 0 R >>"
            ).encode("ascii")
        )
        page_ids.append(page_id)

    kids = " ".join(f"{p} 0 R" for p in page_ids)
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("ascii")
    catalog_id = add_obj(f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("ascii"))

    pdf = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{i} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")

    xref_pos = len(pdf)
    pdf.extend(f"xref\n0 {len(objects)+1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        (
            f"trailer\n<< /Size {len(objects)+1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_pos}\n%%EOF\n"
        ).encode("ascii")
    )
    out_path.write_bytes(pdf)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert trace JSON to CSV and PDF tables.")
    p.add_argument("input_json", type=Path, help="Trace JSON from dag_implications.py --trace-prediction")
    p.add_argument("--summary-csv", type=Path, default=Path("trace_summary.csv"))
    p.add_argument("--summary-pdf", type=Path, default=Path("trace_summary.pdf"))
    p.add_argument("--paths-csv", type=Path, default=Path("trace_paths.csv"))
    p.add_argument("--paths-pdf", type=Path, default=Path("trace_paths.pdf"))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    payload = json.loads(args.input_json.read_text(encoding="utf-8-sig"))
    summary_fields, summary_rows, path_fields, path_rows = flatten_trace_payload(payload)

    write_csv(summary_fields, summary_rows, args.summary_csv)
    write_pdf_from_lines(make_text_table_lines(summary_fields, summary_rows), args.summary_pdf)

    write_csv(path_fields, path_rows, args.paths_csv)
    write_pdf_from_lines(make_text_table_lines(path_fields, path_rows), args.paths_pdf)

    print(f"Wrote {len(summary_rows)} summary rows to {args.summary_csv}")
    print(f"Wrote summary PDF to {args.summary_pdf}")
    print(f"Wrote {len(path_rows)} path rows to {args.paths_csv}")
    print(f"Wrote path PDF to {args.paths_pdf}")


if __name__ == "__main__":
    main()
