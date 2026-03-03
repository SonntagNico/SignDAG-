#!/usr/bin/env python3
"""Convert dag_implications.py JSON output into CSV and PDF tables."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


def prediction_key(x: str, y: str, given: Sequence[str]) -> str:
    g = ",".join(sorted(given)) if given else "{}"
    return f"{x},{y}|{g}"


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


def model_prediction_map(model_payload: Dict) -> Dict[str, str]:
    pred: Dict[str, str] = {}
    for item in model_payload.get("independencies", []):
        key = prediction_key(item["x"], item["y"], item.get("given", []))
        pred[key] = "0"
    for item in model_payload.get("dependencies_d_connected", []):
        key = prediction_key(item["x"], item["y"], item.get("given", []))
        pred[key] = sign_symbol(item.get("sign_prediction", ""))
    return pred


def load_rows_single(payload: Dict) -> Tuple[List[str], List[Dict[str, str]]]:
    mapping = model_prediction_map(payload)
    rows: List[Dict[str, str]] = []
    for key in sorted(mapping):
        symbol = mapping[key]
        rows.append(
            {
                "prediction": key,
                "outcome": symbol,
                "rendered": f"{key}: {symbol}",
            }
        )
    fields = ["prediction", "outcome", "rendered"]
    return fields, rows


def load_rows_multi(payload: Dict) -> Tuple[List[str], List[Dict[str, str]]]:
    model_entries = payload.get("models", [])
    model_names = [m["name"] for m in model_entries]
    per_model = {m["name"]: model_prediction_map(m) for m in model_entries}

    all_predictions = sorted({k for mp in per_model.values() for k in mp})
    rows: List[Dict[str, str]] = []
    for pred in all_predictions:
        row: Dict[str, str] = {"prediction": pred}
        for name in model_names:
            row[name] = per_model[name].get(pred, "")
        rows.append(row)
    fields = ["prediction"] + model_names
    return fields, rows


def load_rows(payload: Dict) -> Tuple[List[str], List[Dict[str, str]]]:
    if isinstance(payload.get("models"), list):
        return load_rows_multi(payload)
    return load_rows_single(payload)


def write_csv(fields: List[str], rows: List[Dict[str, str]], out_path: Path) -> None:
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def print_markdown(fields: List[str], rows: List[Dict[str, str]]) -> None:
    print("| " + " | ".join(fields) + " |")
    print("|" + "|".join(["---"] * len(fields)) + "|")
    for r in rows:
        print("| " + " | ".join(r.get(k, "") for k in fields) + " |")


def escape_pdf_text(s: str) -> str:
    return s.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def paginate(lines: List[str], page_lines: int) -> List[List[str]]:
    pages: List[List[str]] = []
    for i in range(0, len(lines), page_lines):
        pages.append(lines[i : i + page_lines])
    return pages


def make_text_table_lines(fields: List[str], rows: List[Dict[str, str]]) -> List[str]:
    widths = {f: len(f) for f in fields}
    for r in rows:
        for f in fields:
            widths[f] = max(widths[f], len(r.get(f, "")))

    def fmt_row(vals: Dict[str, str]) -> str:
        return " | ".join(vals.get(f, "").ljust(widths[f]) for f in fields)

    header = {f: f for f in fields}
    sep = "-+-".join("-" * widths[f] for f in fields)
    lines = [fmt_row(header), sep]
    for r in rows:
        lines.append(fmt_row(r))
    return lines


def write_pdf_from_lines(lines: List[str], out_path: Path) -> None:
    # Minimal built-in PDF writer (landscape A4), monospaced text.
    page_width = 842
    page_height = 595
    line_height = 10
    top_margin = 560
    page_lines = 52

    pages = paginate(lines, page_lines)
    objects: List[bytes] = []

    def add_obj(content: bytes) -> int:
        objects.append(content)
        return len(objects)

    font_id = add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>")
    pages_id = add_obj(b"<< /Type /Pages /Kids [] /Count 0 >>")
    page_ids: List[int] = []

    for page in pages:
        text_lines = ["BT", "/F1 8 Tf", f"{line_height} TL", f"24 {top_margin} Td"]
        for idx, line in enumerate(page):
            text_lines.append(f"({escape_pdf_text(line)}) Tj")
            if idx != len(page) - 1:
                text_lines.append("T*")
        text_lines.append("ET")
        stream_data = ("\n".join(text_lines) + "\n").encode("latin-1", errors="replace")
        contents_id = add_obj(
            f"<< /Length {len(stream_data)} >>\nstream\n".encode("ascii")
            + stream_data
            + b"endstream"
        )
        page_id = add_obj(
            (
                f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {contents_id} 0 R >>"
            ).encode("ascii")
        )
        page_ids.append(page_id)

    kids = " ".join(f"{pid} 0 R" for pid in page_ids)
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("ascii")
    catalog_id = add_obj(f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("ascii"))

    buf = bytearray()
    buf.extend(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for i, obj in enumerate(objects, start=1):
        offsets.append(len(buf))
        buf.extend(f"{i} 0 obj\n".encode("ascii"))
        buf.extend(obj)
        buf.extend(b"\nendobj\n")

    xref_pos = len(buf)
    buf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    buf.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        buf.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    buf.extend(
        (
            f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_pos}\n%%EOF\n"
        ).encode("ascii")
    )
    out_path.write_bytes(buf)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Turn DAG implication JSON into a table.")
    p.add_argument("input_json", type=Path, help="Input JSON from dag_implications.py --json")
    p.add_argument(
        "--output-csv",
        type=Path,
        default=Path("implications_table.csv"),
        help="Output CSV path (default: implications_table.csv)",
    )
    p.add_argument(
        "--output-pdf",
        type=Path,
        default=Path("implications_table.pdf"),
        help="Output PDF path (default: implications_table.pdf)",
    )
    p.add_argument(
        "--markdown",
        action="store_true",
        help="Also print the table in Markdown format to stdout.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    payload = json.loads(args.input_json.read_text(encoding="utf-8-sig"))
    fields, rows = load_rows(payload)
    write_csv(fields, rows, args.output_csv)
    lines = make_text_table_lines(fields, rows)
    write_pdf_from_lines(lines, args.output_pdf)
    if args.markdown:
        print_markdown(fields, rows)
    print(f"Wrote {len(rows)} rows to {args.output_csv}")
    print(f"Wrote PDF table to {args.output_pdf}")


if __name__ == "__main__":
    main()
