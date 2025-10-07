import csv
import io
import json
from typing import Any, Dict, Iterable, List, Sequence


def to_scalar(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False)


def flatten_one_level(record: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, dict):
            for sub_key, sub_val in value.items():
                flat[f"{key}.{sub_key}"] = to_scalar(sub_val)
        else:
            flat[key] = to_scalar(value)
    return flat


def csv_bytes_from_dicts_dynamic(records: List[Dict[str, Any]]) -> tuple[List[str], bytes]:
    header: List[str] = []
    seen: set[str] = set()
    for rec in records:
        for k in rec.keys():
            if k not in seen:
                seen.add(k)
                header.append(k)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(header)
    for rec in records:
        row = [rec.get(k, "") for k in header]
        writer.writerow(row)
    return header, buffer.getvalue().encode("utf-8")


def csv_bytes_from_dicts_fixed(records: Iterable[Dict[str, Any]], fieldnames: Sequence[str]) -> bytes:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for rec in records:
        writer.writerow(rec)
    return buffer.getvalue().encode("utf-8")
