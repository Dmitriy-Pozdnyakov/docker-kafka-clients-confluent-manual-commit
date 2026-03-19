#!/usr/bin/env python3
"""Генератор JSON Schema файлов для Oracle LogMiner CDC + Schema Registry.

Что делает:
1) Генерирует пары файлов `<topic>.key.json` и `<topic>.value.json`.
2) Умеет работать в двух режимах:
   - через готовый список topic'ов;
   - через список таблиц `SCHEMA.TABLE` + topic-prefix.

Примеры:
  python app/oracle_logminer_cdc/generate_json_schemas.py \
    --topics oracle.cdc.hr.employees,oracle.cdc.hr.departments

  python app/oracle_logminer_cdc/generate_json_schemas.py \
    --tables HR.EMPLOYEES,HR.DEPARTMENTS \
    --topic-prefix oracle.cdc
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List


def _parse_csv(raw: str) -> List[str]:
    items: List[str] = []
    for item in raw.split(","):
        value = item.strip()
        if value and value not in items:
            items.append(value)
    return items


def _sanitize_topic_part(value: str) -> str:
    chars: List[str] = []
    for ch in value.strip().lower():
        chars.append(ch if (ch.isalnum() or ch in {"-", "_", "."}) else "_")
    out = "".join(chars).strip("._-")
    return out or "unknown"


def _tables_to_topics(tables: List[str], prefix: str, sep: str) -> List[str]:
    topics: List[str] = []
    topic_prefix = _sanitize_topic_part(prefix)
    for table in tables:
        parts = table.strip().split(".")
        if len(parts) != 2:
            raise ValueError(f"Table must be in SCHEMA.TABLE format: {table!r}")
        schema_name = _sanitize_topic_part(parts[0])
        table_name = _sanitize_topic_part(parts[1])
        base = sep.join([schema_name, table_name])
        topic = sep.join([topic_prefix, base]) if topic_prefix else base
        if topic not in topics:
            topics.append(topic)
    return topics


def _key_schema(topic: str) -> Dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"{topic}-key",
        "type": "object",
        "properties": {
            "schema": {"type": "string"},
            "table": {"type": "string"},
            "commit_scn": {"type": "integer"},
            "redo_sequence": {"type": "integer"},
            "rs_id": {"type": "string"},
            "ssn": {"type": "integer"},
        },
        "required": ["schema", "table", "commit_scn", "redo_sequence", "rs_id", "ssn"],
        "additionalProperties": False,
    }


def _value_schema(topic: str) -> Dict:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": f"{topic}-value",
        "type": "object",
        "properties": {
            "op": {"type": "string", "enum": ["c", "u", "d"]},
            "source": {
                "type": "object",
                "properties": {
                    "schema": {"type": "string"},
                    "table": {"type": "string"},
                    "commit_scn": {"type": "integer"},
                    "redo_sequence": {"type": "integer"},
                    "rs_id": {"type": "string"},
                    "ssn": {"type": "integer"},
                    "ts_ms": {"type": ["integer", "null"]},
                },
                "required": ["schema", "table", "commit_scn", "redo_sequence", "rs_id", "ssn"],
                "additionalProperties": False,
            },
            "before": {"type": ["object", "null"]},
            "after": {"type": ["object", "null"]},
        },
        "required": ["op", "source", "before", "after"],
        "additionalProperties": False,
    }


def _write_json(path: Path, payload: Dict, overwrite: bool) -> str:
    if path.exists() and not overwrite:
        return "skip"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return "write"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate JSON Schema files for SR topics.")
    parser.add_argument(
        "--topics",
        default="",
        help="CSV list of full topic names (e.g. oracle.cdc.hr.employees,oracle.cdc.hr.departments)",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="CSV list SCHEMA.TABLE; used with --topic-prefix to build topic names",
    )
    parser.add_argument("--topic-prefix", default="oracle.cdc", help="Prefix for --tables mode")
    parser.add_argument("--topic-separator", default=".", help="Topic separator (default: .)")
    parser.add_argument(
        "--schema-dir",
        default="app/oracle_logminer_cdc/schemas",
        help="Output directory for generated schema files",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing schema files")
    args = parser.parse_args()

    topics = _parse_csv(args.topics)
    if not topics and args.tables:
        topics = _tables_to_topics(_parse_csv(args.tables), args.topic_prefix, args.topic_separator)
    if not topics:
        raise SystemExit("Provide --topics or --tables")

    out_dir = Path(args.schema_dir)
    written = 0
    skipped = 0

    print(f"[schema-generator] output_dir={out_dir}")
    for topic in topics:
        key_path = out_dir / f"{topic}.key.json"
        value_path = out_dir / f"{topic}.value.json"

        key_state = _write_json(key_path, _key_schema(topic), args.overwrite)
        value_state = _write_json(value_path, _value_schema(topic), args.overwrite)

        for state, path in ((key_state, key_path), (value_state, value_path)):
            if state == "write":
                written += 1
                print(f"[schema-generator] write  {path}")
            else:
                skipped += 1
                print(f"[schema-generator] skip   {path} (exists)")

    print(
        f"[schema-generator] done topics={len(topics)} written={written} skipped={skipped} "
        f"overwrite={args.overwrite}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
