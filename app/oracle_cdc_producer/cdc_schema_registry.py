"""Schema Registry + CDC envelope utilities for Oracle LogMiner SR-CDC runner.

Вынесено в отдельный модуль, чтобы основной runtime-скрипт
оставался компактным и читабельным.

Структура модуля:
1) `SchemaRuntime`:
   - лениво создает serializer key/value для каждого topic;
   - читает JSON Schema из локальной папки;
   - сериализует key/value в wire-format Confluent SR.
2) Метаданные таблиц:
   - `load_table_metadata()` вытягивает колонки и PK;
   - используется для режима ключа `cdc_key_mode=pk`.
3) Parser SQL_REDO/SQL_UNDO вынесен в `cdc_sql_parser.py`.
4) `build_cdc_event()`:
   - собирает итоговые key/value для CDC envelope.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import oracledb

try:
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.json_schema import JSONSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext
except Exception:  # pragma: no cover
    SchemaRegistryClient = None
    JSONSerializer = None
    SerializationContext = None
    MessageField = None

try:
    from .cdc_sql_parser import parse_cdc_row_images
except ImportError:  # pragma: no cover
    from cdc_sql_parser import parse_cdc_row_images

# Простые алиасы для читаемости сигнатур.
RowDict = Dict[str, Any]
TableMeta = Dict[str, Any]


def schema_registry_dependencies_available() -> bool:
    """Проверяет, доступен ли стек Schema Registry в установленном confluent-kafka."""
    return all(
        item is not None
        for item in (SchemaRegistryClient, JSONSerializer, SerializationContext, MessageField)
    )


class SchemaRuntime:
    """Lazy-загрузка сериализаторов по topic из локальных schema-файлов.

    Для topic `oracle.cdc.hr.employees` ожидаем файлы:
    - ./schemas/oracle.cdc.hr.employees.key.json
    - ./schemas/oracle.cdc.hr.employees.value.json

    Это прототип: только URL, без SSL/auth к SR.
    """

    def __init__(self, cfg: Any):
        # Ленивые кэши сериализаторов по topic.
        # Это снижает overhead: схема читается/инициализируется один раз на topic.
        self.cfg = cfg
        self._key_serializers: Dict[str, Any] = {}
        self._value_serializers: Dict[str, Any] = {}
        self._preflight_topics: set[str] = set()

        if not cfg.cdc_envelope_enabled:
            self.client = None
            return

        if not schema_registry_dependencies_available():
            raise RuntimeError("Schema Registry dependencies are unavailable")

        self.client = SchemaRegistryClient({"url": cfg.schema_registry_url})

    def _sr_request_json(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Выполняет REST-запрос к SR URL из конфига и возвращает JSON-ответ."""
        if self.client is None:
            raise RuntimeError("SchemaRuntime SR request called while CDC_ENVELOPE_ENABLED=false")
        # Шаг 1: готовим endpoint и (опционально) JSON body.
        endpoint = self.cfg.schema_registry_url.rstrip("/") + path
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None

        # Шаг 2: формируем HTTP request.
        req = urllib.request.Request(endpoint, data=body, method=method.upper())
        if body is not None:
            req.add_header("Content-Type", "application/vnd.schemaregistry.v1+json")

        # Шаг 3: выполняем запрос и парсим JSON-ответ.
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}

    def _schema_file_path(self, topic: str, kind: str) -> Path:
        return Path(self.cfg.schema_dir) / f"{topic}.{kind}.json"

    def _load_schema_text(self, topic: str, kind: str) -> str:
        path = self._schema_file_path(topic, kind)
        if not path.exists():
            raise RuntimeError(f"Schema file not found: {path}")
        return path.read_text(encoding="utf-8")

    def _json_serializer(self, schema_str: str) -> Any:
        # to_dict возвращает объект без преобразований:
        # мы уже формируем dict в runtime и отдаём его сериализатору как есть.
        return JSONSerializer(
            schema_str=schema_str,
            schema_registry_client=self.client,
            to_dict=lambda obj, _ctx: obj,
            conf={
                # Флаги ниже синхронизированы с env-параметрами producer-а.
                "auto.register.schemas": self.cfg.schema_auto_register,
                "use.latest.version": self.cfg.schema_use_latest_version,
                "normalize.schemas": self.cfg.schema_normalize,
            },
        )

    def _preflight_subject_compatibility(self, subject: str, schema_text: str) -> None:
        """Проверяет совместимость schema с latest версией subject в SR.

        Поведение:
        - если subject отсутствует и `SCHEMA_AUTO_REGISTER=true`, считаем preflight успешным;
        - если subject отсутствует и `SCHEMA_AUTO_REGISTER=false`, это ошибка;
        - если `is_compatible=false`, это ошибка.
        """
        # Шаг 1: URL-экранируем subject и формируем payload в формате SR compatibility API.
        subject_q = urllib.parse.quote(subject, safe="")
        payload = {
            "schemaType": "JSON",
            "schema": schema_text,
        }
        try:
            # Шаг 2: проверяем совместимость с latest версией subject.
            result = self._sr_request_json(
                method="POST",
                path=f"/compatibility/subjects/{subject_q}/versions/latest",
                payload=payload,
            )
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            if exc.code == 404:
                # 404 означает "subject не существует".
                # Для auto-register допускаем этот случай как первый запуск новой схемы.
                if self.cfg.schema_auto_register:
                    return
                # Если auto-register выключен, отсутствие subject — это блокирующая ошибка.
                raise RuntimeError(
                    f"Schema Registry subject not found for preflight: {subject} "
                    f"(SCHEMA_AUTO_REGISTER=false, HTTP 404 {body})"
                ) from exc
            # Прочие HTTP-ошибки считаем проблемой preflight.
            raise RuntimeError(
                f"Schema Registry preflight failed for subject={subject}: HTTP {exc.code} {body}"
            ) from exc

        # Шаг 3: явная несовместимость останавливает запуск до чтения LogMiner batch.
        if result.get("is_compatible") is False:
            raise RuntimeError(f"Schema Registry compatibility check failed for subject={subject} (latest)")

    def key_serializer(self, topic: str) -> Any:
        if topic not in self._key_serializers:
            # Ленивая инициализация: schema читается/парсится один раз на topic.
            self._key_serializers[topic] = self._json_serializer(self._load_schema_text(topic, "key"))
        return self._key_serializers[topic]

    def value_serializer(self, topic: str) -> Any:
        if topic not in self._value_serializers:
            # Аналогичный lazy-cache для value schema.
            self._value_serializers[topic] = self._json_serializer(self._load_schema_text(topic, "value"))
        return self._value_serializers[topic]

    def preflight_topic(self, topic: str) -> None:
        """Preflight topic для CDC SR режима:
        1) проверяем наличие локальных schema-файлов;
        2) проверяем совместимость `<topic>-key/value` с latest subject в SR.
        """
        if not self.cfg.cdc_envelope_enabled:
            return
        if topic in self._preflight_topics:
            # Уже проверяли этот topic в текущем запуске, повторять не нужно.
            return

        # Шаг 1: убеждаемся, что локальные схемы реально существуют.
        key_schema_text = self._load_schema_text(topic, "key")
        value_schema_text = self._load_schema_text(topic, "value")
        # Шаг 2: проверяем SR compatibility отдельно для key/value subject-ов.
        self._preflight_subject_compatibility(f"{topic}-key", key_schema_text)
        self._preflight_subject_compatibility(f"{topic}-value", value_schema_text)
        # Шаг 3: кэшируем успешный preflight topic, чтобы не дергать SR повторно.
        self._preflight_topics.add(topic)

    def serialize_key(self, topic: str, key_obj: RowDict) -> bytes:
        if not self.cfg.cdc_envelope_enabled:
            raise RuntimeError("serialize_key called while CDC_ENVELOPE_ENABLED=false")
        serializer = self.key_serializer(topic)
        return serializer(key_obj, SerializationContext(topic, MessageField.KEY))

    def serialize_value(self, topic: str, value_obj: RowDict) -> bytes:
        if not self.cfg.cdc_envelope_enabled:
            raise RuntimeError("serialize_value called while CDC_ENVELOPE_ENABLED=false")
        serializer = self.value_serializer(topic)
        return serializer(value_obj, SerializationContext(topic, MessageField.VALUE))


def qualified_table_name(row: RowDict) -> str:
    """Возвращает OWNER.TABLE в upper-case."""
    return f"{str(row.get('seg_owner', '')).upper()}.{str(row.get('table_name', '')).upper()}"


def is_cdc_table_enabled(cfg: Any, row: RowDict) -> bool:
    """Фильтрует таблицы для CDC path по списку CDC_SUPPORTED_TABLES."""
    if not cfg.cdc_supported_tables:
        return True
    return qualified_table_name(row) in cfg.cdc_supported_tables


_TABLE_META_CACHE: Dict[str, TableMeta] = {}


def load_table_metadata(conn: oracledb.Connection, owner: str, table: str) -> TableMeta:
    """Грузит метаданные таблицы (колонки + PK) с кэшированием.

    Эти метаданные используются для построения CDC key/value,
    прежде всего для режима ключа `cdc_key_mode=pk`.
    """
    cache_key = f"{owner.upper()}.{table.upper()}"
    if cache_key in _TABLE_META_CACHE:
        return _TABLE_META_CACHE[cache_key]

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, nullable, column_id
            FROM all_tab_columns
            WHERE owner = :owner AND table_name = :table_name
            ORDER BY column_id
            """,
            {"owner": owner.upper(), "table_name": table.upper()},
        )
        columns = [
            {
                "name": str(name).upper(),
                "oracle_type": str(data_type).upper(),
                "nullable": str(nullable).upper() == "Y",
                "column_id": int(column_id),
            }
            for name, data_type, nullable, column_id in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT c.column_name, c.position
            FROM all_constraints a
            JOIN all_cons_columns c
              ON a.owner = c.owner
             AND a.constraint_name = c.constraint_name
             AND a.table_name = c.table_name
            WHERE a.owner = :owner
              AND a.table_name = :table_name
              AND a.constraint_type = 'P'
            ORDER BY c.position
            """,
            {"owner": owner.upper(), "table_name": table.upper()},
        )
        pk_columns = [str(column_name).upper() for column_name, _position in cur.fetchall()]

    result = {
        "owner": owner.upper(),
        "table": table.upper(),
        "columns": columns,
        "pk_columns": pk_columns,
    }
    _TABLE_META_CACHE[cache_key] = result
    return result


def _build_source_block(row: RowDict) -> RowDict:
    """Формирует source-блок CDC envelope из служебных полей LogMiner."""
    ts_ms: Optional[int] = None
    ts_value = row.get("timestamp")
    if isinstance(ts_value, str):
        try:
            # best-effort: если timestamp не ISO, оставляем ts_ms=None
            # и не валим весь batch только из-за source.ts_ms.
            ts_ms = int(datetime.fromisoformat(ts_value).timestamp() * 1000)
        except Exception:
            ts_ms = None

    return {
        "schema": str(row.get("seg_owner", "")).upper(),
        "table": str(row.get("table_name", "")).upper(),
        "commit_scn": int(row.get("commit_scn") or 0),
        "redo_sequence": int(row.get("redo_sequence") or 0),
        "rs_id": str(row.get("rs_id") or ""),
        "ssn": int(row.get("ssn") or 0),
        "ts_ms": ts_ms,
    }


def _build_pk_key(row_image: RowDict, table_meta: TableMeta) -> RowDict:
    """Собирает key объект по PK колонкам таблицы."""
    pk_columns = [str(col).upper() for col in table_meta.get("pk_columns", [])]
    if not pk_columns:
        raise RuntimeError(
            f"No PK metadata found for {table_meta.get('owner')}.{table_meta.get('table')}"
        )
    key_obj: RowDict = {}
    for pk_col in pk_columns:
        if pk_col not in row_image:
            raise RuntimeError(f"PK column {pk_col} not found in parsed row image")
        key_obj[pk_col] = row_image[pk_col]
    return key_obj


def _build_technical_key_object(row: RowDict) -> RowDict:
    """Технический fallback-key (schema/table + позиция в redo stream)."""
    return {
        "schema": str(row.get("seg_owner", "")).upper(),
        "table": str(row.get("table_name", "")).upper(),
        "commit_scn": int(row.get("commit_scn") or 0),
        "redo_sequence": int(row.get("redo_sequence") or 0),
        "rs_id": str(row.get("rs_id") or ""),
        "ssn": int(row.get("ssn") or 0),
    }


def build_cdc_event(row: RowDict, table_meta: TableMeta, cfg: Any) -> Tuple[RowDict, RowDict]:
    """Строит CDC key/value для SR serialization.

    Поддерживает INSERT/UPDATE/DELETE в рамках прототипного парсера.
    Для сложных SQL/DDL parser может не подойти — в этом случае
    исключение поднимается наверх в fail-fast режиме.
    """
    op_code, before, after = parse_cdc_row_images(
        operation=str(row.get("operation") or ""),
        sql_redo=str(row.get("sql_redo") or ""),
        sql_undo=str(row.get("sql_undo") or ""),
        table_meta=table_meta,
        parser_backend=str(getattr(cfg, "cdc_sql_parser_backend", "auto") or "auto"),
    )

    # Для INSERT берем ключ из after, для DELETE — из before, для UPDATE
    # приоритет after (но fallback на before сохранён для устойчивости).
    key_basis = after or before or {}
    key_obj = (
        _build_pk_key(key_basis, table_meta)
        if cfg.cdc_key_mode == "pk"
        else _build_technical_key_object(row)
    )

    value_obj = {
        "op": op_code,
        "source": _build_source_block(row),
        "before": before,
        "after": after,
    }
    return key_obj, value_obj
