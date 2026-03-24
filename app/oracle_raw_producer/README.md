# Oracle Raw Producer

Этот модуль отвечает за отдельный raw-only процесс:
- читает изменения из Oracle LogMiner;
- публикует сырой JSON из `v$logmnr_contents` в Kafka;
- не использует CDC envelope и Schema Registry.

## Запуск

Сервис в `docker-compose.yaml`:

```bash
docker compose run --rm oracle-producer-archivelog-raw
```

Точка входа:
- `python app/oracle_raw_producer/producer_archivelog_raw.py`

Env:
- `app/oracle_raw_producer/env/oracle-producer-archivelog-raw.env`

## Когда использовать

- Когда нужен технический зеркальный поток LogMiner в Kafka без схем SR.
- Для отладки/диагностики SQL_REDO/SQL_UNDO до CDC-трансформаций.

Для прод CDC-потока используйте `app/oracle_cdc_producer`.
