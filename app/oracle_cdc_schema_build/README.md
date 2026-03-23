# Oracle CDC Schema Build

Этот модуль отвечает за процесс подготовки схем:
- генерация JSON Schema из Oracle metadata;
- опциональная регистрация схем в Schema Registry.

Основные скрипты:
- `build_schemas_from_oracle.py`
- `generate_json_schemas.py`

Конфиг:
- `env/oracle-schema-build.env`

Каталог схем:
- `schemas/` (корневая папка проекта, в контейнере `/app/schemas`)
