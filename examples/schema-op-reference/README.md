# Schema Operation Reference (Local)

Локальные примеры веток value-schema для `oracle.cdc.hr.employees`.

Назначение:
- быстро увидеть, как выглядит контракт для каждой операции CDC;
- использовать как шпаргалку при обсуждении совместимости в Schema Registry;
- не зависеть от `schemas/`, который находится под `.gitignore`.

Файлы:
- `oracle.cdc.hr.employees.value.op-c.schema.json`
- `oracle.cdc.hr.employees.value.op-u.schema.json`
- `oracle.cdc.hr.employees.value.op-d.schema.json`

Смысл операций:
- `op="c"`: `before=null`, `after=strict row-image` (обязательные non-null поля присутствуют).
- `op="u"`: `before/after=partial row-image` (частичные образы, без `required` внутри row-image).
- `op="d"`: `before=strict row-image`, `after=null`.

Глоссарий ключей JSON Schema:
- `properties`: какие поля вообще разрешены в объекте и какого они типа.
- `required`: какие поля обязательно должны присутствовать в payload.
- `additionalProperties: false`: запрещает любые поля, которых нет в `properties`.
- `description`: человекочитаемое пояснение для поля/блока.
- `$comment`: технический комментарий по логике контракта (для людей и ревью).

Источник:
- Ветки взяты из текущего op-aware `oneOf` в локальной схеме
  `schemas/oracle.cdc.hr.employees.value.json`.
