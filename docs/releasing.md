# Выпуск релизов

CI и tag-driven release автоматически выбирают минимальный безопасный объём
проверок по изменениям относительно base commit или предыдущего стабильного
SemVer-тега.

## Режимы проверки

| Scope | Когда выбирается | Проверки | Docker |
| --- | --- | --- | --- |
| `indicators` | Менялись только индикаторы, detector policy, их конфигурация, тесты и документация | Целевой pytest-набор, Clean Architecture, JavaScript, MkDocs, package build и lightweight wheel smoke | Нет |
| `docs` | Менялась только документация | MkDocs, статические проверки и package build при релизе | Нет |
| `metadata` | Менялись только версия, changelog и release notes | Статические проверки, package build и lightweight wheel smoke | Нет |
| `full` | Зависимости, lockfile не только с версией, миграции, Docker/Compose, runtime или любой неизвестный путь | Полный pytest, все статические проверки, package build и production image smoke | Да |

Классификатор намеренно fail-safe: новый путь, который ещё не описан как часть
индикаторного компонента, получает `full`. Правила находятся в
`.github/release-scopes.json`, а решение с причиной и списком файлов попадает в
GitHub Actions Job Summary.

Изменение только `project.version` в `pyproject.toml` и версии корневого пакета
в `uv.lock` считается release metadata. Любое изменение зависимостей в этих
файлах автоматически включает полный режим.

## Обычный релиз индикатора

1. Добавьте или измените доменную логику индикатора и тесты. Для новых модулей
   используйте `domain/indicators`, `application/indicators` и при необходимости
   `adapters/indicators`, сохраняя Clean Architecture dependency rule.
2. Откройте PR. CI покажет scope `indicators` и выполнит целевые тесты без
   Docker build.
3. Подготовьте patch/minor-версию и release notes:

   ```bash
   uv version --bump patch
   ```

   Команда одновременно обновит `pyproject.toml` и `uv.lock`. Добавьте файл
   `docs/releases/v1.0.1.md` и запись в `CHANGELOG.md`. Для нового совместимого
   набора возможностей используйте `uv version --bump minor`.
4. После merge поставьте тег на commit из `main`:

   ```bash
   git tag -a v1.0.1 <main-commit> -m "Investment Signals Core v1.0.1"
   git push origin v1.0.1
   ```

Release workflow сравнит тег с предыдущим стабильным релизом. Если весь diff
остаётся внутри индикаторного scope, огромный Docker-стек собираться не будет;
GitHub Release получит проверенные wheel и source archive.

## Когда полный режим обязателен

- изменение `pyproject.toml` или `uv.lock`, кроме номера версии;
- миграции PostgreSQL/ClickHouse, protobuf и публичные контракты;
- Dockerfile, Compose, GitHub Actions и deployment-конфигурация;
- общие сервисы, API, persistence, Kafka/Redis/ClickHouse adapters;
- изменение самого классификатора или его manifest.

Перед расширением списка быстрых путей добавьте тесты классификатора и убедитесь,
что целевой pytest-набор покрывает новый компонент. Ослаблять scope только ради
ускорения релиза нельзя.
