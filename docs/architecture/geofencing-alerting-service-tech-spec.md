# Техническая реализация и Архитектура

**Документ:** `docs/architecture/geofencing-alerting-service-tech-spec.md`
**Статус:** Approved for Development
**Сервис:** geofencing-alerting-service (Поддомен: Telemetry Ingestion & Real-Time IoT)

## 1. Технологический Стек и Зависимости

| Компонент | Технология | Назначение / Обоснование |
| :--- | :--- | :--- |
| **Runtime** | Java 21 (Virtual Threads) | Легковесная обработка событий потоковой аналитики. |
| **Framework** | Kafka Streams API / Spring Cloud Stream | Потоковая обработка данных "на лету" со встроенным сбоем состояний (State Stores). |
| **Spatial Engine** | JTS (Java Topology Suite) / Spatial4j | Выполнение геометрических операций в памяти (Point-in-Polygon, Distance calculation). |
| **In-Memory Spatial Index** | R-Tree (STRtree в JTS) | Пространственный индекс для поиска пересечений точек и полигонов за $O(\log N)$. |
| **State Store** | RocksDB (Embedded in Kafka Streams) | Локальное быстрое хранилище предыдущих состояний машин (была ли машина в зоне на прошлом шаге). |
| **Database** | PostgreSQL 16 + PostGIS | Синхронизация и хранение мастер-данных гео-зон. |
| **Cache** | Redis | Быстрое считывание динамических правил алертинга. |

## 2. Архитектура Потоковой Аналитики (Kafka Streams Pipeline)

Сервис строится как Stateful Stream Processor:

```text
 ┌────────────────────────────────┐
 │ Topic: telemetry.points.v1     │
 └───────────────┬────────────────┘
                 │
                 ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                      KAFKA STREAMS PROCESSOR                           │
 │                                                                        │
 │   [1. KStream Consumer]                                                │
 │            │                                                           │
 │            ▼                                                           │
 │   [2. R-Tree Spatial Lookup] (Is Point in Polygon?)                    │
 │            │                                                           │
 │            ▼                                                           │
 │   [3. RocksDB State Store]   ◄── (Compare with previous state:         │
 │            │                       WAS_OUTSIDE -> NOW_INSIDE)          │
 │            ▼                                                           │
 │   [4. Sensor Threshold Evaluator]                                      │
 └───────────────┬────────────────────────────────────────────────────────┘
                 │
                 ▼
 ┌────────────────────────────────┐
 │ Topic: geofence.events.v1      │
 └────────────────────────────────┘
```

*   **R-Tree Spatial Index:** Все активные гео-зоны загружаются при старте в STRtree (R-Tree Index) в памяти JVM. Вместо проверки точки со всеми полигонами в системе, R-Tree сокращает поиск до 2-3 кандидатов за $O(\log N)$.
*   **State Store (RocksDB):** Для каждого `device_id` в локальном RocksDB хранятся атрибуты предыдущей точки ($T_{n-1}$) и список зон, в которых машина находилась.
*   **Hysteresis Filter (Защита от дребезга):** Если машина стоит на границе гео-зоны и GPS-координата "дышит", алерт генерируется только при уходе от границы более чем на 10 метров или после 3 точек подряд.

## 3. Межсервисное Взаимодействие и Интеграции

### 3.1 Схема интеграционных связей

```text
                                       ┌─────────────────────────┐
                                       │    routing-service      │
                                       └────────────┬────────────┘
                                                    │
                                         Kafka: GeofenceZoneCreatedEvent
                                                    │
                                                    ▼
┌──────────────────────────┐   Kafka:   ┌─────────────────────────┐   Kafka:                    ┌─────────────────────────┐
│telemetry-ingestion-service├──Telemetry├►geofencing-alerting-service├──GeofenceEvent/Alert─────►│ predictive-risk-service │
└──────────────────────────┘   Points   └─────────────────────────┘                             └─────────────────────────┘
```

### 3.2 Описание контрактов взаимодействия

**Входящие события (Kafka Consumers):**
*   `TelemetryPointReceivedEvent` (из топика `logistics.telemetry.points.v1`): Поток гео-точек и показаний датчиков.
*   `GeofenceZoneCreatedEvent` (из топика `logistics.routing.geofence.created`): Событие создания нового склада/коридора. Сервис на лету обновляет свой внутренний R-Tree индекс.

**Исходящие события (Kafka Producers):**
*   `GeofenceEvent`:
    *   **Topic:** `logistics.events.geofencing.v1`
    *   **Payload (Avro):** `event_id`, `device_id`, `zone_id`, `event_type` (ENTERED, EXITED, CORRIDOR_DEVIATED), `timestamp`, `location`.
*   `TelemetryAlertEvent`:
    *   **Topic:** `logistics.alerts.telemetry.v1`
    *   **Payload (Avro):** `alert_id`, `device_id`, `alert_type` (TEMPERATURE_EXCEEDED, DOOR_OPENED), `severity` (WARNING, CRITICAL), `details_map`.

## 4. Оптимизация Производительности и Масштабирование

### 4.1 Пространственный поиск через R-Tree (Spatial Indexing)
Прямая геометрическая проверка точки внутри сложного полигона (алгоритм Ray-Casting) — дорогостоящая операция.
*   **Решение:** Проверка делится на два этапа:
    1.  **Bounding Box Check:** Поиск по R-Tree индексу в памяти дает полигоны, чьи прямоугольные границы пересекаются с точкой (занимает наносекунды).
    2.  **Exact Polygon Check:** Точный алгоритм Ray-Casting запускается только для 1-2 полигонов, прошедших этап 1.

### 4.2 Партицирование и Масштабирование в Kafka Streams
*   Топик `logistics.telemetry.points.v1` партицируется по `device_id` (например, 32 партиции).
*   Каждая партиция обрабатывается выделенным Stream Thread.
*   При увеличении нагрузки поднимаются новые Docker-контейнеры сервиса, Kafka автоматически перераспределяет партиции между ними (Rebalancing).

## 5. Требования к Хранению Данных (Database & State)

### 5.1 Local State Store (RocksDB)
*   Хранилище StateStore управляется фреймворком Kafka Streams.
*   Каждая партиция имеет свой бинарный RocksDB файл на диске.
*   Все изменения статусов резервируются в Kafka Changelog Topics (`-changelog`), что обеспечивает мгновенное восстановление состояния при падении пода в Kubernetes.

## 6. Observability и Эксплуатация

*   **Metrics (Micrometer + Prometheus):**
    *   `geofencing_point_processing_latency_seconds` (histogram) — время обработки одной гео-точки в Kafka Streams.
    *   `geofencing_events_generated_total` (counter с тегами `type=ENTER|EXIT|CORRIDOR_DEVIATED`) — количество сгенерированных событий.
    *   `kafka_streams_state_store_rocksdb_bytes` (gauge) — размер локального состояния RocksDB.
*   **Logging:** Минимальное логирование в основном потоке. В лог выводится только факт генерации нового бизнес-алерта с `trace_id`.
