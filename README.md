ТАКОЕ БЫЛО ЗАДАНИЕ:

Задание 1.

Дано:
«prices.csv» - цены продукта, действует от даты начала цены до следующей даты изменения или последней даты реализации;
«vol.csv» объемы продаж

Нужно посчитать следующие показатели:

- Сумма продаж
- "Эффект" от изменения цены - разница цены * объем продаж

Задание 2.

Из таблицы («RevCrosTab.csv»), нужно перевернуть значение любого из полей в столбцы.

(!) Код должен легко переписываться на переворот любого из данных столбцов.

Задание 3.

Нужно построить таблицу остатков топлива ТС-1 на каждый календарный день за 2025 год. Таблица должна содержать поля: дата, Организация наименование, номенклатура наименование, фактическое количество топлива в конце дня.

Источники данных:

«Регистры Накопления Нефтепродукты организаций» - данные по нефтепродуктам организации (данные движения топлива: слив-/налив+)

«Резервуары Складов Остатки Топлива» - данные по остаткам топлива

# ClickHouse ETL - задания 1, 2, 3

## Структура проекта

```text
clickhouse_etl/
├── dags/
│   └── clickhouse_etl_dag.py
├── sql/
│   ├── 01_ddl.sql
│   ├── 02_task1_sales_effect.sql
│   ├── 03_task2_pivot_template.sql
│   └── 04_task3_daily_balance.sql
├── data/
│   ├── prices.csv
│   ├── vol.csv
│   ├── RevCrosTab.csv
│   ├── Регистры Накопления Нефтепродукты организаций.csv
│   └── Резервуары Складов Остатки Топлива.csv
├── scripts/
│   └── smoke_test.py
└── docker-compose.yml
```

## Быстрая проверка SQL без Airflow

Нужен только Docker Desktop и Python. В PowerShell из корня проекта:

```powershell
docker compose up -d clickhouse
python .\scripts\smoke_test.py --reset
```

Ожидаемый финал:

```text
Smoke test OK
```

Smoke-тест создает DDL, загружает все CSV в ClickHouse и выполняет SQL для трех заданий.

## Запуск через Airflow в Docker

1. Поднять ClickHouse, PostgreSQL и подготовить Airflow:

```powershell
docker compose up airflow-init
```

2. Запустить webserver и scheduler:

```powershell
docker compose up -d airflow-webserver airflow-scheduler
```

3. Открыть Airflow:

```text
http://localhost:8080
login: admin
password: admin
```

4. Снять DAG с паузы и запустить:

```powershell
docker compose exec airflow-webserver airflow dags unpause clickhouse_etl_tasks_1_2_3
docker compose exec airflow-webserver airflow dags trigger clickhouse_etl_tasks_1_2_3
```

Или через UI: DAGs -> `clickhouse_etl_tasks_1_2_3` -> Trigger DAG.

## Настройки, которые уже зашиты в docker-compose

```text
AIRFLOW_CONN_CLICKHOUSE_DEFAULT=http://clickhouse:8123
AIRFLOW_VAR_CLICKHOUSE_ETL_DATA_DIR=/opt/airflow/data
```

То есть вручную создавать Airflow Connection и Variable при Docker-запуске не нужно.

## Если запускать без docker-compose

В Airflow нужно создать HTTP connection:

| Поле | Значение |
|---|---|
| Conn Id | `clickhouse_default` |
| Conn Type | `HTTP` |
| Host | `http://localhost` или `http://<clickhouse-host>` |
| Port | `8123` |
| Login | `default` |
| Password | пусто или пароль ClickHouse |

И задать путь к CSV:

```powershell
airflow variables set clickhouse_etl_data_dir C:\absolute\path\to\clickhouse_etl\data
```

Либо через переменную окружения:

```powershell
$env:CLICKHOUSE_ETL_DATA_DIR = "C:\absolute\path\to\clickhouse_etl\data"
```

## Задание 2: смена оси pivot

Открой `dags/clickhouse_etl_dag.py`, найди функцию `pivot_task2` и измени одну строку:

```python
PIVOT_COLUMN = "flev"   # разворачивает flev в колонки, строки = (slev, dat1)
PIVOT_COLUMN = "slev"   # разворачивает slev в колонки, строки = (flev, dat1)
PIVOT_COLUMN = "dat1"   # разворачивает даты в колонки, строки = (flev, slev)
```

Остальное: чтение уникальных значений, генерация `sumIf`, `GROUP BY` и сортировка - строится автоматически.

## Результирующие таблицы

| Задание | Таблица | Описание |
|---|---|---|
| 1 | `sales.result_task1` | Сумма продаж и эффект изменения цены |
| 2 | - | Pivot выводится в лог задачи и XCom |
| 3 | `fuel.result_daily_balance` | Дневные остатки ТС-1 за 2025 год |

## Логика задания 3

```text
qty_end_of_day(date) =
    остаток на конец предыдущего месяца
  + сумма движений с первого дня текущего месяца по date включительно
```

Если движений за день нет, остаток переносится за счет оконной накопительной суммы.
