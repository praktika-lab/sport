
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

Задание 3

Нужно построить таблицу остатков топлива ТС-1 на каждый календарный день за 2025 год. Таблица должна содержать поля: дата, Организация наименование, номенклатура наименование, фактическое количество топлива в конце дня

Источники данных:

«Регистры Накопления Нефтепродукты организаций» – данные по нефтепродуктам организации (данные движения топлива(слив-/налив+))

«Резервуары Складов Остатки Топлива» - данные по остаткам топлив

ЭТО РЕШЕНИЕ:

# ClickHouse ETL — Задания 1, 2, 3

## Структура проекта

```
clickhouse_etl/
├── dags/
│   └── clickhouse_etl_dag.py       # Airflow DAG (все три задания)
├── sql/
│   ├── 01_ddl.sql                  # DDL: CREATE DATABASE / TABLE
│   ├── 02_task1_sales_effect.sql   # Задание 1: сумма продаж + эффект цены
│   ├── 03_task2_pivot_template.sql # Задание 2: примеры pivot SQL
│   └── 04_task3_daily_balance.sql  # Задание 3: ежедневные остатки
└── data/                           # Папка для CSV-файлов
    ├── prices.csv
    ├── vol.csv
    ├── RevCrosTab.csv
    ├── Регистры_Накопления_Нефтепродукты_организаций.csv
    └── Резервуары_Складов_Остатки_Топлива.csv
```

---

## Быстрый старт

### 1. Зависимости

```bash
pip install apache-airflow
pip install apache-airflow-providers-http
pip install clickhouse-driver   # опционально, для альтернативного клиента
pip install pandas
```

### 2. Airflow Connection

В Airflow UI → Admin → Connections создай соединение:

| Поле       | Значение                         |
|------------|----------------------------------|
| Conn Id    | `clickhouse_default`             |
| Conn Type  | `HTTP`                           |
| Host       | `http://localhost` (или IP)      |
| Port       | `8123`                           |
| Login      | `default` (или твой юзер)        |
| Password   | `<пароль>`                       |

Для проверки:
```bash
curl 'http://localhost:8123/?user=default&password=<pw>' --data 'SELECT version()'
```

### 3. Airflow Variable (путь к CSV)

```bash
airflow variables set clickhouse_etl_data_dir /absolute/path/to/clickhouse_etl/data
```

Или задай переменную окружения `CLICKHOUSE_ETL_DATA_DIR`.

### 4. Положи CSV в папку data/

```bash
cp /path/to/*.csv /absolute/path/to/clickhouse_etl/data/
```

### 5. Скопируй DAG

```bash
cp dags/clickhouse_etl_dag.py $AIRFLOW_HOME/dags/
```

### 6. Запуск

```bash
airflow dags trigger clickhouse_etl_tasks_1_2_3
```

Или через UI: DAGs → `clickhouse_etl_tasks_1_2_3` → Trigger DAG.

---

## Задание 2: Смена оси pivot

Открой `dags/clickhouse_etl_dag.py`, найди функцию `pivot_task2`.
Измени **одну строку**:

```python
PIVOT_COLUMN = "flev"   # разворачивает flev в колонки, строки = (slev, dat1)
PIVOT_COLUMN = "slev"   # разворачивает slev в колонки, строки = (flev, dat1)
PIVOT_COLUMN = "dat1"   # разворачивает даты в колонки, строки = (flev, slev)
```

Всё остальное (получение уникальных значений, генерация SQL, группировка) — автоматически.

---

## Задание 3: Логика расчёта остатков

```
qty_end_of_day(date) =
    last_month_end_balance(date)    ← из fuel.monthly_balance (конец предыдущего месяца)
  + SUM(movement)                   ← из fuel.registers за [1-е числа месяца .. date]
```

- Если за день нет движений — остаток равен базовому + накопленное до предыдущего дня.
- Источник `monthly_balance` содержит даты конец-месяца → используются как начальная точка для следующего месяца.

---

## Результирующие таблицы

| Задание | Таблица                         | Описание                            |
|---------|---------------------------------|-------------------------------------|
| 1       | `sales.result_task1`            | Сумма продаж + эффект изменения цены |
| 2       | —                               | Результат выводится в лог (XCom)     |
| 3       | `fuel.result_daily_balance`     | Ежедневные остатки ТС-1 за 2025      |

Для задания 2 можно добавить материализацию в таблицу —
схема в `01_ddl.sql` уже содержит `pivot_db.result_pivot_flev` как пример.
