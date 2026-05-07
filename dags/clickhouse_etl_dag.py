"""
=============================================================================
Airflow DAG: clickhouse_etl_tasks_1_2_3
=============================================================================
Граф задач:
  start
    ├── create_schemas          (DDL: CREATE DATABASE / TABLE IF NOT EXISTS)
    │
    ├── [task1_group]
    │     ├── load_prices       (CSV → ClickHouse: sales.prices)
    │     ├── load_volumes      (CSV → ClickHouse: sales.volumes)
    │     └── calc_task1        (INSERT INTO sales.result_task1)
    │
    ├── [task2_group]
    │     ├── load_rev_cross    (CSV → ClickHouse: pivot_db.rev_cross_tab)
    │     └── pivot_task2       (динамический pivot → лог + ClickHouse)
    │
    └── [task3_group]
          ├── load_registers    (CSV → ClickHouse: fuel.registers)
          ├── load_monthly_bal  (CSV → ClickHouse: fuel.monthly_balance)
          └── calc_task3        (INSERT INTO fuel.result_daily_balance)

Конфигурация подключения к ClickHouse задаётся через Airflow Connection:
  conn_id = 'clickhouse_default'
  conn_type = 'http'
  host = <ClickHouse host>
  port = 8123
  login = <user>
  password = <password>

CSV-файлы кладём в папку DATA_DIR (настраивается через Airflow Variable
'clickhouse_etl_data_dir' или переменную окружения CLICKHOUSE_ETL_DATA_DIR).
=============================================================================
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.hooks.http import HttpHook

# ---------------------------------------------------------------------------
# Вспомогательный клиент ClickHouse через HTTP API
# ---------------------------------------------------------------------------

log = logging.getLogger(__name__)


def ch_query(sql: str, conn_id: str = "clickhouse_default") -> str:
    """Выполняет произвольный SQL в ClickHouse через HTTP.
    Возвращает тело ответа (TSV/текст).
    """
    hook = HttpHook(method="POST", http_conn_id=conn_id)
    response = hook.run(
        endpoint="/",
        data=sql.encode("utf-8"),
        headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    return response.text


def ch_insert_df(
    df: pd.DataFrame,
    table: str,
    conn_id: str = "clickhouse_default",
) -> None:
    """Загружает DataFrame в ClickHouse через INSERT … FORMAT CSV."""
    csv_data = df.to_csv(index=False, header=False)
    sql = f"INSERT INTO {table} FORMAT CSV\n{csv_data}"
    ch_query(sql, conn_id=conn_id)
    log.info("Загружено %d строк в %s", len(df), table)


# ---------------------------------------------------------------------------
# Путь к CSV-файлам
# ---------------------------------------------------------------------------

def get_data_dir() -> Path:
    try:
        data_dir = Variable.get("clickhouse_etl_data_dir")
    except Exception:
        data_dir = os.environ.get(
            "CLICKHOUSE_ETL_DATA_DIR",
            str(Path(__file__).parent.parent / "data"),
        )
    return Path(data_dir)


def _split_sql_statements(sql_text: str) -> list[str]:
    lines = [
        line
        for line in sql_text.splitlines()
        if not line.strip().startswith("--")
    ]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def _existing_data_file(data_dir: Path, *names: str) -> Path:
    for name in names:
        path = data_dir / name
        if path.exists():
            return path
    return data_dir / names[0]


# ---------------------------------------------------------------------------
# Функции-задачи
# ---------------------------------------------------------------------------

# ── DDL ──────────────────────────────────────────────────────────────────────

def create_schemas(**kwargs: Any) -> None:
    """Создаёт базы данных и таблицы (идемпотентно — IF NOT EXISTS)."""
    ddl_path = Path(__file__).parent.parent / "sql" / "01_ddl.sql"
    ddl_text = ddl_path.read_text(encoding="utf-8")

    # Разбиваем на отдельные выражения по ";" и выполняем по одному
    statements = _split_sql_statements(ddl_text)
    for stmt in statements:
        ch_query(stmt + ";")
    log.info("DDL выполнен успешно (%d выражений)", len(statements))


# ── ЗАДАНИЕ 1: Загрузка ──────────────────────────────────────────────────────

def _parse_date_ddmmyyyy(series: pd.Series) -> pd.Series:
    """Парсит даты формата DD.MM.YYYY → YYYY-MM-DD (строка для ClickHouse)."""
    return pd.to_datetime(series, format="%d.%m.%Y").dt.strftime("%Y-%m-%d")


def load_prices(**kwargs: Any) -> None:
    """Загружает prices.csv → sales.prices."""
    data_dir = get_data_dir()
    df = pd.read_csv(data_dir / "prices.csv")
    df = df[["prod", "dat", "price"]].copy()
    df["dat"] = _parse_date_ddmmyyyy(df["dat"])
    ch_query("TRUNCATE TABLE sales.prices")
    ch_insert_df(df, "sales.prices")


def load_volumes(**kwargs: Any) -> None:
    """Загружает vol.csv → sales.volumes."""
    data_dir = get_data_dir()
    df = pd.read_csv(data_dir / "vol.csv")
    df = df[["prod", "dat", "w"]].copy()
    df["dat"] = _parse_date_ddmmyyyy(df["dat"])
    ch_query("TRUNCATE TABLE sales.volumes")
    ch_insert_df(df, "sales.volumes")


def calc_task1(**kwargs: Any) -> None:
    """Запускает аналитический запрос задания 1, результат в sales.result_task1."""
    sql_path = Path(__file__).parent.parent / "sql" / "02_task1_sales_effect.sql"
    sql = sql_path.read_text(encoding="utf-8")

    ch_query("TRUNCATE TABLE sales.result_task1")
    ch_query(sql)

    # Логируем итог
    result = ch_query(
        "SELECT prod, dat, sales_amount, price_effect "
        "FROM sales.result_task1 ORDER BY prod, dat LIMIT 20 FORMAT TabSeparatedWithNames"
    )
    log.info("Результат задания 1 (первые 20 строк):\n%s", result)


# ── ЗАДАНИЕ 2: Загрузка + динамический pivot ─────────────────────────────────

def load_rev_cross(**kwargs: Any) -> None:
    """Загружает RevCrosTab.csv → pivot_db.rev_cross_tab."""
    data_dir = get_data_dir()
    df = pd.read_csv(data_dir / "RevCrosTab.csv")
    df = df[["flev", "slev", "dat1", "w"]].copy()
    df["dat1"] = _parse_date_ddmmyyyy(df["dat1"])
    ch_query("TRUNCATE TABLE pivot_db.rev_cross_tab")
    ch_insert_df(df, "pivot_db.rev_cross_tab")


def pivot_task2(**kwargs: Any) -> None:
    """
    Динамический PIVOT в ClickHouse.

    Параметр PIVOT_COLUMN задаёт поле, которое разворачивается в колонки.
    Меняй ТОЛЬКО эту строку — всё остальное генерируется автоматически.

    Допустимые значения: 'flev', 'slev', 'dat1'
    """
    # ════════════════════════════════════════════
    # ▼▼▼  ЕДИНСТВЕННОЕ МЕСТО ДЛЯ ИЗМЕНЕНИЯ  ▼▼▼
    PIVOT_COLUMN = "flev"          # <-- поменяй на 'slev' или 'dat1'
    # ▲▲▲  ЕДИНСТВЕННОЕ МЕСТО ДЛЯ ИЗМЕНЕНИЯ  ▲▲▲
    # ════════════════════════════════════════════

    all_columns = ["flev", "slev", "dat1"]
    assert PIVOT_COLUMN in all_columns, f"PIVOT_COLUMN должен быть одним из {all_columns}"

    # Остальные колонки (идут в GROUP BY)
    group_cols = [c for c in all_columns if c != PIVOT_COLUMN]

    # Получаем уникальные значения pivot-колонки из ClickHouse
    raw_values = ch_query(
        f"SELECT DISTINCT {PIVOT_COLUMN} FROM pivot_db.rev_cross_tab ORDER BY {PIVOT_COLUMN} FORMAT TabSeparated"
    )
    pivot_values = [v.strip() for v in raw_values.strip().split("\n") if v.strip()]
    log.info("Значения для pivot по '%s': %s", PIVOT_COLUMN, pivot_values)

    # Генерируем sumIf-выражения
    def make_col_expr(val: str) -> str:
        # Для строковых значений — кавычки, для чисел/дат — как есть
        if PIVOT_COLUMN == "dat1":
            # Даты хранятся как Date, сравниваем через toDate
            safe_alias = val.replace("-", "")
            return f"sumIf(w, {PIVOT_COLUMN} = toDate('{val}')) AS {PIVOT_COLUMN}_{safe_alias}"
        else:
            safe_val = re.sub(r"[^a-zA-Z0-9_]", "_", str(val))
            return f"sumIf(w, {PIVOT_COLUMN} = {val}) AS {PIVOT_COLUMN}_{safe_val}"

    pivot_exprs = ",\n    ".join(make_col_expr(v) for v in pivot_values)
    group_by_str = ", ".join(group_cols)

    sql = f"""
SELECT
    {group_by_str},
    {pivot_exprs}
FROM pivot_db.rev_cross_tab
GROUP BY {group_by_str}
ORDER BY {group_by_str}
FORMAT TabSeparatedWithNames
"""
    log.info("Сгенерированный pivot SQL:\n%s", sql)

    result = ch_query(sql)
    log.info("Результат pivot (PIVOT_COLUMN='%s'):\n%s", PIVOT_COLUMN, result)

    # Сохраняем результат в XCom для downstream-задач при необходимости
    kwargs["ti"].xcom_push(key="pivot_result", value=result[:4000])  # первые 4000 символов


# ── ЗАДАНИЕ 3: Загрузка + ежедневные остатки ────────────────────────────────

def _parse_decimal_ru(series: pd.Series) -> pd.Series:
    """Парсит числа с запятой как десятичным разделителем (русская локаль)."""
    return series.astype(str).str.replace(",", ".").astype(float)


def load_registers(**kwargs: Any) -> None:
    """Загружает Регистры_Накопления_Нефтепродукты_организаций.csv → fuel.registers."""
    data_dir = get_data_dir()
    df = pd.read_csv(
        _existing_data_file(
            data_dir,
            "Регистры Накопления Нефтепродукты организаций.csv",
            "Регистры_Накопления_Нефтепродукты_организаций.csv",
        ),
        sep=";",
    )
    df.columns = ["dat", "nomenclature", "organization", "movement"]
    df["dat"] = _parse_date_ddmmyyyy(df["dat"])
    df["movement"] = _parse_decimal_ru(df["movement"])
    ch_query("TRUNCATE TABLE fuel.registers")
    ch_insert_df(df, "fuel.registers")


def load_monthly_balance(**kwargs: Any) -> None:
    """Загружает Резервуары_Складов_Остатки_Топлива.csv → fuel.monthly_balance."""
    data_dir = get_data_dir()
    df = pd.read_csv(
        _existing_data_file(
            data_dir,
            "Резервуары Складов Остатки Топлива.csv",
            "Резервуары_Складов_Остатки_Топлива.csv",
        ),
        sep=";",
    )
    df.columns = ["dat", "nomenclature", "organization", "qty"]
    df["dat"] = _parse_date_ddmmyyyy(df["dat"])
    df["qty"] = _parse_decimal_ru(df["qty"])
    ch_query("TRUNCATE TABLE fuel.monthly_balance")
    ch_insert_df(df, "fuel.monthly_balance")


def calc_task3(**kwargs: Any) -> None:
    """Запускает аналитический запрос задания 3, результат в fuel.result_daily_balance."""
    sql_path = Path(__file__).parent.parent / "sql" / "04_task3_daily_balance.sql"
    sql = sql_path.read_text(encoding="utf-8")

    ch_query("TRUNCATE TABLE fuel.result_daily_balance")
    ch_query(sql)

    # Логируем итог
    result = ch_query(
        "SELECT dat, organization, nomenclature, qty_end_of_day "
        "FROM fuel.result_daily_balance "
        "WHERE toYear(dat) = 2025 "
        "ORDER BY organization, nomenclature, dat "
        "LIMIT 31 FORMAT TabSeparatedWithNames"
    )
    log.info("Результат задания 3 (январь 2025, первые 31 строка):\n%s", result)


# ===========================================================================
# DAG
# ===========================================================================

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="clickhouse_etl_tasks_1_2_3",
    description="ETL: загрузка CSV + аналитика (Задания 1, 2, 3) в ClickHouse",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,          # запуск вручную или по триггеру
    catchup=False,
    default_args=default_args,
    tags=["clickhouse", "etl", "analytics"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # ── DDL ──────────────────────────────────────────────────────────────────
    t_ddl = PythonOperator(
        task_id="create_schemas",
        python_callable=create_schemas,
    )

    # ── Задание 1 ─────────────────────────────────────────────────────────────
    t1_load_prices = PythonOperator(
        task_id="task1__load_prices",
        python_callable=load_prices,
    )
    t1_load_volumes = PythonOperator(
        task_id="task1__load_volumes",
        python_callable=load_volumes,
    )
    t1_calc = PythonOperator(
        task_id="task1__calc_sales_effect",
        python_callable=calc_task1,
    )

    # ── Задание 2 ─────────────────────────────────────────────────────────────
    t2_load = PythonOperator(
        task_id="task2__load_rev_cross",
        python_callable=load_rev_cross,
    )
    t2_pivot = PythonOperator(
        task_id="task2__pivot",
        python_callable=pivot_task2,
    )

    # ── Задание 3 ─────────────────────────────────────────────────────────────
    t3_load_reg = PythonOperator(
        task_id="task3__load_registers",
        python_callable=load_registers,
    )
    t3_load_bal = PythonOperator(
        task_id="task3__load_monthly_balance",
        python_callable=load_monthly_balance,
    )
    t3_calc = PythonOperator(
        task_id="task3__calc_daily_balance",
        python_callable=calc_task3,
    )

    # ── Зависимости ──────────────────────────────────────────────────────────
    #
    #  start → create_schemas
    #            ├── load_prices  ┐
    #            ├── load_volumes ┘→ calc_task1
    #            ├── load_rev_cross → pivot_task2
    #            ├── load_registers    ┐
    #            └── load_monthly_bal  ┘→ calc_task3
    #
    start >> t_ddl

    t_ddl >> [t1_load_prices, t1_load_volumes]
    [t1_load_prices, t1_load_volumes] >> t1_calc

    t_ddl >> t2_load >> t2_pivot

    t_ddl >> [t3_load_reg, t3_load_bal]
    [t3_load_reg, t3_load_bal] >> t3_calc
