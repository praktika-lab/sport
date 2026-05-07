from __future__ import annotations

import argparse
import csv
import re
from datetime import datetime
from io import StringIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CH_URL = "http://localhost:8123/"


def ch(sql: str, url: str) -> str:
    request = Request(
        url,
        data=sql.encode("utf-8"),
        method="POST",
        headers={"Content-Type": "text/plain; charset=utf-8"},
    )
    try:
        with urlopen(request, timeout=90) as response:
            return response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"ClickHouse HTTP {exc.code}: {body}\nSQL:\n{sql[:1200]}"
        ) from exc


def split_sql(text: str) -> list[str]:
    lines = [
        line
        for line in text.splitlines()
        if not line.strip().startswith("--")
    ]
    return [stmt.strip() for stmt in "\n".join(lines).split(";") if stmt.strip()]


def parse_date(value: str) -> str:
    return datetime.strptime(value, "%d.%m.%Y").strftime("%Y-%m-%d")


def parse_decimal_ru(value: str) -> str:
    return value.replace(",", ".")


def to_csv_body(rows: list[list[str]]) -> str:
    output = StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerows(rows)
    return output.getvalue()


def insert_rows(rows: list[list[str]], table: str, url: str) -> None:
    ch(f"INSERT INTO {table} FORMAT CSV\n{to_csv_body(rows)}", url)
    print(f"loaded {len(rows)} rows -> {table}")


def load_data(url: str) -> None:
    data_dir = ROOT / "data"

    with (data_dir / "prices.csv").open(encoding="utf-8-sig", newline="") as file:
        rows = [
            [row["prod"], parse_date(row["dat"]), row["price"]]
            for row in csv.DictReader(file)
        ]
    insert_rows(rows, "sales.prices", url)

    with (data_dir / "vol.csv").open(encoding="utf-8-sig", newline="") as file:
        rows = [
            [row["prod"], parse_date(row["dat"]), row["w"]]
            for row in csv.DictReader(file)
        ]
    insert_rows(rows, "sales.volumes", url)

    with (data_dir / "RevCrosTab.csv").open(encoding="utf-8-sig", newline="") as file:
        rows = [
            [row["flev"], row["slev"], parse_date(row["dat1"]), row["w"]]
            for row in csv.DictReader(file)
        ]
    insert_rows(rows, "pivot_db.rev_cross_tab", url)

    registers_path = data_dir / "Регистры Накопления Нефтепродукты организаций.csv"
    with registers_path.open(encoding="utf-8-sig", newline="") as file:
        rows = [
            [
                parse_date(row["%Дата"]),
                row["Номенклатура Наименование"],
                row["Организация Наименование"],
                parse_decimal_ru(row["ДвижениеНП"]),
            ]
            for row in csv.DictReader(file, delimiter=";")
        ]
    insert_rows(rows, "fuel.registers", url)

    balances_path = data_dir / "Резервуары Складов Остатки Топлива.csv"
    with balances_path.open(encoding="utf-8-sig", newline="") as file:
        rows = [
            [
                parse_date(row["%Дата"]),
                row["Номенклатура Наименование"],
                row["Организация Наименование"],
                parse_decimal_ru(row["Фактическое количество"]),
            ]
            for row in csv.DictReader(file, delimiter=";")
        ]
    insert_rows(rows, "fuel.monthly_balance", url)


def run_pivot_check(url: str) -> str:
    raw = ch(
        "SELECT DISTINCT flev "
        "FROM pivot_db.rev_cross_tab "
        "ORDER BY flev FORMAT TabSeparated",
        url,
    )
    values = [value.strip() for value in raw.strip().split("\n") if value.strip()]
    expressions = ",\n    ".join(
        f"sumIf(w, flev = {value}) AS flev_{re.sub(r'[^a-zA-Z0-9_]', '_', value)}"
        for value in values
    )
    sql = f"""
SELECT
    slev,
    dat1,
    {expressions}
FROM pivot_db.rev_cross_tab
GROUP BY slev, dat1
ORDER BY slev, dat1
LIMIT 5 FORMAT TabSeparatedWithNames
"""
    return ch(sql, url)


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-check ClickHouse SQL tasks.")
    parser.add_argument("--url", default=DEFAULT_CH_URL, help="ClickHouse HTTP URL.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate sales, pivot_db and fuel databases before running.",
    )
    args = parser.parse_args()

    print("ClickHouse", ch("SELECT version()", args.url).strip())

    if args.reset:
        for database in ["sales", "pivot_db", "fuel"]:
            ch(f"DROP DATABASE IF EXISTS {database}", args.url)

    statements = split_sql((ROOT / "sql" / "01_ddl.sql").read_text(encoding="utf-8"))
    for statement in statements:
        ch(statement + ";", args.url)
    print(f"DDL OK ({len(statements)} statements)")

    for table in [
        "sales.prices",
        "sales.volumes",
        "pivot_db.rev_cross_tab",
        "fuel.registers",
        "fuel.monthly_balance",
        "sales.result_task1",
        "fuel.result_daily_balance",
    ]:
        ch(f"TRUNCATE TABLE {table}", args.url)

    load_data(args.url)

    ch((ROOT / "sql" / "02_task1_sales_effect.sql").read_text(encoding="utf-8"), args.url)
    print(
        ch(
            "SELECT count() AS rows, sum(sales_amount) AS sales_sum, "
            "sum(price_effect) AS effect_sum, countIf(isNull(prev_price)) AS null_prev "
            "FROM sales.result_task1 FORMAT TabSeparatedWithNames",
            args.url,
        )
    )

    print(run_pivot_check(args.url))

    ch((ROOT / "sql" / "04_task3_daily_balance.sql").read_text(encoding="utf-8"), args.url)
    print(
        ch(
            "SELECT count() AS rows, min(dat), max(dat), "
            "min(qty_end_of_day), max(qty_end_of_day) "
            "FROM fuel.result_daily_balance FORMAT TabSeparatedWithNames",
            args.url,
        )
    )

    print("Smoke test OK")


if __name__ == "__main__":
    main()
