-- =============================================================================
-- DDL: Создание схем и таблиц в ClickHouse
-- Задания 1, 2, 3
-- =============================================================================

-- -------------------------
-- ЗАДАНИЕ 1: Цены и объёмы
-- -------------------------

CREATE DATABASE IF NOT EXISTS sales;

-- Исходник: цены продуктов (интервальные)
CREATE TABLE IF NOT EXISTS sales.prices
(
    prod   UInt32,
    dat    Date,        -- дата начала действия цены
    price  Decimal(18, 4)
)
ENGINE = MergeTree()
ORDER BY (prod, dat);

-- Исходник: объёмы продаж
CREATE TABLE IF NOT EXISTS sales.volumes
(
    prod  UInt32,
    dat   Date,
    w     Decimal(18, 4)   -- объём продаж
)
ENGINE = MergeTree()
ORDER BY (prod, dat);

-- Результат задания 1
CREATE TABLE IF NOT EXISTS sales.result_task1
(
    prod          UInt32,
    dat           Date,
    w             Decimal(18, 4),
    price         Decimal(18, 4),
    prev_price    Nullable(Decimal(18, 4)),
    sales_amount  Decimal(18, 4),   -- сумма продаж = price * w
    price_effect  Nullable(Decimal(18, 4))    -- эффект = (price - prev_price) * w
)
ENGINE = MergeTree()
ORDER BY (prod, dat);

-- -------------------------
-- ЗАДАНИЕ 2: Pivot / Cross-tab
-- -------------------------

CREATE DATABASE IF NOT EXISTS pivot_db;

-- Исходник RevCrosTab
CREATE TABLE IF NOT EXISTS pivot_db.rev_cross_tab
(
    flev  Int32,
    slev  Int32,
    dat1  Date,
    w     Decimal(18, 4)
)
ENGINE = MergeTree()
ORDER BY (flev, slev, dat1);

-- Результат pivot будет создаваться динамически в DAG
-- (т.к. колонки зависят от данных)
-- Промежуточная таблица для хранения результата pivot по flev
CREATE TABLE IF NOT EXISTS pivot_db.result_pivot_flev
(
    slev  Int32,
    dat1  Date,
    flev_0  Decimal(18,4),
    flev_1  Decimal(18,4),
    flev_2  Decimal(18,4)
)
ENGINE = MergeTree()
ORDER BY (slev, dat1);

-- -------------------------
-- ЗАДАНИЕ 3: Остатки топлива
-- -------------------------

CREATE DATABASE IF NOT EXISTS fuel;

-- Исходник 1: регистры накопления (движения топлива)
CREATE TABLE IF NOT EXISTS fuel.registers
(
    dat            Date,
    nomenclature   String,   -- Номенклатура Наименование
    organization   String,   -- Организация Наименование
    movement       Decimal(18, 4)  -- ДвижениеНП (+налив, -слив)
)
ENGINE = MergeTree()
ORDER BY (organization, nomenclature, dat);

-- Исходник 2: остатки на конец месяца
CREATE TABLE IF NOT EXISTS fuel.monthly_balance
(
    dat            Date,
    nomenclature   String,
    organization   String,
    qty            Decimal(18, 4)  -- Фактическое количество
)
ENGINE = MergeTree()
ORDER BY (organization, nomenclature, dat);

-- Результат задания 3: ежедневные остатки ТС-1 за 2025 год
CREATE TABLE IF NOT EXISTS fuel.result_daily_balance
(
    dat            Date,
    organization   String,
    nomenclature   String,
    qty_end_of_day Decimal(18, 4)
)
ENGINE = MergeTree()
ORDER BY (organization, nomenclature, dat);
