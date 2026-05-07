-- =============================================================================
-- ЗАДАНИЕ 3: Ежедневные остатки топлива ТС-1 за 2025 год
--
-- Алгоритм:
--   1. Генерируем calendar spine: все дни 2025-01-01 .. 2025-12-31
--   2. Берём остаток на 31.12.2024 из monthly_balance как точку отсчёта
--      (если есть записи за более поздние месяцы — используем ближайший
--       конец месяца <= текущей дате для каждого месяца)
--   3. Суммируем движения (registers) нарастающим итогом с начала 2025
--      относительно базовой точки
--   4. Для дней без движений — forward fill (берём последний известный остаток)
--
-- Итоговая формула на каждый день:
--   qty_end_of_day = last_month_end_balance + SUM(movements от начала месяца до текущей даты)
-- =============================================================================

INSERT INTO fuel.result_daily_balance

WITH

-- -----------------------------------------------------------------------
-- Шаг 1: Календарь 2025 (все 365 дней)
-- -----------------------------------------------------------------------
calendar AS (
    SELECT
        toDate('2025-01-01') + toIntervalDay(number) AS cal_date
    FROM numbers(365)
    WHERE toYear(toDate('2025-01-01') + toIntervalDay(number)) = 2025
),

-- -----------------------------------------------------------------------
-- Шаг 2: Кросс-продукт календаря × уникальные комбинации (org, nom)
-- Только ТС-1
-- -----------------------------------------------------------------------
dims AS (
    SELECT DISTINCT organization, nomenclature
    FROM fuel.registers
    WHERE nomenclature LIKE '%ТС-1%'
    UNION DISTINCT
    SELECT DISTINCT organization, nomenclature
    FROM fuel.monthly_balance
    WHERE nomenclature LIKE '%ТС-1%'
),

cal_dims AS (
    SELECT
        c.cal_date,
        d.organization,
        d.nomenclature
    FROM calendar c
    CROSS JOIN dims d
),

-- -----------------------------------------------------------------------
-- Шаг 3: Остатки на конец каждого предыдущего месяца
-- Для каждого дня 2025 берём последний закрытый месяц <= cal_date
-- Источник: monthly_balance (даты — последний день месяца)
-- -----------------------------------------------------------------------
monthly_base AS (
    SELECT
        organization,
        nomenclature,
        dat                               AS month_end_date,
        toStartOfMonth(addMonths(dat, 1)) AS next_month_start,  -- 1-е число следующего месяца
        qty
    FROM fuel.monthly_balance
    WHERE nomenclature LIKE '%ТС-1%'
),

-- Для каждой даты находим актуальный базовый остаток:
-- последний month_end_date < 1-го числа текущего месяца
cal_with_base AS (
    SELECT
        cd.cal_date,
        cd.organization,
        cd.nomenclature,
        -- Базовый остаток: конец предыдущего месяца
        toStartOfMonth(cd.cal_date)       AS cur_month_start,
        argMaxIf(
            mb.qty,
            mb.month_end_date,
            mb.organization = cd.organization
            AND mb.nomenclature = cd.nomenclature
            AND mb.month_end_date < toStartOfMonth(cd.cal_date)
        )                                 AS base_qty,
        argMaxIf(
            mb.month_end_date,
            mb.month_end_date,
            mb.organization = cd.organization
            AND mb.nomenclature = cd.nomenclature
            AND mb.month_end_date < toStartOfMonth(cd.cal_date)
        )                                 AS base_date
    FROM cal_dims cd
    LEFT JOIN monthly_base mb
        ON cd.organization = mb.organization
        AND cd.nomenclature = mb.nomenclature
    GROUP BY cd.cal_date, cd.organization, cd.nomenclature
),

-- -----------------------------------------------------------------------
-- Шаг 4: Накопленные движения с начала текущего месяца до cal_date включительно
-- -----------------------------------------------------------------------
movements_cumsum AS (
    SELECT
        r.dat,
        r.organization,
        r.nomenclature,
        -- Нарастающий итог с 1-го числа месяца
        sumIf(
            r.movement,
            r.dat <= r2.cal_date
            AND r.dat >= toStartOfMonth(r2.cal_date)
        ) AS cum_movement
    FROM fuel.registers r
    -- self-join на каждый день календаря (эффективно через JOIN)
    JOIN (SELECT cal_date, organization, nomenclature FROM cal_with_base) r2
        ON r.organization = r2.organization
        AND r.nomenclature = r2.nomenclature
        AND r.dat >= toStartOfMonth(r2.cal_date)
        AND r.dat <= r2.cal_date
    WHERE r.nomenclature LIKE '%ТС-1%'
      AND toYear(r.dat) = 2025
    GROUP BY r.dat, r.organization, r.nomenclature, r2.cal_date
),

-- -----------------------------------------------------------------------
-- Шаг 5: Агрегация по каждому дню
-- -----------------------------------------------------------------------
daily_raw AS (
    SELECT
        cb.cal_date                               AS dat,
        cb.organization,
        cb.nomenclature,
        cb.base_qty,
        COALESCE(
            sumIf(
                r.movement,
                r.dat <= cb.cal_date
                AND r.dat >= cb.cur_month_start
            ),
            0
        )                                         AS month_movement,
        cb.base_qty + COALESCE(
            sumIf(
                r.movement,
                r.dat <= cb.cal_date
                AND r.dat >= cb.cur_month_start
            ),
            0
        )                                         AS qty_raw
    FROM cal_with_base cb
    LEFT JOIN fuel.registers r
        ON cb.organization = r.organization
        AND cb.nomenclature = r.nomenclature
        AND r.dat >= cb.cur_month_start
        AND r.dat <= cb.cal_date
    GROUP BY
        cb.cal_date,
        cb.organization,
        cb.nomenclature,
        cb.base_qty,
        cb.cur_month_start
)

-- -----------------------------------------------------------------------
-- Финальная выборка
-- -----------------------------------------------------------------------
SELECT
    dat,
    organization,
    nomenclature,
    round(qty_raw, 3)   AS qty_end_of_day
FROM daily_raw
WHERE toYear(dat) = 2025
ORDER BY organization, nomenclature, dat;
