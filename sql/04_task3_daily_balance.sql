-- =============================================================================
-- ЗАДАНИЕ 3: Ежедневные остатки топлива ТС-1 за 2025 год
--
-- Алгоритм:
--   1. Генерируем календарь: все дни 2025-01-01 .. 2025-12-31.
--   2. Берём все комбинации (организация, номенклатура) по ТС-1.
--   3. Для каждого месяца находим базовый остаток: последний остаток на конец
--      месяца строго раньше первого дня текущего месяца.
--   4. Движения сворачиваем до одного значения на день.
--   5. Для каждого дня считаем: базовый остаток + накопленные движения
--      с начала текущего месяца по этот день включительно.
-- =============================================================================

INSERT INTO fuel.result_daily_balance

WITH

calendar AS (
    SELECT
        toDate('2025-01-01') + toIntervalDay(number) AS cal_date
    FROM numbers(365)
),

dims AS (
    SELECT DISTINCT organization, nomenclature
    FROM fuel.registers
    WHERE nomenclature LIKE '%ТС-1%'
    UNION DISTINCT
    SELECT DISTINCT organization, nomenclature
    FROM fuel.monthly_balance
    WHERE nomenclature LIKE '%ТС-1%'
),

months AS (
    SELECT
        toStartOfMonth(cal_date) AS month_start
    FROM calendar
    GROUP BY month_start
),

month_base AS (
    SELECT
        d.organization AS organization,
        d.nomenclature AS nomenclature,
        m.month_start,
        argMaxIf(
            mb.qty,
            mb.dat,
            mb.dat < m.month_start
        ) AS base_qty
    FROM dims d
    CROSS JOIN months m
    LEFT JOIN fuel.monthly_balance mb
        ON mb.organization = d.organization
        AND mb.nomenclature = d.nomenclature
        AND mb.nomenclature LIKE '%ТС-1%'
    GROUP BY d.organization, d.nomenclature, m.month_start
),

daily_movements AS (
    SELECT
        dat,
        organization,
        nomenclature,
        sum(movement) AS movement
    FROM fuel.registers
    WHERE nomenclature LIKE '%ТС-1%'
      AND toYear(dat) = 2025
    GROUP BY dat, organization, nomenclature
),

cal_dims AS (
    SELECT
        c.cal_date AS dat,
        d.organization AS organization,
        d.nomenclature AS nomenclature,
        toStartOfMonth(c.cal_date) AS month_start
    FROM calendar c
    CROSS JOIN dims d
),

daily_grid AS (
    SELECT
        cd.dat AS dat,
        cd.organization AS organization,
        cd.nomenclature AS nomenclature,
        cd.month_start AS month_start,
        COALESCE(mb.base_qty, 0) AS base_qty,
        COALESCE(dm.movement, 0) AS movement
    FROM cal_dims cd
    LEFT JOIN month_base mb
        ON mb.organization = cd.organization
        AND mb.nomenclature = cd.nomenclature
        AND mb.month_start = cd.month_start
    LEFT JOIN daily_movements dm
        ON dm.organization = cd.organization
        AND dm.nomenclature = cd.nomenclature
        AND dm.dat = cd.dat
)

SELECT
    dat,
    organization,
    nomenclature,
    round(
        base_qty
        + sum(movement) OVER (
            PARTITION BY organization, nomenclature, month_start
            ORDER BY dat
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        3
    ) AS qty_end_of_day
FROM daily_grid
WHERE toYear(dat) = 2025
ORDER BY organization, nomenclature, dat;
