-- =============================================================================
-- ЗАДАНИЕ 1: Сумма продаж и "Эффект" от изменения цены
--
-- Логика:
--   1. Для каждой строки vol находим актуальную цену через ASOF JOIN:
--      берём максимальную цену с датой <= даты продажи (по тому же prod)
--   2. Предыдущую цену берём через LAG() по окну (prod ORDER BY dat)
--   3. sales_amount  = price * w
--   4. price_effect  = (price - prev_price) * w
--      (если предыдущей цены нет — NULL, т.е. первое изменение)
-- =============================================================================

INSERT INTO sales.result_task1
WITH

-- Шаг 1: к каждой записи объёма прикрепляем актуальную цену
-- ASOF JOIN в ClickHouse: берёт ближайшую строку <=
vol_with_price AS (
    SELECT
        v.prod,
        v.dat,
        v.w,
        p.price,
        p.dat AS price_start_dat
    FROM sales.volumes AS v
    ASOF LEFT JOIN sales.prices AS p
        ON v.prod = p.prod AND v.dat >= p.dat
),

-- Шаг 2: считаем предыдущую цену через LAG по каждому продукту
with_prev AS (
    SELECT
        prod,
        dat,
        w,
        price,
        lagInFrame(toNullable(price), 1, NULL) OVER (
            PARTITION BY prod
            ORDER BY dat
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS prev_price
    FROM vol_with_price
)

SELECT
    prod,
    dat,
    w,
    price,
    prev_price,
    price * w                          AS sales_amount,
    (price - prev_price) * w           AS price_effect
FROM with_prev
ORDER BY prod, dat;
