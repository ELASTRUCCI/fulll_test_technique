-- Number of Transaction on 14/01/2022
SELECT COUNT(*)
FROM transactions
WHERE transaction_date = '2022-01-14'
;

-- What is the total amount, including tax, of all SELL transactions?
SELECT SUM(amount_inc_tax)
FROM transactions
WHERE category = 'SELL'
;

-- Consider the product Amazon Echo Dot:
SELECT
    transaction_date,
    ROUND(ifnull(sell,0) - ifnull(buy,0), 2) AS balance
FROM
    ( 
    SELECT
        transaction_date,
        SUM(amount_excl_tax) filter (where category = 'BUY') as buy,
        SUM(amount_excl_tax) filter (where category = 'SELL') as sell
    FROM transactions
    WHERE name = 'Amazon Echo Dot'
    GROUP BY transaction_date
    )
;

-- (Optional) What is the cumulated balance (SELL - BUY) by date?
SELECT
    transaction_date,
    ROUND(SUM(ifnull(sell,0) - ifnull(buy,0)) OVER (ORDER BY transaction_date), 2) AS cumulative_balance
FROM
    ( 
    SELECT
        transaction_date,
        SUM(amount_excl_tax) filter (where category = 'BUY') as buy,
        SUM(amount_excl_tax) filter (where category = 'SELL') as sell
    FROM transactions
    WHERE name = 'Amazon Echo Dot'
    GROUP BY transaction_date
    )
;