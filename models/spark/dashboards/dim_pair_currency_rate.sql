SELECT
    t1.effective_date AS effective_date,
    t1.next_effective_date AS next_effective_date,
    t1.currency_code AS currency_code,
    t2.currency_code AS currency_code_to,
    t1.rate / t2.rate AS rate
FROM {{ source('mart','dim_currency_rate') }} AS t1
INNER JOIN {{ source('mart','dim_currency_rate') }} AS t2 USING(effective_date)