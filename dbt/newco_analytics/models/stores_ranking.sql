SELECT
    YEAR(sales.created_at) AS year
    ,sales.store_id
    ,stores.name AS store_name
    ,SUM(sales.quantity) AS quantity
    ,SUM(sales.amount) AS amount
FROM
    {{ source('sales_sys', 'sales') }} sales
LEFT JOIN
    {{ source('assets_sys','stores') }} stores
    ON sales.store_id = stores.id
GROUP BY
    1,2,3