SELECT
    YEAR(sales.created_at) AS year
    ,sales.product_id
    ,products.name AS product_name
    ,SUM(sales.quantity) AS quantity
    ,SUM(sales.amount) AS amount
FROM
    {{ source('sales_sys', 'sales') }} sales
LEFT JOIN
    {{ source('inventory_sys','products') }} products
    ON sales.product_id = products.id
GROUP BY
    1,2,3