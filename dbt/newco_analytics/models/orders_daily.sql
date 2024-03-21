SELECT
    sales.created_at::DATE AS sale_date
    ,sales.order_id
    ,sales.store_id
    ,sales.employee_id
    ,sales.customer_id
    ,SUM(sales.quantity) AS quantity
    ,SUM(sales.amount) AS amount
FROM
    {{ source('sales_sys', 'sales') }} sales
GROUP BY
    1,2,3,4,5