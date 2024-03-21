WITH sales_monthly AS
(
SELECT
    DATE_TRUNC(MONTH,sale_date) AS month_date,
    SUM(quantity) AS quantity,
    SUM(amount) AS amount
FROM
    {{ ref('orders_daily') }} orders_daily
GROUP BY 
    1
)
SELECT  
    month_date
    ,quantity
    ,amount
    ,(amount/ SUM(amount) OVER (PARTITION BY YEAR(month_date)))*100 AS seasonality_pct
FROM
    sales_monthly
