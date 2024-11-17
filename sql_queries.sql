--check if timestamp is consistent with price
SELECT strftime('%m', timestamp) AS Month,
    SUM(price) AS total_price
FROM sales
GROUP BY MONTH
ORDER BY MONTH;

--check records with quality=7
SELECT product_id, price
FROM sales
WHERE quantity ='7.0'
ORDER BY product_id;

--check total number of rows
SELECT COUNT(transaction_id) AS number_Of_transaction
FROM sales;

--check for null in quantity
SELECT COUNT(*) - COUNT(quantity) AS number_Of_nulls
FROM sales;

--check for null in transaction_id
SELECT COUNT(*) - COUNT(transaction_id) AS number_Of_nulls
FROM sales;

--check for null in price
SELECT COUNT(*) - COUNT(price) AS number_Of_nulls
FROM sales;

--check for null in product_id
SELECT COUNT(*) - COUNT(product_id) AS number_Of_nulls
FROM sales;

-- check number of products per day
SELECT COUNT(product_id) as number_Of_products_that_day,
    strftime('%m-%d', timestamp) AS month_Day
FROM sales
GROUP BY month_Day
ORDER BY month_Day;

-- check number of products per day
SELECT COUNT(product_id) as number_Of_products_that_day,
    timestamp AS month_Day
FROM sales
GROUP BY month_Day
ORDER BY month_Day;

-- check if product_id  is consistent
SELECT * FROM sales
WHERE product_id='P007';

-- test for repeated transraction_id
SELECT transaction_id, COUNT(*) AS count
FROM sales
GROUP BY transaction_id
HAVING COUNT(*) > 1;
