CREATE OR REPLACE  TABLE orders
(
    order_id int,
    customer_id int,
    product_id int
);

INSERT INTO orders VALUES 
(1, 1, 1),
(1, 1, 2),
(1, 1, 3),
(2, 2, 1),
(2, 2, 2),
(2, 2, 4),
(3, 1, 5);

CREATE OR REPLACE TABLE products 
(
    id int,
    name varchar(10)
);

INSERT INTO products VALUES 
(1, 'A'),
(2, 'B'),
(3, 'C'),
(4, 'D'),
(5, 'E');

SELECT * FROM orders;
SELECT * FROM products;


-- Find the frequency of Products Combo Purchased:


WITH product_name_cte AS
(
SELECT o.*,
       p.name 
FROM orders o
LEFT JOIN products p ON o.product_id = p.id
),
product_name_self_join AS
(
    SELECT t1.order_id,
           t1.name AS product_name_1,
           t2.name AS product_name_2,
           t1.product_id,
           t2.product_id
    FROM product_name_cte t1
    JOIN product_name_cte t2 
    ON t1.order_id = t2.order_id AND t1.product_id <> t2.product_id AND t1.product_id < t2.product_id
),

concat_product_name AS
(
    SELECT *,CONCAT(product_name_1, product_name_2) AS pair FROM 
    product_name_self_join
)

SELECT pair,COUNT(*) FROM concat_product_name
GROUP BY pair;
