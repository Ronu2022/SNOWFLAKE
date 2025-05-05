
CREATE OR REPLACE TABLE user_purchases (
    user_id INTEGER,
    purchase_date DATE,
    product VARCHAR,
    amount NUMBER
);

INSERT INTO user_purchases (user_id, purchase_date, product, amount) VALUES
(101, '2024-01-05', 'Laptop', 1000),
(101, '2024-03-10', 'Mouse', 50),
(101, '2024-04-02', 'Keyboard', 80),
(102, '2024-02-14', 'Monitor', 300),
(102, '2024-02-20', 'Mouse', 50),
(103, '2024-03-01', 'Laptop', 1200),
(104, '2024-01-25', 'Mouse', 50),
(104, '2024-01-30', 'Keyboard', 70),
(104, '2024-02-15', 'Chair', 200);


SELECT * FROM user_purchases;

-- Write a SQL query to get the first product each user purchased, along with the user_id, purchase_date, and amount.

SELECT 
    f.user_id,f.DATE_OF_FIRST_PURCHASE as purchase_date,
    m.product,
    m.amount
From
    
(
SELECT DISTINCT
    USER_ID,
    FIRST_VALUE(PURCHASE_DATE) OVER (PARTITION BY USER_ID ORDER BY PURCHASE_DATE ASC) AS date_of_first_purchase 
FROM user_purchases
ORDER BY USER_ID ASC
) as f
LEft join user_purchases as m
on f.user_id = m.user_id and f.DATE_OF_FIRST_PURCHASE = m.PURCHASE_DATE;
