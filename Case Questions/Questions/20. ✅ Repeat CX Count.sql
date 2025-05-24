USE DATABASE PRACTISE;

CREATE OR REPLACE TABLE customer_transactions (
    transaction_date DATE,
    customer_name STRING
);

INSERT INTO customer_transactions (transaction_date, customer_name)
VALUES 
    ('2025-01-01', 'Alice'),
    ('2025-01-15', 'Bob'),
    ('2025-02-01', 'Alice'),  -- Alice repeats in Feb
    ('2025-02-20', 'Charlie'),
    ('2025-03-01', 'Alice'),  -- Alice repeats in March
    ('2025-03-10', 'Bob'),    -- Bob repeats in March
    ('2025-04-05', 'Charlie'); -- Charlie repeats in April

select * from customer_transactions order by transaction_date asc;

// Without Window Function

with cte as 
(
SELECT 
    t1.transaction_date,
    t1.customer_name,
    MAX(t2.transaction_date) AS last_date  -- Only the latest previous txn
  FROM customer_transactions AS t1
  LEFT JOIN customer_transactions AS t2
    ON t2.transaction_date < t1.transaction_date 
    AND t1.customer_name = t2.customer_name
  GROUP BY t1.transaction_date, t1.customer_name
),
flagged as
(
    select *,
    IFF(LAST_DATE IS NULL, 'NR', 'R') AS cx_status
    FROM cte
) 

SELECT --date_trunc('month', transaction_date) as month,
--To_CHAR(YEAR(DATE_TRUNC('month',transaction_date))) as month_year
--from flagged;

CONCAT( MONTHNAME(date_trunc('month',transaction_date)),' ',To_CHAR(YEAR(DATE_TRUNC('month',transaction_date)))) as month_year,
COUNT(DISTINCT CUSTOMER_NAME) as total_cx_count,
COUNT( DISTINCT CASE WHEN cx_status = 'R' THEN CUSTOMER_NAME END) AS repeat_cx_count
FROM flagged
group by CONCAT( MONTHNAME(date_trunc('month',transaction_date)),' ',To_CHAR(YEAR(DATE_TRUNC('month',transaction_date)))) ;


// Using Window Function:

with cte as

(
select
    transaction_date,
    customer_name,
    ROW_NUMBER() OVER(PARTITION BY CUSTOMER_NAME ORDER BY TRANSACTION_DATE ASC) As txn_no
from customer_transactions
),
flagged as
(
    select 
    transaction_date,
    customer_name,
    txn_no,
    IFF(txn_no = 1,'NR','R') as txn_flag
    from cte
)
SELECT 
CONCAT( MONTHNAME(date_trunc('month',transaction_date)),' ',To_CHAR(YEAR(DATE_TRUNC('month',transaction_date)))) as month_year,
COUNT(DISTINCT CUSTOMER_NAME) as total_cx_count,
COUNT( DISTINCT CASE WHEN txn_flag = 'R' THEN CUSTOMER_NAME END) AS repeat_cx_count
FROM flagged 
GROUP BY CONCAT( MONTHNAME(date_trunc('month',transaction_date)),' ',To_CHAR(YEAR(DATE_TRUNC('month',transaction_date))));
