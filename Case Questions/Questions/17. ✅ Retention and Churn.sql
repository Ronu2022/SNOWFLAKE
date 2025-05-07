-------------------------------------------------------------------------------------------------------------------------------------------------
/*
Customer retention refers to the ability of a company or product to retain its customers over some specified period. High customer retention means customers of the product or business tend to return to, continue to buy or in some other way not defect to another product or business, or to non-use entirely. 
Company programs to retain customers: Zomato Pro , Cashbacks, Reward Programs etc.
Once these programs in place we need to build metrics to check if programs are working or not. That is where we will write SQL to drive customer retention count.  */



create or replace table transactions
(
order_id int,
cust_id int,
order_date date,
amount int
);
delete from transactions;
insert into transactions values 
(1,1,'2020-01-15',150)
,(2,1,'2020-02-10',150)
,(3,2,'2020-01-16',150)
,(4,2,'2020-02-25',150)
,(5,3,'2020-01-10',150)
,(6,3,'2020-02-20',150)
,(7,4,'2020-01-20',150)
,(8,5,'2020-02-20',150)
;


SELECT * FROM transactions;


-- Retention Customer

SELECT MONTH(ORDER_DATE),
COUNT(CASE WHEN RETENTION_FLAG = 'Retained'  THEN 1 END) AS retained_cx
FROM 
(
SELECT 
    order_id,
    cust_id,
    order_date,
    LAG(order_date) OVER (PARTITION BY cust_id ORDER BY order_date) AS prev_order_date,
    CASE 
        WHEN DATEDIFF('month', LAG(order_date) OVER (PARTITION BY cust_id ORDER BY order_date), order_date) = 1 THEN 'Retained' 
        ELSE 'Not Retained'
    END AS retention_flag
FROM transactions
ORDER BY cust_id ASC, order_date ASC
) GROUP BY MONTH(ORDER_DATE)





-- Churned Customer
with cte as 
(
SELECT 
    order_id,
    cust_id,
    order_date,
    LEAD(order_date) OVER (PARTITION BY cust_id ORDER BY order_date ASC) AS next_order_date
    
FROM transactions
ORDER BY cust_id ASC, order_date ASC
)
SELECT 
    MONTH(ORDER_DATE) as month,
    SUM(CAsE WHEN next_order_date IS NULL THEN 1 ELSE 0 END) AS chur_cx_count
FROM cte GROUP BY MONTH
ORDER BY MONTH ASC;
