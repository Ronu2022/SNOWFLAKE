/* Question : You are given an orders table with the following columns:

Customer_id : ID of the customer
Order_date : Date when the order was placed
coupon_code : Coupon code used in the order (can be Null if no coupon was applied)


We want an SQL query to return the number of new customers who satisfy all of the following conditions: 

1 - They place orders in each of their first 3 consecutive months since their very first order. 

2 - The number of orders in the second month is exactly double the number of orders in the first month.

3 - The number of orders in the third month is exactly triple the number of orders in the first month.

4 - Their last order (latest by date) was placed with a coupon code (i.e., Coupon Code is not NULL) 

*/




CREATE TABLE orders (
    customer_id INT,
    order_date DATE,
    coupon_code VARCHAR(50)
);

TRUNCATE TABLE Orders;

-- ✅ Customer 1: First order in Jan, valid pattern
INSERT INTO Orders VALUES (1, '2025-01-10', NULL);
INSERT INTO Orders VALUES (1, '2025-02-05', NULL);
INSERT INTO Orders VALUES (1, '2025-02-20', NULL);
INSERT INTO Orders VALUES (1, '2025-03-01', NULL);
INSERT INTO Orders VALUES (1, '2025-03-10', NULL);
INSERT INTO Orders VALUES (1, '2025-03-15', 'DISC10'); -- last order with coupon ✅

-- ✅ Customer 2: First order in Feb, valid pattern
INSERT INTO Orders VALUES (2, '2025-02-02', NULL);   -- Month1 = 1
INSERT INTO Orders VALUES (2, '2025-02-05', NULL);   -- Month1 = 1
INSERT INTO Orders VALUES (2, '2025-03-05', NULL);   -- Month2 = 2
INSERT INTO Orders VALUES (2, '2025-03-18', NULL);
INSERT INTO Orders VALUES (2, '2025-03-20', NULL);   -- Month2 = 2
INSERT INTO Orders VALUES (2, '2025-03-22', NULL);
INSERT INTO Orders VALUES (2, '2025-04-02', NULL);   -- Month3 = 3
INSERT INTO Orders VALUES (2, '2025-04-10', NULL);
INSERT INTO Orders VALUES (2, '2025-04-15', 'DISC20'); -- last order with coupon ✅
INSERT INTO Orders VALUES (2, '2025-04-16', NULL);   -- Month3 = 3
INSERT INTO Orders VALUES (2, '2025-04-18', NULL);
INSERT INTO Orders VALUES (2, '2025-04-20', 'DISC20'); -- last order with coupon ✅

-- ❌ Customer 3: First order in March but wrong multiples
INSERT INTO Orders VALUES (3, '2025-03-05', NULL);  -- Month1 = 1
INSERT INTO Orders VALUES (3, '2025-04-10', NULL);  -- Month2 should have 2, but only 1 ❌
INSERT INTO Orders VALUES (3, '2025-05-15', 'DISC30');

-- ❌ Customer 4: First order in Feb but missing March (gap)
INSERT INTO Orders VALUES (4, '2025-02-01', NULL);  -- Month1
INSERT INTO Orders VALUES (4, '2025-04-05', 'DISC40'); -- Skipped March ❌

-- ❌ Customer 5: Valid multiples but last order has no coupon
INSERT INTO Orders VALUES (5, '2025-01-03', NULL);  -- M1 = 1
INSERT INTO Orders VALUES (5, '2025-02-05', NULL);  -- M2 = 2
INSERT INTO Orders VALUES (5, '2025-02-15', NULL);
INSERT INTO Orders VALUES (5, '2025-03-01', NULL);  -- M3 = 3
INSERT INTO Orders VALUES (5, '2025-03-08', 'DISC50'); -- coupon mid
INSERT INTO Orders VALUES (5, '2025-03-20', NULL);     -- last order no coupon ❌

-- ❌ Customer 6: Skips month 2, should be excluded
INSERT INTO Orders VALUES (6, '2025-01-05', NULL);     -- Month1 = 1 order
-- (no orders in Feb, so Month2 is missing ❌)
INSERT INTO Orders VALUES (6, '2025-03-02', NULL);     -- Month3 = 1st order
INSERT INTO Orders VALUES (6, '2025-03-15', NULL);     -- Month3 = 2nd order
-- Jump to May (Month5 relative to Jan)
INSERT INTO Orders VALUES (6, '2025-05-05', NULL);     
INSERT INTO Orders VALUES (6, '2025-05-10', NULL);     
INSERT INTO Orders VALUES (6, '2025-05-25', 'DISC60'); -- Last order with coupon




with cte_1 as
(
    select 
    customer_id, coupon_code,
    order_date,
    date_trunc(month,order_date) as order_month,
    min(date_trunc(month,order_date)) over(partition by customer_id order by order_date) as min_order_month,
    datediff(month,min(date_trunc(month,order_date)) over(partition by customer_id order by order_date),date_trunc(month,order_date)) as month_num,
    max(order_date) over(partition  by  customer_id) as last_order_date
    from orders
),
monthly_purchase_cte as
(
select 
    customer_id,
    sum(case when month_num = 0 then 1 else 0 end) as first_month_purchase,
    sum(case when month_num = 1 then 1 else 0 end) as second_month_purchase,
    sum(case when month_num = 2 then 1 else 0 end) as third_month_purchase
from cte_1
group by customer_id
having first_month_purchase <> 0 and second_month_purchase <> 0 and third_month_purchase <> 0
and  second_month_purchase = 2* first_month_purchase 
and  third_month_purchase  = 3 * first_month_purchase
),
last_purchase_with_coupon as
(
    select
 * from cte_1 where coupon_code is not null and 
 order_date = last_order_date

)
select
    t1.customer_id
from monthly_purchase_cte as t1
where exists
(
    select 1 from last_purchase_with_coupon as t2 where t1.customer_id = t2.customer_id
)



