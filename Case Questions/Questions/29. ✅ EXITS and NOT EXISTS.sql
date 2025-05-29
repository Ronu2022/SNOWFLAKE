CREATE OR REPLACE TABLE Customers (
    id INT PRIMARY KEY,
    name STRING
);

CREATE OR REPLACE TABLE Orders (
    id INT PRIMARY KEY,
    customerId INT,
    order_total NUMBER(10,2)
);

INSERT INTO Customers (id, name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'David');

INSERT INTO Orders (id, customerId, order_total) VALUES
(1, 1, 120.50),
(2, 3, 99.99);


Select * from orders;
/*
Customers
+----+-----------+
| id | name      |
+----+-----------+
| 1  | Alice     |
| 2  | Bob       |
| 3  | Charlie   |
| 4  | David     |
+----+-----------+

Orders
+----+------------+-------------+
| id | customerId | order_total |
+----+------------+-------------+
| 1  | 1          | 120.50      |
| 2  | 3          | 99.99       |
+----+------------+-------------+
*/

--Write a query to return the names of customers who have never placed an order.
-- Try solving it using NOT EXISTS.

select name 
from Customers as c
where not exists(
select 1 from Orders as o where c.id = o.customerID
);

//❓ Question (Using EXISTS):
//Find all customers who have placed at least one order, and display their name and id.

Select name, id 
from Customers as c
where exists (
select 1 from Orders as o where c.id = o.customerID
);

//❓ Question (Using EXISTS):
// Find all customers who have placed at least one order worth more than $100, and return their id and name.
select name, id 
from Customers as c
where  exists(
select 1 from Orders as o where c.id = o.customerid and o.order_total > 100

);
