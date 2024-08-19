CREATE DATABASE subquery; 
USE DATABASE subquery;


CREATE OR REPLACE SEQUENCE emp_seq
START = 1
INCREMENT = 1
Comment = 'This is my emp Sequence';

CREATE OR REPLACE  TABLE employee
(
    emp_id INTEGER IDENTITY(120,1),
    emp_name VARCHAR(40),
    dept_name VARCHAR(8),
    salary INTEGER
)
COMMENT = 'This is my employee table';

INSERT INTO employee( emp_name, dept_name,salary) VALUES
('Monica', 'Admin', 5000),
('Rossalin', 'IT', 6000),
('Ibrahim', 'IT', 8000),
('Vikram', 'IT', 8000),
('Dheeraj', 'IT', 11000);

SELECT * FROM employee;



CREATE OR REPLACE TABLE department 
(
    dept_id  INT, 
    dept_name VARCHAR(10),
    location VARCHAR(20)

)
COMMENT = 'This is my department table';


INSERT INTO department VALUES
(2, 'HR', 'Bengaluru'),
(3, 'IT', 'Bengaluru'),
(4, 'Finance', 'Mumbai'),
(5, 'Marketing', 'Kolkata'),
(6, 'Sales','Mumbai');

SELECT * FROM department;

-- Find those employees whose salary is > average salary

SELECT * FROM employee WHERE salary > (SELECT AVG(salary) FROM employee); -- second part is the subquery
-- so subquery = Query within a query.


/* Different types of Subqueries: 
  1. Scalar Subquery.
  2. Multiple Row Subquery.
  3. Coorelated Subquery
  */


  /* Scalar Sub Query
     - Returns only one row and colum column*/

     SELECT AVG(salary) FROM employee; --  returns 1 row and colum

SELECT * FROM employee WHERE salary > (SELECT AVG(salary) FROM employee); 

-- here in the subquery it returns only 1 value i.e the average of salary => Sclar Subquery. 

-- The Above could be written in multiple ways

SELECT 
    emp.emp_id, 
    emp.emp_name, 
    emp.dept_name, 
    emp.salary
FROM 
(SELECT * FROM employee) emp
JOIN 
(SELECT AVG(salary) AS avg_sal FROM employee) average
ON emp.salary > average.avg_sal;



/* Multiple Row Subquery: 
  - 2 types: 
        - Subquery returning multiple rows and multiple columns.
        - Subquery returning Single Column with Multiple Subquery
*/


-- Q. Find the employees in each department who have the higest Salary. 


-- 1st way using subquery and Join

SELECT rnk.dept_name,LISTAGG(rnk.emp_name,',') WITHIN GROUP (ORDER BY emp.salary DESC) FROM
(SELECT * FROM employee) emp
JOIN
(SELECT emp_id,dept_name, emp_name, DENSE_RANK() OVER(PARTITION BY dept_name ORDER BY salary DESC) AS drank FROM  employee) rnk
ON emp.emp_id = rnk.emp_id
Where rnk.drank IN (1)
GROUP BY rnk.dept_name;


-- using CTE:

WITH salary_rank AS
(

SELECT emp_id, dept_name, emp_name,salary, DENSE_RANK() OVER(PARTITION BY dept_name ORDER BY salary DESC) AS drank FROM employee

) SELECT dept_name, LISTAGG(emp_name,'|') WITHIN GROUP (ORDER BY salary desc ) AS emp_rank FROM salary_rank
  WHERE drank IN (1)
  GROUP BY dept_name


-- Using Only Sub Quey : Multiple Row Subquery


SELECT dept_name, MAX(salary) AS max_salary FROM employee GROUP BY dept_name; -- gives the ma salary of each department
                                                                              -- returns Multiple rows and columns.

SELECT dept_name, emp_name, salary FROM employee 
WHERE (dept_name,salary) IN (SELECT dept_name, MAX(salary) AS max_salary FROM employee 
                             GROUP BY dept_name); -- used the above subquery which returned multiple rows and colums



/* Single column multiple row subquery*/

-- Find Departments Who Donot Have any employees.

SELECT dept_name, COUNT(emp_name) as emp_count FROM employee GROUP BY dept_name HAVING COUNT(emp_name) = 0; -- gives the Dept_name

SELECT dept_name FROM employee GROUP BY dept_name HAVING COUNT(emp_name) = 0 ;-- gives the names of dept having count = 0 one column might have >1 rows. 

SELECT * FROM employee WHERE dept_name IN (SELECT dept_name FROM employee GROUP BY dept_name HAVING COUNT(emp_name) = 0 ); -- This gives the final result. 


-- Note there was another table named department, we got to find those departments where there are no employees. 


SELECT dept_name FROM EMPLOYEE GROUP BY dept_name HAVING COUNT(emp_name) > 0;

SELECT dept_name FROM department WHERE dept_name NOT IN (SELECT dept_name FROM EMPLOYEE GROUP BY dept_name HAVING COUNT(emp_name) > 0) -- gives you the desired results. 


-- or 

SELECT DISTINCT(dept_name) FROM employee; -- gives two names meaning they have employees

SELECT dept_name FROM department WHERE dept_name NOT IN (SELECT DISTINCT(dept_name) FROM employee); -- This also gives the desired results. 



/* CO-Related Subquery*/

-- Find the employes in Each Department Who earn Salary > Avg Salary of that Department 

-- way 1:

SELECT emp.emp_name FROM
(SELECT * FROM employee)emp
JOIN
(SELECT dept_name, AVG(salary) as avg_salry FROM employee  -- Admin (avg - 5000), IT average Salary(8250)
GROUP BY dept_name) avgsal
ON emp.dept_name = avgsal.dept_name
WHERE emp.salary > avgsal.avg_salry ;


-- way 2:

SELECT * FROM employee e1 WHERE salary > (SELECT AVG(salary) FROM employee e2 WHERE 
                                          e2.dept_name = e1.dept_name); -- if you look at this the subquery is dependednt upon the outer query 
-- if yoy could observe for every record one eecution is for outer the other for the inner subquery => if millions of records > time consuming 
-- and that is why we genrally try avoinding, if we could alternatively do it using joins or scalar or multi row subquery.




-- Find Departments Who Donot Have any employees.

SELECT dept_name FROM employee GROUP BY dept_name HAVING COUNT(emp_id) > 0;

SELECT dept_name FROM department WHERE dept_name NOT IN (SELECT dept_name FROM employee GROUP BY dept_name HAVING COUNT(emp_id) > 0); -- This was
-- the way we had known , this is multirow subquery. 


SELECT * FROM department d
WHERE NOT EXISTS (SELECT 1 FROM employee e WHERE e.dept_name = d.dept_name)

-- subquery gives those names which is present and 
--  NOT EXISTS negates that.
            

/* Nested Subquery*/ 


CREATE OR REPLACE TABLE apple_stores 
(
    store_id INT,
    store_name VARCHAR(100),
    product_name VARCHAR(100),
    quantity INT,
    price DECIMAL(10, 2)
);


INSERT INTO apple_stores (store_id, store_name, product_name, quantity, price)
VALUES
    (1, 'Apple Store - Fifth Avenue', 'iPhone 14', 50, 999.99),
    (1, 'Apple Store - Fifth Avenue', 'MacBook Pro', 20, 1999.99),
    (2, 'Apple Store - Union Square', 'iPad Air', 30, 599.99),
    (2, 'Apple Store - Union Square', 'AirPods Pro', 100, 249.99),
    (3, 'Apple Store - Regent Street', 'Apple Watch Series 8', 40, 399.99),
    (3, 'Apple Store - Regent Street', 'HomePod', 15, 299.99),
    (4, 'Apple Store - Dubai Mall', 'iMac', 10, 1799.99),
    (4, 'Apple Store - Dubai Mall', 'Magic Keyboard', 25, 99.99);

ALTER TABLE apple_stores ADD COLUMN revenue NUMBER;
UPDATE apple_stores SET revenue = quantity * price;

SELECT * FROM apple_stores;


SELECT store_name FROM apple_stores WHERE revenue >
(SELECT AVG(total_rev) FROM
(SELECT store_name, SUM(revenue) as total_rev FROM apple_stores GROUP BY store_name; -- kind of a query inside a query 


-- Subqueries Can be used in: 
 -- Select
 -- FROM
 -- Where 
 -- Having

 -- USAGE of Subquery in Select Clause: 

 -- Let's say , for every employee you have to mention if he is earning more than the average salary
 -- and oly for those emloyees where it is greater, for others print NULL.

 SELECT *, CASE WHEN salary > (SELECT AVG(salary) FROM employee) THEN 'Higher than Average'
                ELSE NULL END AS remarks
 FROM employee;


 -- Subquery Inside Having: 

-- FInd the names of those stores where quantity sold > average quantity sold.


SELECT store_name, SUM(quantity) as tot_quant FROM apple_stores GROUP BY store_name
HAVING tot_quant > (SELECT AVG(tot_quant) FROM (SELECT store_name, SUM(quantity) as tot_quant FROM apple_stores GROUP BY store_name))



-- Usage of sub query in insert statement: 

CREATE OR REPLACE TABLE employee_history
(
    emp_id INT,
    emp_name VARCHAR(30),
    dept_name VARCHAR(40),
    salary NUMBER
) comment = 'this is my employee_history table';

-- Insert into employee_history table but ensure there shouldnt be any duplicate records there. 

select * from employee;
select * from department; 


-- 
INSERT INTO employee_history(emp_id,emp_name, dept_name, salary)
SELECT e.emp_id,e.emp_name,e.dept_name, e.salary
FROM employee e 
JOIN department d 
ON e.dept_name = d.dept_name
WHERE NOT EXISTS (SELECT 1 FROM employee_history eh WHERE eh.emp_id = e.emp_id );




