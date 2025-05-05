USE DATABASE PRACTISE;

CREATE OR REPLACE TABLE sales_data 
(
    region VARCHAR,
    salesperson VARCHAR,
    sales_amount NUMBER
);

INSERT INTO sales_data (region, salesperson, sales_amount) VALUES
('East', 'Alice', 500),
('East', 'Bob', 600),
('East', 'Charlie', 550),
('West', 'David', 700),
('West', 'Eva', 900),
('West', 'Frank', 850),
('South', 'Grace', 400),
('South', 'Heidi', 400),
('South', 'Ivan', 450);


// FIND SEOND HIGHEST SALE PER REGION


SELECT * FROM sales_data;


SELECT 
    REGION,
    SALESPERSON,
    SALES_AMOUNT,
    
FROM
(
SELECT 
    REGION,
    SALESPERSON,
    SALES_AMOUNT,
     DENSE_RANK() OVER (PARTITION BY REGION ORDER BY SALES_AMOUNT DESC) AS sales_rank
FROM sales_data  ORDER BY REGION ASC, SALES_AMOUNT DESC

) WHERE sales_rank = 2
