
CREATE OR REPLACE TABLE sales_data 
(
    id INT,
    order_date DATE,
    sales_amount FLOAT
);

-- Inserting records into the table
INSERT INTO sales_data (id, order_date, sales_amount)
VALUES
    (1, '2024-07-01', 1000.00),
    (2, '2024-07-01', 1500.00),
    (3, '2024-07-02', 2000.00),
    (4, '2024-07-02', 2500.00),
    (5, '2024-07-03', 3000.00),
    (6, '2024-07-01', 1000.00),  -- Duplicate record
    (7, '2024-07-03', 3000.00),  -- Duplicate record
    (8, '2024-07-02', 2500.00),  -- Duplicate record
    (9, '2024-07-04', 3500.00),
    (10, '2024-07-04', 4000.00);


    Way 1: 

    DELETE FROM sales_data WHERE ID IN
(
SELECT id FROM
(
    SELECT id, order_date,sales_amount,
        ROW_NUMBER() oVER (PARTITION BY order_date, sales_amount ORDER BY id) AS row_numbering 
        FROM sales_data
) As alias_name WHERE row_numbering > 1
); 


Way 2: Using Having and Qualify and Join 

DELETE FROM sales_data
USING 
(
    SELECT id
    FROM 
    (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY order_date, sales_amount ORDER BY id) AS row_numbering
        FROM sales_data
    )
    QUALIFY row_numbering > 1
) AS duplicates
WHERE sales_data.id = duplicates.id;
