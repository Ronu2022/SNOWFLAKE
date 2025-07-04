// STORED PROCEDURE:

-- allow you to write procedural code that includes sql statements, conditional statements, looping statements and cursors. 
-- Supports  5 languages for writing stored procedures.
    -- Snwoflake Scripting (SQL)
    -- Java Scripting
    -- Java
    -- Scala
    -- Python
-- From a stored Procedure you can return singel or Tabular Data.
--Supports Branching and Looping
-- Dynamically Create a SQL Statement and Execute it. 

/*
CREATE OR REPLACE PROCEDURE load_table()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    ----
    ---
$$
;
*/ 


/* UDF VS STORED PROCEDURES */ 

/*
-- SP may or may not return results, UD -> returns results (table or value)
-- Called as independent Statements , UDF -> are Called IN SQL Statements.
-- Values returned by Stored Procedure not directly usable in SQL , UDF can be directly usable in SQL
-- Single Procedure per CALL statement, UDF -> Single Statement Can call Multiple Functions
        e.g: SELECT *, 
                UDF.MY_FUNCTIONS.cal_sales_tax(PRICE,TAX_RATE) as GST,
                UDF.MY_FUNCTIONS.APPLY_DISCOUNT(PRICE,DISCOUNT_PCT) AS discounted_price , 
                UDF.MY_FUNCTIONS.APPLY_DISCOUNT(PRICE,DISCOUNT_PCT) + UDF.MY_FUNCTIONS.cal_sales_tax(PRICE,TAX_RATE) as effective_price
                FROM  UDF.my_tables.SALES;

-- Can Perform DDL and DML operations , UDF -> Can't do so 
*/
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE DATABASE proc_db;
CREATE OR REPLACE SCHEMA my_proc;
CREATE OR REPLACE SCHEMA my_views;
CREATE OR REPLACE SCHEMA my_tables;
CREATE OR REPLACE SCHEMA my_functions;


CREATE OR REPLACE TABLE proc_db.my_tables.CUST_SALES
(CID INT, CNAME VARCHAR, PRODNAME VARCHAR, PROD_CAT VARCHAR, PRICE NUMBER);

INSERT INTO proc_db.my_tables.CUST_SALES VALUES
(101, 'RAMU', 'REFRIGERATOR', 'ELECTRONICS', 25000 ),
(101, 'RAMU', 'TV', 'ELECTRONICS' , 33500),
(101, 'RAMU', 'MAGGIE', 'FOOD', 200),
(102, 'LATHA', 'T-SHIRT', 'FASHION', 1099),
(102, 'LATHA', 'JEANS', 'FASHION', 2999),
(102, 'LATHA', 'WALLNUTS', 'FOOD', 2000),
(102, 'LATHA', 'WASHING MACHINE', 'ELECTRONICS', 29000),
(102, 'LATHA', 'SMART WATCH', 'ELECTRONICS', 12000),
(102, 'LATHA', 'ALMOND', 'FOOD', 1500),
(102, 'LATHA', 'TOUR DAL', 'FOOD', 500),
(102, 'LATHA', 'RICE', 'FOOD', 1300);


SELECT * FROM PROC_DB.MY_TABLES.CUST_SALES;

// Find the total Spending of a customer per product_cat


CREATE OR REPLACE PROCEDURE proc_db.my_proc.total_spend(cx_id INT, pr_cat VARCHAR)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
DECLARE 
    total_spent FLOAT;
    cur1 CURSOR FOR SELECT CID, CNAME, PROD_CAT, PRICE  FROM PROC_DB.MY_TABLES.CUST_SALES;
BEGIN
total_spent  := 0;

FOR rec IN cur1
DO
    IF (rec.CID = :cx_id and rec.PROD_CAT = :pr_cat) THEN
    total_spent := total_spent + rec.PRICE;
    END IF;
END FOR;

RETURN total_spent;
END;
$$
;
























CALL proc_db.my_proc.total_spend(102,'ELECTRONICS');

DROP FUNCTION  proc_db.my_functions.func_total_cx_spend(INT,VARCHAR);

// Using Functions: 

CREATE OR REPLACE FUNCTION proc_db.my_functions.func_total_cus_spend(t_cx_id INT, t_prod_cat VARCHAR)
RETURNS TABLE
(
    CNAME VARCHAR,
    TOTAL_PRICE NUMBER(38,0)
)
AS
$$


    SELECT 
        CNAME, 
        SUM(PRICE) AS TOTAL_PRICE
    FROM PROC_DB.MY_TABLES.CUST_SALES
    WHERE CID = t_cx_id AND PROD_CAT = t_prod_cat
    GROUP BY CNAME


$$;


SELECT * FROM TABLE(proc_db.my_functions.func_total_cus_spend(102,'ELECTRONICS'));





