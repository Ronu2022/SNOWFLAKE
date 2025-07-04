CREATE OR REPLACE DATABASE UDF;
CREATE OR REPLACE SCHEMA my_functions;
CREATE OR REPLACE SCHEMA my_tables;

// UDF:

-- Supports 4 languages for writing udfs:
    -- SQL
    -- Java Script
    -- Java
    -- Python

-- Can return Sclar (Just a value or a String) and Tabiular Results.
-- Snowflake supports UDF overloading means support Functions with same name but different Parameters 
    -- func_cal_area() is different from fun__cal_area(radius FLOAT);

-- Sample UDF- SCALAR:

/*
 create function area_circle(radius float)
 returns float
 as
 $$
  pi() * radius * radius
  $$
  ; 

  select area_circle(4.5);

*/

-- Sample UDF - TABULAR:

create function t()
returns table(name varchar, age number)
as
$$
 select 'Ravi', 34
 union 
 select 'Latha', 27
 union
 select 'Madhu' 25
$$



-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
//SCALAR FUNCTIONS:
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Scenario 1: Fixed Tax:

CREATE OR REPLACE FUNCTION UDF.MY_FUNCTIONS.CUST_TAX(PRICE FLOAT)
RETURNS FLOAT
AS
$$
(PRICE * 10)/100
$$;

-- Granting acess to functions:

GRANT USAGE ON FUNCTION UDF.MY_FUNCTIONS.CUST_TAX(FLOAT) TO PUBLIC;



SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

SELECT O_ORDERKEY, 
O_ORDERDATE, O_TOTALPRICE, CUST_TAX(O_TOTALPRICE) as tax from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;


-- Scenario 2:  Variable Tax: 

CREATE OR REPLACE FUNCTION UDF.MY_FUNCTIONS.CUST_TAX_DYNAMIC(PRICE FLOAT, TAX_PER FLOAT)
RETURNS FLOAT
AS
$$
    (PRICE * TAX_PER)/100
$$;

GRANT USAGE ON FUNCTION UDF.MY_FUNCTIONS.CUST_TAX_DYNAMIC(FLOAT,FLOAT) TO PUBLIC; 


SELECT O_ORDERKEY, 
O_ORDERDATE, O_TOTALPRICE, CUST_TAX_DYNAMIC(O_TOTALPRICE,18) as tax from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;




----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
// TABULAR FUNCTIONS
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ Create some sampel tables:

CREATE OR REPLACE TABLE UDF.my_tables.COUNTRIES
(
    COUNTRY_CODE CHAR(2),
    COUNTRY_NAME VARCHAR
);

INSERT INTO UDF.my_tables.COUNTRIES(COUNTRY_CODE,COUNTRY_NAME) VALUES
    ('FR', 'FRANCE'),
    ('US', 'UNITED STATES'),
    ('IN', 'INDIA'),
    ('SP', 'SPAIN');


CREATE OR REPLACE TABLE UDF.my_tables.USER_ADDRESSES 
(
    USER_ID INTEGER, 
    COUNTRY_CODE CHAR(2)
);

INSERT INTO UDF.my_tables.USER_ADDRESSES (USER_ID, COUNTRY_CODE) VALUES 
    (100, 'SP'),
    (123, 'FR'),
    (567, 'US'),
    (420, 'IN');




// Create a function to fetch country name of customer	

 DROP FUNCTION UDF.MY_FUNCTIONS.get_country_name(INTEGER);

CREATE OR REPLACE FUNCTION UDF.MY_FUNCTIONS.func_getcountry_name(ID INTEGER)
RETURNS TABLE
(
    USER_ID INTEGER,
    COUNTRY_CODE CHAR ,
    COUNTRY_NAME VARCHAR
)
AS
$$

    SELECT ua.USER_ID,ua.COUNTRY_CODE,c.COUNTRY_NAME
    FROM UDF.my_tables.USER_ADDRESSES as ua
    LEFT JOIN UDF.my_tables.COUNTRIES as c
    ON TRIM(ua.COUNTRY_CODE) = TRIM(c.COUNTRY_CODE)
    WHERE ua.USER_ID = ID
 $$;


    SELECT ua.USER_ID,ua.COUNTRY_CODE,c.COUNTRY_NAME
    FROM UDF.my_tables.USER_ADDRESSES as ua
    LEFT JOIN UDF.my_tables.COUNTRIES as c
    ON TRIM(ua.COUNTRY_CODE) = TRIM(c.COUNTRY_CODE)
    WHERE ua.USER_ID = ID

    
 GRANT USAGE ON FUNCTION  UDF.MY_FUNCTIONS.func_getcountry_name(INTEGER) TO PUBLIC;

 SELECT * FROM TABLE(UDF.MY_FUNCTIONS.func_getcountry_name(420));

 -- You can do this:
 SELECT * FROM TABLE(UDF.MY_FUNCTIONS.func_getcountry_name(420))
 UNION ALL
 SELECT * FROM TABLE(UDF.MY_FUNCTIONS.func_getcountry_name(420))



 

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-- USING Multiple Functions in One Select :


CREATE OR REPLACE TABLE UDF.my_tables.SALES (
    ORDER_ID      INT,
    ITEM          STRING,
    PRICE         FLOAT,      -- list price
    TAX_RATE      FLOAT,      -- GST / VAT etc. (%)
    DISCOUNT_PCT  FLOAT       -- promo discount (%)
);


INSERT INTO UDF.my_tables.SALES (ORDER_ID, ITEM, PRICE, TAX_RATE, DISCOUNT_PCT) VALUES
    (1, 'Headphones',  120, 18, 10),
    (2, 'Keyboard',     80, 18, 15),
    (3, 'Mouse',        35, 18,  0),
    (4, 'Webcam',       60, 18,  5);


SELECT * FROM  UDF.my_tables.SALES;


// SQL UDF — CALC_TAX

CREATE OR REPLACE FUNCTION UDF.MY_FUNCTIONS.cal_sales_tax(p FLOAT, t FLOAT)
RETURNS FLOAT
LANGUAGE SQL 
AS
$$
    (p * t)/100
$$;

GRANT USAGE ON FUNCTION  UDF.MY_FUNCTIONS.cal_sales_tax(FLOAT,FLOAT) TO PUBLIC;


// SQL UDF — DISCOUNT

CREATE OR REPLACE FUNCTION UDF.MY_FUNCTIONS.APPLY_DISCOUNT(PRICE FLOAT, DISCOUNT_PCT FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    (price * (100 - DISCOUNT_PCT))/100
$$; 

GRANT USAGE ON FUNCTION UDF.MY_FUNCTIONS.APPLY_DISCOUNT(FLOAT,FLOAT) TO PUBLIC;




SELECT *, 
UDF.MY_FUNCTIONS.cal_sales_tax(PRICE,TAX_RATE) as GST,
UDF.MY_FUNCTIONS.APPLY_DISCOUNT(PRICE,DISCOUNT_PCT) AS discounted_price , 
UDF.MY_FUNCTIONS.APPLY_DISCOUNT(PRICE,DISCOUNT_PCT) + UDF.MY_FUNCTIONS.cal_sales_tax(PRICE,TAX_RATE) as effective_price
FROM  UDF.my_tables.SALES;









--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
