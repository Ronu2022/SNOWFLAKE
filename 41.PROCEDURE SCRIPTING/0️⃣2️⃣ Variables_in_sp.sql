create or replace database procedure_variables_db;
create or replace schema practise;

-- assigned using := 
-- to access the value a = :sum(b)  => accessed by :
-- Example:


  /*  declare
        first_name varchar default 'This is ';
        last_name varchar;
    begin
        let middle_name :='';
        let last_name  default 'Snowflake Scripting';
        full_name := first_name || middle_name || last_name; */

-- begin:
    -- Let middle_name := ''; 
    -- Let last_name default 'Snowflake Scripting';
-----------------------------------------------------------------------------------------------------------------------------------------------------------


SELECT SYSTEM$CLUSTERING_INFORMATION('ORDERS_CLUSTER');



-- Step 1: Create a sample table
CREATE OR REPLACE TABLE sample_orders (
    order_id INT,
    customer_name STRING,
    order_amount NUMBER
);

-- Step 2: Insert sample records
INSERT INTO sample_orders (order_id, customer_name, order_amount) VALUES
(1, 'Ronu', 1000),
(2, 'Ronu', 1500),
(3, 'Alex', 2000),
(4, 'Ronu', 1800),
(5, 'Meera', 1700);


SELECT * FROM procedure_variables_db.practise.sample_orders;


/*Find the sum of orders for Ronu*/
-- WAY 1:
create or replace procedure sample_procedure(cx_name varchar)
returns float
language sql
as
$$
declare total_sum  float;
begin
select sum(order_amount) into total_sum 
from procedure_variables_db.practise.sample_orders where customer_name = :cx_name;
return total_sum; 
end;
$$;

call sample_procedure('Ronu');





/* WAAY 2*/

CREATE OR REPLACE PROCEDURE sample_procedre_2(cx_name VARCHAR)
RETURNS FLOAT
LANGUAGE SQL 
AS
$$
DECLARE 
    total_sum FLOAT; 
    
BEGIN
    LET cx_name_lower := LOWER(cx_name);

    SELECT COALESCE(SUM(order_amount), 0)
    INTO total_sum
    FROM procedure_variables_db.practise.sample_orders 
    WHERE LOWER(customer_name) = :cx_name_lower;

    RETURN total_sum;
END;
$$;



call sample_procedre_2('RONU');


-------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE procedure_variables_db.practise.sample_orders (
    customer_name VARCHAR,
    order_id VARCHAR,
    order_amount FLOAT
);

INSERT INTO procedure_variables_db.practise.sample_orders (customer_name, order_id, order_amount)
VALUES 
    ('Ronu', 'O1', 100),
    ('Ronu', 'O2', 150),
    ('Alice', 'O3', 200),
    ('Ronu', 'O4', 50),
    ('Bob', 'O5', 300),
    ('ronu', 'O6', 120);

select * from  procedure_variables_db.practise.sample_orders;
/*
Input: customer name, amount threshold

Output: how many orders that customer made above that amount
*/






// Procedure
create or replace procedure above_thresh(cx_name varchar, thres int)
returns int
language sql
as
$$
declare
order_count int default 0; 
begin
let cx_name_lower  := lower(cx_name);
select coalesce(count(distinct order_id),0) into order_count  from 
procedure_variables_db.practise.sample_orders where 
lower(customer_name) =:cx_name_lower and order_amount > :thres;

return order_count;
end;
$$;

call above_thresh('RONU',160); -- o/p = 0

-------------------------------------------------------------------------------------------------------------------------------------------------------------


// Session variables: 

-- variables used only in one session, that is scope limited to current session only. 
-- we use set to declare session variables. 
--  set a = 10; 
--  set name = 'Jana'
--  set (b,c,d) = (100,25,'hyderabad')
--  Session variable can be acessed using $

-------------------------------------------------------------------------------------------------------------------------------------------------------------
// EXECUTE IMMEDIATE:

-- used to get you the code imemddiately


execute immediate
$$
declare
    order_count int default 0; 
begin
    let thresh int := 100;
    let cx_name_lower := 'ronu';
select coalesce(count(distinct order_id),0) into order_count  from 
procedure_variables_db.practise.sample_orders where 
lower(customer_name) =:cx_name_lower and order_amount > :thresh;
end;
$$;

----------------------------------------------------------------------------------------------------------------------------------------------------------

/* PRACTISE */ 


EXECUTE IMMEDIATE
$$
DECLARE
    first_name varchar default 'This is ';
    last_name varchar;
    full_name varchar;
begin
    let middle_name := ' ';
    last_name := 'SNOWFLAKE SCRIPTING';
    full_name := first_name || middle_name || last_name;
    return full_name;
end;
$$;

--------------------------------------------------------------------------------------------------------------------------------------------------------
EXECUTE IMMEDIATE
$$
DECLARE
    profit FLOAT DEFAULT 0.0; 
BEGIN
    LET S_P FLOAT  := 200.50;
    LET C_P FLOAT := 100.11; 
    PROFIT := S_P - C_P; 
RETURN PROFIT;
END;
$$;

---------------------------------------------------------------------------------------------------------------------------------------------------------

// SESSION LEVEL VARIABLES:

SET PIE = 3.12; -- LOOK CAN BE USED ANBYWHERE IN THE SESSION;

EXECUTE IMMEDIATE 
$$
DECLARE AREA FLOAT DEFAULT 0.0;
BEGIN
    LET r FLOAT := 22;
    AREA := $PIE * r * r;  -- ACCESSED THROUGH $
    RETURN AREA;
END;
$$;


-- EXAMPLE 2:
SET (A,B,place) = (30,52,'HYD'); -- CAn't be modified inside the begin. 

EXECUTE IMMEDIATE
$$
DECLARE 
    SENTENCE VARCHAR DEFAULT '';
BEGIN
    LET FIRST_PART VARCHAR := 'HIS AGE IS ';
    LET SECOND_PART VARCHAR := ' AND HE HAS ';
    LET THIRD_PART VARCHAR := ' CARS ';
    LET FOURTH_PART VARCHAR := 'STAYS IN ';
    SENTENCE := FIRST_PART || $A || SECOND_PART || $B || THIRD_PART || FOURTH_PART || $place;
    RETURN SENTENCE;
END;
$$;
    




        
