-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh; 


-- Create Sequence without start/increment values

create or replace sequence my_seq_object_01;

-- Create a table: 
create or replace transient table  customer_02 (
    id int default my_seq_object_01.nextval,
    cust_key number,
    name text,
    address text,
    nation_name text,
    phone text,
    acct_bal number,
    mkt_segment text
);


insert into customer_02
    (cust_key,name,address,nation_name,phone,acct_bal,mkt_segment)
    values
    (2590,'Customer#000002590','4ljncwzZkWWu','GERMANY','17-483-833-5072',9852.99,'BUILDING'),
    (2604,'Customer#000002604','xwadGtfw2eby','GERMANY','17-102-545-8181',3382.45,'MACHINERY'),
    (2622,'Customer#000002622','vocY5xVIZ8XW','GERMANY','17-329-378-5573',5263.92,'MACHINERY'),
    (2671,'Customer#000002671','r7PFEEl8sFMl','GERMANY','17-527-728-3381',3981.92,'MACHINERY'),
    (2699,'Customer#000002699','GWZ023qBegxZ','GERMANY','17-131-640-7765',3193.80,'AUTOMOBILE');



-- run select query on my customer table and check teh columns and column value

select * from customer_02;

-- lets create a stream object 

create or replace stream customer_stm on table customer_02; 

select * from customer_stm; -- no records. 

-- add one more record: 
insert into customer_02
    (cust_key,name,address,nation_name,phone,acct_bal,mkt_segment)
    values
    (2832,'Customer#000002832','pbS7wyddGLiXrqyiuNvc0sF','GERMANY','17-524-362-3344',960.50,'AUTOMOBILE');

select * from customer_02;
select * from customer_stm;
/* 
    METADATA$ACTION	    METADATA$ISUPDATE	          METADATA$ROW_ID
    INSERT	             FALSE	                      018d4b943a975061e28a85d1243c42dca85389bc

*/ 

-- yOU CAN CHOSE ONLY TO CAPTURE APPEND OR DELETE

create or replace stream stream_name on table table_name append_only = true; 

--> Delete wont be captured
--> Only insert works.













