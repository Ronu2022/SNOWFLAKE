-- change context
use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse


-- without start/increment values
create or replace sequence my_seq_object_01;

-- list all sequences
show sequences like 'MY%';


-- desc sequence object
desc sequence my_seq_object_01;


show sequences like 'MY%'; 

desc sequence my_seq_object_01;  

/*
name	             database_name	        schema_name	    next_value	interval	created_on	                     owner	          comment	    owner_role_type	    ordered
MY_SEQ_OBJECT_01	DEMO_DB	                   PUBLIC	     1	          1	         2025-11-30 04:28:22.854 -0800	   ACCOUNTADMIN		              ROLE	           N
*/



-- with start value
create or replace sequence my_seq_object_02
    start = 0
    increment = 2;


-- negative increment
create or replace sequence my_seq_object_03
start = 0
increment = -2; 


-- select from seq object  :

select my_seq_object_01.nextval, my_seq_object_02.nextval, my_seq_object_03.nextval; 








