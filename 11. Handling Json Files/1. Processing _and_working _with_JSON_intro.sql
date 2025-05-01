
CREATE DATABASE json_2; 
CREATE schema yl; 

-- let's create a tbale named json_tbl with one column only of variant type

create or replace transient table json_tbl
(
 json_col variant
);

-- let's insert a record into it 

insert into json_tbl(json_col) values('{"firstName": "John", "empid" : 1001}'); -- error
  -- not possible direct insert in case of a variant 

insert into json_tbl(json_col) select parse_json('{"firstName": "John", "empid" : 1001}');

describe  table json_tbl;

select * from json_tbl; -- loook while insertion first_name was before empid
-- thus, order of keys inside a json is not guaranteed


-- For inserting multiple values inside a json variable; 


truncate table json_tbl;

select * from json_tbl; 


insert into json_tbl(json_col) 
select parse_json(column1) from values
('{"firstName": "Name-1","empid": 1001}'),
('{"firstName": "Name-2", "empid": 1002}'),
('{"firstName": "Name-3", "empid": 1004}'),
('{"firstName": "Name-4", "empid": 10004}')
;

select * from json_tbl; -- again order of teh keys arent the same




truncate table json_tbl; 




  insert into json_tbl(json_col)
select parse_json(
  '{
    "employee": {
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "married": true,
      "has_kids": false,
      "stock_options": null,
      "phone": [
        "+1 123-456-7890",
        "+1 098-765-4321"
      ],
      "Address": {
        "street": "4132 Grant Street",
        "city": "Rockville",
        "state": "Minnesota"
      }
    }
  }'
);
L_name(col_name)




truncate table json_tbl; 


drop table employee_tbl;

---------------------------------------------------------------------------------------------------------------------------

create or replace table employee_tbl
(
 emp_json variant
);


insert into employee_tbl(emp_json)
select parse_json(
  '{
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "married": true,
      "has_kids": false,
      "stock_options": null
    }');

select *  from employee_tbl; 


-- accessing individual element and name using colon notation 

select emp_json:name from employee_tbl; 
select emp_json: age from employee_tbl;
select emp_json : height from employee_tbl; 


select emp_json:name,  -- these are case sensitive hencee must be careful
       emp_json: age,
       emp_json: height,
       emp_json: married,
       emp_json: has_kids,
       emp_json: stock_options
from employee_tbl; 


select emp_json:name as Name,
       emp_json: age as age,
       emp_json: height as height,
       emp_json: married as Married,
       emp_json: has_kids as kids,
       emp_json: stock_options as stock
from employee_tbl; 


-- let's check the type of these objects to see how snofalke treats them 

select typeof(emp_json:name) from employee_tbl; -- varchar
select typeof(emp_json: age) from employee_tbl; -- integer
select typeof(emp_json: has_kids) from employee_tbl; 

-- generally the varrchar is in dfouble quotes you can do typecast

select * from employee_tbl;

select emp_json:name:: varchar as name_empl from employee_tbl;
select emp_json : age:: integer as age_emp from employee_tbl; 
select emp_json:name::string as name from employee_tbl; 



-- let's insert more complex employee object
-- delete old records

truncate table employee_tbl; 



insert into employee_tbl(emp_json)
select parse_json(
  '{
    "employee": {
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "married": true,
      "has_kids": false,
      "stock_options": null,
      "phone": [
        "+1 123-456-7890",
        "+1 098-765-4321"
      ],
      "Address": {
        "street": "3621 McDonald Avenue",
        "city": "Orlando",
        "State": "Florida"
      }
    }
  }');

select * from employee_tbl;


select 
    emp_json:employee.name:: varchar as name, 
    emp_json:employee.age :: integer as age, 
    emp_json:employee.height_in_ft :: float as height, 
    emp_json:employee.married as is_married, 
    emp_json:employee.has_kids as has_kids,
    emp_json:employee.stock_options as stock_options, 
    emp_json:employee.phone as all_phone, 
    emp_json:employee.phone[0] as work_phone, 
    emp_json:employee.phone[1] as office_phone, 
    emp_json:employee.Address as all_address, 
    emp_json:employee.Address.street:: VARCHAR as street,
    emp_json:employee.Address.city:: VARCHAR as city,
    emp_json:employee.Address.State:: VARCHAR as State
from employee_tbl; 


select * from employee_tbl;


-- in place of dot notation colon can alos be used

select emp_json:employee.name:: varchar as name from employee_tbl; 

select emp_json:employee:name::varchar as name from employee_tbl; 

select emp_json:employee:Address:street::varchar from employee_tbl;

select emp_json: employee: phone[0]:: varchar from employee_tbl;


-- apply other functions and mathematical operation without casting it 

select 
        emp_json:employee:name:: varchar as name,
        emp_json:employee:age as age, 
        emp_json:employee:height_in_ft * (12*2.54) as height_in_cm,
        typeof(emp_json:employee:phone) as type_of,
        ARRAY_SIZE(emp_json:employee:phone) as how_many_phone
From employee_tbl;
        
-----------------------------------------------------------------------------------------------------------------------------------


create table emp_table_4
(
 emp_json Variant
);

insert into emp_table_4(emp_json)
select parse_json(
  '{
      "name": "John",
      "age": 30,
      "height_in_ft": 5.11,
      "dob":"2022-12-11",
      "dob_timestemp":"2022-12-11T00:19:06.043-08:00",
      "married": true,
      "has_kids": false,
      "stock_options": null
    }');
-- check timestamp values, dates are given within double quotes
select * from emp_table_4; 

select emp_json:dob,typeof(emp_json:dob) as date_type, emp_json:dob_timestemp , typeof(emp_json:dob_timestemp) from emp_table_4;
-- note it is not showing as date or timestamp type, thus we would need double colon contation i.e type casting 

select emp_json:dob:: date as dob_date, emp_json:dob_timestemp::timestamp  from emp_table_4; -- converted 


-------------------------------------------------------------------------------------------------------------------------------------


Create or replace table emp_details_6
(
 emp_json variant
);


--1st record
insert into emp_details_6(emp_json)
select parse_json(
  '{
    "employee": {"name": "John-1","age": 30,"height_in_ft": 5.11,"married": true,"has_kids": false,
      "stock_options": null,"email":"john1@ttips.com","phone": ["+1 123-456-7890","+1 098-765-4321"],
      "Address": {"street": "3621 McDonald Avenue","city": "Orlando","State": "Florida"}
               }
    }');


--2nd record
insert into emp_details_6(emp_json)
select parse_json(
  '{
    "employee": {"name": "John-2","age": 33,"height_in_ft": 5.09,"married": false,"has_kids": false,
      "stock_options": 10,"email":"john2@ttips.com","phone": ["+1 222-456-0987"],
      "Address": {"street": "532 Locust View Drive","city": "San Jose","State": "California"}
               }
    }');


select * from emp_details_6; 


-- Task: 
-- get 3 -- tables Employee_csv first_name, last_name, DOB
-- Phone CSV -- phone number
-- Address csv -- Stret, city, stage


-- Let's access each elements first 

select
    emp_json:employee:name:: varchar as name,
    emp_json:employee:age:: integer as age, 
    emp_json:employee:height_in_ft:: float as height, 
    emp_json:employee:married: as is_married,
    emp_json:employee:has_kids as has_kids, 
    emp_json:employee:stock_options as stock_options, 
    emp_json:employee:email as email_id,
    emp_json:employee:phone[0]:: STRING as home_phone,
    emp_json:employee:phone[1]:: STRING as work_phone,
    emp_json:employee:Address:street:: varchar as street,
    emp_json:employee:Address:city:: varchar as city,
    emp_json:employee:Address:State :: varchar as state
from emp_details_6;



-- let's create sequence for unique identification 

Create or replace sequence emp_seq
start 1
increment 1
comment = 'employee sequence';


create or replace sequence phone_sequence
start 1
increment 1
comment = 'phone sequence';


create or replace sequence address_sequence
 start 1
 increment 1
 comment = 'address sequence';

 

 -- Create a Master Table 

 CREATE or REPLACE TABLE employee_master
 (
  emp_pk integer default emp_seq.nextval,
  name string,
  age integer,
  height_in_cm float,
  is_married boolean,
  has_kids boolean,
  stocks_options integer,
  email varchar(100)
  );


-- Table holding the Phones

CREATE OR REPLACE TABLE  emp_phones
(
  phone_pk integer default phone_sequence.nextval,
  emp_fk number, -- reference to master table
  phone_type varchar(30),
  phone_number varchar(40)
);



create or replace table emp_address
(
  address_pk integer default address_sequence.nextval,
  emp_fk number, 
  street_address varchar(200),
  city varchar(50),
  state varchar(50)
);


-- Insert into master table:


select * from employee_master;


insert into employee_master (name, age,height_in_cm, is_married, has_kids, stocks_options,email)
select 
    emp_json:employee:name as name,
    emp_json:employee:age as age,
    emp_json:employee:height_in_ft * (12*2.54) as height_in_cm,
    emp_json:employee:is_married as is_married,
    emp_json:employee:has_kids as has_kids,
    emp_json:employee:sticks_options as stocks_options,
    emp_json:employee:email as emai
from emp_details_6; 

select * from employee_master;


-- Insert into phone table:

select * from emp_phones;

select 
    emp_json:employee:name as name,
    emp_json:employee:age as age,
    emp_json:employee:height_in_ft * (12*2.54) as height_in_cm,
    emp_json:employee:is_married as is_married,
    emp_json:employee:has_kids as has_kids,
    emp_json:employee:sticks_options as stocks_options,
    emp_json:employee:email as emai
from emp_details_6; 

select * from employee_master;





insert into  emp_phones(EMP_FK,PHONE_TYPE,PHONE_NUMBER)
 
select 
    m.emp_pk as EMP_FK,
    'home_phone' as PHONE_TYPE,
    e.emp_json:employee:phone[0]::string as PHONE_NUMBER
FROM 
    emp_details_6 e
JOIN 
    employee_master m
ON 
    e.emp_json:employee:email::string = m.email::string

UNION ALL

select 
  m.emp_pk as emp_fk,
  'work_phone', 
  e.emp_json:employee:phone[1]::string
FROM 
  emp_details_6 e
join 
  employee_master m 
on 
  e.emp_json:employee:email::string= m.email::string;

select * from emp_phones;


-- Insert into address table:


select * from employee_master;
select * from emp_address;

Select emp_json:employee:Address:street:: varchar as street,
    emp_json:employee:Address:city:: varchar as city,
    emp_json:employee:Address:State :: varchar as state from emp_details_6;

insert into  emp_address(emp_pk,street_address,city,state)
SELECT 
        m.emp_pk,
        e.emp_json:employee:Address:street::string as  STREET_ADDRESS,
        e.emp_json:employee:Address:city::string as CITY,
        e.emp_json:employee:Address:State::string as STATE
FROM emp_details_6 e 
JOIN employee_master m
ON e.emp_json:employee:email = m.email
        
