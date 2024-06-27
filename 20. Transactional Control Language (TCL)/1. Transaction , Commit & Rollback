Transaction Control Languange



Alter session set autocommit = False; -- we need to set this at first  because
-- sql commit is set to True, unless set to false
-- commit means : every time I d o something it gets reflected, 
-- for example created a table, inserted , it gets reflected then and there. 
-- when autocommit => false => it doesnt reflects till it is commited. 


-- Starting Transaction: 
begin name transaction_dummy2;


show transactions;  -- shows currently running; 

select currenT_transaction();
-- let's create a table:

create or replace table new_employees1(employee_id number,
                     empl_join_date date,
                     dept varchar(10),
                     salary number,
                     manager_id number); -- create stemens are not included in transactions 
                     -- only insert update and delete are 
                     

insert into new_employees1 values(8,'2014-10-01','HR',40000,4),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (3,'2018-09-01','Marketing',30000,5),
                                 (4,'2017-09-01','HR',10000,5),
                                 (25,'2019-09-01','HR',35000,9),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (86,'2015-09-01','Tech',90000,4),
                                 (73,'2016-09-01','Marketing',20000,1);
                                 
-- check in the database zero rows would be reflecting in the table

-- but , if you do select statement it would reflect; 

select * from new_employees1; -- this shows the entry

-- beecause you havent yet commited , you have started the transaction whicch is running
-- but have not yet made the commit; 

commit; 

-- now check rows would have been updated


begin name transaction_dummy4;

create or replace table new_employees2(employee_id number,
                     empl_join_date date,
                     dept varchar(10),
                     salary number,
                     manager_id number);


insert into new_employees2 values(8,'2014-10-01','HR',40000,4),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (3,'2018-09-01','Marketing',30000,5),
                                 (4,'2017-09-01','HR',10000,5),
                                 (25,'2019-09-01','HR',35000,9),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (86,'2015-09-01','Tech',90000,4),
                                 (73,'2016-09-01','Marketing',20000,1);


select * from new_employees2; -- 8 rows

-- mark records inserted but not yet reflecting in the original Table. 
-- if we commit it gets refelected.

-- bu now, let's say we want to retreat the steps. 

rollback; 

select * from new_employees2; -- 0 rows

-- reinserting
insert into new_employees2 values(8,'2014-10-01','HR',40000,4),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (3,'2018-09-01','Marketing',30000,5),
                                 (4,'2017-09-01','HR',10000,5),
                                 (25,'2019-09-01','HR',35000,9),
                                 (12,'2014-09-01','Tech',50000,9),
                                 (86,'2015-09-01','Tech',90000,4),
                                 (73,'2016-09-01','Marketing',20000,1);

select * from new_employees2; -- 8 rows but not yet reflected

commit; 

show transactions; -- currently no transactions; because created and also commited. 



