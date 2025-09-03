/* FIND THE FIRST NAME, MIDDLE NAME, LAST NAME OF A GIVEN NAME */


create or replace table customers
(customer_name varchar(30));



insert into customers values ('Ankit Bansal')
,('Vishal Pratap Singh')
,('Michael'),
('Arun Yogiraj Rath Sharma'),
('Anand Ranganathan N K B');


select * from customers;

with cte_a as
(
select customer_name,
length(customer_name),
length(customer_name) - length(replace(customer_name,' ',''))+1 as  word_count
from customers
)
select customer_name,
case when
    word_count >= 1 then split_part(customer_name,' ',1)
    end as first_name,
case
    when word_count = 1 then null
    when word_count = 2 then null
    when word_count >= 3  then split_part(customer_name,' ',2) 
    end as mid_name,
case 
    when word_count = 1 then null
    when word_count = 2 then split_part(customer_name,' ',2) 
    when word_count = 3 then split_part(customer_name,' ',3) 
    when word_count > 3 then replace(customer_name, concat(split_part(customer_name,' ',1) || ' ' || split_part(customer_name,' ',2) || ' ' ,''))
    end as last_name

from cte_a;
