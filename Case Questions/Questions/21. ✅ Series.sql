-- I have a table with one column and values, 1,2,3,4,5.
-- I want o/p : 1,1,2,1,2,3,1,2,3,4,1,2,3,4,5 (i.e for 1 = 1 row, for 2 = 2 rows, for 3 = 3 rows and so on):

with table_cte as 
(
    select 1 as column_a,
    union
    select 2 as column_b,
    union
    select 3 as column_c,
    union 
    select 4 as column_d,
    union 
    select 5 as column_e
) 

select t1.column_a
from table_cte as t1
join table_cte as t2 on t2.column_a <= t1.column_a;
