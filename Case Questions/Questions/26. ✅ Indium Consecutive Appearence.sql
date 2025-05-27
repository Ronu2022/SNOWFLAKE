
CREATE OR REPLACE TABLE LOGS
(
    id INT IDENTITY(1,1),
    num Int
);

insert into logs(num)
values
(1),
(1),
(1),
(2),
(2),
(2),
(1),
(2),
(2);


select * from logs;

// Find all numbers that appear at least 3 times consecutively;


with grouping_cte as
(
    select 
        id, num,
        row_number() over(order by id asc) as rn,
        row_number() over(partition by num order by id asc) as par,
         row_number() over(order by id asc) - row_number() over(partition by num order by id asc) as grouping_tag
    from logs order by id asc
),
consecutive_counts_cte as
(
    select 
    num, grouping_tag,
    count(*) as streak_length from grouping_cte
    group by num, grouping_tag
)

select num from consecutive_counts_cte where streak_length >= 3;
