/* Return the Records which has more than 3 Consecutive rows where people_count > 100 */

CREATE OR REPLACE TABLE hospital_visits
(
    visit_date DATE,
    people_count INT
);

INSERT INTO hospital_visits (visit_date, people_count) VALUES
('2024-01-01',  98),
('2024-01-02', 102),
('2024-01-03', 110),
('2024-01-04', 125),
('2024-01-05', 101),
('2024-01-06', 105),
('2024-01-07',  99),
('2024-01-08', 107),
('2024-01-09', 108),
('2024-01-10', 120),
('2024-01-11', 121),
('2024-01-12', 122),
('2024-01-13',  80),
('2024-01-14',  90),
('2024-01-15',  100),
('2024-01-16',  101),
('2024-01-17',  90);

select * from hospital_visits;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
// This gives the desired date range
-----------------------------------------------------------------------------------------------------------------------------------------------------------------
with row_number_cte as
(
select
visit_date,
people_count,
datediff('day','2024-01-01',visit_date) as day_diff,
row_number() over(order by visit_date asc) as rn

from hospital_visits where people_count >= 100
),
flag as
(
    select *,
    day_diff - RN as category_types
    from row_number_cte
), 
grouped_flags as
(
    select category_types,
        count(*) as no_of_consecutive_days
    from flag 
    group by category_types
    having count(*) > 3
)
,
master_join_cte AS
(
    select 
    f.visit_date,
    f.people_count,
    f.day_diff,
    f.rn,
    f.category_types
    from flag as f
    left join grouped_flags as g
    on f.category_types = g.category_types
) 
select 
date_range,no_of_days
from 
(
    SELECT 
    CATEGORY_TYPES,
    min(visit_date),
    max(visit_date),
    datediff('day', min(visit_date),max(visit_date))+ 1 as no_of_days,
    CONCAT(min(visit_date),' - ', max(visit_date)) as date_range
    from master_join_cte
    group by category_types
);

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
-- IF you wish to just display the records
-----------------------------------------------------------------------------------------------------------------------------------------------------------------


WITH visits_with_flag AS (
    SELECT 
        visit_date, 
        people_count, 
        CASE WHEN people_count >= 100 THEN 1 ELSE 0 END AS flag
    FROM hospital_visits
),

grouped_visits AS (
    SELECT 
        visit_date,
        people_count,
        flag,
         ROW_NUMBER() OVER (ORDER BY visit_date),
        ROW_NUMBER() OVER (ORDER BY visit_date) 
        - SUM(flag) OVER (ORDER BY visit_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM visits_with_flag
) select * from grouped_visits
,

filtered_groups AS (
    SELECT 
        grp, 
        COUNT(*) AS streak_length
    FROM grouped_visits
    WHERE flag = 1
    GROUP BY grp
    HAVING COUNT(*) >= 3
) select * from filtered_groups

SELECT 
    gv.visit_date, 
    gv.people_count
FROM grouped_visits gv
JOIN filtered_groups fg ON gv.grp = fg.grp
WHERE gv.flag = 1
ORDER BY gv.visit_date;
