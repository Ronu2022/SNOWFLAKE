Create or replace table activity_table
(
    user_id INTEGER,
    actividy_date DATE,
    activity vARCHAR
);


INSERT INTO activity_table  (user_id,actividy_date,activity) 
VALUES 
(1, '2020-01-15','login'),
(1, '2020-01-16','search'),
(1,'2020-01-17','logout'),
(1, '2020-01-18','login'),
(2, '2020-01-09','login'),
(2, '2020-01-10','logout'),
(2, '2020-01-11','search'),
(2, '2020-01-13','login');


Select  * FROm  activity_table;

-- COde:

// FIND THE USER_ID WHO WAS ACTIVE FOR 4 CONSECUTIVE DAYS WITHOUT A BREAK IN BETWEEN 
WITH numbered AS 
(
    SELECT 
        user_id,
        actividy_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY actividy_date) AS rn
    FROM activity_table
),
grouped  AS
(
SELECT 
        user_id,
        actividy_date,
        rn,
        DATEADD(DAY, -rn, actividy_date) AS grp_date
    FROM numbered
),
grouped_counts  AS
(

 SELECT 
        user_id,
        grp_date,
        COUNT(*) AS consecutive_days
    FROM grouped
    GROUP BY user_id, grp_date
)
SELECT user_id
FROM grouped_counts
WHERE consecutive_days >= 4;
