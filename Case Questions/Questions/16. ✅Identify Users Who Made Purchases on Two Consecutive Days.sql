// ✅ Practice Problem 3: Identify Users Who Made Purchases on Two Consecutive Days



CREATE OR REPLACE TABLE user_activity (
    user_id INTEGER,
    activity_date DATE,
    activity_type VARCHAR
);

INSERT INTO user_activity (user_id, activity_date, activity_type) VALUES
(201, '2024-04-01', 'purchase'),
(201, '2024-04-02', 'purchase'),
(201, '2024-04-05', 'login'),
(202, '2024-04-10', 'purchase'),
(202, '2024-04-11', 'purchase'),
(202, '2024-04-13', 'purchase'),
(203, '2024-04-15', 'purchase'),
(204, '2024-04-20', 'login'),
(204, '2024-04-21', 'purchase'),
(204, '2024-04-22', 'purchase');




// ❓ Question for You:
-- Find all user_ids who performed "purchase" activities on two consecutive days.

// 📌 Requirements:

-- Only consider rows where activity_type = 'purchase'.
--- Use a window function (you can use LAG or LEAD).
-- Return: user_id, activity_date, previous_day, and a flag is_consecutive (TRUE/FALSE).




Select user_id, activity_date,previous_activity_date as previous_day,
CASE 
    when 
      (datediff(day,PREVIOUS_ACTIVITY_DATE,ACTIVITY_DATE))   =1 THEN TRUE
      ELSE FALSE END as is_consecutive FROM
(
SELECT 
    user_id,
    activity_date,
    LAG(activity_date) over (partition by user_id order by activity_date asc) As previous_activity_date,
    ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY activity_date ASC) as rn
FROM user_activity WHERE activity_type = 'purchase'
ORDER BY user_id ASC, activity_date ASC
) ORDER BY USER_ID ASC, ACTIVITY_DATE ASC;




-- Find those users who have had consecutive days purchase 



with rn_cte as
(
SELECT 
    user_id,
    activity_date,
    ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY activity_date ASC) as rn
FROM user_activity WHERE activity_type = 'purchase'
ORDER BY user_id ASC, activity_date ASC
),
diff_cte as
(
Select 
    user_id,
    activity_date,
    rn,
    DATEADD(day,-rn,activity_date) AS diff
FROM rn_cte
),
grouped_cte as
(
Select 
    user_id,
    diff,
    COUNT(*) as counting
From diff_cte group by user_id, diff
)
Select user_id from  grouped_cte WHERE counting = 2;


