/* 
Prime Subsription Rate by Product Action
Given the following two Tables,
return the fraction of Users, rounded to 2 decimal Places,
who accessed Amazon Music and upgraded to prime membership within 30 days of Signing up
*/




CREATE OR REPLACE TABLE users
(
user_id INTEGER,
name VARCHAR(20),
join_date DATE
);

INSERT INTO  USERS VALUES
(1, 'Jon', TO_DATE('2-14-2020','mm-dd-yyyy')), 
(2, 'Jane', TO_DATE('2-14-2020', 'mm-dd-yyyy')), 
(3, 'Jill', TO_DATE('2-15-2020' ,'mm-dd-yyyy')), 
(4, 'Josh', TO_DATE('2-15-2020' ,'mm-dd-yyyy')), 
(5, 'Jean', TO_DATE('2-16-2020' ,'mm-dd-yyyy')), 
(6, 'Justin', TO_DATE('2-17-2020' ,'mm-dd-yyyy')),
(7, 'Jeremy', TO_DATE('2-18-2020', 'mm-dd-yyyy'));


SELECT * FROM users;


CREATE OR REPLACE TABLE events
(
    user_id INTEGER,
    type VARCHAR(10),
    access_date DATE
);

INSERT  INTO events VALUES
(1, 'Pay', TO_DATE('3-1-2020' ,'mm-dd-yyyy')), 
(2, 'Music', TO_DATE('3-2-2020' ,'mm-dd-yyyy')), 
(2, 'P', TO_DATE('3-12-2020', 'mm-dd-yyyy')),
(3, 'Music', TO_DATE('3-15-2020' ,'mm-dd-yyyy')), 
(4, 'Music', TO_DATE('3-15-2020', 'mm-dd-yyyy')), 
(1, 'P', TO_DATE('3-16-2020','mm-dd-yyyy')), 
(3, 'P', TO_DATE('3-22-2020', 'mm-dd-yyyy'));


Select * from users;
SELECT * FROM events;



/* WITH CTE : LONG*/

WITH cte AS
(
    SELECT 
        u.user_id,
        u.name,
        e.type,
        u.join_date AS signing_date,
        e.access_date,
        DATEDIFF('DAY',u.join_date,e.access_date) AS date_diff
    FROM users u
    LEFT JOIN events e ON u.user_id = e.user_id
    ORDER BY user_id ,type
),
distinct_user_cte AS
(
    SELECT COUNT(DISTINCT user_id) AS distinct_user_count
    FROM cte
),
music_subscribed_user AS
(
    SELECT * FROM CTE WHERE type = 'Music'
),
premium_subsribed_user AS
(
    SELECT user_id,name, access_date AS premium_taken
    FROM CTE WHERE Type = 'P'
),
music_and_premium_join AS
(
    SELECT 
        m.user_id,
        m.name,
        m.type,
        m.signing_date,
        p.premium_taken,
        DATEDIFF('DAY',signing_date,premium_taken) as days_taken_for_premium
    FROM music_subscribed_user m
    LEFT JOIN premium_subsribed_user p
    ON m.user_id = p.user_id        
        
),
count_music_premium AS
(
    SELECT 
        COUNT(CASE WHEN type = 'Music' AND signing_date IS NOT NULL AND premium_taken IS NOT NULL 
             AND days_taken_for_premium <=30 THEN 1 END) as music_premium_subscription_count FROM music_and_premium_join
),

fraction_count AS
(
    SELECT ROUND(music_premium_subscription_count/NULLIF(distinct_user_count,0),2) AS fraction
    FROM count_music_premium, distinct_user_cte
    
) SELECT * FROM fraction_count;


/*Without CTE, note here we have divided by the distinct count of users who have music not the total distinct user count*/


SELECT COUNT(DISTINCT user_id) as total_users,
       COUNT(CASE WHEN days_for_subscription < 30 THEN 1 END) AS users_count_within_30_days_subscription,
       ROUND((COUNT(CASE WHEN days_for_subscription < 30 THEN 1 END)/COUNT(DISTINCT user_id)),2) AS fraction_count
FROM
(   
    SELECT COALESCE(u.user_id,p.user_id) AS user_id, 
           join_date AS signup_date,
           p.access_date AS premium_date,
           DATEDIFF('day',u.join_date,p.access_date) AS days_for_subscription
           FROM users  AS u
           FULL OUTER  JOIN (SELECT * FROM events WHERE type = 'P') AS p
           ON u.user_id = p.user_id
    WHERE u.user_id IN (SELECT user_id FROM events WHERE type = 'Music')
) AS alias_name;
