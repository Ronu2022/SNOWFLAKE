/* User purchase platform.
-- The table logs the spendings history of users that make purchases from an online shopping website which has a desktop 
and a mobile application.
-- Write an SQL query to find the total number of users and the total amount spent using mobile only, desktop only 
and both mobile and desktop together for each date.
*/




CREATE OR REPLACE TABLE spending 
(
user_id int,
spend_date date,
platform varchar(10),
amount int
);

INSERT INTO spending VALUES
(1,'2019-07-01','mobile',100),
(1,'2019-07-01','desktop',100),
(2,'2019-07-01','mobile',100),
(2,'2019-07-02','mobile',100),
(3,'2019-07-01','desktop',100),
(3,'2019-07-02','desktop',100);


SELECT * FROM spending;



WITH CTE AS
(
SELECT spend_date, MAX(platform) AS platform,  user_id, SUM(amount) AS amount 
FROM spending
GROUP BY spend_date, user_id HAVING COUNT(DISTINCT platform) = 1

UNION ALL

SELECT spend_date,'both'AS platform,  user_id, SUM(amount) AS amount 
FROM spending
GROUP BY spend_date, user_id HAVING COUNT(DISTINCT platform) = 2
)
 SELECT spend_date,platform, SUM(amount) AS amount, COUNT(DISTINCT user_id)
FROM CTE
GROUP BY spend_date,platform;
