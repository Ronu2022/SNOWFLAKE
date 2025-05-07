/*
Where we need to find second most recent activity and if user has only 1 activoty then return that as it is. 
We will use SQL window functions to solve this problem.*/



create or replace table UserActivity
(
username      varchar(20) ,
activity      varchar(20),
startDate     Date   ,
endDate      Date
);

insert into UserActivity values 
('Alice','Travel','2020-02-12','2020-02-20')
,('Alice','Dancing','2020-02-21','2020-02-23')
,('Alice','Travel','2020-02-24','2020-02-28')
,('Bob','Travel','2020-02-11','2020-02-18')
,('Bob','Travel','2020-02-20','2020-02-24')
,('Lynn','Travel','2020-02-18','2020-02-20')
,('Srijoy','Travel','2020-02-27','2020-03-02');


Select * from UserActivity;

// CODE:
    
with cte as
(
    Select 
        username,
        count(*) as total_records
    from UserActivity
    GROUP BY username
),
dense_rank_cte as
(
SELECT 
    ua.username,
    ua.activity,
    ua.startDate,
    ua.ENDDATE,
    cte.total_records,
    dense_rank() OVER (PARTITION BY ua.USERNAME ORDER BY ua.ENDDATE DESC) as dr
    
FROM UserActivity as ua
LEFT JOIN cte ON ua.username = cte.username
) SELECT username,activity,startDate,ENDDATE
FROM dense_rank_cte
WHERE ((total_records > 1) AND dr = 2)
OR ((total_records = 1) AND  dr = 1 );
