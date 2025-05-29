CREATE OR REPLACE TABLE Insurance (
    pid INT PRIMARY KEY,
    tiv_2015 FLOAT,
    tiv_2016 FLOAT,
    lat FLOAT,
    lon FLOAT
);



INSERT INTO Insurance (pid, tiv_2015, tiv_2016, lat, lon) VALUES
    (1, 10, 5, 10, 10),
    (2, 20, 20, 20, 20),
    (3, 10, 30, 20, 20),
    (4, 10, 40, 40, 40);



/* +-------------+-------+
| Column Name | Type  |
+-------------+-------+
| pid         | int   |
| tiv_2015    | float |
| tiv_2016    | float |
| lat         | float |
| lon         | float |
+-------------+-------+
pid is the primary key (column with unique values) for this table.
Each row of this table contains information about one policy where:
pid is the policyholder's policy ID.
tiv_2015 is the total investment value in 2015 and tiv_2016 is the total investment value in 2016.
lat is the latitude of the policy holder's city. It's guaranteed that lat is not NULL.
lon is the longitude of the policy holder's city. It's guaranteed that lon is not NULL.
*/


/* Write a solution to report the sum of all total investment values in 2016 tiv_2016, for all policyholders who:

-- have the same tiv_2015 value as one or more other policyholders, 
-- and are not located in the same city as any other policyholder (i.e., the (lat, lon) attribute pairs must be unique).
Round tiv_2016 to two decimal places. 
*/

-- what does this mean:
-- condition 1 I am looking for those records where tiv_2015 are repeated i.e the count of policies with same tiv_2015 > 1
    -- for instance tiv_2015 of 10 is repeated 3 times, thus, those records are to be considered.
-- condition 2 and are not located in the same city as any other policyholder:
    -- lat and lon must have unique pairs.
    -- for instance 20,20 is repeated taht means people can't be from the same area.


-- Way 1: 

with tiv_2015_repeat_cte as
(
    select tiv_2015, count(distinct pid) as count_no
    From Insurance 
    group by  tiv_2015 
    having count(*) > 1
    ORDER BY tiv_2015  ASC
),
distinct_lat_log as
(
    select lat,lon, count(*) as count_no
    from Insurance
    group by lat,lon
    having count(*) = 1
),
final_join as
(
    select i.pid,i.tiv_2015,i.tiv_2016,i.lat,i.lon
    from Insurance as i
    join tiv_2015_repeat_cte on i.tiv_2015  = tiv_2015_repeat_cte.tiv_2015
    join distinct_lat_log on i.lat  = distinct_lat_log.lat and i.lon = distinct_lat_log.lon
    
) select round(sum(tiv_2016),2) as tiv_2016  from final_join;


-- way 2 (using not exists)

select round(sum(tiv_2016),2) as tiv_2016 from 
Insurance i
where tiv_2015 in
    (
        select tiv_2015
        from Insurance
        group by tiv_2015
        having COUNT(*) > 1
    )
and not exists
 (
    select 1 from Insurance i2
    where i.lat = i2.lat and i.lon = i2.lon and i.pid <> i2.pid
 );

