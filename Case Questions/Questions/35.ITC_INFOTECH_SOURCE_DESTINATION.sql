CREATE or replace TABLE city_distance
(
    distance INT,
    source VARCHAR(512),
    destination VARCHAR(512)
);

delete from city_distance;
INSERT INTO city_distance(distance, source, destination) VALUES ('100', 'New Delhi', 'Panipat');
INSERT INTO city_distance(distance, source, destination) VALUES ('200', 'Ambala', 'New Delhi');
INSERT INTO city_distance(distance, source, destination) VALUES ('150', 'Bangalore', 'Mysore');
INSERT INTO city_distance(distance, source, destination) VALUES ('150', 'Mysore', 'Bangalore');
INSERT INTO city_distance(distance, source, destination) VALUES ('250', 'Mumbai', 'Pune');
INSERT INTO city_distance(distance, source, destination) VALUES ('250', 'Pune', 'Mumbai');
INSERT INTO city_distance(distance, source, destination) VALUES ('2500', 'Chennai', 'Bhopal');
INSERT INTO city_distance(distance, source, destination) VALUES ('2500', 'Bhopal', 'Chennai');
INSERT INTO city_distance(distance, source, destination) VALUES ('60', 'Tirupati', 'Tirumala');
INSERT INTO city_distance(distance, source, destination) VALUES ('80', 'Tirumala', 'Tirupati');

select * from city_distance;


/* for question follow this link
https://www.youtube.com/watch?v=JHUlQZrviCI */




with cte_a as
(
select 
    distance,
    destination,
    source,
    least(source,destination) as city_1,
    greatest(source,destination) as city_2
from city_distance
) ,
cte_b as
(
select *,
count(*) over (partition by city_1,city_2,distance) as cnt
from cte_a
)
select * from cte_b
where cnt = 1 -- this would give those which has no duplicates
or destination > source; -- this would give only 1 row out of many simialr rows


