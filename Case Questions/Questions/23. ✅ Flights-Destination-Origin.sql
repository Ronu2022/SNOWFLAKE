
CREATE OR REPLACE TABLE Flights
(
    cust_id INT,
    flight_id VARCHAR(40),
    origin VARchar(40),
    DESTINATION VARCHAR(40)
) CLUSTER BY (cust_id);


INSERT INTO Flights(cust_id,flight_id,origin,destination)
values
(1,'SG1234','Delhi','Hyderabad'),
(1,'SG3476','Kochi','Mangalore'),
(1,'69876','Hyderabad','Kochi'),
(2,'68749','Mumbai','Varanasi'),
(2,'SG5723','Varanasi','Delhi');

select * from Flights Order by cust_id asc; 




with origins_cte as 
(
    SELECT cust_id, origin AS location FROM Flights -- this just  gives us the cites at origin
),
destination_cte as
(
    SELECT cust_id, destination AS location FROM Flights -- this jist gives us the cities at destination
),
starting_point_cte as
(
    select o.cust_id,o.location as start_origin, d.location as dest
    from origins_cte as o
    left join destination_cte as d on o.cust_id  = d.cust_id and o.location = d.location
    where d.location is NULL -- if we do left join with the destination we will find for city that's not present in desstination
    -- will have to be the origin city because other cities since connecting will be present in both origin and destination
),
end_point_cte as
(
    select d.cust_id, d.location as end_location, o.location as origin
    from destination_cte as d
    left join origins_cte as o on d.cust_id = o.cust_id and o.location = d.location
    where o.location is null -- same goes if a flight present in destination but not in origin , then clearly that's the destination city.
)

select sp.cust_id, sp.start_origin as origin, ep.end_location as destination
from starting_point_cte as sp 
left join end_point_cte as ep  on sp.cust_id = ep.cust_id;
