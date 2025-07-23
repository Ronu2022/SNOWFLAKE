create or replace database practise_db;
create or replace schema practise_sh;

CREATE OR REPLACE TABLE practise_db.practise_sh.ticket_sales 
(
  event_id INT,
  event_name STRING,
  total_price FLOAT
);

INSERT INTO practise_db.practise_sh.ticket_sales(event_id, event_name, total_price) VALUES
  (1, 'RockFest', 1200.00),
  (2, 'JazzNights', 900.00),
  (3, 'EDM Blast', 1600.00),
  (4, 'Pop Fiesta', 750.00),
  (5, 'Classical Evening', 500.00),
  (6, 'Indie Live', 800.00),
  (7, 'Fusion Beats', 1300.00),
  (8, 'HipHop Crew', 950.00),
  (9, 'Country Mix', 600.00),
  (10, 'MetalStorm', 1100.00);

select event_id, event_name, total_price, ntile(3) over(order by total_price desc) as event_rank_bucket
from practise_db.practise_sh.ticket_sales
order by total_price desc;

select event_id, event_name, total_price, ntile(3) over(order by total_price desc) as event_rank_bucket
from practise_db.practise_sh.ticket_sales
order by total_price desc;


-- ROW NUMBER:

select t.*, row_number() over (order by t.total_price desc) as rn
from practise_db.practise_sh.ticket_sales as t
order by total_price desc;

select  event_id,event_name,total_price, row_number() over (order by total_price desc) as rn
from practise_db.practise_sh.ticket_sales
order by total_price desc;

-- RANK():

select event_id,event_name,total_price,rank() over(order by total_price desc) as ra
from practise_db.practise_sh.ticket_sales
order by total_price desc;


-- DENSE_RANK():

select event_id, event_name, total_price,dense_rank() over(order by total_price desc) as d_ra
from practise_db.practise_sh.ticket_sales
order by total_price desc;

-- LEAD():

select event_id, event_name, total_price,
lead(total_price) over(order by total_price desc) as next_exp_event 
from   practise_db.practise_sh.ticket_sales
order by total_price desc;

-- LAG():
select event_id, event_name, total_price,
lag(total_price) over(order by total_price desc) as previous_exp_price 
from practise_db.practise_sh.ticket_sales
order by total_price desc;

-- FIRST_VALUE():

select event_id, event_name, total_price,
first_value(total_price) over(order by total_price desc) as highest_total_price
from practise_db.practise_sh.ticket_sales
order by total_price desc;


--  LAST_VALUE()

select event_id, event_name, total_price,
last_value(total_price) over(order by total_price desc) as min_price
from practise_db.practise_sh.ticket_sales
order by total_price desc;

select event_id, event_name, total_price,
last_value(total_price) over(order by total_price desc rows between unbounded preceding and unbounded following) as min_price
from practise_db.practise_sh.ticket_sales
order by total_price desc;


-- Running Total with SUM()

select event_id, event_name, total_price,
sum(total_price) over(order by event_id asc rows between unbounded preceding and current row) as running_sum
from practise_db.practise_sh.ticket_sales
order by event_id asc;


select event_id, event_name, total_price,
sum(total_price) over(order by event_id asc rows between unbounded preceding and current row) as running_sum
from practise_db.practise_sh.ticket_sales
order by event_id asc;

-- Range between 

    select event_id, event_name, total_price,
    sum(total_price) over(order by total_price asc range between 100  preceding and current row )  as sum_giv
    from practise_db.practise_sh.ticket_sales
    order by event_id asc;

-- lets say you want to include all those records where the date is within 2 days before the current row's date and current row
sum(total_price) over (order by date_of_order asc range between interval '2 days' preceding and current row);


-- PERCENT_RANK():

    select  event_id, event_name, total_price,
    percent_rank() over (order by total_price desc) as p_rank 
    from practise_db.practise_sh.ticket_sales
    order by event_id asc;


-- CUME_DIST():

    select event_id, event_name, total_price,
    cume_dist() over (order by total_price desc) as c_dist
    from practise_db.practise_sh.ticket_sales
    order by total_price asc;

-- percent_rank => what is my rank over others, i/e what percentage of people are above me with better yardstics
-- cume_dist  => what is the no of people either same or greater than me.

-- MOVING AVERAGE:

select event_id, event_name, 
total_price, 
avg(total_price) over(order by event_id asc rows between 2 preceding and current row) as rolling_avg
from practise_db.practise_sh.ticket_sales
    order by EVENT_ID;



-- 🔧 Create the extended table
CREATE OR REPLACE TABLE practise_db.practise_sh.ticket_sales_extended (
  event_id INT,
  event_name STRING,
  event_type STRING,
  total_price INT
);

-- 📝 Insert sample records
INSERT INTO practise_db.practise_sh.ticket_sales_extended VALUES
(1, 'RockFest', 'Rock', 1200),
(2, 'JazzNights', 'Jazz', 900),
(3, 'EDM Blast', 'EDM', 1600),
(4, 'Pop Fiesta', 'Pop', 750),
(5, 'Classical Evening', 'Classical', 500),
(6, 'Indie Live', 'Rock', 800),
(7, 'Fusion Beats', 'EDM', 1300),
(8, 'HipHop Crew', 'HipHop', 950),
(9, 'MetalStorm', 'Rock', 1100),
(10, 'Smooth Jazz', 'Jazz', 950),
(11, 'EDM Surge', 'EDM', 1400),
(12, 'Pop Reloaded', 'Pop', 850);

select event_type, event_id,event_name, total_price,
rank() over (partition by event_type order by total_price desc) as price_rank
from practise_db.practise_sh.ticket_sales_extended
order by event_type asc;


