CREATE OR REPLACE TABLE HORSES
(
    RECORD_id int,
    HORSE_NAME VARCHAR(20)
)CLUSTER BY (RECORD_ID);
INSERT INTO HORSES(RECORD_ID,HORSE_NAME) VALUES
(1,'Sally'),
(2,'Elmer'),
(3, 'Sally'),
(4, 'Mr.Ed');

select * from HORSES;

// Get the possible competitors for each horse. 

select 
    t1.record_id,
    t1.horse_name as defender_horse_name,
    t2.horse_name as oppenent_horse_name,
    t2.record_id as oppoent_horse_record_id
from horses as t1
left join horses as t2
on t1.record_id <> t2.record_id;


// Get the possible positions:
-- for instance, first : sally, second: elmer, third: Mr.Ed, fourth: Sally 
-- Gett all possible combinations.

-- Way 1: Self Join (LEFT)
select t1.horse_name as first_position,
t2.horse_name as seccond_position,
t3.horse_name as third_position,
t4.horse_name as fourth_position

from Horses as t1 
left join horses as t2 on t1.record_id <> t2.record_id
left join horses as t3 on t1.record_id <> t3.record_id and t2.record_id <> t3.record_id
left join horses as t4 on t1.record_id <> t4.record_id and t2.record_id <> t4.record_id and t3.record_id <> t4.record_id;


-- Way2: CROSS JOIN

select t1.horse_name as first_position,
t2.horse_name as seccond_position,
t3.horse_name as third_position,
t4.horse_name as fourth_position

from horses as t1
join horses as t2 on t1.record_id != t2.record_id
join horses as t3 on t3.record_id not in (t1.record_id, t2.record_id)
join horses as t4 on t4.record_id not in (t1.record_id,t2.record_id,t3.record_id);
