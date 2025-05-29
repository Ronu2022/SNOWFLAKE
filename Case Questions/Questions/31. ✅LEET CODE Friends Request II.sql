/*
Table: RequestAccepted

+----------------+---------+
| Column Name    | Type    |
+----------------+---------+
| requester_id   | int     |
| accepter_id    | int     |
| accept_date    | date    |
+----------------+---------+
(requester_id, accepter_id) is the primary key (combination of columns with unique values) for this table.
This table contains the ID of the user who sent the request, the ID of the user who received the request, and the date when the request was accepted.
 

Write a solution to find the people who have the most friends and the most friends number.

The test cases are generated so that only one person has the most friends.

The result format is in the following example.

 

Example 1:

Input: 
RequestAccepted table:
+--------------+-------------+-------------+
| requester_id | accepter_id | accept_date |
+--------------+-------------+-------------+
| 1            | 2           | 2016/06/03  |
| 1            | 3           | 2016/06/08  |
| 2            | 3           | 2016/06/08  |
| 3            | 4           | 2016/06/09  |
+--------------+-------------+-------------+
Output: 
+----+-----+
| id | num |
+----+-----+
| 3  | 3   |
+----+-----+
Explanation: 
The person with id 3 is a friend of people 1, 2, and 4, so he has three friends in total, which is the most number than any others.*/


CREATE OR REPLACE TABLE RequestAccepted (
    requester_id INT,
    accepter_id INT,
    accept_date DATE,
    PRIMARY KEY (requester_id, accepter_id)
);

INSERT INTO RequestAccepted (requester_id, accepter_id, accept_date) VALUES
    (1, 2, '2016-06-03'),
    (1, 3, '2016-06-08'),
    (2, 3, '2016-06-08'),
    (3, 4, '2016-06-09');

Select * from RequestAccepted;


-- Initailly did a very long Code:

with requester_id_cte as
(
    select 
        requester_id as sender_request_id,
        count(distinct accepter_id) as firend_count_a
        from RequestAccepted
        where accepter_id is not null and accept_date is not null
        group by requester_id
        
) ,
accepter_id_cte as 
(
    select
        accepter_id,
        count(distinct requester_id) as friend_count_b
        from RequestAccepted
        where accepter_id is not null and accept_date is not null
    group by accepter_id
) ,

all_ids as
(
    select distinct sender_request_id as id from requester_id_cte
    union
    select distinct accepter_id  as id from accepter_id_cte
)

select
  m.id as id,
  --r.firend_count_a,
  --coalesce(a.friend_count_b,0) as friend_count_b,
  Coalesce(r.firend_count_a,0) +  coalesce(a.friend_count_b,0) as num
  from all_ids as m
left join  requester_id_cte as r on m.id = r.sender_request_id
left join accepter_id_cte as a on m.id = a.accepter_id;



-- A very easy one but common sense was missing.

