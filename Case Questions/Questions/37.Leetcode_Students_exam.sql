/* GET THE STUDENTS WHO HAVE GIVEN ATELAST 1 EXAM AND THEIR SCORE IS NEITHER THE LOWEST SCORE WITHIN THAT PARTICUALAR EXAM NOR THE HIGHEST.*/

// Students Table:
  
create or replace table students
(
student_id int,
student_name varchar(20)
);


insert into students values
(1,'Daniel'),(2,'Jade'),(3,'Stella'),(4,'Jonathan'),(5,'Will');


// EXAMS TABLE:
create or replace table exams
(
exam_id int,
student_id int,
score int
);

insert into exams values
(10,1,70),(10,2,80),(10,3,90),(20,1,80),(30,1,70),(30,3,80),(30,4,90),(40,1,60)
,(40,2,70),(40,4,80);


select * from exams;

with cte as
(
    select * from students where student_id in (select distinct student_id from exams)
),
min_max_score_cte as
(
    select *,
    min(score) over (partition by exam_id ) as min_score,-- 60
    max(score) over(partition by exam_id) as max_score -- 90
    from exams

) 
select
t1.student_id,
t2.student_name
from
(   select student_id,
    sum(case when score = min_score then 1 else 0 end) as min_score_flag_count,
    sum(case when score = max_score then 1 else 0 end) as max_score_flag_count
    from min_max_score_cte
    group by student_id
    having min_score_flag_count = 0 and max_score_flag_count = 0
) as t1
left join cte as t2 on t1.student_id = t2.student_id
order by student_id asc;
