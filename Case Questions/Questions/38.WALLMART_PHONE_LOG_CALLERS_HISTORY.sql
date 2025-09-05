-----------------------------------------------------------------------------------------------------------------------------------------------
/* FIND THE CALLERS FOR WHOM THE FIRST RECIPENT AND THE LAST RECIPIENT OF THE DAY REMAINS SAME */


create or replace table phonelog(
    Callerid int, 
    Recipientid int,
    Datecalled datetime
);

insert into phonelog(Callerid, Recipientid, Datecalled)
values(1, 2, '2019-01-01 09:00:00.000'),
       (1, 3, '2019-01-01 17:00:00.000'),
       (1, 4, '2019-01-01 23:00:00.000'),
       (2, 5, '2019-07-05 09:00:00.000'),
       (2, 3, '2019-07-05 17:00:00.000'),
       (2, 3, '2019-07-05 17:20:00.000'),
       (2, 5, '2019-07-05 23:00:00.000'),
       (2, 3, '2019-08-01 09:00:00.000'),
       (2, 3, '2019-08-01 17:00:00.000'),
       (2, 5, '2019-08-01 19:30:00.000'),
       (2, 4, '2019-08-02 09:00:00.000'),
       (2, 5, '2019-08-02 10:00:00.000'),
       (2, 5, '2019-08-02 10:45:00.000'),
       (2, 4, '2019-08-02 11:00:00.000');


select * from phonelog;


// way 1: windows method:
with cte as
(
    select *,
    date(date_trunc(day,datecalled)) as date_of_call
    from phonelog
    order by callerID asc ,datecalled asc
    
),
first_and_last_recipient_Cte as
(
select callerid,date_of_call,
first_value(recipientid) over (partition by callerID,date_of_call order by DATECALLED asc) as first_recipient,
last_value(recipientid) over (partition by callerID,date_of_call order by DATECALLED asc) as last_recipient
from cte
) select  DISTINCT Callerid,date_of_call,first_recipient as recipient from first_and_last_recipient_Cte
where first_recipient = last_recipient;



// way 2: self join

with cte as
(
    select *,
    date(date_trunc(day,datecalled)) as date_of_call
    from phonelog
    order by callerID asc ,datecalled asc
    
),
cte_b as
(

select callerid, date_of_call,
min(datecalled) as earliest_calling_time,
max(datecalled) as latest_calling_time
from cte
group by callerid, date_of_call
)

select t1.callerid,
t1.date_of_call,
t2.recipientid as first_recipient,
t3.recipientid as last_recipient
from cte_b as t1
left join cte as t2 on t1.callerID   = t2.callerID and t1.earliest_calling_time = t2.datecalled
left join cte as t3 on t1.callerID  = t3.callerID and t1.latest_calling_time = t3.datecalled
where t2.recipientid = t3.recipientid;
