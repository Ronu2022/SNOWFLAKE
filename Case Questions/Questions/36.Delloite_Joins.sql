/* fill the null values with last not null values */


create table brands 
(
category varchar(20),
brand_name varchar(20)
);
insert into brands values
('chocolates','5-star')
,(null,'dairy milk')
,(null,'perk')
,(null,'eclair')
,('Biscuits','britannia')
,(null,'good day')
,(null,'boost');

select * from brands;

with cte_a as
(
    select *,
    row_number() over (order by (select null)) as rm
    from brands
),
cte_b as
(
    select *,
    lead(rm,1,999) over(order by (select null)) as n_rn
    from cte_a where category is not null
)
select
coalesce(t1.category,t2.category) as category,
t1.brand_name
from cte_a as t1
left join cte_b as t2 on  t1.rm >= t2.rm and t1.rm < t2.n_rn;
