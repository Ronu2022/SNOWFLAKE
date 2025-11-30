use role sysadmin;          -- use sysadmin role
use schema demo_db.public;  -- use default public schema
use warehouse compute_wh;   -- use compute warehouse

-- without start/increment values
create or replace sequence my_seq_object;

with cte_level_1 as 
-- add row number
(
    select 
        cust_key,
        mkt_segment,
        row_number() over (partition by mkt_segment order by acct_bal desc) as ranking,
        acct_bal 
    from 
        customer
    order by mkt_segment, ranking      
),
-- pick top 2 customers
cte_level_2 as 
(
    select mkt_segment, ranking, acct_bal
    from 
        cte_level_1
    where ranking <3
),
-- calculate avg based on top 2 entries
cte_level_3 as
(
    select mkt_segment, avg(acct_bal) as avg_acct_bal
    from cte_level_2
    group by mkt_segment
)
    select l1.mkt_segment,l1.ranking, l1.cust_key,l1.acct_bal,l3.avg_acct_bal, (l1.acct_bal - l3.avg_acct_bal) as acct_bal_gap
        from cte_level_1 l1 
        join cte_level_3 l3 on l1.mkt_segment = l3.mkt_segment
    order by l1.mkt_segment, l1.ranking;
