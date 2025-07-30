CREATE TABLE trial_db.practise.dim_coupons (
    Coupon_Code STRING,
    Discount_Percentage STRING,
    Validity DATE,
    Usage_Count INTEGER,
    source_file STRING,
    current_flag INTEGER,
    start_date DATE,
    end_date DATE
);

INSERT INTO trial_db.practise.dim_coupons VALUES 
('C1', '10%', '2025-12-31', 100, 'file1.csv', 1, '2024-01-01', NULL),
('C2', '20%', '2025-11-30', 50,  'file1.csv', 1, '2024-01-01', NULL);

TRUNCATE TABLE trial_db.practise.dim_coupons;
SELECT * FROM trial_db.practise.dim_coupons;

CREATE TABLE trial_db.practise.stg_coupons (
    Coupon_Code STRING,
    Discount_Percentage STRING,
    Validity DATE,
    Usage_Count INTEGER,
    source_file STRING
);

INSERT INTO trial_db.practise.stg_coupons VALUES
('C1', '15%', '2025-12-31', 100, 'file2.csv'), -- Changed Discount from 10% → 15%
('C2', '20%', '2025-11-30', 50,  'file2.csv'), -- No change
('C3', '5%',  '2025-10-01', 20,  'file2.csv'); -- New coupon (not in dim)

select * from trial_db.practise.stg_coupons;

merge into trial_db.practise.dim_coupons as t
using trial_db.practise.stg_coupons as s
on t.Coupon_Code = s.Coupon_Code
and t.current_flag = 1
and
    (
        t.Discount_Percentage <> s.Discount_Percentage
        or t.Validity <> s.Validity
        or t.Usage_Count <> s.Usage_Count
        or t.source_file <> s.source_file
        
    )
when matched then 
update 
set t.current_flag = 0,
    t.end_date = current_date();

insert into trial_db.practise.dim_coupons
(
    Coupon_Code,
    Discount_Percentage,
    Validity,
    Usage_Count,
    source_file,
    current_flag,
    start_date,
    end_date
)

 select s.Coupon_Code,s.Discount_Percentage,
 s.Validity,s.Usage_Count,s.source_file,1 as current_flag,current_date() as start_date,
 null as end_date

 from trial_db.practise.stg_coupons as s
 left join trial_db.practise.dim_coupons as t
 on s.Coupon_Code = t.Coupon_Code
 and t.current_flag = 1
 where 
    t.Coupon_code is null
or
    (
        t.Discount_Percentage <> s.Discount_Percentage
        or t.Validity <> s.Validity
        or t.Usage_Count <> s.Usage_Count
        or t.source_file <> s.source_file
    );

 select * from  trial_db.practise.stg_coupons;
 select * from trial_db.practise.dim_coupons; -- all 3 included

 -- lets update some records in the source table
 update trial_db.practise.stg_coupons
 set DISCOUNT_PERCENTAGE = '15.99%' where COUPON_CODE  = 'C1';

 SELECT * FROM trial_db.practise.stg_coupons; -- updated.
 -- Lets script the PROCEDURE and Call


 CREATE OR REPLACE PROCEDURE TRIAL_DB.PROC_SCHEMA.SCD_2_impliment()
 RETURNS VARCHAR
 LANGUAGE SQL
 EXECUTE AS CALLER AS
 $$
    BEGIN
        merge into trial_db.practise.dim_coupons as t
        using trial_db.practise.stg_coupons as s
        on t.Coupon_Code = s.Coupon_Code
        and t.current_flag = 1
        and
            (
                t.Discount_Percentage <> s.Discount_Percentage
                or t.Validity <> s.Validity
                or t.Usage_Count <> s.Usage_Count
                or t.source_file <> s.source_file
                
            )
        when matched then 
        update 
        set t.current_flag = 0,
            t.end_date = current_date();
        
        insert into trial_db.practise.dim_coupons
        (
            Coupon_Code,
            Discount_Percentage,
            Validity,
            Usage_Count,
            source_file,
            current_flag,
            start_date,
            end_date
        )
        
         select s.Coupon_Code,s.Discount_Percentage,
         s.Validity,s.Usage_Count,s.source_file,1 as current_flag,current_date() as start_date,
         null as end_date
        
         from trial_db.practise.stg_coupons as s
         left join trial_db.practise.dim_coupons as t
         on s.Coupon_Code = t.Coupon_Code
         and t.current_flag = 1
         where 
            t.Coupon_code is null
        or
            (
                t.Discount_Percentage <> s.Discount_Percentage
                or t.Validity <> s.Validity
                or t.Usage_Count <> s.Usage_Count
                or t.source_file <> s.source_file
            );
    RETURN 'SCD-2 IMPLEMENTED';
    END;
$$;


CALL TRIAL_DB.PROC_SCHEMA.SCD_2_impliment();

SELECT * FROM trial_db.practise.stg_coupons;
SELECT * FROM trial_db.practise.dim_coupons;
SELECT * FROM trial_db.practise.dim_coupons WHERE COUPON_CODE = 'C1'; -- two records one old, the other new.
