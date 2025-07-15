CREATE OR REPLACE DATABASE proc_flow_control_db;
CREATE OR REPLACE SCHEMA CLASS;

/* FLOW  CONTROL STATEMENTS 

--branching constructs --.> used to make decisions based on conditions.
-- snowflake scripting -> supports 2 types of branching constructs.
-- 1. if statement --> 2. case statement.
    -- IF statement:
            to evaluate single condition
            IF <Condition> THEN
                <Statement to eb executed>
            END IF;
    -- WHAT IF is False:
            IF <Condition> THEN
                <statement to eb executed>
            ELSE
                <Statement to be executed>
            END IF;
- 2. CASE STATEMENT:
    - goes through set of conditions and returns and returns the value where the condition is true for the first time.
    - if all conditions are false returns the value in teh else part.
    CASE
        WHEN <condition> THEN <value or expression>
        WHEN <condition> THEN <value or expression>
        WHEN <condition> THEN <value or expression>
        ELSE 
            <value or expression>
        END;
*/
        
----------------------------------------------------------------------------------------------------------------------------------------
// EXAMPLES:

-- PROCEDURE TO FIND IF A GIVEN NUMBER IS EVEN OR ODD:

create or replace procedure proc_name_even_odd_check(a int)
returns varchar
language sql as
$$
begin
    declare b varchar;
    begin
        if (:a%2 = 0) THEN 
        b := ':a is EVEN.';
        elseif (:a%2 <> 0) THEN
        b := ':a is ODD.';
        END IF;
        RETURN b;
    END;
END;
$$;

CALL proc_name_even_odd_check(3); --- check the o.p => :a is ODD or :a is even  thats coming , need to change

-- correct:
create or replace procedure proc_name_even_odd_check(a int)
returns varchar
language sql as

begin
    declare b varchar;
    begin
        if (:a%2 = 0) THEN 
        set b := cast(:a as int) || ' is EVEN.';
        elseif (:a%2 <> 0) THEN
        set b := cast(:a as int) || ' is ODD.';
        END IF;
        RETURN b;
    END;
END;

call proc_name_even_odd_check(4);


-- FIND THE TAX AMOUNT FOR A Yearly  SALARY:



// SImple:
create or replace procedure total_tax(a float)
returns float
language sql as
$$
declare b float default 0.0;
begin
    set b := a - (a * 0.15);
    return b; 
end;
$$;


call total_tax(2400000);

// Using Case:
create or replace procedure total_tax(a float)
returns float
language sql as
$$
declare b float default 0.0;
begin
b:= case when a < 400000 then (a - (0))
         when (a between 400001 and 800000) then (a - (0.15 * a))
         else (a - (0.24 * a))
    end;

return b; 
end;
$$;

call total_tax(2400000);




