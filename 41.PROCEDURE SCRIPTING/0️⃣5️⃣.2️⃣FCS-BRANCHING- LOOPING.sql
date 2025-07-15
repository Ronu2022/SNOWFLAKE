USE DATABASE proc_flow_control_db;
USE SCHEMA CLASS;

/*
    FOR LOOP
    WHILE LOOP 
    REPEAT

    1. FOR LOOP:
    (i) COUNTER BASED:
        Repeats for a specifc number of times or for a each row in a cursor or result set.
        COUNTER BASED - Repeats for a specif number of times:
            FOR Counter in start_value to end_value
            DO
              <Statement to be executed>
            end for; 

    (ii) CURSOR BASED:
         for row_var in cursor_name
         do 
            <statement>
        end; 

   2.  WHILE LOOP:
        -- executes until the  condition is TRUE post meeting of the first condition it comes out of the loop.
        WHILE (Conditions)
        DO
          <Statement>
        END;

   3. REPEAT:
        -- opposite of the While Loop:
        -- Executes until first False
        REPEAT
            <statements to be executed>
        UNTIL (CONDITIONS)
        END REPEAT;


*/

execUTE IMMEDIATE
$$
DECLARE I INTEGER;
DECLARE P VARCHAR;
DECLARE s VARCHAR DEFAULT '*';
begin
FOR I IN 1 AND 5
DO
    p := 2 * :s;
    end for;

return p;
end;
$$;

-- Set a session variable if you still want one (not used below)
-- SET monthly_sal = 70000;

EXECUTE IMMEDIATE 
$$
DECLARE
    i INTEGER;
    j INTEGER;
    pattern VARCHAR default '';
BEGIN
    FOR i IN 1 TO 5 
    DO
        FOR j IN 1 TO i
        DO
            pattern := pattern || '*' || '\t';
        END FOR;
        pattern := pattern || '\n';
    END FOR;
RETURN pattern;
END;
$$

-- explnation

/*

i = 1 (iteration 1):
j = 1
pattern := pattern || '*'|| '\t'  -> *(tab)
loop ends
pattern = pattern || '\n' -> *(tab)
                             ----(line)

i = 2 (iteration 2)
j = 1- 2:
    j = 1:
       pattern = pattern || '\t' -> last pattern was 
       *(tab)
       ----(line) -> this says as it is and to it we add   '*' and tab
        so it becomes *(tab)
                      ---- (line) => upto this was previously present
                      *(tab)
    j = 2: (note we havent exited the inner loop to add new line)
    pattern = pattern || '\t' 
    last pattern 
        *(tab)
        ---- (line) 
        *(tab)
    to this we need to add '*'|| tab
    which is 
        *(tab)
        ---- (line) 
        *(tab) *(tab)
    j = 2 satisfied then exit inner loop 

    pattern = pattern || '\n'
    last pattern 
    *(tab)
    ---- (line) 
    *(tab) *(tab)

    to this we add a new line \n

    becomes
    *(tab)
    ---- (line) 
    *(tab) *(tab)
    ----- (line)

    this visually is:
    *
    ---- (line gap)
    * *
    ---- (line gap)

*/


/*// Find Prime Numbers upto a given number 
*/

                


create or replace procedure check_prime("N" integer)
returns table(prime integer)
language sql 
execute as caller as

declare
    i integer default 2;
    j integer;
    flag integer;
    prime varchar; 
    res RESULTSET;

begin
    create or replace temporary table temp_prime_numbers(prime integer);
    while (i <= N)
    do 
        flag := 0;
        for j in 2 to i - 1
        do 
        if (i % j = 0 and i <> 2) then 
            flag := 1;
            break; 
        end if;
    end for; 
    if (flag = 0) then 
        insert into  temp_prime_numbers values(:i);
        end if; 
    i := i + 1;
    end while; 

    prime := 'Select * from temp_prime_numbers';
    res := (EXECUTE IMMEDIATE :prime);

return table(res);

end;

        
  call check_prime(20);          
            

            
        
        

             
