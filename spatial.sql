-- COMP9311 17s1 Project 2
--
-- Section 2 Template

--------------------------------------------------------------------------------
-- Q4
--------------------------------------------------------------------------------

drop function if exists skyline_naive(text) cascade;

-- This function calculates skyline in O(n^2)
create or replace function skyline_naive(dataset text) 
    returns integer
as $$
declare
    total integer;
begin
    execute
        '
        create or replace view '||dataset||'_skyline_naive(x, y) as
	    select * from '||dataset||' a
	    where not exists(
		select 1 from '||dataset||' b
	        where (b.x >= a.x and b.y > a.y) or (b.x > a.x and b.y >= a.y))
	    order by a.x asc, a.y desc
	';

    execute
        '
	select count(*) from '||dataset||'_skyline_naive
	' into total;

    return total;
end;
$$ language plpgsql;


--------------------------------------------------------------------------------
-- Q5
--------------------------------------------------------------------------------

drop function if exists skyline(text) cascade;

-- This function simply creates a view to store skyline

create or replace function get_row(dataset text)
    returns table(cur_x integer, cur_y integer)
as $$
declare
    emp record;
    pre_x integer;
begin
    
    execute
    '
    create or replace view temp(x, y) as
    select * from '||dataset||' order by y desc, x desc
    ';
    
    select min(x) - 1 into pre_x from temp;

    for emp in select * from temp
    loop
        if emp.x > pre_x then
	    pre_x := emp.x;
            cur_x := emp.x;
            cur_y := emp.y;
            return next;
	end if;
    end loop;

end;
$$ language plpgsql;


create or replace function skyline(dataset text) 
    returns integer
as $$
declare
    total integer;
begin
    execute
        '
	create or replace view '||dataset||'_skyline(x, y) as
	    select * from get_row('''||dataset||''')
	';

    execute
        '
	select count(*) from '||dataset||'_skyline
	' into total;

    return total;

end;
$$ language plpgsql;


--------------------------------------------------------------------------------
-- Q6
--------------------------------------------------------------------------------

drop function if exists skyband_naive(text) cascade;

-- This function calculates skyband in O(n^2)
create or replace function skyband_naive(dataset text, k integer) 
    returns integer 
as $$
declare
    total integer;
begin
    execute
        '
        create or replace view '||dataset||'_skyband_naive(x, y) as
	    select * from '||dataset||' a
	    where (
		select count(*) from '||dataset||' b
	        where (b.x >= a.x and b.y > a.y) or (b.x > a.x and b.y >= a.y)) < '||k||'
	    order by a.x asc, a.y desc
	';

    execute
        '
	select count(*) from '||dataset||'_skyband_naive
	' into total;

    return total;
end;
$$ language plpgsql;


--------------------------------------------------------------------------------
-- Q7
--------------------------------------------------------------------------------

drop function if exists skyband(text, integer) cascade;

-- This function simply creates a view to store skyband

create or replace function skyband(dataset text, k integer) 
    returns setof bigint
as $$
declare
    n integer;
    total integer;
    emp record;
    pre_x integer;
begin

    drop table if exists container cascade;
    create table container(x integer, y integer);

    execute
    '
    create or replace view temp_temp(x, y) as
    select * from '||dataset||' order by y desc, x desc 
    ';

    for n in 1..k
    loop
	select min(x) - 1 into pre_x from temp_temp;
        for emp in (select * from temp_temp except select * from container order by y desc, x desc)
        loop
            if emp.x > pre_x then
	        pre_x := emp.x;
                insert into container(x, y) values (emp.x, emp.y);
     	    end if;
        end loop;
    end loop;
    
    for emp in (select * from container)
    loop
	if (select count(*) from container where (x >= emp.x and y > emp.y) or (x > emp.x and y >= emp.y)) >= k then
	    delete from container where x = emp.x and y = emp.y;
	end if;
    end loop;

    execute
    '
    create or replace view '||dataset||'_skyband(x, y) as
        select * from container
    ';

    return query select count(*) from container;
end;
$$ language plpgsql;
