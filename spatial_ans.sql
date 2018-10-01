-- COMP9311 17s1 Project 2
--
-- Section 2 Solution

drop type if exists MyPoint cascade;

-- You may or may not need this data type
create type MyPoint as (x integer, y integer);

--------------------------------------------------------------------------------
-- This is a helper function that simply checks if the specified table exists
--------------------------------------------------------------------------------

create or replace function table_exists(tname text) 
    returns boolean
as $$
declare
    status integer := 0;
begin
    select count(*) into status from pg_class
    where relname=tname and relkind='r';
    return (status = 1);
end;
$$ language plpgsql;

--------------------------------------------------------------------------------
-- Q4
--------------------------------------------------------------------------------

drop function if exists skyline_naive(text) cascade;

-- This function calculates skyline in O(n^2)
create or replace function skyline_naive(dataset text) 
    returns integer 
as $$
declare
    res integer;
begin
    if(not table_exists(dataset)) then
        return -1;
    end if;
	
    execute 'create or replace view '||dataset||'_skyline_naive(x, y)
             as
             select A.x, A.y
             from '||dataset||' A
             where ( select count(*)
                     from '||dataset||' B
                     where ( B.x > A.x and B.y >= A.y ) or ( B.x >= A.x and B.y > A.y )
                   ) = 0
             order by A.x asc, A.y desc
            ';

    execute 'select count(*) from '||dataset||'_skyline_naive' into res;
    return res;
end;
$$ language plpgsql;

--------------------------------------------------------------------------------
-- Q5
--------------------------------------------------------------------------------

drop function if exists find(text) cascade;

-- This function calculates skyline in O(nlogn)
create or replace function find(dataset text)
    returns setof MyPoint
as $$
declare
    find integer;
    p MyPoint;
    pre MyPoint;
begin
    find := 0;
	for p in execute 'select *
                      from '||dataset||'
		              order by y desc, x desc'
	loop
        if(find = 0 or p.x > pre.x) then
            pre.x := p.x;
            pre.y := p.y;
			find := 1;
            return next p;
        end if;
	end loop;
end;
$$ language plpgsql;

drop function if exists skyline(text) cascade;

-- This function simply creates a view to store skyline
create or replace function skyline(dataset text) 
    returns integer 
as $$
declare
    res integer;
begin
    if(not table_exists(dataset)) then
        return -1;
    end if;
	
    execute 'create or replace view '||dataset||'_skyline(x, y)
             as
             select *
             from find('''||dataset||''')
			 order by x asc, y desc
            ';

    execute 'select count(*) from '||dataset||'_skyline' into res;
    return res;
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
    res integer;
begin
    if(not table_exists(dataset)) then
        return -1;
    end if;
	
    execute 'create or replace view '||dataset||'_skyband_naive(x, y)
             as
             select A.x, A.y
             from '||dataset||' A
             where ( select count(*)
                     from '||dataset||' B
                     where ( B.x > A.x and B.y >= A.y ) or ( B.x >= A.x and B.y > A.y )
                   ) < '||k||'
             order by A.x asc, A.y desc
            ';

    execute 'select count(*) from '||dataset||'_skyband_naive' into res;
    return res;
end;
$$ language plpgsql;

--------------------------------------------------------------------------------
-- Q7
--------------------------------------------------------------------------------

drop function if exists find(text, integer) cascade;

-- This function calculates skyband in O(nlogn + kn + m^2)
create or replace function find(dataset text, k integer)
    returns setof MyPoint
as $$
declare
    lev integer;
    find integer;
    i integer;
    p MyPoint;
    pre MyPoint;
    n integer;
    points MyPoint[] := array[]::MyPoint[];
    lable integer[] default '{}';
    spoints MyPoint[] := array[]::MyPoint[];
    j integer;
    cnt integer;
begin	
    for p in execute 'select * from '||dataset||' order by y desc, x desc'
    loop
        points := points || p;
        lable := lable || 1;
    end loop;

    for lev in 1..k
	loop
        find := 0;
        for i in array_lower(points, 1) .. array_upper(points, 1)
        loop
            if(lable[i] = lev and (find = 0 or points[i].x > pre.x) ) then
                pre.x := points[i].x;
                pre.y := points[i].y;
                find := 1;
                spoints := spoints || points[i];
            else
                lable[i] := lable[i] + 1;
            end if;
        end loop;
    end loop;
	
	for i in array_lower(spoints, 1) .. array_upper(spoints, 1)
	loop
        cnt := 0;
        for j in array_lower(spoints, 1) .. array_upper(spoints, 1)
        loop
            if( ( spoints[j].x > spoints[i].x and spoints[j].y >= spoints[i].y ) 
                or ( spoints[j].x >= spoints[i].x and spoints[j].y > spoints[i].y )
              ) then
                cnt := cnt + 1;
            end if;
        end loop;
        if(cnt < k) then
            return next spoints[i];
        end if;
	end loop;
end;
$$ language plpgsql;

drop function if exists skyband(text, integer) cascade;

-- This function simply creates a view to store skyband
create or replace function skyband(dataset text, k integer) 
    returns integer 
as $$
declare
    res integer;
begin
    if(not table_exists(dataset)) then
        return -1;
    end if;
	
    execute 'create or replace view '||dataset||'_skyband(x, y)
             as
             select *
             from find('''||dataset||''', '||k||')
			 order by x asc, y desc
            ';

    execute 'select count(*) from '||dataset||'_skyband' into res;
    return res;
end;
$$ language plpgsql;