-- Q1:

create type IncorrectRecord as (pattern_number integer, uoc_number integer);

create or replace view q1_temp_1(code, uoc, eftsload, compare) as
	select code, uoc,eftsload, case when eftsload = 0 and uoc = 0 then 48
	when eftsload > 0 then cast(uoc/eftsload as integer)
	end
	from subjects;

create or replace function Q1(pattern text, uoc_threshold integer)
	returns IncorrectRecord
as $$
declare
	pattern_num integer;
	uoc_num integer;
begin
	select count(compare) into pattern_num from q1_temp_1 where compare != 48 and code like pattern;
	select count(uoc) into uoc_num from q1_temp_1 where compare != 48 and code like pattern and uoc > uoc_threshold;
	return (pattern_num, uoc_num);
end;
$$ language plpgsql;


-- Q2:

create type TranscriptRecord as (cid integer, term char(4), code char(8), name text, uoc integer, mark integer, grade char(2), rank integer, totalEnrols integer);

create or replace view q2_temp_1(n1, n2, n3, n4, n5, n6, n7, n8, unswid) as
	select courses.id, substring(cast(semesters.year as text) from 3 for 2) || lower(semesters.term) as year_term, subjects.code, subjects.name,
	subjects.uoc, course_enrolments.mark, course_enrolments.grade, 
	case when mark is null then null else rank() over(partition by courses.id order by 0 - course_enrolments.mark) end, people.unswid
	from course_enrolments, courses, semesters, subjects, people
	where course_enrolments.course = courses.id
	and courses.subject = subjects.id
	and courses.semester = semesters.id
	and course_enrolments.student = people.id
	group by courses.id, year_term, subjects.code, subjects.name, subjects.uoc, course_enrolments.grade, course_enrolments.mark, people.unswid;

create or replace function Q2(stu_unswid integer)
	returns setof TranscriptRecord
as $$
begin
	return query 
	with R as (select course, sum(case when mark is null then 0 else 1 end) as n9 from course_enrolments group by course)
	select n1, cast(n2 as char(4)), n3, cast(n4 as text), case when n7 in ('SY', 'RS', 'PT', 'PC', 'PS', 'CR', 'DN', 'HD', 'A', 'B', 'C', 'D', 'E') then n5 else 0 end,
	n6, cast(n7 as char(2)), cast(n8 as integer), cast(n9 as integer)
	from q2_temp_1, R where n1 = R.course and unswid = stu_unswid;
end;
$$ language plpgsql;


-- Q3:

create type TeachingRecord as (unswid integer, staff_name text, teaching_records text);

create or replace function get_all_org(org_id int4)
	returns int4[] 
as $$
declare
	process_parents int4[] := array[org_id];
	children int4[] := array[org_id];
	new_children int4[];
begin
	while (array_upper(process_parents, 1) is not null) loop
		new_children := array(select member from orgunit_groups where owner = any(process_parents) and member <> all(children));
		children := children || new_children;
		process_parents := new_children;
	end loop;
	return children;
end;
$$ language plpgsql;


-- recursive for org

with recursive r as(
	select * from orgunit_groups where owner = 52
	union all
	select orgunit_groups.* from orgunit_groups, r where orgunit_groups.owner = r.member
	)
select * from r;


create or replace view q3_temp_1(course, sid, oid, orgunit, unswid, name, subject) as
	select courses.id, subjects.id, orgunits.id, orgunits.name, people.unswid, people.name, subjects.code
	from course_staff, courses, orgunits, people, staff_roles, subjects
	where course_staff.course = courses.id
	and course_staff.staff = people.id
	and course_staff.role = staff_roles.id
	and courses.subject = subjects.id
	and orgunits.id = subjects.offeredby
	and staff_roles.name not like 'Course Tutor';


create or replace function q3_temp_2(org_id integer, num_sub integer, num_times integer) 
	returns setof TeachingRecord 
as $$
	with R as (select name as name_check from q3_temp_1 where oid = any(get_all_org(org_id))
	group by unswid, name having count(distinct sid) > num_sub)
	select unswid, cast(name as text), subject || ', ' || count(subject) || ', ' || orgunit || e'\n'
	from q3_temp_1 where exists(select 1 from R where name_check = name) and oid = any(get_all_org(org_id))
	group by unswid, name, subject, orgunit, sid having count(sid) > num_times order by name, sid;
$$ language sql;


create or replace function Q3(org_id integer, num_sub integer, num_times integer) 
	returns setof TeachingRecord 
as $$
begin
	return query 
	select unswid, staff_name, string_agg(teaching_records, '')
	from (select * from q3_temp_2(org_id, num_sub, num_times)) as R
	group by unswid, staff_name order by staff_name;

end;
$$ language plpgsql;


-- test

select sid, subject  from q3_temp_1 where unswid = 9479451 and oid = any(get_all_org(112)) and subject like 'MATH2089';

create or replace view temp(name) as
	select name from q3_temp_1 where oid = any(get_all_org(112)) group by name having count(distinct sid) > 5;
