with cte as (
	select distinct keyword from keywords 
)
insert into unique_keywords
select * from cte


INSERT INTO unique_keywords
select distinct keywords from keywords