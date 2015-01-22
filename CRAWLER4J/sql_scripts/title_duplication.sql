# we have to create an index over titles
# it must be done otherwise the duplication metrics would be too low to produce
CREATE INDEX ON crawl_results (title);

select distinct title, count(*) from crawl_results where magasin = 'dvd' group by title
select count(*) from crawl_results where magasin = 'dvd'


