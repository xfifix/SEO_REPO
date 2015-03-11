select distinct url, count(*) as num_url into duplicate_urls_crawl_results from crawl_results group by url having count(*)>1 ;
select * from crawl_results where url in (select url from duplicate_urls_crawl_results); 
