SELECT DATE_PART('day', last_update) from arbocrawl_results;
SELECT now()-last_update from arbocrawl_results;
SELECT * from arbocrawl_results where now()-last_update > INTERVAL '15 days';
SELECT count(*) from crawl_results where now()-last_update > INTERVAL '10 days';