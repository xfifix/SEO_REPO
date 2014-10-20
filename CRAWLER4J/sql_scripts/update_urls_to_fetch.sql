UPDATE URLS_TO_FETCH AS u 
SET u.depth = r.depth
FROM CRAWL_RESULTS AS r
WHERE u.url = r.url 