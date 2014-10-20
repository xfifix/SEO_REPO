# with upsert as 
#(update actor set last_update = now() where first_name = 'AMBER' and last_name = 'LEE' returning actor_id) 
# insert into actor (first_name, last_name, last_update) 
# select 'AMBER', 'LEE', now() WHERE NOT EXISTS (SELECT 1 FROM upsert) ; 

update CRAWL_RESULTS set WHOLE_TEXT=?,TITLE=?,LINKS_SIZE=?,LINKS=?,H1=?,FOOTER_EXTRACT=?,ZTD_EXTRACT=?,SHORT_DESCRIPTION=?,VENDOR=?,ATTRIBUTES=?,NB_ATTRIBUTES=?,STATUS_CODE=?,HEADERS=?,DEPTH=? WHERE URL=?


with upsert as (update CRAWL_RESULTS set WHOLE_TEXT=?,TITLE=?,LINKS_SIZE=?,LINKS=?,H1=?,FOOTER_EXTRACT=?,ZTD_EXTRACT=?,SHORT_DESCRIPTION=?,VENDOR=?,ATTRIBUTES=?,NB_ATTRIBUTES=?,STATUS_CODE=?,HEADERS=?,DEPTH=? WHERE URL=?)

