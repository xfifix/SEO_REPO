select * from arbocrawl_results where height_average='nan'::float;

update arbocrawl_results set width_average=0 where width_average='nan'::float;