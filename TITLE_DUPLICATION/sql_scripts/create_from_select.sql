select rayon, sum(nb_urls) as global_count into rayon_aggregate from duplicates group by rayon order by global_count desc

select magazin, sum(nb_urls) as global_count into magasin_aggregate from duplicates group by magazin order by global_count desc

select product, sum(nb_urls) as global_count into product_aggregate from duplicates group by product order by global_count desc


select distinct duplicates.product, duplicates.rayon into referential from duplicates where product not like ''