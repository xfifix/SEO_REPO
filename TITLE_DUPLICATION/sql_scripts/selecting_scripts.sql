select * from current_duplicates order by nb_urls desc


# duplication par magasins
select magazin, sum(nb_urls) as global_count from current_duplicates group by magazin order by global_count desc

# duplication par rayon
select rayon,sum(nb_urls) as global_count from current_duplicates group by rayon order by global_count desc

# duplication par produit
select product, sum(nb_urls) as global_count from current_duplicates where product not like '' group by product order by global_count desc

# duplication par produit avec rayon
select product_aggregate.product, product_aggregate.global_count, referential.rayon from product_aggregate, referential where referential.product=product_aggregate.product
order by global_count desc



# getting the unique url and their date
select distinct urls, count(*) as numb into duplicates_unique_url from duplicates group by urls order by numb desc

select duplicates.urls, duplicates.duplicate_time from duplicates, duplicates_unique_url where duplicates.urls = duplicates_unique_url.urls and duplicates_unique_url.numb =1