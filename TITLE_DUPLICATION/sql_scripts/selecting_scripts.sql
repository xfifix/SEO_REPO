select * from duplicates order by nb_urls desc


# duplication par magasins
select magazin, sum(nb_urls) as global_count from duplicates group by magazin order by global_count desc

# duplication par rayon
select rayon,sum(nb_urls) as global_count from duplicates group by rayon order by global_count desc

# duplication par produit
select product, sum(nb_urls) as global_count from duplicates where product not like '' group by product order by global_count desc

# duplication par produit avec rayon
select product_aggregate.product, product_aggregate.global_count, referential.rayon from product_aggregate, referential where referential.product=product_aggregate.product
order by global_count desc
