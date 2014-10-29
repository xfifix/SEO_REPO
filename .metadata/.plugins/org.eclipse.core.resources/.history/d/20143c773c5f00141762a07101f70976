
select magasin, count(*) as global_count into magasin_sumup from CURRENT_DUPLICATES group by magasin order by global_count desc

delete  from magasin_sumup where global_count>3

update current_duplicates set magasin = 'Unknown' where magasin in (select magasin from magasin_sumup)
