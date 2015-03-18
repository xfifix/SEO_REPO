select distinct nb_distinct_brand, count(*) from CATALOG group by nb_distinct_brand;
select distinct nb_distinct_magasin, count(*) from CATALOG group by nb_distinct_magasin;
select distinct nb_distinct_cat4, count(*) from CATALOG group by nb_distinct_cat4;
select distinct nb_distinct_cat5, count(*) from CATALOG group by nb_distinct_cat5;
select distinct nb_distinct_vendor, count(*) from CATALOG group by nb_distinct_vendor;
select distinct nb_distinct_state, count(*) from CATALOG group by nb_distinct_state;
