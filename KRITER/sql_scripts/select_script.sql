select distinct nb_distinct_brand, count(*) from CATALOG group by nb_distinct_brand;
select distinct nb_distinct_magasin, count(*) from CATALOG group by nb_distinct_magasin;
select distinct nb_distinct_cat4, count(*) from CATALOG group by nb_distinct_cat4;
select distinct nb_distinct_cat5, count(*) from CATALOG group by nb_distinct_cat5;
select distinct nb_distinct_vendor, count(*) from CATALOG group by nb_distinct_vendor;
select distinct nb_distinct_state, count(*) from CATALOG group by nb_distinct_state;

select distinct categorie_niveau_4, count(*), true as to_fetch into CATEGORY_FOLLOWING from CATALOG group by categorie_niveau_4;

# assessing the homogeneity of the links
select distinct counter, count(*) from LINKING_SIMILAR_PRODUCTS group by counter;

select distinct categorie_niveau_4, count(*), true as to_fetch into CATEGORY_FOLLOWING from CATALOG group by categorie_niveau_4;

# getting all the unfetched elements
select count(*) from CATALOG where to_fetch = true;
# getting the count of all elements in CATEGORY LEVEL 4 greater than 10000
select sum(count) from CATEGORY_FOLLOWING where count > 10000
# getting the count of all elements in CATEGORY LEVEL 4 smaller than 6
select sum(count) from CATEGORY_FOLLOWING where count < 6
