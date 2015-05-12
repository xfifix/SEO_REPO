update CATALOG set CATEGORIE_NIVEAU_4=CATEGORIE_NIVEAU_3 where CATEGORIE_NIVEAU_4='';
update CATALOG set CATEGORIE_NIVEAU_4=CATEGORIE_NIVEAU_2 where CATEGORIE_NIVEAU_4='';
update CATALOG set CATEGORIE_NIVEAU_4=CATEGORIE_NIVEAU_1 where CATEGORIE_NIVEAU_4='';
CREATE INDEX ON catalog (sku);
CREATE INDEX ON catalog (categorie_niveau_4);
CREATE INDEX ON catalog (categorie_niveau_3);
CREATE INDEX ON catalog (categorie_niveau_2);
CREATE INDEX ON catalog (categorie_niveau_1);
select distinct categorie_niveau_4, count(*), true as to_fetch into CATEGORY_FOLLOWING from CATALOG group by categorie_niveau_4;

