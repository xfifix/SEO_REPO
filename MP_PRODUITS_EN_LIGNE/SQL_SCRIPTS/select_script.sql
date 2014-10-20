# listing all different magasins
select distinct MAGASIN from MAGASIN_PRODUITS where MAGASIN not like 'TOTAL'

# listing all different rayons
select distinct RAYON from MAGASIN_PRODUITS where RAYON not like 'TOTAL'

# listing all different magasins
select distinct MAGASIN from MAGASIN_PRODUITS

# all the dates of the already digested files
select distinct report_date from MAGASIN_PRODUITS

# aggregated results for all magasins
select * from MAGASIN_PRODUITS where MAGASIN='TOTAL' order by REPORT_DATE asc

 
# aggregated results for all rayons per specific magasin
select * from MAGASIN_PRODUITS where MAGASIN='AUTO' AND RAYON='TOTAL' order by REPORT_DATE asc

# aggregated results for a specific rayon and a specific magasin
select * from MAGASIN_PRODUITS where MAGASIN='AUTO' AND RAYON='PNEUS AUTO' order by REPORT_DATE asc
 