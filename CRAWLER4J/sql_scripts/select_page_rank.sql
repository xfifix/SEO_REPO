select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' order by page_rank desc limit 100;

select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=1 order by page_rank desc limit 100;
select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=2 order by page_rank desc limit 100;
select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=3 order by page_rank desc limit 100;
select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=4 order by page_rank desc limit 100;
select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=5 order by page_rank desc limit 100;
select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=6 order by page_rank desc limit 100;
select url, page_type, magasin, produit, page_rank from crawl_results where magasin = 'maison' and depth=7 order by page_rank desc limit 100;

