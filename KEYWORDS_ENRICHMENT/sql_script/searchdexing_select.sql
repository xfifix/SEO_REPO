## selecting everything :)
select keyword, cdiscount_search_volume,cdiscount_search_position, amazon_search_volume, amazon_search_position, magasin, rayon from SEARCHDEXING_KEYWORDS where amazon_search_volume>0 and cdiscount_search_volume is null
select keyword, cdiscount_search_volume,cdiscount_search_position, amazon_search_volume, amazon_search_position, magasin, rayon from SEARCHDEXING_KEYWORDS where cdiscount_search_volume>0 and  amazon_search_volume is null


## selecting per magasin
select keyword, cdiscount_search_volume,cdiscount_search_position, amazon_search_volume, amazon_search_position, magasin, rayon from SEARCHDEXING_KEYWORDS where amazon_search_volume>0 and cdiscount_search_volume is null and magasin='informatique' order by amazon_search_volume desc
select keyword, cdiscount_search_volume,cdiscount_search_position, amazon_search_volume, amazon_search_position, magasin, rayon from SEARCHDEXING_KEYWORDS where cdiscount_search_volume>0 and  amazon_search_volume is null and magasin='informatique'

