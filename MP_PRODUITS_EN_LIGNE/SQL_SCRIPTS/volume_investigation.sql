select keyword, search_position, search_volume INTO AMAZON_PRICING from pricing_keywords where domain='amazon.fr' order by search_volume desc
select keyword, search_position, search_volume INTO CDISCOUNT_PRICING from pricing_keywords where domain='cdiscount.com' order by search_volume desc


select am.keyword, am.search_volume, am.search_position from AMAZON_PRICING as am where am.keyword not in (select distinct keyword from CDISCOUNT_PRICING) order by am.search_volume desc