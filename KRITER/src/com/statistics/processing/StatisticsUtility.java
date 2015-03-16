package com.statistics.processing;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StatisticsUtility {
	
	public static StatisticsMeasures computeStatistics(String[] skus_in_order, Map<String, CatalogEntry> filled_up ){
		CatalogEntry current_sku = filled_up.get(skus_in_order[0]);
		
		CatalogEntry sku1 =filled_up.get(skus_in_order[1]);
		CatalogEntry sku2 =filled_up.get(skus_in_order[2]);
		CatalogEntry sku3 =filled_up.get(skus_in_order[3]);
		CatalogEntry sku4 =filled_up.get(skus_in_order[4]);
		CatalogEntry sku5 =filled_up.get(skus_in_order[5]);
		CatalogEntry sku6 =filled_up.get(skus_in_order[6]);

		
		Set<String> distinct_category5 = new HashSet<String>();
		Set<String> distinct_category4 = new HashSet<String>();
		Set<String> distinct_brand = new HashSet<String>();
		
		distinct_category5.add(sku1.getCATEGORIE_NIVEAU_5());
		distinct_category5.add(sku2.getCATEGORIE_NIVEAU_5());
		distinct_category5.add(sku3.getCATEGORIE_NIVEAU_5());
		distinct_category5.add(sku4.getCATEGORIE_NIVEAU_5());
		distinct_category5.add(sku5.getCATEGORIE_NIVEAU_5());
		distinct_category5.add(sku6.getCATEGORIE_NIVEAU_5());
		
		distinct_category4.add(sku1.getCATEGORIE_NIVEAU_4());
		distinct_category4.add(sku2.getCATEGORIE_NIVEAU_4());
		distinct_category4.add(sku3.getCATEGORIE_NIVEAU_4());
		distinct_category4.add(sku4.getCATEGORIE_NIVEAU_4());
		distinct_category4.add(sku5.getCATEGORIE_NIVEAU_4());
		distinct_category4.add(sku6.getCATEGORIE_NIVEAU_4());

		distinct_brand.add(sku1.getMARQUE());
		distinct_brand.add(sku2.getMARQUE());
		distinct_brand.add(sku3.getMARQUE());
		distinct_brand.add(sku4.getMARQUE());
		distinct_brand.add(sku5.getMARQUE());
		distinct_brand.add(sku6.getMARQUE());

		
		StatisticsMeasures measures = new StatisticsMeasures();

		
		measures.setNb_distinct_category5(distinct_category5.size());
		measures.setNb_distinct_category4(distinct_category4.size());
		measures.setNb_distinct_brands(distinct_brand.size());
		measures.setDistinct_category5(distinct_category5.toString());
		measures.setDistinct_category4(distinct_category4.toString());
		measures.setDistinct_brands(distinct_brand.toString());
		measures.setCurrentSku(current_sku.getSKU());
		measures.setCurrentVendor(current_sku.getVENDEUR());
		measures.setMagasin(current_sku.getMAGASIN());
		measures.setState(current_sku.getETAT());
		
		return measures;
	}
	

}
