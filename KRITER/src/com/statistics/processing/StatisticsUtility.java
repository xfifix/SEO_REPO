package com.statistics.processing;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import crawl4j.vsm.CorpusCache;

public class StatisticsUtility {

	public static StatisticsMeasures computeStatistics(String[] skus_in_order, Map<String, CatalogEntry> filled_up ){
		CatalogEntry current_sku = filled_up.get(skus_in_order[0]);

		Set<String> distinct_category5 = new HashSet<String>();
		Set<String> distinct_category4 = new HashSet<String>();
		Set<String> distinct_brand = new HashSet<String>();

		Double[] tf_libelle_distances = new Double[6];
		Double[] tf_idf_libelle_distances = new Double[6];
		Integer[] levenshtein_libelle_distances = new Integer[6];

		Double[] tf_description80_distances = new Double[6];
		Double[] tf_idf_description80_distances = new Double[6];
		Integer[] levenshtein_description80_distances = new Integer[6];

		CatalogEntry sku1 =filled_up.get(skus_in_order[1]);
		if (sku1!=null){
			distinct_category5.add(sku1.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku1.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku1.getMARQUE());
			tf_libelle_distances[0] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku1.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[0] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku1.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[0] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku1.getLIBELLE_PRODUIT());
			tf_description80_distances[0] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku1.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[0] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku1.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[0] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku1.getDESCRIPTION_LONGUEUR80());
		}
		CatalogEntry sku2 =filled_up.get(skus_in_order[2]);
		if (sku2 != null){
			distinct_category5.add(sku2.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku2.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku2.getMARQUE());

			tf_libelle_distances[1] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku2.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[1] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku2.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[1] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku2.getLIBELLE_PRODUIT());
			tf_description80_distances[1] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku2.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[1] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku2.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[1] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku2.getDESCRIPTION_LONGUEUR80());
		}
		CatalogEntry sku3 =filled_up.get(skus_in_order[3]);
		if (sku3 != null){
			distinct_category5.add(sku3.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku3.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku3.getMARQUE());

			tf_libelle_distances[2] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku3.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[2] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku3.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[2] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku3.getLIBELLE_PRODUIT());
			tf_description80_distances[2] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku3.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[2] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku3.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[2] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku3.getDESCRIPTION_LONGUEUR80());
		}
		CatalogEntry sku4 =filled_up.get(skus_in_order[4]);
		if (sku4 != null){
			distinct_category5.add(sku4.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku4.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku4.getMARQUE());

			tf_libelle_distances[3] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku4.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[3] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku4.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[3] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku4.getLIBELLE_PRODUIT());
			tf_description80_distances[3] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku4.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[3] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku4.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[3] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku4.getDESCRIPTION_LONGUEUR80());

		}
		CatalogEntry sku5 =filled_up.get(skus_in_order[5]);
		if (sku5 != null){
			distinct_category5.add(sku5.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku5.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku5.getMARQUE());

			tf_libelle_distances[4] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku5.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[4] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku5.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[4] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku5.getLIBELLE_PRODUIT());
			tf_description80_distances[4] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku5.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[4] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku5.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[4] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku5.getDESCRIPTION_LONGUEUR80());

		}
		CatalogEntry sku6 =filled_up.get(skus_in_order[6]);
		if (sku6 != null){
			distinct_category5.add(sku6.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku6.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku6.getMARQUE());
			tf_libelle_distances[5] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku6.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[5] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku6.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[5] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku6.getLIBELLE_PRODUIT());
			tf_description80_distances[5] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku6.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[5] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku6.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[5] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku6.getDESCRIPTION_LONGUEUR80());
		}

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

	public static Double computeTFdistance(String text1,String text2){
		return CorpusCache.computeTFSimilarity(text1, text2);
	}

	public static Double computeTFIDFdistance(String text1,String text2){
		return CorpusCache.computeTFSIDFimilarity(text1, text2);
	}

	public static Integer computeLevenshteindistance(String text1,String text2){
		return StringUtils.getLevenshteinDistance(text1, text2);
	}
}
