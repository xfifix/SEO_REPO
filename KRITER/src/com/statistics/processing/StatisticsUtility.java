package com.statistics.processing;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import crawl4j.vsm.CorpusCache;

public class StatisticsUtility {

	public static StatisticsMeasures computeStatistics(String[] skus_in_order, Map<String, CatalogEntry> filled_up ){
		CatalogEntry current_sku = filled_up.get(skus_in_order[0]);
		if (current_sku == null)
		{
			System.out.println("Trouble finding current sku");
			return null;
		}

		Set<String> distinct_category5 = new HashSet<String>();
		Set<String> distinct_category4 = new HashSet<String>();
		Set<String> distinct_brand = new HashSet<String>();
		Set<String> distinct_vendor = new HashSet<String>();
		Set<String> distinct_magasin = new HashSet<String>();
		Set<String> distinct_states = new HashSet<String>();
		
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
			distinct_vendor.add(sku1.getVENDEUR());
			distinct_magasin.add(sku1.getMAGASIN());
			distinct_states.add(sku1.getETAT());
			
			tf_libelle_distances[0] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku1.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[0] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku1.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[0] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku1.getLIBELLE_PRODUIT());
			tf_description80_distances[0] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku1.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[0] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku1.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[0] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku1.getDESCRIPTION_LONGUEUR80());
		} else {
			System.out.println("Trouble finding sku 1");
		}
		CatalogEntry sku2 =filled_up.get(skus_in_order[2]);
		if (sku2 != null){
			distinct_category5.add(sku2.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku2.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku2.getMARQUE());
			distinct_vendor.add(sku2.getVENDEUR());
			distinct_magasin.add(sku2.getMAGASIN());
			distinct_states.add(sku2.getETAT());
			
			tf_libelle_distances[1] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku2.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[1] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku2.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[1] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku2.getLIBELLE_PRODUIT());
			tf_description80_distances[1] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku2.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[1] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku2.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[1] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku2.getDESCRIPTION_LONGUEUR80());
		} else {
			System.out.println("Trouble finding sku 2");
		}
		CatalogEntry sku3 =filled_up.get(skus_in_order[3]);
		if (sku3 != null){
			distinct_category5.add(sku3.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku3.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku3.getMARQUE());
			distinct_vendor.add(sku3.getVENDEUR());
			distinct_magasin.add(sku3.getMAGASIN());
			distinct_states.add(sku3.getETAT());

			tf_libelle_distances[2] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku3.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[2] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku3.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[2] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku3.getLIBELLE_PRODUIT());
			tf_description80_distances[2] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku3.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[2] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku3.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[2] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku3.getDESCRIPTION_LONGUEUR80());
		} else {
			System.out.println("Trouble finding sku 3");
		}
		CatalogEntry sku4 =filled_up.get(skus_in_order[4]);
		if (sku4 != null){
			distinct_category5.add(sku4.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku4.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku4.getMARQUE());
			distinct_vendor.add(sku4.getVENDEUR());
			distinct_magasin.add(sku4.getMAGASIN());
			distinct_states.add(sku4.getETAT());
			
			tf_libelle_distances[3] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku4.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[3] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku4.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[3] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku4.getLIBELLE_PRODUIT());
			tf_description80_distances[3] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku4.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[3] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku4.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[3] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku4.getDESCRIPTION_LONGUEUR80());
		} else {
			System.out.println("Trouble finding sku 4");
		}
		CatalogEntry sku5 =filled_up.get(skus_in_order[5]);
		if (sku5 != null){
			distinct_category5.add(sku5.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku5.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku5.getMARQUE());
			distinct_vendor.add(sku5.getVENDEUR());
			distinct_magasin.add(sku5.getMAGASIN());
			distinct_states.add(sku5.getETAT());
			
			tf_libelle_distances[4] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku5.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[4] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku5.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[4] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku5.getLIBELLE_PRODUIT());
			tf_description80_distances[4] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku5.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[4] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku5.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[4] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku5.getDESCRIPTION_LONGUEUR80());
		} else {
			System.out.println("Trouble finding sku 5");
		}
		CatalogEntry sku6 =filled_up.get(skus_in_order[6]);
		if (sku6 != null){
			distinct_category5.add(sku6.getCATEGORIE_NIVEAU_5());
			distinct_category4.add(sku6.getCATEGORIE_NIVEAU_4());
			distinct_brand.add(sku6.getMARQUE());
			distinct_vendor.add(sku6.getVENDEUR());
			distinct_magasin.add(sku6.getMAGASIN());
			distinct_states.add(sku6.getETAT());

			tf_libelle_distances[5] = computeTFdistance(current_sku.getLIBELLE_PRODUIT(),sku6.getLIBELLE_PRODUIT());
			//tf_idf_libelle_distances[5] = computeTFIDFdistance(current_sku.getLIBELLE_PRODUIT(),sku6.getLIBELLE_PRODUIT());
			levenshtein_libelle_distances[5] = computeLevenshteindistance(current_sku.getLIBELLE_PRODUIT(),sku6.getLIBELLE_PRODUIT());
			tf_description80_distances[5] = computeTFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku6.getDESCRIPTION_LONGUEUR80());
			//tf_idf_description80_distances[5] = computeTFIDFdistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku6.getDESCRIPTION_LONGUEUR80());
			levenshtein_description80_distances[5] = computeLevenshteindistance(current_sku.getDESCRIPTION_LONGUEUR80(),sku6.getDESCRIPTION_LONGUEUR80());
		} else {
			System.out.println("Trouble finding sku 6");
		}
		StatisticsMeasures measures = new StatisticsMeasures();

		measures.setNb_distinct_category5(distinct_category5.size());
		measures.setNb_distinct_category4(distinct_category4.size());
		measures.setNb_distinct_brands(distinct_brand.size());
		distinct_brand.remove("AUCUNE");
		measures.setNb_distinct_brands_without_default(distinct_brand.size());
		measures.setNb_distinct_magasins(distinct_magasin.size());
		measures.setNb_distinct_states(distinct_states.size());
		measures.setNb_distinct_vendors(distinct_vendor.size());
		
		measures.setDistinct_category5(distinct_category5.toString());
		measures.setDistinct_category4(distinct_category4.toString());
		measures.setDistinct_vendors(distinct_vendor.toString());
		measures.setDistinct_states(distinct_states.toString());
		measures.setDistinct_magasins(distinct_magasin.toString());

		measures.setLevenshtein_distances_libelle(levenshtein_libelle_distances);
		measures.setLevenshtein_description80(levenshtein_description80_distances);
		measures.setTf_description80(tf_description80_distances);
		measures.setTf_distances_libelle(tf_libelle_distances);
		measures.setTf_idf_description80(tf_idf_description80_distances);
		measures.setTf_idf_distances_libelle(tf_idf_libelle_distances);

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
	
	public static Double computeAlgoWeightedDistance(String text1,String text2){
		Double tf_distance = CorpusCache.computeTFSimilarity(text1, text2);
		Integer levenshteing_distance = StringUtils.getLevenshteinDistance(text1, text2);
		return tf_distance+(double)levenshteing_distance;
	}

}
