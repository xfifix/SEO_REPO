package com.ranks;

import java.util.StringTokenizer;

public class URL_Utilities {
	private static String Vitrine = "^http://([a-z0-9]*\\.)*www.cdiscount.com.*/v-.*";
	private static String FicheProduit  ="^http://([a-z0-9]*\\.)*www.cdiscount.com.*/f-.*";
	private static String ListeProduitFiltre = "^http://([a-z0-9]*\\.)*www.cdiscount.com.*/lf-.*";
	private static String PageMarque = "^http://([a-z0-9]*\\.)*www.cdiscount.com.*/m-.*";
	private static String PageConcept = "^http://([a-z0-9]*\\.)*www.cdiscount.com.*/ct-.*";
	private static String SearchDexing = "^http://([a-z0-9]*\\.)*www.cdiscount.com.*/r-.*";

	public static String checkType(String url){
		if (url.matches(Vitrine)){
			return "Vitrine";
		}
		if (url.matches(FicheProduit)){
			return "FicheProduit";
		}
		if (url.matches(ListeProduitFiltre)){
			return "ListeProduitFiltre";
		}
		if (url.matches(PageMarque)){
			return "PageMarque";
		}
		if (url.matches(PageConcept)){
			return "PageConcept";
		}
		if (url.matches(SearchDexing)){
			return "SearchDexing";
		}	
		return "Unknown";
	}

	public static String checkMagasin(String url){
		String magasin = "Unknown";
		url = url.replace("http://www.cdiscount.com/","");
		StringTokenizer tokenize = new StringTokenizer(url,"/");
		if (tokenize.hasMoreTokens()){
			magasin = tokenize.nextToken();
		}
		return magasin;
	}

	public static String checkRayon(String url){
		String rayon = "Unknown";
		url = url.replace("http://www.cdiscount.com/","");
		StringTokenizer tokenize = new StringTokenizer(url,"/");
		if (tokenize.hasMoreTokens()){
			tokenize.nextToken();
		}
		if (tokenize.hasMoreTokens()){
			rayon = tokenize.nextToken();
		}
		return rayon;
	}

	public static String checkProduit(String url){
		String produit = "Unknown";
		url = url.replace("http://www.cdiscount.com/","");
		StringTokenizer tokenize = new StringTokenizer(url,"/");
		if (tokenize.hasMoreTokens()){
			tokenize.nextToken();
		}
		if (tokenize.hasMoreTokens()){
			tokenize.nextToken();
		}
		if (tokenize.hasMoreTokens()){
			produit = tokenize.nextToken();
		}
		return produit;
	}
	
}
