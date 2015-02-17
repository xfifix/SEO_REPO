package com.parsing.utility;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class ProductsListParseUtility {
	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");

	@SuppressWarnings("unchecked")
	public static String getJSONStringToStore(List<FacettesInfo> facettes_info){
		JSONArray facettesArray = new JSONArray();
		for (FacettesInfo info : facettes_info){
			JSONObject facetteObject = new JSONObject();
			facetteObject.put("facette_name", info.getFacetteName());
			facetteObject.put("facette_value", info.getFacetteValue());
			facetteObject.put("facette_count", info.getFacetteCount());
			facettesArray.add(facetteObject);
		}
		return facettesArray.toJSONString();
	}

	public static URLComparisonListProductsInfo parse_page_source_code(String page_code){
		// fetching data using Jsoup and jQuery
		URLComparisonListProductsInfo url_info = new URLComparisonListProductsInfo();
		org.jsoup.nodes.Document doc =  Jsoup.parse(page_code);

		// geolocalisation
		Elements geoloc_elems = doc.select("#lpGeoloc");
		url_info.setGeoloc_selection(geoloc_elems.toString());
		// above blocks
		StringBuilder aboveBlocsString = new StringBuilder();
		Elements aboveBigBlocks = doc.select("div.lpTBig");
		aboveBlocsString.append(aboveBigBlocks.toString());
		Elements aboveSmallBlocks = doc.select("div.lpTSmall");
		aboveBlocsString.append(aboveSmallBlocks.toString());
		url_info.setBest_sales_products(aboveBlocsString.toString());
		// body page blocks
		Elements core_listed_blocs = doc.getElementsByAttribute("data-sku");
		url_info.setBody_page_products(core_listed_blocs.toString());
		// number of products on the first page
		Elements number_of_products_blocs = doc.select("div.lpStTit");
		url_info.setNumber_of_products(number_of_products_blocs.toString());

		// facettes summary aggregation
		List<FacettesInfo> list_facettes = new ArrayList<FacettesInfo>();
		FacettesInfo my_info = new FacettesInfo();

		Elements facette_elements = doc.select("div.mvFilter");			
		for (Element facette : facette_elements ){
			//System.out.println(e.toString());
			Elements facette_name = facette.select("div.mvFTit");
			my_info.setFacetteName(facette_name.text());
			Elements facette_values = facette.select("a");
			for (Element facette_value : facette_values){
				String categorie_value = facette_value.text();
				if ("".equals(categorie_value)){
					categorie_value = facette_value.attr("title");
				}
				Matcher matchPattern = bracketPattern.matcher(categorie_value);
				String categorieCount ="";
				while (matchPattern.find()) {		
					categorieCount=matchPattern.group();
				}
				categorie_value=categorie_value.replace(categorieCount,"");
				categorieCount=categorieCount.replace("(", "");
				categorieCount=categorieCount.replace(")", "");	
				//System.out.println(categorie_value);
				try{
					my_info.setFacetteCount(Integer.valueOf(categorieCount));
					//System.out.println(Integer.valueOf(categorieCount));	
				} catch (NumberFormatException e){
					e.printStackTrace();
					System.out.println("Trouble while formatting a facette");
				}
				my_info.setFacetteValue(categorie_value);
				list_facettes.add(my_info);
				my_info = new FacettesInfo();
				my_info.setFacetteName(facette_name.text());
			}		
		}

		String facette_json=ProductsListParseUtility.getJSONStringToStore(list_facettes);
		url_info.setFacette_summary(facette_json);
		return url_info;
	}
}
