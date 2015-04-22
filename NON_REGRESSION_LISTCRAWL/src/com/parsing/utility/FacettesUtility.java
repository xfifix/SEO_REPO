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


public class FacettesUtility {
	public static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");

	public static FacettesParsingOutput parse_page_code_source(String page_source_code){
		org.jsoup.nodes.Document doc = Jsoup.parse(page_source_code);
		FacettesParsingOutput output = new FacettesParsingOutput();

		List<FacettesInfo> list_facettes = new ArrayList<FacettesInfo>();
		FacettesInfo my_info = new FacettesInfo();
		Elements nb_products = doc.select("div.lpStTit");
		output.setNb_products(nb_products.text());
		
		Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
		for (Element facette : facette_elements ){
			//System.out.println(e.toString());
			Elements facette_name = facette.select("div.mvFTitle.noSel");
			my_info.setFacetteName(facette_name.text());
			Elements facette_values = facette.select("a");
			for (Element facette_value : facette_values){		
				System.out.println(facette_value);
				// old way
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
					System.out.println("Trouble while formatting a facette");
					my_info.setFacetteCount(0);
				}
				my_info.setFacetteValue(categorie_value);
				list_facettes.add(my_info);
				my_info = new FacettesInfo();
				my_info.setFacetteName(facette_name.text());
			}		
		}
		String facette_json=FacettesUtility.getFacettesJSONStringToStore(list_facettes);
		output.setFacettes(facette_json);
		return output;
	}

	public static String getFacettesJSONStringToStore(List<FacettesInfo> facettes_info){
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
}
