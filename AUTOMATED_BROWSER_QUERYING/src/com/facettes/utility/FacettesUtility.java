package com.facettes.utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;

public class FacettesUtility {

	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");

	public static List<AdvancedFacettesInfo> extract_facettes_infos(URLFacettesData to_fetch){
		String my_user_agent= "CdiscountBot-crawler";
		List<AdvancedFacettesInfo> my_fetched_infos = new ArrayList<AdvancedFacettesInfo>();
		int idUrl = to_fetch.getId();
		String url = to_fetch.getUrl();
		// fetching the URL and parsing the results
		org.jsoup.nodes.Document doc;
		try {
			doc =  Jsoup.connect(url)
					.userAgent(my_user_agent)
					.ignoreHttpErrors(true)
					.timeout(0)
					.get();
			AdvancedFacettesInfo my_info = new AdvancedFacettesInfo();
			my_info.setId(idUrl);
			my_info.setUrl(url);

			Elements counter_elements = doc.select(".lpStTit strong");		
			String product_size_text = counter_elements.text();
			my_info.setProducts_size(product_size_text);
			boolean isFacetteOpened = false;

			Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
			for (Element facette : facette_elements ){
				//System.out.println(e.toString());
				Elements facette_name = facette.select("div.mvFTitle.noSel");
				my_info.setFacetteName(facette_name.text());
				Elements facette_values = facette.select("a");
				for (Element facette_value : facette_values){		
					//System.out.println(facette_value);
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
					my_info.setIs_opened(isFacetteOpened);
					my_fetched_infos.add(my_info);
					my_info = new AdvancedFacettesInfo();
					my_info.setId(idUrl);
					my_info.setUrl(url);
					my_info.setProducts_size(product_size_text);
					my_info.setFacetteName(facette_name.text());
				}		
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return my_fetched_infos;
	}

	public static List<AdvancedFacettesInfo> extract_facettes_from_source_code(String sourceCodePage,URLFacettesData to_fetch){
		List<AdvancedFacettesInfo> my_fetched_infos = new ArrayList<AdvancedFacettesInfo>();

		org.jsoup.nodes.Document doc =  Jsoup.parse(sourceCodePage);
		AdvancedFacettesInfo my_info = new AdvancedFacettesInfo();
		my_info.setId(to_fetch.getId());
		my_info.setUrl(to_fetch.getUrl());

		Elements counter_elements = doc.select(".lpStTit strong");		
		String product_size_text = counter_elements.text();
		my_info.setProducts_size(product_size_text);
		boolean isFacetteOpened = false;

		Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
		for (Element facette : facette_elements ){
			//System.out.println(e.toString());
			Elements facette_name = facette.select("div.mvFTitle.noSel");
			my_info.setFacetteName(facette_name.text());
			Elements facette_values = facette.select("a");
			for (Element facette_value : facette_values){		
				//System.out.println(facette_value);
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
				my_info.setIs_opened(isFacetteOpened);
				my_fetched_infos.add(my_info);
				my_info = new AdvancedFacettesInfo();
				my_info.setId(to_fetch.getId());
				my_info.setUrl(to_fetch.getUrl());
				my_info.setProducts_size(product_size_text);
				my_info.setFacetteName(facette_name.text());
			}		
		}
		return my_fetched_infos;
	}
}
