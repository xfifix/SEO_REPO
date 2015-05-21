package com.facettes.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.facettes.data.AdvancedFacettesInfo;
import com.facettes.data.URLFacettesData;

public class FacettesUtility {

	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");

	public static List<AdvancedFacettesInfo> extract_facettes_infos(org.jsoup.nodes.Document doc, URLFacettesData to_fetch){
		List<AdvancedFacettesInfo> my_fetched_infos = new ArrayList<AdvancedFacettesInfo>();
		int idUrl = to_fetch.getId();
		String url = to_fetch.getUrl();
		// fetching the URL and parsing the results


		AdvancedFacettesInfo my_info = new AdvancedFacettesInfo();
		my_info.setId(idUrl);
		my_info.setUrl(url);

		Elements counter_elements = doc.select(".lpStTit strong");		
		String product_size_text = counter_elements.text();
		product_size_text=product_size_text.replace("produits", "");
		product_size_text=product_size_text.trim();
		int pd_size = 0;
		try{
			pd_size = Integer.valueOf(product_size_text);
		} catch (NumberFormatException e){
			System.out.println("Trouble while formatting a facette");
		}
		my_info.setProducts_size(pd_size);
		boolean isFacetteOpened = false;

		Elements facette_elements = doc.select("div.mvFacets.jsFCategory.mvFOpen");			
		for (Element facette : facette_elements ){
			//System.out.println(e.toString());
			Elements facette_name = facette.select("div.mvFTitle.noSel");
			my_info.setFacetteName(facette_name.text());
			Elements facette_values = facette.select("a");
			for (Element facette_value : facette_values){		
				// checking if the facette value is opened
				String lftiretURL = facette_value.attr("href");
				if (!"".equals(lftiretURL)){
					my_info.setIs_opened(true);
					my_info.setOpenedURL(lftiretURL);
				}
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
				my_info.setProducts_size(pd_size);
				my_info.setFacetteName(facette_name.text());
			}		
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
		product_size_text=product_size_text.replace("produits", "");
		product_size_text=product_size_text.trim();
		int pd_size = 0;
		try{
			pd_size = Integer.valueOf(product_size_text);
		} catch (NumberFormatException e){
			System.out.println("Trouble while formatting a facette");
		}
		my_info.setProducts_size(pd_size);
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
				my_info.setProducts_size(pd_size);
				my_info.setFacetteName(facette_name.text());
			}		
		}
		return my_fetched_infos;
	}

	public static List<AdvancedFacettesInfo> merge_facettes(List<AdvancedFacettesInfo> market_place_facettes, List<AdvancedFacettesInfo> all_facettes){
		Map<String, Integer> market_place_counter = new HashMap<String, Integer>();
		for (AdvancedFacettesInfo market_place_info : market_place_facettes){
			int facetteValueInt = 0;
			try{
				facetteValueInt = Integer.valueOf(market_place_info.getFacetteCount());
			} catch (NumberFormatException e){
				System.out.println("Trouble while formatting a facette");
			}
			String key = market_place_info.getFacetteName()+market_place_info.getFacetteValue();
			System.out.println(key);
			market_place_counter.put(key,facetteValueInt);
		}

		Map<String, Integer> all_counter = new HashMap<String, Integer>();
		for (AdvancedFacettesInfo place_info : all_facettes){
			int facetteValueInt = 0;
			try{
				facetteValueInt = Integer.valueOf(place_info.getFacetteCount());
			} catch (NumberFormatException e){
				System.out.println("Trouble while formatting a facette");
			}
			String key = place_info.getFacetteName()+place_info.getFacetteValue();
			all_counter.put(key,facetteValueInt);
		}

		List<AdvancedFacettesInfo> merged_facettes = new ArrayList<AdvancedFacettesInfo>();
		for (AdvancedFacettesInfo place_info_to_add : all_facettes){			
			AdvancedFacettesInfo to_replicate = new AdvancedFacettesInfo();
			to_replicate.setId(place_info_to_add.getId());
			to_replicate.setUrl(place_info_to_add.getUrl());
			to_replicate.setFacetteName(place_info_to_add.getFacetteName());
			to_replicate.setFacetteValue(place_info_to_add.getFacetteValue());
			to_replicate.setFacetteCount(place_info_to_add.getFacetteCount());
			to_replicate.setIs_opened(place_info_to_add.isIs_opened());
			to_replicate.setOpenedURL(place_info_to_add.getOpenedURL());
			to_replicate.setProducts_size(place_info_to_add.getProducts_size());
			// computing here the market place ration
			String key = place_info_to_add.getFacetteName()+place_info_to_add.getFacetteValue();
			Integer allCounter = all_counter.get(key);
			Integer marketPlaceCounter = market_place_counter.get(key);
			to_replicate.setMarketPlaceFacetteCount(marketPlaceCounter == null ? 0 : marketPlaceCounter);
			double market_place_quote_part = 0.;
			try{
				market_place_quote_part = (((double)marketPlaceCounter)/((double)allCounter)*100.0);
			} catch (Exception e){
				System.out.println("Trouble while formatting a facette");
			}
			if (market_place_quote_part>100.0){
				to_replicate.setFacetteCount(place_info_to_add.getFacetteCount());
				to_replicate.setMarketPlaceFacetteCount(place_info_to_add.getFacetteCount());
				to_replicate.setMarket_place_quote_part(100.0);
			}else {
				to_replicate.setMarket_place_quote_part(market_place_quote_part);	
			}
			merged_facettes.add(to_replicate);
		}

		return merged_facettes;
	}

	public static String[] getFirstLevels(String url){
		String[] to_return = {"","",""};
		url = url.replace("http://www.cdiscount.com/","");
		StringTokenizer tokenize = new StringTokenizer(url,"/");
		int counter = 0 ;
		while (tokenize.hasMoreTokens() && counter<3){
			to_return[counter] = tokenize.nextToken();
			counter ++;
		}		
		return to_return;
	}


}
