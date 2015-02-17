package com.parsing.utility;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class ProductsListParseUtility {
	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");
	public static URLComparisonListProductsInfo parse_page_source_code(String page_code){
		// fetching data using Jsoup and jQuery
	
	
		org.jsoup.nodes.Document doc =  Jsoup.parse(page_code);
	

//			<select id="lpGeoloc" name="GeolocForm.GeolocAddress"><option value="">Localisez vous</option>
//			<option value="06 - Juan les pins">06 - Juan les pins</option>
//			<option value="13 - Marseille">13 - Marseille</option>
//			<option value="30 - Nimes">30 - Nimes</option>
//			<option value="33 - Begles">33 - Begles</option>
//			<option value="34 - Peret">34 - Peret</option>
//			<option value="54 - Longwy-haut">54 - Longwy-haut</option>
//			<option value="67 - Saverne">67 - Saverne</option>
//			<option value="76 - St pierre les elbeuf">76 - St pierre les elbeuf</option>
//			<option value="77 - La ferte-gaucher">77 - La ferte-gaucher</option>

			Elements geoloc_elems = doc.select("#lpGeoloc");
			// blocs du dessus
			Elements short_desc_el = doc.select("div.lpTBig");
			Elements above_blocs = doc.select("div.lpTSmall");

			// corps de page : blocs du milieu
			//<li data-sku="1254484"><div class="prdtBloc">   
			Elements core_listed_blocs = doc.getElementsByAttribute("data-sku");
			
			// number of products on the first page
			//<div class="lpStTit">                    <strong>1&nbsp;745 produits</strong> triés par :</div>
			Elements number_of_products_blocs = doc.select("div.lpStTit");
			
			
			// facettes
			Elements facette_elements = doc.select("div.mvFilter");			
			for (Element facette : facette_elements ){
				//System.out.println(e.toString());
				Elements facette_name = facette.select("div.mvFTit");
				System.out.println(facette_name.text());
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
						System.out.println(Integer.valueOf(categorieCount));
						//System.out.println(Integer.valueOf(categorieCount));	
					} catch (NumberFormatException e){
						e.printStackTrace();
					
					}
					System.out.println(categorie_value);
				}		
			}



			//
			//
			//

			for (Element e : short_desc_el ){
				System.out.println("@@@@@@@@@@");
				//System.out.println(e.toString());
				Elements s = e.select("div.mvFTit");
				System.out.println(s.text());
				//Elements ss = e.getElementsByAttributeValue("name","FacetForm.SelectedFacets");
				Elements ss = e.select("a");
				//System.out.println(ss.toString());
				for (Element sss : ss){
					System.out.println(sss.text());
				}		
			}

			System.out.println(short_desc_el.toString());
			Elements short_desc_e = short_desc_el.select("div.mvFit");
		
	
		return null;
	}
}
