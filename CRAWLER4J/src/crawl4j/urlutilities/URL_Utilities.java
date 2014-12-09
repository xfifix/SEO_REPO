package crawl4j.urlutilities;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

public class URL_Utilities {
	//	Voici le principe de hiérarchie pour les navid (première séquence de chiffres indiquée après identifiant type de page dansl’url ( ex : /v- , /l-, /f-)…
	//	+ 2 chiffres pour chaque sous-strate ( car 99 nœuds max pourune même strate)
	//	 
	//	Root/Home : 1
	//	Root-1/Home magasin = 1XX
	//	Root-2/Rayon : 1XXXX
	//	Root-3/Sous-rayon : 1XXXXXX
	//	Root-4 : Sous-sous rayon : 1XXXXXXXX
	//	Root-5 : 1XXXXXXXXXX
	private static String Vitrine = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/v-.*";
	private static String FicheProduit  ="^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/f-.*";
	private static String ListeProduit = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/l-.*";
	private static String ListeProduitFiltre = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/lf-.*";
	private static String PageMarque = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/m-.*";
	private static String PageConcept = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/ct-.*";
	private static String SearchDexing = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*/r-.*";

	public static String checkType(String url){
		if (url.matches(Vitrine)){
			return "Vitrine";
		}
		if (url.matches(ListeProduit)){
			return "ListeProduit";
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
		String type = checkType(url);
		if ("Vitrine".equals(type)||"ListeProduit".equals(type)||"FicheProduit".equals(type)||"SearchDexing".equals(type)){
			url = url.replace("http://www.cdiscount.com/","");
			StringTokenizer tokenize = new StringTokenizer(url,"/");
			if (tokenize.hasMoreTokens()){
				magasin=tokenize.nextToken();
			}
		}
		if ("ListeProduitFiltre".equals(checkType(url))){
			// it will soon behave as the searchdexing
		}
		return magasin;
	}

	public static String[] checkMagasinAndRayonAndProduct(String url){
		String[] output = {"Unknown","Unknown","Unknown"};
		String type = checkType(url);
		if ("Vitrine".equals(type)||"ListeProduit".equals(type)||"FicheProduit".equals(type)||"SearchDexing".equals(type)){
			url = url.replace("http://www.cdiscount.com/","");
			StringTokenizer tokenize = new StringTokenizer(url,"/");
			List<String> tokenList = new ArrayList<String>();
			while (tokenize.hasMoreTokens()){
				tokenList.add(tokenize.nextToken());
			}
			// magasin
			output[0]=tokenList.get(0);
			// rayon
			if (!"SearchDexing".equals(type)){
				output[1]=tokenList.get(1);
			}
		}

		return output;

	}

	public static String checkRayon(String url){
		String rayon = "Unknown";
		String type = checkType(url);
		if ("Vitrine".equals(type)||"ListeProduit".equals(type)||"FicheProduit".equals(type)){
			//			int vitrine_index=url.indexOf("/v-");
			//			int listeproduit_index=url.indexOf("/l-");
			//			int ficheproduit_index=url.indexOf("/f-");
			//
			//			int index = Math.max(Math.max(vitrine_index, listeproduit_index),ficheproduit_index);
			//			// we here catch the navid			
			//			String truncated_url=url.substring(0,index);
			//			String navid = url.substring(index+3, url.indexOf("-", index+3));
			//			int navid_length = navid.length();
			//			System.out.println("Navid : "+navid);
			//
			//			// we here slice the truncated url
			url = url.replace("http://www.cdiscount.com/","");
			StringTokenizer tokenize = new StringTokenizer(url,"/");
			List<String> tokenList = new ArrayList<String>();
			while (tokenize.hasMoreTokens()){
				tokenList.add(tokenize.nextToken());
			}
			if (tokenList.size()>= 2){
				rayon=tokenList.get(1);
			}
		}
		return rayon;
	}

	public static String getNavid(String url){
		String navid = "Unknown";
		if ("Vitrine".equals(checkType(url))||"ListeProduit".equals(checkType(url))||"FicheProduit".equals(checkType(url))){
			int vitrine_index=url.indexOf("/v-");
			int listeproduit_index=url.indexOf("/l-");
			int ficheproduit_index=url.indexOf("/f-");

			int index = Math.max(Math.max(vitrine_index, listeproduit_index),ficheproduit_index);
			// we here catch the navid			
			navid = url.substring(index+3, url.indexOf("-", index+3));
		}
		return navid;

	}

	public static String checkProduit(String url){
		String produit = "Unknown";
		//		url = url.replace("http://www.cdiscount.com/","");
		//		StringTokenizer tokenize = new StringTokenizer(url,"/");
		//		if (tokenize.hasMoreTokens()){
		//			tokenize.nextToken();
		//		}
		//		if (tokenize.hasMoreTokens()){
		//			tokenize.nextToken();
		//		}
		//		if (tokenize.hasMoreTokens()){
		//			produit = tokenize.nextToken();
		//		}
		return produit;
	}

	public static Set<String> parse_nodes_out_links(String output_links){
		output_links = output_links.replace("[", "");
		output_links = output_links.replace("]", "");
		String[] url_outs = output_links.split(",");
		Set<String> outputSet = new HashSet<String>();
		for (String url_out : url_outs){
			url_out=url_out.trim();
			outputSet.add(url_out);
		}
		return outputSet;
	}

	public static String drop_parameters(String url_with_possibly_parameters){
		int interrogation_index = url_with_possibly_parameters.indexOf("?");
		if (interrogation_index != -1){
			url_with_possibly_parameters=url_with_possibly_parameters.substring(0, interrogation_index);
		}
		int pound_index = url_with_possibly_parameters.indexOf("#");
		if (pound_index != -1){
			url_with_possibly_parameters=url_with_possibly_parameters.substring(0, pound_index);
		}
		return url_with_possibly_parameters;
	}

}
