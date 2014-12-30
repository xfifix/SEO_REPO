package com.omniture;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class SoftOmnitureCrawler {

	private static String userAgent = "CdiscountBot-crawler";
	private static String[] scenario1 = {
		"prodView", 
		"prop1", 
		"prop2", 
		"prop3", 
		"prop4", 
		"prop5", 
		"prop6", 
		"prop7", 
		"prop14", 
		"prop16", 
		"prop17", 
		"prop18", 
		"eVar7", // et pas evar7
		"eVar52",// et pas evar52
		"prop1=\"Cdiscount\"",
		"prop2=\"Informatique\"",
		"prop3=\"Tablettes tactiles\"",
		"prop4=\"Tablette tactile\"",
		"prop6=\"Samsung\"",
		"prop7=\"FicheProduit CD\""
	};

	private static String[] scenario2 = {
		"prop1", 
		"prop2",
		"prop7",
		"prop14",
		"prop16",
		"prop17",
		"prop18",
		"prop63",
		"eVar4", // et pas evar4
		"eVar58", // et pas evar58
		"prop1=\"Cdiscount\"",
		"prop2=\"HomePage\"",
		"prop7=\"Homepage\"", // et pas "prop7=\"HomePage\""
		"eVar4=\"HomePage\"",
		"eVar58=((typeof $.cookies === \"object\" && typeof $.cookies.get === \"function\") ? $.cookies.get(\"MID\").substring($.cookies.get(\"MID\").length - 100).split(\"&\")[0] : \"\")+\":\"+((typeof $.cookies === \"object\" && typeof $.cookies.get === \"function\") ? $.cookies.get(\"GUID\").substring($.cookies.get(\"GUID\").length - 100).split(\"&\")[0] : \"\");"		
	};
	
	private static String[] scenario3 = {
		"scView", 
		"prop1", 
		"prop2", 
		"prop3", 
		"prop7", 
		"prop14", 
		"prop16", 
		"prop17", 
		"prop18", 
		"prop62", 
		"prop63",
		"prop1=\"Cdiscount\"",
		"prop2=\"Orderprocess\"",
		"prop3=\"Panier\"",
		"prop7=\"Panier\"",
		"prop14=(typeof $.cookies === \"object\" && typeof $.cookies.get === \"function\") ? $.cookies.get(\"SiteVersionCookie\").substring($.cookies.get(\"SiteVersionCookie\").length - 100) : \"\";",
		"eVar8=\"\"",
		"eVar11=\"\"",
		"eVar30=(typeof $.cookies === \"object\" && typeof $.cookies.get === \"function\") ? $.cookies.get(\"ClientId\").substring($.cookies.get(\"ClientId\").length - 100).split(\"&\")[0] : \"\""
	};

	public static void main(String[] args) throws IOException{	
		
		String firstURL = "http://www.cdiscount.com/informatique/tablettes-tactiles-ebooks/samsung-galaxy-tab-3-10-1-16go/f-10798010207-gtp5210zwaxef.html";	
		String firstContent = fetch_content(firstURL);
		System.out.println("Checking first scenario : " + checkFirstScenario(firstContent));
		
		String secondURL = "http://www.cdiscount.com/";
		String secondContent = fetch_content(secondURL);
		System.out.println("Checking second scenario : " + checkSecondScenario(secondContent));

		String thirdURL = "http://www.cdiscount.com/Basket.html";  
		String thirdContent = fetch_content(thirdURL);
		System.out.println("Checking third scenario : " + checkThirdScenario(thirdContent));
	}

	public static String fetch_content(String url_to_fetch) throws IOException{
		HttpURLConnection connection = null;
		System.out.println(Thread.currentThread().getName()+" fetching URL : "+url_to_fetch);
		URL url = new URL(url_to_fetch);
		connection = (HttpURLConnection)url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("User-Agent",userAgent);
		connection.setInstanceFollowRedirects(true);
		connection.setConnectTimeout(3000);
		connection.connect();
		// getting the status from the connection
		System.out.println(" Response code " + connection.getResponseCode());
		// getting the content to parse
		InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
		BufferedReader buff = new BufferedReader(in);
		String content_line;
		StringBuilder builder=new StringBuilder();
		do {
			content_line = buff.readLine();
			builder.append(content_line);
		} while (content_line != null);
		String html = builder.toString();

		if (connection != null){
			connection.disconnect();
		}
		return html;
	}
	
	public static boolean checkFirstScenario(String content){

		boolean isScenarioOK = true;
		for (String localScenario : scenario1){
			if (!content.contains(localScenario)){
				isScenarioOK = false;
				System.out.println("Content missing for scenario 1 : "+localScenario);
			}
		}
		return isScenarioOK;
	}
	
	public static boolean checkSecondScenario(String content){
		boolean isScenarioOK = true;
		for (String localScenario : scenario2){
			if (!content.contains(localScenario)){
				isScenarioOK = false;
				System.out.println("Content missing for scenario 2 : "+localScenario);
			}
		}
		return isScenarioOK;
	}
	
	public static boolean checkThirdScenario(String content){
		// scView, prop1, prop2, prop3, prop7, prop14, prop16, prop17, prop18, prop62, prop63 must be present
		// they must take the following values
		//prop1="Cdiscount"
		//prop2="Orderprocess"
		//prop3="Panier"
		//prop7="Panier"
		//prop14=(typeof $.cookies === "object" && typeof $.cookies.get === "function") ? $.cookies.get("SiteVersionCookie").substring($.cookies.get("SiteVersionCookie").length - 100) : "";    s.eVar8="";    s.eVar11="";    s.eVar30=(typeof $.cookies === "object" && typeof $.cookies.get === "function") ? $.cookies.get("ClientId").substring($.cookies.get("ClientId").length - 100).split("&")[0] : ""
		boolean isScenarioOK = true;
		for (String localScenario : scenario3){
			if (!content.contains(localScenario)){
				isScenarioOK = false;
				System.out.println("Content missing for scenario 3 : "+localScenario);
			}
		}
		return isScenarioOK;
	}
} 
