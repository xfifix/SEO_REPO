package com.utility;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class AttributsUtility {

	public static String category_name = "Catégorie";
	public static String product_name = "Nom du produit";
	public static String brand_name = "Marque";

	private static String sku_fetching_url = "www.cdiscount.com/dp.aspx?sku=";
	private static String userAgent = "CdiscountBot-crawler";
	public static String crawl_sku(String SKU){
		String constructed_url = sku_fetching_url+SKU;
		System.out.println(Thread.currentThread().getName()+" fetching SKU : "+constructed_url);
		HttpURLConnection connection = null;
		try{
			//System.out.println(Thread.currentThread().getName()+" fetching URL : "+line_info.getUrl());
			URL url = new URL(constructed_url);
			connection = (HttpURLConnection)url.openConnection();
			connection.setRequestMethod("HEAD");
			connection.setRequestProperty("User-Agent",userAgent);
			connection.setInstanceFollowRedirects(true);
			connection.setConnectTimeout(30000);
			connection.connect();
			// getting the status from the connection
			int status=connection.getResponseCode();
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


			System.out.println(Thread.currentThread().getName()+" Status " +status+ " fetching URL : "+constructed_url);
		} catch (Exception e){
			System.out.println("Error with : "+SKU+" "+constructed_url);
			e.printStackTrace();
		}
		return constructed_url;
	}


	public static String extractAttributsJSON(String html){
		Document doc = Jsoup.parse(html);
		List<AttributesInfo> attributesList = new ArrayList<AttributesInfo>();
		Elements attributes = doc.select(".fpDescTb tr");
		int nb_arguments = 0 ;
		for (Element tr_element : attributes){
			Elements td_elements = tr_element.select("td");
			if (td_elements.size() == 2){
				nb_arguments++;
				AttributesInfo toAdd = new AttributesInfo();
				String category = td_elements.get(0).text();
				toAdd.setData_name(category);
				String description = td_elements.get(1).text();                                    
				toAdd.setData(description);
				//				attributesList.add(toAdd);
				//				if (category_name.equals(category)){
				//					info.setCategory(description);
				//				}
				//				if (brand_name.equals(category)){
				//					info.setBrand(description);
				//				}
				//				if (product_name.equals(category)){
				//					info.setProduit(description);
				//				}
			}
		}
		//info.setAtt_number(nb_arguments);
		String attribute_json=getAttributesJSONStringToStore(attributesList);
		//info.setAtt_desc(attribute_json);

		return attribute_json;
	}


	@SuppressWarnings("unchecked")
	public static String getAttributesJSONStringToStore(List<AttributesInfo> attributes_info){
		JSONArray attributesArray = new JSONArray();
		for (AttributesInfo info : attributes_info){
			JSONObject attributeObject = new JSONObject();
			attributeObject.put("attribute_name", info.getData_name());
			attributeObject.put("attribute_value", info.getData());
			attributesArray.add(attributeObject);
		}
		return attributesArray.toJSONString();
	}

	public static List<AttributesInfo> unserializeJSONString(String storedJSONString){
		List<AttributesInfo> to_return = new ArrayList<AttributesInfo>();
		JSONParser jsonParser = new JSONParser();
		try {
			JSONArray attributesArray = (JSONArray) jsonParser.parse(storedJSONString);
			@SuppressWarnings("rawtypes")
			Iterator i = attributesArray.iterator();
			while (i.hasNext()) {
				JSONObject innerObj = (JSONObject) i.next();
				AttributesInfo info = new AttributesInfo();
				info.setData_name((String)innerObj.get("attribute_name"));
				info.setData((String)innerObj.get("attribute_value"));
				to_return.add(info);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return to_return;
	}

	public static Map<String,String> unserializeJSONStringtoAttributesMap(String storedJSONString){
		Map<String,String> to_return = new HashMap<String,String>();
		if (!"".equals(storedJSONString)){
			JSONParser jsonParser = new JSONParser();
			try {
				JSONArray attributesArray = (JSONArray) jsonParser.parse(storedJSONString);
				@SuppressWarnings("rawtypes")
				Iterator i = attributesArray.iterator();
				while (i.hasNext()) {
					JSONObject innerObj = (JSONObject) i.next();
					to_return.put((String)innerObj.get("attribute_name"),(String)innerObj.get("attribute_value"));
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return to_return;
	}

}
