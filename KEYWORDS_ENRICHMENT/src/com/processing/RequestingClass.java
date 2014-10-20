package com.processing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.Language;

public class RequestingClass {

	public static List<String> fetch(String seeds) throws IOException, ParseException, LangDetectException{
		List<String> my_words = new ArrayList<String>();

		// just append your keywords at the end of the url
		// beware to use proxies
		String example_seeds="toto";
		String google_auto_completion="http://google.com/complete/search?output=toolbar&q="+example_seeds;
		String rdc_auto_completion = "http://search-async.rueducommerce.fr/sengine/SearchCompletionController.php?s="+example_seeds;
		String amazon_completion = "http://completion.amazon.co.uk/search/complete?method=completion&q="+example_seeds+"&search-alias=aps&client=amazon-search-ui&mkt=5&fb=1&xcat=0&cf=0";
		String price_minister = "http://www.priceminister.com/completion?q=gs_"+example_seeds+"&c=frc";
		String darty = "http://www.darty.com/nav/extra/ajax/autosug?qr="+example_seeds;
						
		// to do pixmania && mistergooddeal
		
		
		// french language auto completion url
		// http://completion.amazon.co.uk/search/complete?method=completion&q=tortue&search-alias=aps&client=amazon-search-ui&mkt=5&fb=1&xcat=0&cf=0&x=updateISSCompletion&sc=1&noCacheIE=1407139871789
		// french epurated URL :
		// http://completion.amazon.co.uk/search/complete?method=completion&q=tortue&search-alias=aps&client=amazon-search-ui&mkt=5&fb=1&xcat=0&cf=0		

		// all languages epurated requesting URLs
		// http://completion.amazon.co.uk/search/complete?method=completion&q=terrasse&search-alias=aps&client=amazon-search-ui&mkt=5&fb=1&xcat=0&cf=0&x=updateISSCompletion&sc=1&noCacheIE=1407139871789	

		// http://completion.amazon.com/search/complete?method=completion&q=terrasse&search-alias=aps&client=amazon-search-ui&mkt=1
		// french epurated URL
		URL url_amazon = new URL("http://completion.amazon.co.uk/search/complete?method=completion&q="+seeds+"&search-alias=aps&client=amazon-search-ui&mkt=5&fb=1&xcat=0&cf=0");
		//		// english epurated URL
		//		URL url_amazon = new URL("http://completion.amazon.com/search/complete?method=completion&q="+seeds+"&search-alias=aps&client=amazon-search-ui&mkt=1");
		BufferedReader in = new BufferedReader(
				new InputStreamReader(url_amazon.openStream(),"UTF-8"));

		try {
			JSONParser jsonParser = new JSONParser();
			JSONArray jsonContent = (JSONArray)jsonParser.parse(in);

			Iterator<?> result = jsonContent.iterator();
			if (result.hasNext()) {
				String you_typed=(String)result.next();
				// Here I try to take the title element from my slide but it doesn't work!
				//     String title = (String) jsonObject.get("title");
				JSONArray you_got=(JSONArray)result.next();
				Iterator<?> suggestion_iterator = you_got.iterator();
				while(suggestion_iterator.hasNext()){
					String suggested_item=(String)suggestion_iterator.next();
					//				String word_language = "";
					//				try {
					//				Detector detector = DetectorFactory.create();
					//				detector.append(suggested_item);
					//				word_language = detector.detect();
					//				//ArrayList<Language> langlist = detector.getProbabilities();
					//				} catch (Exception e){
					//					e.printStackTrace();
					//				}
					//				if ("fr".equals(word_language)){
					//					my_words.add(suggested_item);
					//				}
					////				// another politics
					////				if (!"en".equals(word_language)){
					////					my_words.add(suggested_item);
					////				}				
					my_words.add(suggested_item);
				}
			}
		}catch (Exception e){
			e.printStackTrace();
		}
		return my_words;
	}

	public static void main(String[] args) throws IOException, ParseException, LangDetectException{

		String[] test = {"piscine","chaussure","sucre","pioneer"};
		for (int i = 0; i< test.length; i++){
			URL url_amazon = new URL("http://completion.amazon.com/search/complete?method=completion&q="+test[i]+"&search-alias=aps&client=amazon-search-ui&mkt=1");
			BufferedReader in = new BufferedReader(
					new InputStreamReader(url_amazon.openStream(),"UTF-8"));

			JSONParser jsonParser = new JSONParser();
			JSONArray jsonContent = (JSONArray)jsonParser.parse(in);

			Iterator<?> result = jsonContent.iterator();
			if (result.hasNext()) {
				String you_typed=(String)result.next();
				System.out.println("Your request :"+you_typed);	
				// Here I try to take the title element from my slide but it doesn't work!
				//     String title = (String) jsonObject.get("title");
				JSONArray you_got=(JSONArray)result.next();
				Iterator<?> suggestion_iterator = you_got.iterator();
				while(suggestion_iterator.hasNext()){
					String suggested_item=(String)suggestion_iterator.next();
					//String yesorno=LangDetectSample.detect(suggested_item);
					System.out.println("You got :"+suggested_item);
					//	System.out.println(yesorno);					
				}
			}
		}


		//		JSONObject jsonObject = (JSONObject) jsonParser.parse(in);
		//		String firstName = (String) jsonObject.get("firstname");
		//		System.out.println("The first name is: " + firstName);
		//
		//		// get a number from the JSON object
		//		long id =  (long) jsonObject.get("id");
		//		System.out.println("The id is: " + id);
		//
		//		// get an array from the JSON object
		//		JSONArray lang= (JSONArray) jsonObject.get("languages");
		//
		//		// take the elements of the json array
		//		for(int i=0; i<lang.size(); i++){
		//			System.out.println("The " + i + " element of the array: "+lang.get(i));
		//		}
		//
		//		Iterator i = lang.iterator();
		//
		//		// take each value from the json array separately
		//		while (i.hasNext()) {
		//
		//			JSONObject innerObj = (JSONObject) i.next();
		//			System.out.println("language "+ innerObj.get("lang") +
		//					" with level " + innerObj.get("knowledge"));
		//
		//		}
		//		// handle a structure into the json object
		//		JSONObject structure = (JSONObject) jsonObject.get("job");
		//		System.out.println("Into job structure, name: " + structure.get("name"));

	}
}
