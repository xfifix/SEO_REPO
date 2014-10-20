package com.processing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.cybozu.labs.langdetect.LangDetectException;

public class RDCRequestingClass {

	public static List<String> fetch(String seeds) throws IOException, ParseException, LangDetectException{
		List<String> my_words = new ArrayList<String>();

		URL url_rdc = new URL("http://search-async.rueducommerce.fr/sengine/SearchCompletionController.php?s="+seeds);	
		BufferedReader in = new BufferedReader(
				new InputStreamReader(url_rdc.openStream(),"UTF-8"));
		String inputLine;
		StringBuilder toparse = new StringBuilder();
		while ((inputLine = in.readLine()) != null){
			System.out.println(inputLine);
			toparse.append(inputLine);
		}
		in.close();
        String my_string=toparse.toString();
        // dropping the parentheses in between
        my_string=my_string.replace("(","");
        my_string=my_string.replace(")","");
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonContent = (JSONObject)jsonParser.parse(my_string);
			JSONArray keyword_array = (JSONArray)jsonContent.get("keywords");
			for (Object my_keyword  : keyword_array){
				my_words.add(my_keyword.toString());
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
