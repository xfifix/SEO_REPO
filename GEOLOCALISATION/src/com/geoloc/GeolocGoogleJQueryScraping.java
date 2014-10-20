package com.geoloc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class GeolocGoogleJQueryScraping {
	public static void main(String[] args) throws UnsupportedEncodingException, IOException{
		String[] seeds = {"d%E9guisement","guitare", "smartphone","ordinateur%20portable","cigarette%20%E9lectronique"};
		for (String seed : seeds){
			//seed=seed+"%20Nantes";
			String target_name = "cdiscount.com";
			String location = "Nantes,Pays de la Loire,France";
			int my_length = location.length();
			KeysCodec.populate();
			String codec = KeysCodec.getCodecs().get(my_length);
			// encode data on your side using BASE64
			byte[]  bytesEncoded = Base64.encodeBase64(location.getBytes());
			System.out.println("ecncoded value is " + new String(bytesEncoded ));
			String encoded_location=new String(bytesEncoded);
			// Decode data on other side, by processing encoded data
			byte[] valueDecoded= Base64.decodeBase64(bytesEncoded );
			System.out.println("Decoded value is " + new String(valueDecoded));
			String final_string = KeysCodec.stub+codec+encoded_location;
			// depth parameter
			int nb_depth = 5;
			int my_rank=50;
			int depth=0;
			int nb_results=0;
			String my_url = "";
			String found_url="";
			boolean found = false;
			while (depth<nb_depth && !found){
				// building the URL
				//my_url = "https://www.google.fr/search?q="+seed+"&start="+Integer.toString(depth*10);	
				my_url = "https://www.google.fr/search?q="+seed+"&uule="+final_string+"&start="+Integer.toString(depth*10);
				URL url = new URL(my_url);
				HttpURLConnection connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)");
				connection.setInstanceFollowRedirects(false);
				connection.connect();
				System.out.println(connection.getResponseCode());
				InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
				BufferedReader buff = new BufferedReader(in);
				StringBuilder builder = new StringBuilder();
				String line;
				do {
					line = buff.readLine();
					builder.append(line);
				} while (line != null);
				String to_parse = builder.toString();
				Document doc = Jsoup.parse(to_parse);
				// checking the rank with Jquery

				Elements serps = doc.select("h3[class=r]");
				for (Element serp : serps) {
					Element link = serp.getElementsByTag("a").first();
					String linkref = link.attr("href");
					if (linkref.startsWith("/url?q=")){
						nb_results++;
						linkref = linkref.substring(7,linkref.indexOf("&"));
					}
					if (linkref.contains(target_name)){
						my_rank=nb_results;
						found_url=linkref;
						found=true;
					}			
					//					System.out.println("Link ref: "+linkref);
					//					System.out.println("Title: "+serp.text());
				}
				if (nb_results == 0){
					System.out.println("Warning captcha");
				}
				depth++;
			}
			System.out.println(seed + " ranks :"+my_rank);
			System.out.println(found_url);
		}
	}

	public static int randInt(int min, int max) {
		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
}
