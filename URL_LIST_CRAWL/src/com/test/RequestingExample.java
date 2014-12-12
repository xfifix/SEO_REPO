package com.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.xml.sax.SAXException;

import com.parsing.utility.XPathUtility;

public class RequestingExample {
	public static void main(String[] args) throws MalformedURLException, IOException, ParserConfigurationException, SAXException, XPathExpressionException{
		String[] urls = {
				"http://www.cdiscount.com/electromenager/tous-nos-accessoires/joint-hublot-d-30-30-cm/f-11029-ind3662734065501.html",
				"http://www.cdiscount.com/le-sport/vetements-de-sport/kappa-survetement-armor-homme/f-121020526-3025ej0005.html",
				"http://www.cdiscount.com/animalerie/chiens/lot-de-3-sofas-pour-chien/f-1621004-ifd19945rouge.html",
				"http://www.cdiscount.com/maison/tapis/rio-tapis-shaggy-anthracite-30-mm-160x230-cm/f-1172512-r252an160230.html",
		"http://www.cdiscount.com/maison/tapis/torino-tapis-de-salon-shaggy-gris-160x230-cm/f-1172512-7326gri160230.html"};

		String[] xpaths = {
				"//h1/text()",
				"/p[@class=\"fpMb\"]/text()",
				"//div[@class='fpBlk extCode']",
				"//form/p[1]/a/text()",
		"//table[@class=\"fpDescTb\"]/tbody/tr/td[1]"};

		//String my_url_to_fetch = "http://www.cdiscount.com/electromenager/tous-nos-accessoires/joint-hublot-d-30-30-cm/f-11029-ind3662734065501.html#mpos=2|mp";
		//String my_url_to_fetch = "http://www.cdiscount.com/le-sport/vetements-de-sport/kappa-survetement-armor-homme/f-121020526-3025ej0005.html#mpos=1|cd";
		//String my_url_to_fetch = "http://www.cdiscount.com/animalerie/chiens/lot-de-3-sofas-pour-chien/f-1621004-ifd19945rouge.html";
		//String my_url_to_fetch = "http://www.cdiscount.com/maison/tapis/rio-tapis-shaggy-anthracite-30-mm-160x230-cm/f-1172512-r252an160230.html";
		//String my_url_to_fetch = "http://www.cdiscount.com/maison/tapis/torino-tapis-de-salon-shaggy-gris-160x230-cm/f-1172512-7326gri160230.html";
		// fetching data using jQuery


		//		String charset = "UTF-8";  // Or in Java 7 and later, use the constant: java.nio.charset.StandardCharsets.UTF_8.name()
		//		String param1 = "value1";
		//		String param2 = "value2";
		//		// ...

		//		String query = String.format("param1=%s&param2=%s", 
		//		     URLEncoder.encode(param1, charset), 
		//		     URLEncoder.encode(param2, charset));
		//		URLConnection connection = new URL(my_url_to_fetch + "?" + query).openConnection();
		//		connection.setRequestProperty("Accept-Charset", charset);


		for (String my_url_to_fetch : urls){
			HttpURLConnection connection = (HttpURLConnection)new URL(my_url_to_fetch).openConnection();
			connection.setRequestProperty("User-Agent","CdiscountBot-crawler");
			connection.connect();
			InputStreamReader in = new InputStreamReader(connection.getInputStream());
			BufferedReader buff = new BufferedReader(in);
			StringBuilder builder = new StringBuilder();
			String line;
			do {
				line = buff.readLine();
				builder.append(line);
			} while (line != null);
			String pageString = builder.toString();
			for (String xpath : xpaths){
				String content = XPathUtility.parseContent(pageString, xpath);
				System.out.println("URL content " + content);
			}
			connection.disconnect();		
		}
	}



}
