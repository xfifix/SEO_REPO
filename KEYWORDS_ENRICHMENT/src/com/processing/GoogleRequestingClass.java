package com.processing;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.json.simple.parser.ParseException;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.cybozu.labs.langdetect.LangDetectException;

public class GoogleRequestingClass {

	public static List<String> fetch(String seeds) throws IOException, ParseException, LangDetectException, SAXException, ParserConfigurationException, XPathExpressionException{
		List<String> my_words = new ArrayList<String>();	
		String url = "http://google.com/complete/search?output=toolbar&q="+seeds;
		// XPATH version
		URL url_google = new URL(url);
		BufferedReader in = new BufferedReader(
				new InputStreamReader(url_google.openStream(),"UTF-8"));
		String inputLine;
		StringBuilder toparse = new StringBuilder();
		while ((inputLine = in.readLine()) != null){
			System.out.println(inputLine);
			toparse.append(inputLine);
		}
		in.close();
		String my_string=toparse.toString();
		System.out.println(my_string);

		DocumentBuilderFactory builderFactory =
				DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		builder = builderFactory.newDocumentBuilder();
		//db.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
		Document xmlDocument = builder.parse(new InputSource(new ByteArrayInputStream(my_string.getBytes("utf-8"))));
		XPath xPath =  XPathFactory.newInstance().newXPath();
		String expression = "/toplevel/CompleteSuggestion/suggestion";
		NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
		System.out.println(nodeList);
		for (int l = 0; l < nodeList.getLength(); l++) {
			NamedNodeMap map = nodeList.item(l).getAttributes();
			Node dataNode = map.getNamedItem("data");
			System.out.println(dataNode.getTextContent()); 
			my_words.add(dataNode.getTextContent());
		}
		return my_words;
	}

	public static void main(String[] args) throws IOException, ParseException, LangDetectException, ParserConfigurationException, SAXException, XPathExpressionException{
		String[] test = {"piscine","chaussure","sucre","pioneer"};
		for (int i = 0; i< test.length; i++){
			String url = "http://google.com/complete/search?output=toolbar&q="+test[i];
			URL url_google = new URL(url);
			BufferedReader in = new BufferedReader(
					new InputStreamReader(url_google.openStream(),"UTF-8"));
			String inputLine;
			StringBuilder toparse = new StringBuilder();
			while ((inputLine = in.readLine()) != null){
				System.out.println(inputLine);
				toparse.append(inputLine);
			}
			in.close();
			String my_string=toparse.toString();
			System.out.println(my_string);


			DocumentBuilderFactory builderFactory =
					DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = null;

			builder = builderFactory.newDocumentBuilder();
			//db.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
			Document xmlDocument = builder.parse(new InputSource(new ByteArrayInputStream(my_string.getBytes("utf-8"))));
			XPath xPath =  XPathFactory.newInstance().newXPath();
			String expression = "/toplevel/CompleteSuggestion/suggestion";
			NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODESET);
			System.out.println(nodeList);
			for (int l = 0; l < nodeList.getLength(); l++) {
				NamedNodeMap map = nodeList.item(l).getAttributes();
				Node dataNode = map.getNamedItem("data");
				System.out.println(dataNode.getTextContent()); 
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
