package com.parsing.utility;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

public class XPathUtility {

	private static String xpathconf_path = "/home/sduprey/My_Data/My_Xpath_Conf/xpath.conf";

	public static String parseContent(String content, String xpathExpression) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		TagNode tagNode = new HtmlCleaner().clean(content);
		org.w3c.dom.Document doc = new DomSerializer(
				new CleanerProperties()).createDOM(tagNode);
		XPath xpath = XPathFactory.newInstance().newXPath();
		String str = (String) xpath.evaluate(xpathExpression, 
				doc, XPathConstants.STRING);
		return str;
	}

	public static String[] loadXPATHConf(){
		String[] xpath_expression = new String[5];
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(xpathconf_path));

			String line="";
			int counter = 0;
			while (((line = br.readLine()) != null ) && counter < 5) {
				xpath_expression[counter] = line;
				counter ++;
			}
			br.close();	
		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return xpath_expression;
	}

	public static ParsingOutput parse_page_code_source(String page_source_code, String[] xpathExpressions) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{
		ParsingOutput output = new ParsingOutput();
		org.jsoup.nodes.Document doc = Jsoup.parse(page_source_code);
		Elements h1s = doc.select("h1");
		String conc_h1="";
		for (Element h1 : h1s) {
			conc_h1=conc_h1+h1.text();
		}	
		output.setH1(conc_h1);
		Elements titles = doc.select("title");
		String conc_title="";
		for (Element title : titles) {
			conc_title=conc_title+title.text();
		}				
		output.setTitle(conc_title);
		String[] xpathResults = new String[5];
		int local_counter = 0;
		for (String xpath : xpathExpressions){
			String content = XPathUtility.parseContent(page_source_code, xpath);
			xpathResults[local_counter]=content;
			local_counter++;
		}
		output.setXpathResults(xpathResults);
		return output;
	}

}
