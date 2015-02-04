package crawl4j.xpathutility;

import java.io.BufferedReader;
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
import org.xml.sax.SAXException;

public class XPathUtility {

	private static String xpathconf_path = "/home/sduprey/My_Data/My_Xpath_Conf/xpath.conf";
	private static int xpath_size = 10;
	public static String parseContent(String content, String xpathExpression) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		TagNode tagNode = new HtmlCleaner().clean(content);
		org.w3c.dom.Document doc = new DomSerializer(
				new CleanerProperties()).createDOM(tagNode);
		XPath xpath = XPathFactory.newInstance().newXPath();

		String str = "";
		if (xpathExpression != null){
			str = (String) xpath.evaluate(xpathExpression, 
					doc, XPathConstants.STRING);
		}
		return str;
	}

	public static String[] loadXPATHConf(){
		String[] xpath_expression = new String[xpath_size];
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(xpathconf_path));

			String line="";
			int counter = 0;
			while (((line = br.readLine()) != null ) && counter < xpath_size) {
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

	public static String[] parse_page_code_source(String page_source_code, String[] xpathExpressions) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{
		String[] xpathResults = new String[xpath_size];
		int local_counter = 0;
		for (String xpath : xpathExpressions){
			String content = XPathUtility.parseContent(page_source_code, xpath);
			xpathResults[local_counter]=content;
			local_counter++;
		}
		return xpathResults;
	}

}
