package com.parsing.utility;

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
	public static String parseContent(String content, String xpathExpression) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException{
		TagNode tagNode = new HtmlCleaner().clean(content);
		org.w3c.dom.Document doc = new DomSerializer(
		        new CleanerProperties()).createDOM(tagNode);
		XPath xpath = XPathFactory.newInstance().newXPath();
		String str = (String) xpath.evaluate(xpathExpression, 
                doc, XPathConstants.STRING);
		return str;
	}
}
