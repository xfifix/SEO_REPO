package com.data.processing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SimilarSkusSAXHandler extends DefaultHandler {

	private static String PRODUCT = "Product";
	private static String SIMILAR_PRODUCT = "SimilarProductList";
	private static String SKU = "Sku";

	public Map<Sku, List<Sku>> skus = new HashMap<Sku, List<Sku>>();

	// calling stack
	private Stack<String> elementStack = new Stack<String>();
	// current product
	private Product currentProduct;

	public void startElement(String uri, String localName,
			String qName, Attributes attributes) throws SAXException {
		this.elementStack.push(qName);
		if(PRODUCT.equals(qName)){
			currentProduct = new Product();		
		} 
	}

	public void endElement(String uri, String localName,
			String qName) throws SAXException {
		this.elementStack.pop();
		if(PRODUCT.equals(qName)){
			// we here put every thing in the map from our current product
			skus.put(currentProduct.getSku(), currentProduct.getSimilar_skus());
		}
	}

	public void characters(char ch[], int start, int length)
			throws SAXException {
		String value = new String(ch, start, length).trim();
		if(value.length() == 0) return; // ignore white space
		if(SKU.equals(currentElement()) &&
				PRODUCT.equals(currentElementParent())){
			System.out.println("Adding product : "+value);
			currentProduct.getSku().setName(value);
		}
		if(SKU.equals(currentElement()) &&
				SIMILAR_PRODUCT.equals(currentElementParent())){
			System.out.println("Adding similar product : "+value+" to product "+currentProduct.getSku().getName());
			currentProduct.getSimilar_skus().add(new Sku(value));
		}
	}

	private String currentElement() {
		return this.elementStack.peek();
	}

	private String currentElementParent() {
		if(this.elementStack.size() < 2) return null;
		return this.elementStack.get(this.elementStack.size()-2);
	}
}    