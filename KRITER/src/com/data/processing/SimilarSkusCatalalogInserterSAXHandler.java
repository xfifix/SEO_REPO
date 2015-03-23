package com.data.processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class SimilarSkusCatalalogInserterSAXHandler extends DefaultHandler {

	private Connection con;
	private static int batch_size = 10000;
	private static String update_statement = "UPDATE CATALOG SET KRIT_SKU1=?,KRIT_SKU2=?,KRIT_SKU3=?,KRIT_SKU4=?,KRIT_SKU5=?,KRIT_SKU6=? where SKU=?";
		
	public SimilarSkusCatalalogInserterSAXHandler(Connection con){
		super();
		this.con = con;
	}

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
			if (skus.size() == batch_size){
				insert_cache();
				skus.clear();
			}
		}
	}

	public void insert_cache(){
		System.out.println("Inserting the cache "+batch_size);
		try{
			Iterator<Entry<Sku, List<Sku>>> it = skus.entrySet().iterator();
			int local_counter = 0;
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(update_statement);
			while (it.hasNext()){
				local_counter++;
				Map.Entry<Sku, List<Sku>> pairs = (Map.Entry<Sku, List<Sku>>)it.next();
				Sku current_sku=pairs.getKey();
				List<Sku> similar_skus = pairs.getValue();
				// preparing the statement
				st.setString(1,similar_skus.get(0).getName());
				st.setString(2,similar_skus.get(1).getName());
				st.setString(3,similar_skus.get(2).getName());
				st.setString(4,similar_skus.get(3).getName());
				st.setString(5,similar_skus.get(4).getName());
				st.setString(6,similar_skus.get(5).getName());
				st.setString(7,current_sku.getName());
				st.addBatch();
			}
			st.executeBatch();
			con.commit();
			st.close();
			System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
		} catch (SQLException e){
			//System.out.println("Line already inserted : "+nb_lines);
			e.printStackTrace();  
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException ex1) {
					ex1.printStackTrace();
				}
			}
			e.printStackTrace();
		}	
	}

	public void characters(char ch[], int start, int length)
			throws SAXException {
		String value = new String(ch, start, length).trim();
		if(value.length() == 0) return; // ignore white space
		if(SKU.equals(currentElement()) &&
				PRODUCT.equals(currentElementParent())){
			//System.out.println("Adding product : "+value);
			currentProduct.getSku().setName(value);
		}
		if(SKU.equals(currentElement()) &&
				SIMILAR_PRODUCT.equals(currentElementParent())){
			//System.out.println("Adding similar product : "+value+" to product "+currentProduct.getSku().getName());
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