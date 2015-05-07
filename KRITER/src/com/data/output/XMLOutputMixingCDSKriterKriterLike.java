package com.data.output;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XMLOutputMixingCDSKriterKriterLike {
	private static int nb_similar_skus = 6;
	private static String database_kriter_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private static String outputPath = "/home/sduprey/My_Data/My_Outgoing_Data/My_KriterLike_Output/kriter.output";
	private static String select_all_linked_skus = "SELECT SKU,SKU1,SKU2,SKU3,SKU4,SKU5,SKU6,KRIT_SKU1,KRIT_SKU2,KRIT_SKU3,KRIT_SKU4,KRIT_SKU5,KRIT_SKU6 FROM CATALOG";
	private static Connection con;
	public static void main(String[] args){
		// Reading the property of our database for the continuous crawl
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_kriter_con_path);
			props.load(in);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");
		// we will here insert all the entries from the catalog database
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

			// root elements
			Document doc = docBuilder.newDocument();
			// similar element
			Element rootElement = doc.createElement("similar");
			doc.appendChild(rootElement);

			// staff elements
			Element productListElement = doc.createElement("ProductList");
			rootElement.appendChild(productListElement);
			// opening a database connection
			con = DriverManager.getConnection(url, user, passwd);
			System.out.println("Requesting all linked SKUs as a similar product");
			pst = con.prepareStatement(select_all_linked_skus);
			rs = pst.executeQuery();
			// dispatching to threads
			int global_count=0;
			while (rs.next()) {
				if (global_count%50 == 0){
					System.out.println(Thread.currentThread() +" Having processed "+global_count+" SKUs");
				}
				Element productElement = doc.createElement("Product");
				// the very current first sku is not linked to
				String current_sku=rs.getString(1);
				Element skuElement = doc.createElement("Sku");
				skuElement.appendChild(doc.createTextNode(current_sku));
				productElement.appendChild(skuElement);
				// all the skus getting a linked from the previous one
				Element similarProductListElement = doc.createElement("SimilarProductList");
				productElement.appendChild(similarProductListElement);
				String[] cds_similar_skus = new String[nb_similar_skus];
				String[] krit_similar_skus = new String[nb_similar_skus];
				for (int nb_sku=2;nb_sku<(nb_similar_skus+2);nb_sku++){
					String current_linked_skus = rs.getString(nb_sku);
					if (current_linked_skus == null){
						current_linked_skus = "";
					}
					cds_similar_skus[nb_sku-2]=current_linked_skus;
				}

				for (int nb_sku=(nb_similar_skus+2);nb_sku<(2*nb_similar_skus+2);nb_sku++){
					String current_linked_skus = rs.getString(nb_sku);
					if (current_linked_skus == null){
						current_linked_skus = "";
					}
					krit_similar_skus[nb_sku-(nb_similar_skus+2)]=current_linked_skus;
				}

				if (global_count%2 ==0){
					// if we have with the new CDS algo already computed the similar skus we take them
					if (cds_similar_skus[0] != null && !"".equals(cds_similar_skus[0])){
						for (int loopcounter=0;loopcounter<nb_similar_skus;loopcounter++){
							Element simSkuElement = doc.createElement("Sku");
							simSkuElement.appendChild(doc.createTextNode(cds_similar_skus[loopcounter]));
							similarProductListElement.appendChild(simSkuElement);				
						}
					}else {
						// we use the sku computed by Kryter
						for (int loopcounter=0;loopcounter<nb_similar_skus;loopcounter++){
							Element simSkuElement = doc.createElement("Sku");
							simSkuElement.appendChild(doc.createTextNode(krit_similar_skus[loopcounter]));
							similarProductListElement.appendChild(simSkuElement);				
						}					
					}
				} else {
					for (int loopcounter=0;loopcounter<nb_similar_skus;loopcounter++){
						Element simSkuElement = doc.createElement("Sku");
						simSkuElement.appendChild(doc.createTextNode(krit_similar_skus[loopcounter]));
						similarProductListElement.appendChild(simSkuElement);				
					}	
				}
				productElement.appendChild(similarProductListElement);
				productListElement.appendChild(productElement);
				global_count++;
			}
			rs.close();
			pst.close();
			System.out.println("We have fetched " +global_count + " linked SKUs according to the KRITER database \n");	
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			System.out.println("Writing the whole file to :  \n"+outputPath);	
			StreamResult result = new StreamResult(new File(outputPath).getPath());
			// Output to console for testing
			//			StreamResult result = new StreamResult(System.out);
			transformer.transform(source, result);
			System.out.println("File saved to : "+outputPath);
		} catch (SQLException ex) {
			ex.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}



	}
}
