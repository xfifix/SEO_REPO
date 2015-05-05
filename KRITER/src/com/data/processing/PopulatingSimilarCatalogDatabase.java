package com.data.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class PopulatingSimilarCatalogDatabase {
	private static String database_kriter_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";

	//private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/similar_full_20150313040437.xml";
	private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/similar_full_20150418012952.xml";
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
		// we will here insert all the entries from the catalog csv file
		try {
			Connection con = DriverManager.getConnection(url, user, passwd);


			System.out.println("Inserting similar data from Kriter output file : "+input_file_path);
			SAXParserFactory factory = SAXParserFactory.newInstance();

			InputStream    xmlInput  =
					new FileInputStream(input_file_path);

			SAXParser      saxParser = factory.newSAXParser();
			SimilarSkusCatalalogInserterSAXHandler handler   = new SimilarSkusCatalalogInserterSAXHandler(con);
			saxParser.parse(xmlInput, handler);

			System.out.println("Number of products found" + handler.skus.size());
			System.out.println("Inserting the last batch");
			handler.insert_cache();
			con.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}
	}


}
