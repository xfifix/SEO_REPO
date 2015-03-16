package com.data.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

public class PopulatingSimilarDatabase {
	private static String database_kriter_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private static String drop_SIMILAR_table = "DROP TABLE IF EXISTS SIMILAR_PRODUCTS";
	private static String create_SIMILAR_table = "CREATE TABLE IF NOT EXISTS SIMILAR_PRODUCTS (SKU VARCHAR(100),SKU1 VARCHAR(100),SKU2  VARCHAR(100),SKU3  VARCHAR(100),SKU4  VARCHAR(100),SKU5  VARCHAR(100),SKU6  VARCHAR(100),SKU7  VARCHAR(100)) TABLESPACE mydbspace";

	private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/similar_full_20150311144001.xml";
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
			cleaning_database(con);
			System.out.println("SIMILAR table dropped and recreated");
			System.out.println("New SIMILAR inserted from Kriter xml output file : "+input_file_path);
			System.out.println("Inserting similar data from Kriter output file : "+input_file_path);
			SAXParserFactory factory = SAXParserFactory.newInstance();

			InputStream    xmlInput  =
					new FileInputStream(input_file_path);

			SAXParser      saxParser = factory.newSAXParser();
			SimilarSkusInserterSAXHandler handler   = new SimilarSkusInserterSAXHandler(con);
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

	private static void cleaning_database(Connection con) throws SQLException{
		PreparedStatement drop_catalog_table_st = con.prepareStatement(drop_SIMILAR_table);
		drop_catalog_table_st.executeUpdate();
		System.out.println("Dropping the old SIMILAR table");

		PreparedStatement create_catalog_table_st = con.prepareStatement(create_SIMILAR_table);
		create_catalog_table_st.executeUpdate();
		System.out.println("Creating the new SIMILAR table");
	}
}
