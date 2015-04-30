package com.corpus;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class FlagSkusPresentInTFIDFIndex {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/categorizer.properties";
	public static Properties properties;

	private static String select_all_index= "select doc_list from categorizer_corpus_words";

	public static void main(String[] args) {
		System.out.println("Reading the configuration files : "+database_con_path);

		// it would be best to use a property file to store MD5 password
		//		// Getting the database property
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_con_path);
			props.load(in);
		} catch (IOException ex) {
			System.out.println("Trouble fetching database configuration");
			ex.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				System.out.println("Trouble fetching database configuration");
				ex.printStackTrace();
			}
		}
		//the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		System.out.println("You'll connect to the postgresql CATEGORIZERDB database as "+user);
		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {  
			CategorizerCorpusFrequencyManager manager = new CategorizerCorpusFrequencyManager(url, user, passwd);
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			System.out.println("Requesting all data from categories");
			System.out.println("We here fetch all data even those with the to_fetch flag to false");

			// we here just fetch the SKUs which are not already in the tf/idf index
			pst = con.prepareStatement(select_all_index);
			rs = pst.executeQuery();
			int loop_count = 0;
			while (rs.next()) {
				loop_count++;
				// fetching all
				String doc_list = rs.getString(1);
				System.out.println("Processing entry SKU : "+doc_list+" number : "+loop_count);
				Map<String,Integer> skus_map = parse_skulist_links(doc_list);
				Iterator<Map.Entry<String,Integer>> cat_counter_it = skus_map.entrySet().iterator();
				while (cat_counter_it.hasNext()) {
					Map.Entry<String,Integer> pairs = (Map.Entry<String,Integer>)cat_counter_it.next();
					// we are here just interested by our argument naming
					String SKU =pairs.getKey();
					manager.flagSkuInTFIDF(SKU);
				}
			}		
			rs.close();
			pst.close();			
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static Map<String,Integer> parse_skulist_links(String output_links) {
		output_links = output_links.replace("{", "");
		output_links = output_links.replace("}", "");
		String[] url_outs = output_links.split(",");
		Map<String,Integer> outputSet = new HashMap<String,Integer>();
		for (String url_out : url_outs){
			url_out=url_out.trim();
			String[] skuData = url_out.split("=");
			outputSet.put(skuData[0],Integer.valueOf(skuData[1]));
		}
		return outputSet;
	}
}
