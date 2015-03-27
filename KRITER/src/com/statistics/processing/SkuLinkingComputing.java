package com.statistics.processing;

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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SkuLinkingComputing {
	private static int nb_similar_skus = 6;
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private static String select_all_linked_skus = "SELECT SKU1,SKU2,SKU3,SKU4,SKU5,SKU6 FROM SIMILAR_PRODUCTS";
	//private static String insert_linking_statement = "INSERT INTO LINKING_SIMILAR_PRODUCTS (SKU,COUNTER) values (?,?)";
	private static String update_catalog_statement = "UPDATE CATALOG SET COUNTER=? where SKU=?";

	private static Map<String,Integer> linked_skus_counter = new HashMap<String,Integer>();
	public static void main(String[] args){
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
		System.out.println("You'll connect to the postgresql KRITERDB database as "+user);
		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			System.out.println("Requesting all linked SKUs as a similar product");
			pst = con.prepareStatement(select_all_linked_skus);
			rs = pst.executeQuery();
			// dispatching to threads
			int global_count=0;
			while (rs.next()) {
			    // sku 1
				if (global_count%500 == 0){
					System.out.println(Thread.currentThread() +" Having processed "+global_count+" SKUs");
				}
				for (int nb_sku=1;nb_sku<(nb_similar_skus+1);nb_sku++){
					String current_linked_skus = rs.getString(nb_sku);
					Integer counter = linked_skus_counter.get(current_linked_skus);	
					if (counter == null){
						counter = 1;
					} else {
						counter=counter+1;
					}
					linked_skus_counter.put(current_linked_skus, counter);
				}
				global_count++;
			}
			rs.close();
			pst.close();
			System.out.println("We have fetched " +global_count + " linked SKUs according to the KRITER database \n");
	
			try{
				int local_counter = 0;
				con.setAutoCommit(false);
				//PreparedStatement st = con.prepareStatement(insert_linking_statement);
				PreparedStatement st = con.prepareStatement(update_catalog_statement);
				Iterator<Entry<String, Integer>> it = linked_skus_counter.entrySet().iterator();
				while (it.hasNext()){	
					local_counter++;
					if (local_counter%500 == 0){
						System.out.println(Thread.currentThread() +" Having inserted "+local_counter+" SKUs");
					}
					Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
					String current_sku=pairs.getKey();
					Integer counter = pairs.getValue();
					st.setInt(1,counter);
					st.setString(2,current_sku);
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
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(StatisticsComputingThreadPool.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
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
				Logger lgr = Logger.getLogger(StatisticsComputingThreadPool.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		System.out.println("Finished all threads");
	}
}
