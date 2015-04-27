package com.predictors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import com.data.DataEntry;
import com.parameters.CategorizerParameters;

public class CategoryBagWordsComputation {

	public static String kriter_conf_path = "/home/sduprey/My_Data/My_Categorizer_Conf/categorizer.conf";
	public static Properties properties;

	private static String select_entry_from_category4 = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON, TO_FETCH FROM CATALOG";

	private static String drop_CATEGORY_FOLLOWING_table = "DROP TABLE IF EXISTS CATEGORY_FOLLOWING";
	private static String create_CATEGORY_FOLLOWING_table = "select distinct categorie_niveau_4, count(*), true as to_fetch into CATEGORY_FOLLOWING from CATALOG group by categorie_niveau_4";

	public static void main(String[] args) {
		System.out.println("Reading the configuration files : "+kriter_conf_path);
		try{
			loadProperties();
			CategorizerParameters.database_con_path=properties.getProperty("categorizer.database_con_path");
			CategorizerParameters.small_list_pool_size =Integer.valueOf(properties.getProperty("categorizer.small_list_pool_size")); 
			CategorizerParameters.small_list_size_bucket =Integer.valueOf(properties.getProperty("categorizer.small_list_size_bucket")); 
			CategorizerParameters.big_list_pool_size =Integer.valueOf(properties.getProperty("categorizer.big_list_pool_size")); 
			CategorizerParameters.big_list_size_bucket =Integer.valueOf(properties.getProperty("categorizer.big_list_size_bucket")); 
			CategorizerParameters.max_list_size_separator_string=properties.getProperty("categorizer.max_list_size_separator_string");
			CategorizerParameters.recreate_table=Boolean.parseBoolean(properties.getProperty("categorizer.recreate_table"));
			CategorizerParameters.compute_optimal_parameters=Boolean.parseBoolean(properties.getProperty("categorizer.compute_optimal_parameters"));
			CategorizerParameters.kriter_threshold =Integer.valueOf(properties.getProperty("categorizer.kriter_threshold")); 
			CategorizerParameters.small_computing_max_list_size =Integer.valueOf(properties.getProperty("categorizer.small_computing_max_list_size"));
			CategorizerParameters.big_computing_max_list_size =Integer.valueOf(properties.getProperty("categorizer.big_computing_max_list_size"));
			CategorizerParameters.batch_size =Integer.valueOf(properties.getProperty("categorizer.batch_size"));
			CategorizerParameters.displaying_threshold =Integer.valueOf(properties.getProperty("categorizer.displaying_threshold"));
			CategorizerParameters.computing_max_list_size =CategorizerParameters.small_computing_max_list_size;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}
		System.out.println("Number of threads for list crawler : "+CategorizerParameters.small_list_pool_size);
		System.out.println("Bucket size for list crawler : "+CategorizerParameters.small_list_size_bucket);
		System.out.println("Database configuration path : "+CategorizerParameters.database_con_path);
		System.out.println("Maximum list size separator : "+CategorizerParameters.max_list_size_separator_string);
		System.out.println("Recreating table : "+CategorizerParameters.recreate_table);
		System.out.println("Computing optimal parameters : "+CategorizerParameters.compute_optimal_parameters);
		System.out.println("Kriter threshold : "+CategorizerParameters.kriter_threshold);
		System.out.println("Small computing maximum list size : "+CategorizerParameters.computing_max_list_size);
		System.out.println("Batch size : "+CategorizerParameters.batch_size);
		System.out.println("Displaying threshold : "+CategorizerParameters.displaying_threshold);

		// it would be best to use a property file to store MD5 password
		//		// Getting the database property
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(CategorizerParameters.database_con_path);
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
		// Instantiating the pool thread
		ExecutorService executor = null;
		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {  
			con = DriverManager.getConnection(url, user, passwd);
			System.out.println("Cleaning up and building the category_following table");
			if (CategorizerParameters.recreate_table){
				cleaning_category_scheduler_database(con);
			}

			// getting the number of URLs to fetch
			System.out.println("Requesting all data from categories");
			System.out.println("We here fetch all data even those with the to_fetch flag to false");

			pst = con.prepareStatement(select_entry_from_category4);
			rs = pst.executeQuery();
			Map<String, List<DataEntry>> my_entries = new HashMap<String, List<DataEntry>>();
			while (rs.next()) {
				// fetching all
				DataEntry entry = new DataEntry();
				String sku = rs.getString(1);
				entry.setSKU(sku);
				// category fetching
				String CATEGORIE_NIVEAU_1 = rs.getString(2);
				entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
				String CATEGORIE_NIVEAU_2 = rs.getString(3);
				entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
				String CATEGORIE_NIVEAU_3 = rs.getString(4);
				entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
				String CATEGORIE_NIVEAU_4 = rs.getString(5);
				entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
				// product libelle
				String  LIBELLE_PRODUIT = rs.getString(6);
				entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
				String MARQUE = rs.getString(7);
				entry.setMARQUE(MARQUE);
				// brand description
				String  DESCRIPTION_LONGUEUR80 = rs.getString(8);
				entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
				// vendor and state (available or not)
				String VENDEUR = rs.getString(9);
				entry.setVENDEUR(VENDEUR);
				String ETAT = rs.getString(10);
				entry.setETAT(ETAT);
				String RAYON = rs.getString(11);
				entry.setRAYON(RAYON);
				Boolean to_fetch = rs.getBoolean(12);
				entry.setTO_FETCH(to_fetch);
				// we here just keep the small categories
				List<DataEntry> toprocess = my_entries.get(CATEGORIE_NIVEAU_4);
				if (toprocess == null){
					toprocess = new ArrayList<DataEntry>();
					my_entries.put(CATEGORIE_NIVEAU_4, toprocess);
				}
				toprocess.add(entry);
			}		
			rs.close();
			pst.close();			
			// processing the word bags computation for all categories
			Iterator<Entry<String, List<DataEntry>>> it = my_entries.entrySet().iterator();
			// dispatching to threads
			while (it.hasNext()){	
				Map.Entry<String, List<DataEntry>> pairs = (Map.Entry<String, List<DataEntry>>)it.next();
				String category=pairs.getKey();
				List<DataEntry> my_data = pairs.getValue();
				System.out.println("Processing category : "+category);
				System.out.println("List size : "+my_data.size());		
			}
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
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void cleaning_category_scheduler_database(Connection con) throws SQLException{
		PreparedStatement drop_category_table_st = con.prepareStatement(drop_CATEGORY_FOLLOWING_table);
		drop_category_table_st.executeUpdate();
		System.out.println("Dropping the old CATEGORY_FOLLOWING table");
		drop_category_table_st.close();
		PreparedStatement create_category_table_st = con.prepareStatement(create_CATEGORY_FOLLOWING_table);
		create_category_table_st.executeUpdate();
		System.out.println("Creating the new CATEGORY_FOLLOWING table");
		create_category_table_st.close();
	}

	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(kriter_conf_path)));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}
}
