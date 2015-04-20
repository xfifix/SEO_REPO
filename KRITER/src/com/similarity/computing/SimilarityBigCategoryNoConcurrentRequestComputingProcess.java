package com.similarity.computing;

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
import java.util.concurrent.Executors;

import com.similarity.parameter.KriterParameter;
import com.statistics.processing.CatalogEntry;

public class SimilarityBigCategoryNoConcurrentRequestComputingProcess {
	public static Properties properties;
	public static String kriter_conf_path = "/home/sduprey/My_Data/My_Kriter_Conf/kriter.conf";
	private static List<String> too_big_categories = new ArrayList<String>();
	public static String select_big_category = "select categorie_niveau_4 from CATEGORY_FOLLOWING where count > ";
	private static String select_entry_from_category4 = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON, TO_FETCH FROM CATALOG";

	private static String drop_CATEGORY_FOLLOWING_table = "DROP TABLE IF EXISTS CATEGORY_FOLLOWING";
	private static String create_CATEGORY_FOLLOWING_table = "select distinct categorie_niveau_4, count(*), true as to_fetch into CATEGORY_FOLLOWING from CATALOG group by categorie_niveau_4";

	public static void main(String[] args) {
		System.out.println("Reading the configuration files : "+kriter_conf_path);
		try{
			loadProperties();
			KriterParameter.database_con_path=properties.getProperty("kriter.database_con_path");
			KriterParameter.small_list_pool_size =Integer.valueOf(properties.getProperty("kriter.small_list_pool_size")); 
			KriterParameter.small_list_size_bucket =Integer.valueOf(properties.getProperty("kriter.small_list_size_bucket")); 
			KriterParameter.big_list_pool_size =Integer.valueOf(properties.getProperty("kriter.big_list_pool_size")); 
			KriterParameter.big_list_size_bucket =Integer.valueOf(properties.getProperty("kriter.big_list_size_bucket")); 
			KriterParameter.max_list_size_separator_string=properties.getProperty("kriter.max_list_size_separator_string");
			KriterParameter.recreate_table=Boolean.parseBoolean(properties.getProperty("kriter.recreate_table"));
			KriterParameter.compute_optimal_parameters=Boolean.parseBoolean(properties.getProperty("kriter.compute_optimal_parameters"));
			KriterParameter.kriter_threshold =Integer.valueOf(properties.getProperty("kriter.kriter_threshold")); 
			KriterParameter.small_computing_max_list_size =Integer.valueOf(properties.getProperty("kriter.small_computing_max_list_size"));
			KriterParameter.big_computing_max_list_size =Integer.valueOf(properties.getProperty("kriter.big_computing_max_list_size"));
			KriterParameter.batch_size =Integer.valueOf(properties.getProperty("kriter.batch_size"));
			KriterParameter.displaying_threshold =Integer.valueOf(properties.getProperty("kriter.displaying_threshold"));
			KriterParameter.computing_max_list_size =KriterParameter.big_computing_max_list_size;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}

		System.out.println("Database configuration path : "+KriterParameter.database_con_path);
		System.out.println("Maximum list size separator : "+KriterParameter.max_list_size_separator_string);
		System.out.println("Recreating table : "+KriterParameter.recreate_table);
		System.out.println("Computing optimal parameters : "+KriterParameter.compute_optimal_parameters);
		System.out.println("Kriter threshold : "+KriterParameter.kriter_threshold);
		System.out.println("Big computing maximum list size : "+KriterParameter.computing_max_list_size);
		System.out.println("Batch size : "+KriterParameter.batch_size);
		System.out.println("Displaying threshold : "+KriterParameter.displaying_threshold);

		
		// Getting the database property
		FileInputStream in = null;      
		try {
			in = new FileInputStream(KriterParameter.database_con_path);
			properties.load(in);
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
		String url = properties.getProperty("db.url");
		String user = properties.getProperty("db.user");
		String passwd = properties.getProperty("db.passwd");

		System.out.println("You'll connect to the postgresql KRITERDB database as "+user);


		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		ExecutorService executor = null;
		try {  
			con = DriverManager.getConnection(url, user, passwd);
			System.out.println("Cleaning up and building the category_following table");
			if (KriterParameter.recreate_table){
				cleaning_category_scheduler_database(con);
			}
			// getting the too big categories to exclude
			System.out.println("Requesting all distinct too big categories");
			pst = con.prepareStatement(select_big_category+KriterParameter.max_list_size_separator_string);
			rs = pst.executeQuery();
			while (rs.next()) {
				// fetching all
				String category_level_4 = rs.getString(1);
				too_big_categories.add(category_level_4);
			}
			rs.close();
			pst.close();
			// getting the number of URLs to fetch
			System.out.println("Requesting all distinct categories");
			pst = con.prepareStatement(select_entry_from_category4);
			rs = pst.executeQuery();
			Map<String, List<CatalogEntry>> my_entries = new HashMap<String, List<CatalogEntry>>();
			while (rs.next()) {
				// fetching all
				CatalogEntry entry = new CatalogEntry();
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
				// we here just keep the big categories
				if (too_big_categories.contains(CATEGORIE_NIVEAU_4)){
					List<CatalogEntry> toprocess = my_entries.get(CATEGORIE_NIVEAU_4);
					if (toprocess == null){
						toprocess = new ArrayList<CatalogEntry>();
						my_entries.put(CATEGORIE_NIVEAU_4, toprocess);
					}
					toprocess.add(entry);
				} else {
					System.out.println("Too small category < "+KriterParameter.max_list_size_separator_string+", we drop it : "+CATEGORIE_NIVEAU_4);
				}
			}

			// computing the thread pool parameters for big categories
			if (KriterParameter.compute_optimal_parameters){
				KriterParameter.big_list_pool_size=my_entries.size();
				KriterParameter.big_list_size_bucket=1;
			}

			System.out.println("Number of threads for list crawler : "+KriterParameter.big_list_pool_size);
			System.out.println("Bucket size for list crawler : "+KriterParameter.big_list_size_bucket);

			// Instantiating the pool thread
			executor = Executors.newFixedThreadPool(KriterParameter.big_list_pool_size);
			// iterating over the categories map !!! 
			Map<String, List<CatalogEntry>> thread_list = new HashMap<String, List<CatalogEntry>>();
			Iterator<Entry<String, List<CatalogEntry>>> it = my_entries.entrySet().iterator();
			// dispatching to threads
			int local_count=0;
			int global_count=0;
			while (it.hasNext()){	
				Map.Entry<String, List<CatalogEntry>> pairs = (Map.Entry<String, List<CatalogEntry>>)it.next();
				String current_category=pairs.getKey();
				List<CatalogEntry> category_entries = pairs.getValue();
				if(local_count<KriterParameter.big_list_size_bucket){
					thread_list.put(current_category,category_entries);		
					local_count++;
				}
				if (local_count==KriterParameter.big_list_size_bucket){
					// one new connection per task
					System.out.println("Launching another thread with "+local_count+" Categories to fetch");
					Connection local_con = DriverManager.getConnection(url, user, passwd);
					Runnable worker = new SimilarityComputingNoFetchWorkerThread(local_con,thread_list);
					//Runnable worker = new SimilarityComputingWorkerThread(con,thread_list);
					executor.execute(worker);		
					// we initialize everything for the next thread
					local_count=0;
					thread_list = new HashMap<String, List<CatalogEntry>>();
				}
				global_count++;
			}
			rs.close();
			pst.close();
			// we add one for the euclidean remainder
			// there might be a last task with the euclidean remainder
			if (thread_list.size()>0){
				// one new connection per task
				System.out.println("Launching another thread with "+local_count+ " Categories to fetch");
				Connection local_con = DriverManager.getConnection(url, user, passwd);
				Runnable worker = new SimilarityComputingNoFetchWorkerThread(local_con,thread_list);
				//Runnable worker = new SimilarityComputingWorkerThread(con,thread_list);
				executor.execute(worker);
			}
			System.gc();
			System.out.println("We have : " +global_count + " Categories status to fetch according to the Kriter database \n");
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

