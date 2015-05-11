package com.data.crawling;

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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.data.processing.FetchableSKU;
import com.semantics.parameter.SemanticsParameter;


public class AttributsProcessing {

	public static String semantics_conf_path = "/home/sduprey/My_Data/My_Semantics_Conf/semantic.conf";
	public static Properties properties;

	private static String select_entry_from_category4 = " select SKU, CATEGORIE_NIVEAU_4, TO_FETCH FROM CATALOG";

	public static void main(String[] args) {
		System.out.println("Reading the configuration files : "+semantics_conf_path);
		try{
			loadProperties();
            AttributsComputingThread.batch_size = SemanticsParameter.batch_size;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}
		System.out.println("Number of threads for list crawler : "+SemanticsParameter.list_pool_size);
		System.out.println("Bucket size for list crawler : "+SemanticsParameter.list_size_bucket);
		System.out.println("Database configuration path : "+SemanticsParameter.database_con_path);
		System.out.println("Batch size : "+SemanticsParameter.batch_size);
		System.out.println("Displaying threshold : "+SemanticsParameter.displaying_threshold);

		// it would be best to use a property file to store MD5 password
		//		// Getting the database property
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(SemanticsParameter.database_con_path);
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
			// getting the number of URLs to fetch
			System.out.println("Requesting all data from categories");
			System.out.println("We here fetch all data even those with the to_fetch flag to false");

			pst = con.prepareStatement(select_entry_from_category4);
			rs = pst.executeQuery();
			Map<String, List<FetchableSKU>> all_my_entries = new HashMap<String, List<FetchableSKU>>();
			while (rs.next()) {
				FetchableSKU my_sku = new FetchableSKU();
				// fetching all
				String sku = rs.getString(1);
				String CATEGORIE_NIVEAU_4 = rs.getString(2);
				Boolean to_fetch = rs.getBoolean(3);
				my_sku.setSKU(sku);
				my_sku.setTo_fetch(to_fetch);
				// we here just keep the small categories
				List<FetchableSKU> toprocess = all_my_entries.get(CATEGORIE_NIVEAU_4);
				if (toprocess == null){
					toprocess = new ArrayList<FetchableSKU>();
					all_my_entries.put(CATEGORIE_NIVEAU_4, toprocess);
				}
				toprocess.add(my_sku);
			}		
			rs.close();
			pst.close();

			// getting only the categories which are not empty
			Map<String, List<FetchableSKU>> my_entries = new HashMap<String, List<FetchableSKU>>();
			Iterator<Entry<String, List<FetchableSKU>>> cleaning_it = all_my_entries.entrySet().iterator();
			while (cleaning_it.hasNext()){	
				Map.Entry<String, List<FetchableSKU>> pairs = (Map.Entry<String, List<FetchableSKU>>)cleaning_it.next();
				String current_category=pairs.getKey();
				List<FetchableSKU> datas_to_check = pairs.getValue();
				int to_fetch_counter = 0;
				for (FetchableSKU data_to_check : datas_to_check){
					if (data_to_check.isTo_fetch()){
						to_fetch_counter++;
					}
				}
				if (to_fetch_counter >0){
					my_entries.put(current_category, datas_to_check);
				}
			}


			// we here don't compute the parameters
			// computing the thread pool parameters for small categories
			System.out.println("Number of threads for list crawler : "+SemanticsParameter.list_pool_size);
			System.out.println("Bucket size for list crawler : "+SemanticsParameter.list_size_bucket);
			// Instantiating the pool thread
			executor = Executors.newFixedThreadPool(SemanticsParameter.list_pool_size);

			// iterating over the categories map !!! 
			Map<String, List<FetchableSKU>> thread_list = new HashMap<String, List<FetchableSKU>>();
			Iterator<Entry<String, List<FetchableSKU>>> it = my_entries.entrySet().iterator();
			// dispatching to threads
			int local_count=0;
			int global_count=0;
			while (it.hasNext()){	
				Map.Entry<String, List<FetchableSKU>> pairs = (Map.Entry<String, List<FetchableSKU>>)it.next();
				String current_category=pairs.getKey();
				List<FetchableSKU> category_entries = pairs.getValue();
				if(local_count<SemanticsParameter.list_size_bucket){
					thread_list.put(current_category,category_entries);		
					local_count++;
				}
				if (local_count==SemanticsParameter.list_size_bucket){
					// one new connection per task
					System.out.println("Launching another thread with "+local_count+" Categories to fetch");
					Connection local_con = DriverManager.getConnection(url, user, passwd);
					Runnable worker = new AttributsComputingThread(local_con,thread_list);
					//Runnable worker = new SimilarityComputingWorkerThread(con,thread_list);
					executor.execute(worker);		
					// we initialize everything for the next thread
					local_count=0;
					thread_list = new HashMap<String, List<FetchableSKU>>();
				}
				global_count++;
			}

			// we add one for the euclidean remainder
			// there might be a last task with the euclidean remainder
			if (thread_list.size()>0){
				// one new connection per task
				System.out.println("Launching another thread with "+local_count+ " Categories to fetch");
				Connection local_con = DriverManager.getConnection(url, user, passwd);
				Runnable worker = new AttributsComputingThread(local_con,thread_list);
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


	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(semantics_conf_path)));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}

}
