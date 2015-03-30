package com.statistics.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticsComputingThreadPool {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private static int list_fixed_pool_size = 250;
	private static int list_size_bucket = 32000;
	private static String select_all_similar_skus = "SELECT SKU,SKU1,SKU2,SKU3,SKU4,SKU5,SKU6 FROM SIMILAR_PRODUCTS";

	public static void main(String[] args) {
		System.out.println("Number of threads for list crawler : "+list_fixed_pool_size);
		System.out.println("Bucket size for list crawler : "+list_size_bucket);
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
		// Instantiating the pool thread
		ExecutorService executor = Executors.newFixedThreadPool(list_fixed_pool_size);

		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			System.out.println("Requesting all SKUs");
			pst = con.prepareStatement(select_all_similar_skus);
			rs = pst.executeQuery();
			// dispatching to threads
			int local_count=0;
			int global_count=0;
			List<String[]> thread_list = new ArrayList<String[]>();
			while (rs.next()) {
				String[] skuList = new String[7];
				skuList[0]=rs.getString(1);
				skuList[1]=rs.getString(2);
				skuList[2]=rs.getString(3);
				skuList[3]=rs.getString(4);
				skuList[4]=rs.getString(5);
				skuList[5]=rs.getString(6);
				skuList[6]=rs.getString(7);
				if(local_count<list_size_bucket ){
					thread_list.add(skuList);		
					local_count++;
				}
				if (local_count==list_size_bucket){
					// one new connection per task
					System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
					Connection local_con = DriverManager.getConnection(url, user, passwd);
					Runnable worker = new StatisticsComputingWorkerThread(local_con,thread_list);
					executor.execute(worker);		
					// we initialize everything for the next thread
					local_count=0;
					thread_list = new ArrayList<String[]>();
				}
				global_count++;
			}
			rs.close();
			pst.close();
			// we add one for the euclidean remainder
			// there might be a last task with the euclidean remainder
			if (thread_list.size()>0){
				// one new connection per task

				System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
				Connection local_con = DriverManager.getConnection(url, user, passwd);
				Runnable worker = new StatisticsComputingWorkerThread(local_con,thread_list);
				executor.execute(worker);
			}
			System.out.println("We have : " +global_count + " URL status to fetch according to the NOMATCH database \n");
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
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
	}
}

