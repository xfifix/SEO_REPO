package com.keywords.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.populating.ConcurrencyTableUtilities;

public class ExaleadKeywordsThreadPool {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/keywords_enrichment.properties";
	private static int fixed_pool_size = ConcurrencyTableUtilities.my_concurrency_tables.length;

	public static void main(String[] args) throws SQLException {
		// Reading the property of our database
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
		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");
			
		System.out.println("You'll be using "+fixed_pool_size+" threads ");
		ExecutorService executor = Executors.newFixedThreadPool(fixed_pool_size);

		for (int i=0;i<fixed_pool_size;i++){
			Connection new_connection = DriverManager.getConnection(url, user, passwd);
			System.out.println("Launching another thread with "+ConcurrencyTableUtilities.my_concurrency_tables[i]);
			Runnable worker = new ExaleadKeywordsRequestingWorkerThread(new_connection,ConcurrencyTableUtilities.my_concurrency_tables[i]);
			executor.execute(worker);
			
		}
	}
}