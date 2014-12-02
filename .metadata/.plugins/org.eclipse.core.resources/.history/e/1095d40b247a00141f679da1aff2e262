package com.keywords.processing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.populating.ConcurrencyTableUtilities;

public class ExaleadKeywordsThreadPool {
	private static int fixed_pool_size = ConcurrencyTableUtilities.my_concurrency_tables.length;

	public static void main(String[] args) throws SQLException {
		// database parameters
		String url="jdbc:postgresql://localhost/KEYWORDSDB";
		String user="postgres";
		String passwd="mogette";

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