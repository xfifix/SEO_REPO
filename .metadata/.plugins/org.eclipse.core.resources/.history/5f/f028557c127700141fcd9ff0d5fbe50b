package com.keywords.processing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExaleadKeywordsThreadPool {
	private static int fixed_pool_size = 150;
	private static int size_bucket = 100;
	private static String find_statement="select distinct keywords from PRICING_KEYWORDS";
	private static List<String> tofetch_list = new ArrayList<String>();

	public static void main(String[] args) throws SQLException {
		String url="jdbc:postgresql://localhost/KEYWORDSDB";
		String user="postgres";
		String passwd="root";
		
		if (args.length==1){
			fixed_pool_size= Integer.valueOf(args[0]);
			System.out.println("You specified "+fixed_pool_size + " threads");
		}

		if (args.length==2){
			size_bucket= Integer.valueOf(args[1]);
			System.out.println("You specified a "+size_bucket + " bucket size");
		}

		try {
			fetch_data_from_database();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble reading the file or parsing it");
		}

		System.out.println("You'll be using "+fixed_pool_size+" threads ");
		ExecutorService executor = Executors.newFixedThreadPool(fixed_pool_size);

		// Launching tasks to our thread pool
		int size=tofetch_list.size();
		size=40;
		System.out.println("Dispatching "+size+" to "+fixed_pool_size+" threads");
		// we add one for the euclidean remainder
		int local_count=0;
		List<String> thread_list = new ArrayList<String>();
		for (int size_counter=0; size_counter<size;size_counter ++){
			if(local_count<size_bucket ){
				thread_list.add(tofetch_list.get(size_counter));
				local_count++;
			}
			if (local_count==size_bucket){
				Connection local_con = DriverManager.getConnection(url, user, passwd);
				System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
				Runnable worker = new ExaleadKeywordsRequestingWorkerThread(local_con,thread_list);
				executor.execute(worker);		
				local_count=0;
				thread_list = new ArrayList<String>();
			}
		}
		if (thread_list.size()>0){
			Connection local_con = DriverManager.getConnection(url, user, passwd);
			System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
			Runnable worker = new ExaleadKeywordsRequestingWorkerThread(local_con,thread_list);
			executor.execute(worker);
		}
	}

	public static void fetch_data_from_database() throws SQLException{		
		String url="jdbc:postgresql://localhost/KEYWORDSDB";
		String user="postgres";
		String passwd="root";
		// instantiating a connection
		Connection con = DriverManager.getConnection(url, user, passwd);
		
		PreparedStatement select_st = con.prepareStatement(find_statement);;
		ResultSet rs = select_st.executeQuery();
		while(rs.next()) {
			String keyword=rs.getString(1);
			tofetch_list.add(keyword);
		}
		select_st.close();
		con.close();
	}
}