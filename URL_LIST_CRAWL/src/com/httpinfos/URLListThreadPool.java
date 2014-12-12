package com.httpinfos;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
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

public class URLListThreadPool {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/url_list_infos.properties";
	private static String xpathconf_path = "/home/sduprey/My_Data/My_Xpath_Conf/xpath.conf";
	private static String[] xpath_expression = new String[5];
	private static int fixed_pool_size = 80;
	private static int size_bucket = 2000;
	private static List<Integer> tofetch_list = new ArrayList<Integer>();

	public static void main(String[] args) {
		try {
			loadXpathExpressions();
		} catch (IOException e) {
			System.out.println("Trouble reading xpath expressions to parse");
			e.printStackTrace();
			System.exit(0);
		}
		String my_user_agent= "CdiscountBot-crawler";
		if (args.length>=1){
			my_user_agent= args[0];
		} else {
			System.out.println("You didn't specify any user agent, we'll use : "+my_user_agent);
		}
		if (args.length>=2){
			fixed_pool_size= Integer.valueOf(args[2]);
			System.out.println("You specified "+fixed_pool_size + " threads");
		}else {
			System.out.println("You didn't specify any threads number, we'll use : "+fixed_pool_size);
		}
		if (args.length>=3){
			size_bucket= Integer.valueOf(args[3]);
			System.out.println("You specified a "+size_bucket + " bucket size");
		}else {
			System.out.println("You didn't specify any bucket size, we'll use : "+size_bucket);
		}

		System.out.println("User agent selected : "+my_user_agent);
		System.out.println("Number of threads : "+fixed_pool_size);
		System.out.println("Bucket size : "+size_bucket);


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

		System.out.println("You are connected to the postgresql HTTPINFOS_LIST database as "+user);
		// Instantiating the pool thread
		System.out.println("You'll be using "+fixed_pool_size+" threads ");
		ExecutorService executor = Executors.newFixedThreadPool(fixed_pool_size);

		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			pst = con.prepareStatement("SELECT ID FROM HTTPINFOS_LIST WHERE TO_FETCH = TRUE");
			rs = pst.executeQuery();
			while (rs.next()) {
				tofetch_list.add(rs.getInt(1));
			}
			int size=tofetch_list.size();

			System.out.println("We have : " +size + " URL status to fetch according to the database \n");
			// we add one for the euclidean remainder
			int local_count=0;
			List<Integer> thread_list = new ArrayList<Integer>();
			for (int size_counter=0; size_counter<size;size_counter ++){
				if(local_count<size_bucket ){
					thread_list.add(tofetch_list.get(size_counter));
					local_count++;
				}
				if (local_count==size_bucket){
					// one new connection per task
					Connection local_con = DriverManager.getConnection(url, user, passwd);
					System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
					Runnable worker = new URLListWorkerThread(local_con,thread_list,my_user_agent,xpath_expression);
					executor.execute(worker);		
					// we initialize everything for the next thread
					local_count=0;
					thread_list = new ArrayList<Integer>();
				}
			}
			// there might be a last task with the euclidean remainder
			if (thread_list.size()>0){
				// one new connection per task
				Connection local_con = DriverManager.getConnection(url, user, passwd);
				System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
				Runnable worker = new URLListWorkerThread(local_con,thread_list,my_user_agent,xpath_expression);
				executor.execute(worker);
			}
			tofetch_list.clear();
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(URLListThreadPool.class.getName());
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
				Logger lgr = Logger.getLogger(URLListThreadPool.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

		executor.shutdown();
		while (!executor.isTerminated()) {
		}

		System.out.println("Finished all threads");

	}

	public static void loadXpathExpressions() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(xpathconf_path));
		String line="";
		int counter = 0;
		while (((line = br.readLine()) != null ) && counter < 5) {
			xpath_expression[counter] = line;
			counter ++;
		}
		br.close();
	}
}

