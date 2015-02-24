package crawl4j.nomatch;
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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import crawl4j.continuous.CrawlDataManagement;
import crawl4j.xpathutility.XPathUtility;

public class NoMatchThreadPool {

	public static String crawl_conf_path = "/home/sduprey/My_Data/My_ContinuousCrawl_Conf/crawl.conf";
	public static Properties properties;

	public static boolean isBlobStored=false;
	public static boolean isXPATHparsed=true;
	private static int nomatch_fixed_pool_size = 200;
	private static int nomatch_size_bucket = 800;

	private static List<Integer> tofetch_list = new ArrayList<Integer>();

	private static String nomatch_select = "SELECT ID FROM NOMATCH WHERE TO_FETCH = TRUE";
	
	public static void main(String[] args) {
		System.setProperty("http.agent", "");
		System.out.println("Getting the crawl configuration from : "+crawl_conf_path);	
		// parameter to fill

		String my_user_agent="";		
		try{
			loadProperties();
			nomatch_fixed_pool_size = Integer.valueOf(properties.getProperty("crawl.nomatch_pool_size")); 
			nomatch_size_bucket = Integer.valueOf(properties.getProperty("crawl.nomatch_size_bucket"));
			isBlobStored = Boolean.parseBoolean(properties.getProperty("crawl.isBlobStored"));
			isXPATHparsed = Boolean.parseBoolean(properties.getProperty("crawl.isXPATHparsed"));
			//String user_agent_name = "CdiscountBot-crawler";
			my_user_agent=properties.getProperty("crawl.user_agent_name"); 
			//int maxDepthOfCrawling = 300;
			XPathUtility.xpathconf_path=properties.getProperty("crawl.xpathconf_path"); 
			CrawlDataManagement.database_con_path=properties.getProperty("crawl.database_con_path"); 
			CrawlDataManagement.bulk_size = Integer.valueOf(properties.getProperty("crawl.cache_bulk_size")); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}	

		System.out.println("User agent selected : "+my_user_agent);
		System.out.println("Number of threads : "+nomatch_fixed_pool_size);
		System.out.println("Bucket size : "+nomatch_size_bucket);

		// it would be best to use a property file to store MD5 password
		//		// Getting the database property
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(CrawlDataManagement.database_con_path);
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

		System.out.println("You'll connect to the postgresql CRAWL4J database as "+user);
		// Instantiating the pool thread
		ExecutorService executor = Executors.newFixedThreadPool(nomatch_fixed_pool_size);

		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			pst = con.prepareStatement(nomatch_select);
			rs = pst.executeQuery();
			while (rs.next()) {
				tofetch_list.add(rs.getInt(1));
			}
			rs.close();
			pst.close();
			int size=tofetch_list.size();
			System.out.println("We have : " +size + " URL status to fetch according to the NOMATCH database \n");
			// we add one for the euclidean remainder
			int local_count=0;
			List<Integer> thread_list = new ArrayList<Integer>();
			for (int size_counter=0; size_counter<size;size_counter ++){
				if(local_count<nomatch_size_bucket ){
					thread_list.add(tofetch_list.get(size_counter));
					local_count++;
				}
				if (local_count==nomatch_size_bucket){
					// one new connection per task
					CrawlDataManagement loc_crawl_data_manager = new CrawlDataManagement();

					System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
					Runnable worker = new NoMatchWorkerThread(loc_crawl_data_manager,thread_list,my_user_agent);
					executor.execute(worker);		
					// we initialize everything for the next thread
					local_count=0;
					thread_list = new ArrayList<Integer>();
				}
			}
			// there might be a last task with the euclidean remainder
			if (thread_list.size()>0){
				// one new connection per task
				CrawlDataManagement loc_crawl_data_manager = new CrawlDataManagement();
				System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
				Runnable worker = new NoMatchWorkerThread(loc_crawl_data_manager,thread_list,my_user_agent);
				executor.execute(worker);
			}
			tofetch_list.clear();
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(NoMatchThreadPool.class.getName());
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
				Logger lgr = Logger.getLogger(NoMatchThreadPool.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
	}


	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(crawl_conf_path)));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}

}

