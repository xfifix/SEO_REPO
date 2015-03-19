package crawl4j.continuous.list;

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

import crawl4j.continuous.ContinuousCrawlParameter;
import crawl4j.continuous.CrawlDataManagement;
import crawl4j.xpathutility.XPathUtility;

public class ListThreadPool {

	public static String crawl_conf_path = "/home/sduprey/My_Data/My_ContinuousCrawl_Conf/crawl.conf";
	private static String site_radical = "http://www.cdiscount.com";
	public static Properties properties;
	private static int list_fixed_pool_size = 250;
	private static int list_size_bucket = 40000;
	private static String select_all_urls = "SELECT URL FROM CRAWL_RESULTS";

	public static void main(String[] args) {
		System.setProperty("http.agent", "");
		System.out.println("Getting the crawl configuration from : "+crawl_conf_path);	
		// parameter to fill
		String my_user_agent="";		
		try{
			loadProperties();
			list_fixed_pool_size = Integer.valueOf(properties.getProperty("crawl.list_pool_size")); 
			list_size_bucket = Integer.valueOf(properties.getProperty("crawl.list_size_bucket"));
			ContinuousCrawlParameter.isBlobStored = Boolean.parseBoolean(properties.getProperty("crawl.isBlobStored"));
			ContinuousCrawlParameter.isXPATHparsed = Boolean.parseBoolean(properties.getProperty("crawl.isXPATHparsed"));
			//String user_agent_name = "CdiscountBot-crawler";
			my_user_agent=properties.getProperty("crawl.user_agent_name"); 
			XPathUtility.xpathconf_path=properties.getProperty("crawl.xpathconf_path"); 
			CrawlDataManagement.database_con_path=properties.getProperty("crawl.database_con_path"); 
			ListWorkerThread.batch_size = Integer.valueOf(properties.getProperty("crawl.cache_bulk_size")); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}	
		//		// debug value
		//		nomatch_fixed_pool_size = 1;
		//		nomatch_size_bucket=200;
		//		NoMatchWorkerThread.batch_size=1;
		System.out.println("User agent selected : "+my_user_agent);
		System.out.println("Number of threads for list crawler : "+list_fixed_pool_size);
		System.out.println("Bucket size for list crawler : "+list_size_bucket);
		// loading XPATH expression
		XPathUtility.loadXPATHConf();
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
		ExecutorService executor = Executors.newFixedThreadPool(list_fixed_pool_size);

		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			System.out.println("Requesting all URLs previously crawled through links");
			pst = con.prepareStatement(select_all_urls);
			rs = pst.executeQuery();
			// dispatching to threads
			int local_count=0;
			int global_count=0;
			List<String> thread_list = new ArrayList<String>();
			while (rs.next()) {
				if(local_count<list_size_bucket ){
					thread_list.add(site_radical+rs.getString(1));
					local_count++;
				}
				if (local_count==list_size_bucket){
					// one new connection per task
					CrawlDataManagement loc_crawl_data_manager = new CrawlDataManagement();
					System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
					Runnable worker = new ListWorkerThread(loc_crawl_data_manager,thread_list,my_user_agent);
					executor.execute(worker);		
					// we initialize everything for the next thread
					local_count=0;
					thread_list = new ArrayList<String>();
					thread_list.add(site_radical+rs.getString(1));
				}
				global_count++;
			}
			rs.close();
			pst.close();
			// we add one for the euclidean remainder
			// there might be a last task with the euclidean remainder
			if (thread_list.size()>0){
				// one new connection per task
				CrawlDataManagement loc_crawl_data_manager = new CrawlDataManagement();
				System.out.println("Launching another thread with "+local_count+ " URLs to fetch");
				Runnable worker = new ListWorkerThread(loc_crawl_data_manager,thread_list,my_user_agent);
				executor.execute(worker);
			}
			System.out.println("We have : " +global_count + " URL status to fetch according to the NOMATCH database \n");
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(ListThreadPool.class.getName());
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
				Logger lgr = Logger.getLogger(ListThreadPool.class.getName());
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

