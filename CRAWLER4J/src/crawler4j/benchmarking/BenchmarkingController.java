package crawler4j.benchmarking;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class BenchmarkingController {
	private static Connection con;
	private static String statement="INSERT INTO CRAWL_RESULTS(URL,WHOLE_TEXT,TITLE,LINKS_SIZE,"
			+ "LINKS,H1,FOOTER_EXTRACT,ZTD_EXTRACT,SHORT_DESCRIPTION,VENDOR,ATTRIBUTES,NB_ATTRIBUTES,STATUS_CODE,HEADERS)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static int counter = 0;
	public static void main(String[] args) throws Exception {
		// Database property
		// Reading the property of our database
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream("database.properties");
			props.load(in);
		} catch (IOException ex) {
			Logger lgr = Logger.getLogger(BenchmarkingController.class.getName());
			lgr.log(Level.FATAL, ex.getMessage(), ex);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				Logger lgr = Logger.getLogger(BenchmarkingController.class.getName());
				lgr.log(Level.FATAL, ex.getMessage(), ex);
			}
		}
		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		
		con = DriverManager.getConnection(url, user, passwd);
		// crawler property	
		String crawlStorageFolder = "/home/sduprey/My_Data/My_Crawl4j";
		int numberOfCrawlers = 1000;

		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(crawlStorageFolder);
		config.setUserAgentString("CdiscountBot-crawler");
		// Politeness delay : none by default
		config.setPolitenessDelay(0);

		// Unlimited number of pages can be crawled.
		config.setMaxPagesToFetch(-1);

		/*
		 * Instantiate the controller for this crawl.
		 */
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		/*
		 * For each crawl, you need to add some seed urls. These are the first
		 * URLs that are fetched and then the crawler starts following links
		 * which are found in these pages
		 */
		controller.addSeed("http://www.cdiscount.com/");
		//		controller.addSeed("http://www.ics.uci.edu/~lopes/");
		//		controller.addSeed("http://www.ics.uci.edu/");

		/*
		 * Start the crawl. This is a blocking operation, meaning that your code
		 * will reach the line after this only when crawling is finished.
		 */
		controller.start(BenchmarkingCrawler.class, numberOfCrawlers);    
	}

	public static void insert_parse_result(String url,String text,String title,int links_size,String links_String,String h1,String footer,String ztd,String short_desc,String vendor,String att_desc, int nb_attributes, int status_code, String headers){
		//System.out.println("Inserting line : ");
		counter++;
		System.out.println(counter);
		try{
			PreparedStatement pst = con.prepareStatement(statement);
			pst.setString(1,url);
			pst.setString(2,text);
			pst.setString(3,title);
			pst.setInt(4,links_size);
			pst.setString(5,links_String);
			pst.setString(6,h1);
			pst.setString(7,footer);
			pst.setString(8,ztd);
			pst.setString(9,short_desc);
			pst.setString(10,vendor);
			pst.setString(11,att_desc);
			pst.setInt(12,nb_attributes);
			pst.setInt(13,status_code);
			pst.setString(14,headers);		
			pst.executeUpdate();	
		} catch (SQLException e){
			//System.out.println("Line already inserted : "+nb_lines);
			e.printStackTrace();
		}	
	}
}
