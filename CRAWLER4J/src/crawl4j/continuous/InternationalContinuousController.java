package crawl4j.continuous;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;

import crawl4j.xpathutility.XPathUtility;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class InternationalContinuousController {
	public static String crawl_conf_path = "/home/sduprey/My_Data/My_ContinuousCrawl_Conf/international_crawl.conf";
	public static Properties properties;

	public static void main(String[] args)   {
		System.setProperty("http.agent", "");
		System.out.println("Getting the crawl configuration from : "+crawl_conf_path);	
		// parameter to fill
		int numberOfCrawlers=0;
		String rootFolder="";
		int maxDepthOfCrawling=0;
		String user_agent_name="";		
		try{
			loadProperties();
			ContinuousCrawlParameter.seed = properties.getProperty("crawl.seed"); 
			ContinuousCrawlParameter.crawl_restrainer_start_with =  properties.getProperty("crawl.crawl_restrainer_start_with"); 
			ContinuousCrawlParameter.isBlobStored = Boolean.parseBoolean(properties.getProperty("crawl.isBlobStored"));
			ContinuousCrawlParameter.isSolrIndexed = Boolean.parseBoolean(properties.getProperty("crawl.isSolrIndexed"));
			ContinuousCrawlParameter.isMongoDBStored = Boolean.parseBoolean(properties.getProperty("crawl.isMongoDBStored"));
			ContinuousCrawlParameter.isXPATHparsed = Boolean.parseBoolean(properties.getProperty("crawl.isXPATHparsed"));
			//int// numberOfCrawlers =  400;	
			numberOfCrawlers=Integer.valueOf(properties.getProperty("crawl.numberOfCrawlers")); 
			//String rootFolder = "/home/sduprey/My_Data/My_Crawl4j";
			rootFolder=properties.getProperty("crawl.queueRootFolder"); 
			//int maxDepthOfCrawling = 300;
			maxDepthOfCrawling=Integer.valueOf(properties.getProperty("crawl.maxDepthOfCrawling")); 
			//String user_agent_name = "CdiscountBot-crawler";
			user_agent_name=properties.getProperty("crawl.user_agent_name"); 
			//int maxDepthOfCrawling = 300;
			CrawlDataManagement.bulk_size = Integer.valueOf(properties.getProperty("crawl.cache_bulk_size")); 
			//String user_agent_name = "CdiscountBot-crawler";
			XPathUtility.xpathconf_path=properties.getProperty("crawl.xpathconf_path"); 
			CrawlDataManagement.database_con_path=properties.getProperty("crawl.database_con_path"); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}	
		if (args.length == 2) {
			ContinuousCrawlParameter.seed = args[0];
			numberOfCrawlers=Integer.valueOf(args[1]);
		} 
		// downsizing to test/debug
		// overiding seed for debug
		//numberOfCrawlers =  1;
		//CrawlDataManagement.bulk_size = 5;
		//ContinuousCrawlParameter.seed="http://www.moncornerdeco.com/";
		System.out.println("Seed URL : "+ContinuousCrawlParameter.seed);
		System.out.println("Stub restrainer : "+ContinuousCrawlParameter.crawl_restrainer_start_with);
		
		System.out.println("Number of threads : "+numberOfCrawlers);
		System.out.println("MongoDB stored : "+ContinuousCrawlParameter.isMongoDBStored);
		System.out.println("Blob stored : "+ContinuousCrawlParameter.isBlobStored);
		System.out.println("Solr indexed : "+ContinuousCrawlParameter.isSolrIndexed);		
		System.out.println("XPATH parsed : "+ContinuousCrawlParameter.isXPATHparsed);
		System.out.println("User-agent : "+user_agent_name);
		System.out.println("Max depth of crawling : "+maxDepthOfCrawling);
		System.out.println("Cache bulk size per thread : "+CrawlDataManagement.bulk_size);
		System.out.println("XPATH conf file path : "+XPathUtility.xpathconf_path);
		System.out.println("Database conf file path : "+CrawlDataManagement.database_con_path);
		// loading XPATH expression
		XPathUtility.loadXPATHConf();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(rootFolder);
		config.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
		config.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
		config.setMaxPagesToFetch(-1);
		// we crawl up to depth n
		config.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
		config.setResumableCrawling(false);
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setUserAgentName(user_agent_name);
		robotstxtConfig.setEnabled(true);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = null;
		try {
			controller = new CrawlController(config, pageFetcher, robotstxtServer);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble instantiating CrawlController");
			System.exit(0);
		}

		controller.addSeed(ContinuousCrawlParameter.seed);
		System.out.println("Starting the crawl");
		long startTime = System.currentTimeMillis();
		controller.start(ContinuousCrawler.class, numberOfCrawlers);
		long estimatedTime = System.currentTimeMillis() - startTime;
		List<Object> crawlersLocalData = controller.getCrawlersLocalData();
		long totalLinks = 0;
		long totalTextSize = 0;
		int totalProcessedPages = 0;
		for (Object localData : crawlersLocalData) {
			CrawlDataManagement stat = (CrawlDataManagement) localData;
			totalLinks += stat.getTotalLinks();
			totalTextSize += stat.getTotalTextSize();
			totalProcessedPages += stat.getTotalProcessedPages();
		}
		System.out.println("Aggregated Statistics:");
		System.out.println("   Processed Pages: " + totalProcessedPages);
		System.out.println("   Total Links found: " + totalLinks);
		System.out.println("   Total Text Size: " + totalTextSize);
		System.out.println("   Estimated time (ms): " + estimatedTime);
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