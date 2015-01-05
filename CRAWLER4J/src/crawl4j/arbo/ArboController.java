package crawl4j.arbo;

import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class ArboController {
	public static String site_stub="http://www.cdiscount.com/";
	public static String[] multiple_seeds = {
		"http://www.cdiscount.com/",
		"http://www.amazon.fr/",
		"http://www.darty.com/",
		"http://www.rueducommerce.fr/"
	};

	public static boolean isAllowedSiteforMultipleCrawl(String href){
		boolean found = false;
		for (String seed : multiple_seeds){
			if(href.startsWith(seed)){
				found=true;
			}
		}
		return found;
	}

	public static void main(String[] args) throws Exception {
		System.setProperty("http.agent", "");
		System.out.println("Starting the crawl configuration");		
		String seed = site_stub;
		// we here launch just one thread, enough for a shallow crawl
		int numberOfCrawlers =  1;	
		// downsizing to test
		//int numberOfCrawlers =  1;
		if (args.length == 2) {
			seed = args[0];
			numberOfCrawlers=Integer.valueOf(args[1]);
		} 
		String rootFolder = "/home/sduprey/My_Data/My_Arbo_Crawl4j";
		String user_agent_name = "CdiscountBot-crawler";
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(rootFolder);
		config.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
		config.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
		config.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
		int maxDepthOfCrawling = 5;        
		config.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
		config.setResumableCrawling(false);
		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		robotstxtConfig.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfig.setEnabled(true);
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);
		controller.addSeed(seed);
		System.out.println("Starting the crawl");
		long startTime = System.currentTimeMillis();
		controller.start(ArboCrawler.class, numberOfCrawlers);
		long estimatedTime = System.currentTimeMillis() - startTime;
		List<Object> crawlersLocalData = controller.getCrawlersLocalData();
		long totalLinks = 0;
		long totalTextSize = 0;
		int totalProcessedPages = 0;
		for (Object localData : crawlersLocalData) {
			ArboCrawlDataManagement stat = (ArboCrawlDataManagement) localData;
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
}