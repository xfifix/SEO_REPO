package crawl4j.arbo.multi;

import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class MultiArboController {
	public static void main(String[] args) throws Exception {
		System.setProperty("http.agent", "");
		System.out.println("Starting the crawl configuration");		
		String seed = "http://www.cdiscount.com/";
		// we here launch just a few threads, enough for a shallow crawl
		// maximum twenty otherwise the concurrent update of the Map might get really too slow
		// and become a bottleneck rather than a 
		int numberOfCrawlers =  20;	
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
		int maxDepthOfCrawling =  3;        
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
		controller.start(MultiArboCrawler.class, numberOfCrawlers);
		long estimatedTime = System.currentTimeMillis() - startTime;
		List<Object> crawlersLocalData = controller.getCrawlersLocalData();
		
		// counting the number of inlinks forces us to wait for the very end
		// of the crawl before we update the database
		if(crawlersLocalData.size() > 0){
			System.out.println("Saving the whole crawl to the database");		
			System.out.println("Saving concurrent hashmap to the database");
			MultiArboCrawlDataManagement.saveDatabaseData();
		}
		// listing the work done by each thread
		long totalLinks = 0;
		long totalTextSize = 0;
		int totalProcessedPages = 0;
		for (Object localData : crawlersLocalData) {
			MultiArboCrawlDataManagement stat = (MultiArboCrawlDataManagement) localData;
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