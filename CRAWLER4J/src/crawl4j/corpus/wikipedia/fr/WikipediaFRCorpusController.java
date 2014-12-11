package crawl4j.corpus.wikipedia.fr;

import java.util.List;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class WikipediaFRCorpusController {

	public static String crawler_seed = "https://fr.wikipedia.org/wiki/";
	
	public static void main(String[] args) throws Exception {
		System.setProperty("http.agent", "");
		System.out.println("Starting the crawl configuration");

		// localized seed
		String seed = crawler_seed;
		// downsizing to test
		int numberOfCrawlers =  1;	
		//int numberOfCrawlers =  250;
		if (args.length == 2) {
			seed = args[0];
			numberOfCrawlers=Integer.valueOf(args[1]);
		} 
		String rootFolder = "/home/sduprey/My_Data/My_Corpus_Crawl4j";
		int maxDepthOfCrawling = 200;
		String user_agent_name ="Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
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
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

		controller.addSeed(seed);
		System.out.println("Starting the crawl");
		long startTime = System.currentTimeMillis();
		controller.start(CorpusCrawler.class, numberOfCrawlers);
		long estimatedTime = System.currentTimeMillis() - startTime;
		List<Object> crawlersLocalData = controller.getCrawlersLocalData();
		long totalTextSize = 0;
		int totalProcessedPages = 0;
		for (Object localData : crawlersLocalData) {
			WikipediaFRCorpusCrawlDataManagement stat = (WikipediaFRCorpusCrawlDataManagement) localData;
			totalTextSize += stat.getTotalTextSize();
			totalProcessedPages += stat.getTotalProcessedPages();
		}
		System.out.println("Aggregated Statistics:");
		System.out.println("   Processed Pages: " + totalProcessedPages);
		System.out.println("   Total Text Size: " + totalTextSize);
		System.out.println("   Estimated time (ms): " + estimatedTime);
	}
}