package crawl4j.corpus.multiseed;

import java.util.List;

import crawl4j.corpus.CorpusCrawlDataManagement;
import crawl4j.corpus.CorpusCrawler;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class CorpusMultiSeedController {

	public static void main(String[] args) throws Exception {
	
		// we here hide our identity
		String user_agent_name = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
		System.setProperty("http.agent",user_agent_name);
		System.out.println("Starting the crawl configuration for Crawler1, Crawler2, Crawler3, Crawler4");
		int maxDepthOfCrawling =  2; // common for all
        // Managing data for every crawlers for every site
		// instantiating the seeds for our multiple crawlers
        String nameCrawler1 = "delamaison";
        String nameCrawler2 = "lamaisonduconvertible";
        String nameCrawler3 = "habitat";
        String nameCrawler4 = "enviedemeubles";
        
		String seedCrawler1 = "http://www.delamaison.fr/";
		String seedCrawler2 = "http://www.lamaisonduconvertible.fr/";
		String seedCrawler3 = "http://www.habitat.fr/";
		String seedCrawler4 = "http://www.enviedemeubles.com/";
		// we here launch just a few threads, enough for a shallow crawl
		// maximum twenty otherwise the concurrent update of the Map might get really too slow
		// and become a bottleneck rather than a 
		String rootFolderCrawler1 = "/home/sduprey/My_Data/My_Multi_Crawler1_Corpus_Crawl4j";
		String rootFolderCrawler2 = "/home/sduprey/My_Data/My_Multi_Crawler2_Corpus_Crawl4j";
		String rootFolderCrawler3 = "/home/sduprey/My_Data/My_Multi_Crawler3_Corpus_Crawl4j";
		String rootFolderCrawler4 = "/home/sduprey/My_Data/My_Multi_Crawler4_Corpus_Crawl4j";
		// crawler thread pool size
		int numberOfCrawler1Crawlers =  10;
		int numberOfCrawler2Crawlers =  10;
		int numberOfCrawler3Crawlers =  10;
		int numberOfCrawler4Crawlers =  10;
        
		// Crawler 1
        CrawlConfig configCrawler1 = new CrawlConfig();
        configCrawler1.setCrawlStorageFolder(rootFolderCrawler1);
        configCrawler1.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configCrawler1.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configCrawler1.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configCrawler1.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configCrawler1.setResumableCrawling(false);
		PageFetcher pageFetcherCrawler1 = new PageFetcher(configCrawler1);
		RobotstxtConfig robotstxtConfigCrawler1 = new RobotstxtConfig();
		robotstxtConfigCrawler1.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigCrawler1.setEnabled(true);
		RobotstxtServer robotstxtServerCrawler1 = new RobotstxtServer(robotstxtConfigCrawler1, pageFetcherCrawler1);

		CrawlController controllerCrawler1 = new CrawlController(configCrawler1, pageFetcherCrawler1, robotstxtServerCrawler1);
		controllerCrawler1.addSeed(seedCrawler1);
	
        // Crawler 2
        CrawlConfig configCrawler2 = new CrawlConfig();
        configCrawler2.setCrawlStorageFolder(rootFolderCrawler2);
        configCrawler2.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configCrawler2.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configCrawler2.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configCrawler2.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configCrawler2.setResumableCrawling(false);
		PageFetcher pageFetcherCrawler2 = new PageFetcher(configCrawler2);
		RobotstxtConfig robotstxtConfigCrawler2 = new RobotstxtConfig();
		robotstxtConfigCrawler2.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigCrawler2.setEnabled(true);
		RobotstxtServer robotstxtServerCrawler2 = new RobotstxtServer(robotstxtConfigCrawler2, pageFetcherCrawler2);
		CrawlController controllerCrawler2 = new CrawlController(configCrawler2, pageFetcherCrawler2, robotstxtServerCrawler2);
		controllerCrawler2.addSeed(seedCrawler2);
		
        // Crawler 3
        CrawlConfig configCrawler3 = new CrawlConfig();
        configCrawler3.setCrawlStorageFolder(rootFolderCrawler3);
        configCrawler3.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configCrawler3.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configCrawler3.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configCrawler3.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configCrawler3.setResumableCrawling(false);
		PageFetcher pageFetcherCrawler3 = new PageFetcher(configCrawler3);
		RobotstxtConfig robotstxtConfigCrawler3 = new RobotstxtConfig();
		robotstxtConfigCrawler3.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigCrawler3.setEnabled(true);
		RobotstxtServer robotstxtServerCrawler3 = new RobotstxtServer(robotstxtConfigCrawler3, pageFetcherCrawler3);
		CrawlController controllerCrawler3 = new CrawlController(configCrawler3, pageFetcherCrawler3, robotstxtServerCrawler3);
		controllerCrawler3.addSeed(seedCrawler3);
		
        // Crawler 4
        CrawlConfig configCrawler4 = new CrawlConfig();
        configCrawler4.setCrawlStorageFolder(rootFolderCrawler4);
        configCrawler4.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configCrawler4.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configCrawler4.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configCrawler4.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configCrawler4.setResumableCrawling(false);
		PageFetcher pageFetcherCrawler4 = new PageFetcher(configCrawler4);
		RobotstxtConfig robotstxtConfigCrawler4 = new RobotstxtConfig();
		robotstxtConfigCrawler4.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigCrawler4.setEnabled(true);
		RobotstxtServer robotstxtServerCrawler4 = new RobotstxtServer(robotstxtConfigCrawler4, pageFetcherCrawler4);
		CrawlController controllerCrawler4 = new CrawlController(configCrawler4, pageFetcherCrawler4, robotstxtServerCrawler4);
		controllerCrawler4.addSeed(seedCrawler4);
		
		System.out.println("Starting the crawl for Crawler1");
		controllerCrawler1.startNonBlocking(CorpusCrawler.class, numberOfCrawler1Crawlers);
		System.out.println("Starting the crawl for Crawler2");
		controllerCrawler2.startNonBlocking(CorpusCrawler.class, numberOfCrawler2Crawlers);
		System.out.println("Starting the crawl for Crawler3");
		controllerCrawler3.startNonBlocking(CorpusCrawler.class, numberOfCrawler3Crawlers);
		System.out.println("Starting the crawl for Crawler4");
		controllerCrawler4.startNonBlocking(CorpusCrawler.class, numberOfCrawler4Crawlers);
		
		controllerCrawler1.waitUntilFinish();
        System.out.println("Crawler Crawler1 is finished.");
        controllerCrawler2.waitUntilFinish();
        System.out.println("Crawler Crawler2 is finished.");
        controllerCrawler3.waitUntilFinish();
        System.out.println("Crawler Crawler3 is finished.");
        controllerCrawler4.waitUntilFinish();
        System.out.println("Crawler Crawler4 is finished.");
        
        // Managing data for every crawlers for every site
        updateControllerData(controllerCrawler1,nameCrawler1);
        updateControllerData(controllerCrawler2,nameCrawler2);
        updateControllerData(controllerCrawler3,nameCrawler3);
        updateControllerData(controllerCrawler4,nameCrawler4);
	}

	public static void updateControllerData(CrawlController controller, String name){
		List<Object> crawlersLocalData = controller.getCrawlersLocalData();
		// listing the work done by each thread
		long totalLinks = 0;
		long totalTextSize = 0;
		int totalProcessedPages = 0;
		for (Object localData : crawlersLocalData) {
			CorpusCrawlDataManagement stat = (CorpusCrawlDataManagement) localData;
			totalTextSize += stat.getTotalTextSize();
			totalProcessedPages += stat.getTotalProcessedPages();
		}
		System.out.println("Aggregated Statistics for "+name + " :");
		System.out.println(" Processed Pages for "+name + " :" + totalProcessedPages);
		System.out.println(" Total Links found for "+name + " :" + totalLinks);
		System.out.println(" Total Text Size for "+name + " :" + totalTextSize);
	}
}