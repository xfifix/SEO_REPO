package crawl4j.arbo.multiseed;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;

import crawl4j.urlutilities.MultiArboInfo;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class MultiSeedArboController {
	// here we locally merge all cache
	// that is heavy on RAM memory : we here have to limit the depth to avoid out of memory
	// only shallow crawl will go through this step
	// counting the number of inlinks forces us to wait for the very end
	// of the crawl before we update the database	
	private static Map<String, Set<String>> inlinks_cache = new HashMap<String, Set<String>>();
	private static Connection con;
	
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";

	private static String insert_statement_with_label="INSERT INTO ARBOCRAWL_RESULTS (URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH,"
			+ " OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, PAGE_TYPE, LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	public static void main(String[] args) throws Exception {
		instantiate_connection();	
		// we here hide our identity
		String user_agent_name = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
		System.setProperty("http.agent",user_agent_name);
		System.out.println("Starting the crawl configuration for Cdiscount, Amazon, Darty, RDC");
		int maxDepthOfCrawling =  2; // common for all
		// we here launch just a few threads, enough for a shallow crawl
		// maximum twenty otherwise the concurrent update of the Map might get really too slow
		// and become a bottleneck rather than a 
		String seedCdiscount = "http://www.cdiscount.com/";
		String rootFolderCdiscount = "/home/sduprey/My_Data/My_Multi_Cdiscount_Arbo_Crawl4j";
		int numberOfCdiscountCrawlers =  50;
        CrawlConfig configCdiscount = new CrawlConfig();
        configCdiscount.setCrawlStorageFolder(rootFolderCdiscount);
        configCdiscount.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configCdiscount.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configCdiscount.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
		configCdiscount.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
		configCdiscount.setResumableCrawling(false);
		PageFetcher pageFetcherCdiscount = new PageFetcher(configCdiscount);
		RobotstxtConfig robotstxtConfigCdiscount = new RobotstxtConfig();
		robotstxtConfigCdiscount.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigCdiscount.setEnabled(true);
		RobotstxtServer robotstxtServerCdiscount = new RobotstxtServer(robotstxtConfigCdiscount, pageFetcherCdiscount);

		CrawlController controllerCdiscount = new CrawlController(configCdiscount, pageFetcherCdiscount, robotstxtServerCdiscount);
		controllerCdiscount.addSeed(seedCdiscount);
	
		String seedAmazon = "http://www.amazon.fr/";
		String rootFolderAmazon = "/home/sduprey/My_Data/My_Multi_Amazon_Arbo_Crawl4j";
		int numberOfAmazonCrawlers =  50;
        CrawlConfig configAmazon = new CrawlConfig();
        configAmazon.setCrawlStorageFolder(rootFolderAmazon);
        configAmazon.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configAmazon.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configAmazon.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configAmazon.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configAmazon.setResumableCrawling(false);
		PageFetcher pageFetcherAmazon = new PageFetcher(configAmazon);
		RobotstxtConfig robotstxtConfigAmazon = new RobotstxtConfig();
		robotstxtConfigAmazon.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigAmazon.setEnabled(true);
		RobotstxtServer robotstxtServerAmazon = new RobotstxtServer(robotstxtConfigAmazon, pageFetcherAmazon);
		CrawlController controllerAmazon = new CrawlController(configAmazon, pageFetcherAmazon, robotstxtServerAmazon);
		controllerAmazon.addSeed(seedAmazon);
		
		String seedDarty = "http://www.darty.com/";
		String rootFolderDarty = "/home/sduprey/My_Data/My_Multi_Darty_Arbo_Crawl4j";
		int numberOfDartyCrawlers =  50;
        CrawlConfig configDarty = new CrawlConfig();
        configDarty.setCrawlStorageFolder(rootFolderDarty);
        configDarty.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configDarty.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configDarty.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configDarty.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configDarty.setResumableCrawling(false);
		PageFetcher pageFetcherDarty = new PageFetcher(configDarty);
		RobotstxtConfig robotstxtConfigDarty = new RobotstxtConfig();
		robotstxtConfigDarty.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigDarty.setEnabled(true);
		RobotstxtServer robotstxtServerDarty = new RobotstxtServer(robotstxtConfigDarty, pageFetcherDarty);
		CrawlController controllerDarty = new CrawlController(configDarty, pageFetcherDarty, robotstxtServerDarty);
		controllerDarty.addSeed(seedDarty);
		
		String seedRDC = "http://www.rueducommerce.fr/";
		String rootFolderRDC = "/home/sduprey/My_Data/My_Multi_RDC_Arbo_Crawl4j";
		int numberOfRDCCrawlers =  50;
        CrawlConfig configRDC = new CrawlConfig();
        configRDC.setCrawlStorageFolder(rootFolderRDC);
        configRDC.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
        configRDC.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
        configRDC.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
        configRDC.setMaxDepthOfCrawling(maxDepthOfCrawling);
		// we want the crawl not to be reconfigurable : too slow otherwise
        configRDC.setResumableCrawling(false);
		PageFetcher pageFetcherRDC = new PageFetcher(configRDC);
		RobotstxtConfig robotstxtConfigRDC = new RobotstxtConfig();
		robotstxtConfigRDC.setUserAgentName(user_agent_name);
		// we respect the text robot
		robotstxtConfigRDC.setEnabled(true);
		RobotstxtServer robotstxtServerRDC = new RobotstxtServer(robotstxtConfigRDC, pageFetcherRDC);
		CrawlController controllerRDC = new CrawlController(configRDC, pageFetcherRDC, robotstxtServerRDC);
		controllerRDC.addSeed(seedRDC);
		
		System.out.println("Starting the crawl for Cdiscount");
		controllerCdiscount.startNonBlocking(MultiSeedArboCrawler.class, numberOfCdiscountCrawlers);
		System.out.println("Starting the crawl for Amazon");
		controllerAmazon.startNonBlocking(MultiSeedArboCrawler.class, numberOfAmazonCrawlers);
		System.out.println("Starting the crawl for Darty");
		controllerDarty.startNonBlocking(MultiSeedArboCrawler.class, numberOfDartyCrawlers);
		System.out.println("Starting the crawl for RDC");
		controllerRDC.startNonBlocking(MultiSeedArboCrawler.class, numberOfRDCCrawlers);
		
		controllerCdiscount.waitUntilFinish();
        System.out.println("Crawler Cdiscount is finished.");
        controllerAmazon.waitUntilFinish();
        System.out.println("Crawler Amazon is finished.");
        controllerDarty.waitUntilFinish();
        System.out.println("Crawler Darty is finished.");
        controllerRDC.waitUntilFinish();
        System.out.println("Crawler RDC is finished.");
        
        // Managing data for every crawlers for every site
        updateControllerData(controllerCdiscount,"Cdiscount");
        updateControllerData(controllerAmazon,"Amazon");
        updateControllerData(controllerDarty,"Darty");
        updateControllerData(controllerRDC,"RDC");
	}

	public static void updateControllerData(CrawlController controller, String name){
		List<Object> crawlersLocalData = controller.getCrawlersLocalData();
		// listing the work done by each thread
		long totalLinks = 0;
		long totalTextSize = 0;
		int totalProcessedPages = 0;
		for (Object localData : crawlersLocalData) {
			MultiSeedArboCrawlDataCache stat = (MultiSeedArboCrawlDataCache) localData;
			totalLinks += stat.getTotalLinks();
			totalTextSize += stat.getTotalTextSize();
			totalProcessedPages += stat.getTotalProcessedPages();
		}
		System.out.println("Aggregated Statistics for "+name + " :");
		System.out.println(" Processed Pages for "+name + " :" + totalProcessedPages);
		System.out.println(" Total Links found for "+name + " :" + totalLinks);
		System.out.println(" Total Text Size for "+name + " :" + totalTextSize);

		// computing the number of inlinks per pages over the whole crawl
		System.out.println("Computing inlinks hashmap cache to the database for "+name + " :");
		for (Object localData : crawlersLocalData) {
			MultiSeedArboCrawlDataCache stat = (MultiSeedArboCrawlDataCache) localData;
			Map<String, MultiArboInfo> local_thread_cache = stat.getCrawledContent();
			updateInLinksThreadCache(local_thread_cache);
		}

		// saving results to the database
		System.out.println("Saving the whole crawl to the database for "+name + " :");		
		System.out.println("Saving inlinks hashmap to the database for "+name + " :");
		for (Object localData : crawlersLocalData) {
			MultiSeedArboCrawlDataCache stat = (MultiSeedArboCrawlDataCache) localData;
			Map<String, MultiArboInfo> local_thread_cache = stat.getCrawledContent();
			saveDatabaseData(local_thread_cache);
		}
	}
	
	
	public static void updateInLinksThreadCache(Map<String, MultiArboInfo> local_thread_cache){
		Iterator<Map.Entry<String, MultiArboInfo>>  it = local_thread_cache.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, MultiArboInfo> pairs = it.next();
			String url = pairs.getKey();
			MultiArboInfo info = pairs.getValue();
			Set<String> outgoingLinks = info.getOutgoingLinks();
			if (outgoingLinks != null){
				updateInLinks(outgoingLinks,url);
			} else {
				System.out.println(" No outgoing links for this URL : "+url);
				System.out.println(" Status code : "+info.getStatus_code());
			}
		}
	}

	public static void updateInLinks(Set<String> outputSet, String sourceURL){
		for (String targetURL : outputSet){
			Set<String> inLinks = inlinks_cache.get(targetURL);
			if (inLinks == null){
				inLinks= new HashSet<String>();
			}
			inLinks.add(sourceURL);
			inlinks_cache.put(targetURL,inLinks);
		}
	}

	public static void saveDatabaseData(Map<String, MultiArboInfo> local_thread_cache){
		try{
			Iterator<Entry<String, MultiArboInfo>> it = local_thread_cache.entrySet().iterator();
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_statement_with_label);
			int local_counter = 0;
			if (it.hasNext()){
				do {
					local_counter ++;
					//				PreparedStatement st = con.prepareStatement(insert_statement);
					Map.Entry<String, MultiArboInfo> pairs = (Map.Entry<String, MultiArboInfo>)it.next();
					String url=pairs.getKey();
					MultiArboInfo info = pairs.getValue();
					//(URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH, OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, LAST_UPDATE)"
					//  1        2        3    4           5                6        7           8              9             10               11                      12            13              14             15            16             17         18       
					st.setString(1,url);
					st.setString(2,info.getText());
					st.setString(3,info.getTitle());
					st.setString(4,info.getH1());
					st.setString(5,info.getShort_desc());
					st.setInt(6,info.getStatus_code());
					st.setInt(7,info.getDepth());
					st.setInt(8,info.getLinks_size());
					Integer nb_inlinks = 0;
					Set<String> inlinksURL = inlinks_cache.get(url);
					if ( inlinksURL != null){
						nb_inlinks = inlinks_cache.get(url).size();
					}
					st.setInt(9,nb_inlinks);
					st.setInt(10,info.getNb_breadcrumbs());
					st.setInt(11,info.getNb_aggregated_rating());
					st.setInt(12,info.getNb_ratings());
					st.setInt(13,info.getNb_prices());
					st.setInt(14,info.getNb_availabilities());
					st.setInt(15,info.getNb_reviews());
					st.setInt(16,info.getNb_reviews_count());
					st.setInt(17,info.getNb_images());
					st.setString(18,info.getPage_type());
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(19,sqlDate);
					//					st.executeUpdate();
					st.addBatch();
				}while (it.hasNext());	
				st.executeBatch();		 
				con.commit();
				System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
			}
		} catch (SQLException e){
			e.printStackTrace();  
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException ex1) {
					ex1.printStackTrace();
				}
			}
		}		
	}
	
	public static void instantiate_connection() throws SQLException{
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
		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");
		con = DriverManager.getConnection(url, user, passwd);
	}
}