package crawl4j.arbo.semantic.multiseed;

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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import crawl4j.arbo.semantic.SemanticArboCrawlDataCache;
import crawl4j.arbo.semantic.SemanticArboCrawler;
import crawl4j.urlutilities.MultiSeedSemanticArboInfo;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class SemanticMultiSeedController {
	// here we locally merge all cache
	// that is heavy on RAM memory : we here have to limit the depth to avoid out of memory
	// only shallow crawl will go through this step
	// counting the number of inlinks forces us to wait for the very end
	// of the crawl before we update the database	
	private static Map<String, Set<String>> inlinks_cache = new HashMap<String, Set<String>>();
	private static Connection con;
	
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";

	private static String insert_statement="INSERT INTO ARBOCRAWL_RESULTS (URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH,"
			+ " OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES,"
		    + " NB_SEARCH_IN_URL, NB_ADD_IN_TEXT, NB_FILTER_IN_TEXT, NB_SEARCH_IN_TEXT, NB_GUIDE_ACHAT_IN_TEXT, NB_PRODUCT_INFO_IN_TEXT, NB_LIVRAISON_IN_TEXT, NB_GARANTIES_IN_TEXT, NB_PRODUITS_SIMILAIRES_IN_TEXT, NB_IMAGES_TEXT, WIDTH_AVERAGE, HEIGHT_AVERAGE,"
			+ " PAGE_TYPE, SEMANTIC_HITS, CONCURRENT_NAME, LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static String update_statement ="UPDATE ARBOCRAWL_RESULTS SET WHOLE_TEXT=?,TITLE=?,H1=?,SHORT_DESCRIPTION=?,STATUS_CODE=?,DEPTH=?,OUTLINKS_SIZE=?,INLINKS_SIZE=?,NB_BREADCRUMBS=?,NB_AGGREGATED_RATINGS=?,NB_RATINGS_VALUES=?,NB_PRICES=?,NB_AVAILABILITIES=?,NB_REVIEWS=?,NB_REVIEWS_COUNT=?,NB_IMAGES=?,"
			+ "NB_SEARCH_IN_URL=?,NB_ADD_IN_TEXT=?,NB_FILTER_IN_TEXT=?,NB_SEARCH_IN_TEXT=?,NB_GUIDE_ACHAT_IN_TEXT=?,NB_PRODUCT_INFO_IN_TEXT=?,NB_LIVRAISON_IN_TEXT=?,NB_GARANTIES_IN_TEXT=?,NB_PRODUITS_SIMILAIRES_IN_TEXT=?,NB_IMAGES_TEXT=?,WIDTH_AVERAGE=?,HEIGHT_AVERAGE=?,"
			+ "PAGE_TYPE=?,SEMANTIC_HITS=?,CONCURRENT_NAME=?,LAST_UPDATE=? WHERE URL=?";
	
	public static void main(String[] args) throws Exception {
		instantiate_connection();	
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
		String rootFolderCrawler1 = "/home/sduprey/My_Data/My_Multi_Crawler1_Arbo_Crawl4j";
		int numberOfCrawler1Crawlers =  50;
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
	
		String rootFolderCrawler2 = "/home/sduprey/My_Data/My_Multi_Crawler2_Arbo_Crawl4j";
		int numberOfCrawler2Crawlers =  50;
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
		
		
		String rootFolderCrawler3 = "/home/sduprey/My_Data/My_Multi_Crawler3_Arbo_Crawl4j";
		int numberOfCrawler3Crawlers =  50;
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
		
		String rootFolderCrawler4 = "/home/sduprey/My_Data/My_Multi_Crawler4_Arbo_Crawl4j";
		int numberOfCrawler4Crawlers =  50;
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
		controllerCrawler1.startNonBlocking(SemanticArboCrawler.class, numberOfCrawler1Crawlers);
		System.out.println("Starting the crawl for Crawler2");
		controllerCrawler2.startNonBlocking(SemanticArboCrawler.class, numberOfCrawler2Crawlers);
		System.out.println("Starting the crawl for Crawler3");
		controllerCrawler3.startNonBlocking(SemanticArboCrawler.class, numberOfCrawler3Crawlers);
		System.out.println("Starting the crawl for Crawler4");
		controllerCrawler4.startNonBlocking(SemanticArboCrawler.class, numberOfCrawler4Crawlers);
		
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
			SemanticArboCrawlDataCache stat = (SemanticArboCrawlDataCache) localData;
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
			SemanticArboCrawlDataCache stat = (SemanticArboCrawlDataCache) localData;
			Map<String, MultiSeedSemanticArboInfo> local_thread_cache = stat.getCrawledContent();
			updateInLinksThreadCache(local_thread_cache);
		}

		// saving results to the database
		System.out.println("Saving the whole crawl to the database for "+name + " :");		
		System.out.println("Saving inlinks hashmap to the database for "+name + " :");
		for (Object localData : crawlersLocalData) {
			SemanticArboCrawlDataCache stat = (SemanticArboCrawlDataCache) localData;
			Map<String, MultiSeedSemanticArboInfo> local_thread_cache = stat.getCrawledContent();
			updateOrInsertDatabaseData(local_thread_cache,name);
		}
	}
	
	
	public static void updateInLinksThreadCache(Map<String, MultiSeedSemanticArboInfo> local_thread_cache){
		Iterator<Map.Entry<String, MultiSeedSemanticArboInfo>>  it = local_thread_cache.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, MultiSeedSemanticArboInfo> pairs = it.next();
			String url = pairs.getKey();
			MultiSeedSemanticArboInfo info = pairs.getValue();
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

	public static void updateOrInsertDatabaseData(Map<String, MultiSeedSemanticArboInfo> local_thread_cache, String name){
		try{
			Iterator<Entry<String, MultiSeedSemanticArboInfo>> it = local_thread_cache.entrySet().iterator();
			int local_counter = 0;
			if (it.hasNext()){
				local_counter++;
				PreparedStatement st = con.prepareStatement(update_statement);
				do {
					local_counter ++;
					Map.Entry<String, MultiSeedSemanticArboInfo> pairs = (Map.Entry<String, MultiSeedSemanticArboInfo>)it.next();
					String url=pairs.getKey();
					MultiSeedSemanticArboInfo info = pairs.getValue();
					// update statement
					//UPDATE ARBOCRAWL_RESULTS SET WHOLE_TEXT=?,TITLE=?,H1=?,SHORT_DESCRIPTION=?,STATUS_CODE=?,DEPTH=?,OUTLINKS_SIZE=?,INLINKS_SIZE=?,NB_BREADCRUMBS=?,NB_AGGREGATED_RATINGS=?,NB_RATINGS_VALUES=?,NB_PRICES=?,NB_AVAILABILITIES=?,NB_REVIEWS=?,NB_REVIEWS_COUNT=?,NB_IMAGES=?,NB_SEARCH_IN_URL=?,NB_ADD_IN_TEXT=?,NB_FILTER_IN_TEXT=?,NB_SEARCH_IN_TEXT=?,NB_GUIDE_ACHAT_IN_TEXT=?,NB_PRODUCT_INFO_IN_TEXT=?,NB_LIVRAISON_IN_TEXT=?,NB_GARANTIES_IN_TEXT=?,NB_PRODUITS_SIMILAIRES_IN_TEXT=?,NB_IMAGES_TEXT=?,WIDTH_AVERAGE=?,HEIGHT_AVERAGE=?,PAGE_TYPE=?,SEMANTIC_HITS=?,CONCURRENT_NAME=?,LAST_UPDATE=? WHERE URL=?"; 
					//                                  1         2      3         4                   5          6            7             8               9                   10                     11               12               13             14            15              16             17                    18                19               20                       21                      22                      23                        24                      25                         26            27               28              29          30              31               32                33
					st.setString(1,info.getText());
					st.setString(2,info.getTitle());
					st.setString(3,info.getH1());
					st.setString(4,info.getShort_desc());
					st.setInt(5,info.getStatus_code());
					st.setInt(6,info.getDepth());
					st.setInt(7,info.getLinks_size());
					Integer nb_inlinks = 0;
					Set<String> inlinksURL = inlinks_cache.get(url);
					if ( inlinksURL != null){
						nb_inlinks = inlinks_cache.get(url).size();
					}
					st.setInt(8,nb_inlinks);
					st.setInt(9,info.getNb_breadcrumbs());
					st.setInt(10,info.getNb_aggregated_rating());
					st.setInt(11,info.getNb_ratings());
					st.setInt(12,info.getNb_prices());
					st.setInt(13,info.getNb_availabilities());
					st.setInt(14,info.getNb_reviews());
					st.setInt(15,info.getNb_reviews_count());
					st.setInt(16,info.getNb_images());
					st.setInt(17,info.getNb_search_in_url());
					st.setInt(18,info.getNb_add_in_text());
					st.setInt(19,info.getNb_filter_in_text());
					st.setInt(20,info.getNb_search_in_text());
					st.setInt(21,info.getNb_guide_achat_in_text());
					st.setInt(22,info.getNb_product_info_in_text());
					st.setInt(23,info.getNb_livraison_in_text());
					st.setInt(24,info.getNb_garanties_in_text());
					st.setInt(25,info.getNb_produits_similaires_in_text());
					st.setInt(26,info.getNb_total_images());
					st.setDouble(27, info.getWidth_average());
					st.setDouble(28, info.getHeight_average());	
					st.setString(29,info.getPage_type());
					st.setString(30,info.getSemantics_hit());
					st.setString(31,name);
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(32,sqlDate);
					st.setString(33,url);
					int affected_row = st.executeUpdate();
					// if the row has not been updated, we have to insert it !
					if(affected_row == 0){
						PreparedStatement insert_st = con.prepareStatement(insert_statement);
						//(URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH, OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, NB_SEARCH_IN_URL, NB_ADD_IN_TEXT, NB_FILTER_IN_TEXT, NB_SEARCH_IN_TEXT, NB_GUIDE_ACHAT_IN_TEXT, NB_PRODUCT_INFO_IN_TEXT, NB_LIVRAISON_IN_TEXT, NB_GARANTIES_IN_TEXT, NB_PRODUITS_SIMILAIRES_IN_TEXT, NB_IMAGES_TEXT, WIDTH_AVERAGE, HEIGHT_AVERAGE, PAGE_TYPE, SEMANTIC_HITS,  CONCURRENT_NAME, LAST_UPDATE)"
						//  1        2        3    4           5                6        7           8              9             10               11                      12            13              14             15            16             17           18               19                 20                21                  22                      23                      24                       25                        26                       27             28              29          30              31             32             33
						insert_st.setString(1,url); 
						insert_st.setString(2,info.getText());
						insert_st.setString(3,info.getTitle());
						insert_st.setString(4,info.getH1());
						insert_st.setString(5,info.getShort_desc());
						insert_st.setInt(6,info.getStatus_code());
						insert_st.setInt(7,info.getDepth());
						insert_st.setInt(8,info.getLinks_size());
						if ( inlinksURL != null){
							nb_inlinks = inlinks_cache.get(url).size();
						}
						insert_st.setInt(9,nb_inlinks);
						insert_st.setInt(10,info.getNb_breadcrumbs());
						insert_st.setInt(11,info.getNb_aggregated_rating());
						insert_st.setInt(12,info.getNb_ratings());
						insert_st.setInt(13,info.getNb_prices());
						insert_st.setInt(14,info.getNb_availabilities());
						insert_st.setInt(15,info.getNb_reviews());
						insert_st.setInt(16,info.getNb_reviews_count());
						insert_st.setInt(17,info.getNb_images());
						insert_st.setInt(18,info.getNb_search_in_url());
						insert_st.setInt(19,info.getNb_add_in_text());
						insert_st.setInt(20,info.getNb_filter_in_text());
						insert_st.setInt(21,info.getNb_search_in_text());
						insert_st.setInt(22,info.getNb_guide_achat_in_text());
						insert_st.setInt(23,info.getNb_product_info_in_text());
						insert_st.setInt(24,info.getNb_livraison_in_text());
						insert_st.setInt(25,info.getNb_garanties_in_text());
						insert_st.setInt(26,info.getNb_produits_similaires_in_text());
						insert_st.setInt(27,info.getNb_total_images());
						insert_st.setDouble(28, info.getWidth_average());
						insert_st.setDouble(29, info.getHeight_average());
						insert_st.setString(30,info.getPage_type());
						insert_st.setString(31,info.getSemantics_hit());
						insert_st.setString(32,name);
						insert_st.setDate(33,sqlDate);
						insert_st.executeUpdate();
					}
				}while (it.hasNext());	
				System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
			}
		} catch (SQLException e){
			//System.out.println("Line already inserted : "+nb_lines);
			e.printStackTrace();  
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException ex1) {
					ex1.printStackTrace();
				}
			}
			e.printStackTrace();
		}	
	}


	public static void saveDatabaseData(Map<String, MultiSeedSemanticArboInfo> local_thread_cache, String name){
		try{
			Iterator<Entry<String, MultiSeedSemanticArboInfo>> it = local_thread_cache.entrySet().iterator();
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_statement);
			int local_counter = 0;
			if (it.hasNext()){
				do {
					local_counter ++;
					//				PreparedStatement st = con.prepareStatement(insert_statement);
					Map.Entry<String, MultiSeedSemanticArboInfo> pairs = (Map.Entry<String, MultiSeedSemanticArboInfo>)it.next();
					String url=pairs.getKey();
					MultiSeedSemanticArboInfo info = pairs.getValue();
					//(URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH, OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, NB_SEARCH_IN_URL, NB_ADD_IN_TEXT, NB_FILTER_IN_TEXT, NB_SEARCH_IN_TEXT, NB_GUIDE_ACHAT_IN_TEXT, NB_PRODUCT_INFO_IN_TEXT, NB_LIVRAISON_IN_TEXT, NB_GARANTIES_IN_TEXT, NB_PRODUITS_SIMILAIRES_IN_TEXT, NB_IMAGES_TEXT, WIDTH_AVERAGE, HEIGHT_AVERAGE, PAGE_TYPE,   SEMANTIC_HITS, CONCURRENT_NAME, LAST_UPDATE)"
					//  1        2        3    4           5                6        7           8              9             10               11                      12            13              14             15            16             17           18               19                 20                21                  22                      23                      24                       25                        26                       27             28              29          30              31             32               33
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
					st.setInt(18,info.getNb_search_in_url());
					st.setInt(19,info.getNb_add_in_text());
					st.setInt(20,info.getNb_filter_in_text());
					st.setInt(21,info.getNb_search_in_text());
					st.setInt(22,info.getNb_guide_achat_in_text());
					st.setInt(23,info.getNb_product_info_in_text());
					st.setInt(24,info.getNb_livraison_in_text());
					st.setInt(25,info.getNb_garanties_in_text());
					st.setInt(26,info.getNb_produits_similaires_in_text());
					st.setInt(27,info.getNb_total_images());
					st.setDouble(28, info.getWidth_average());
					st.setDouble(29, info.getHeight_average());
					st.setString(30,info.getPage_type());
					st.setString(31,info.getSemantics_hit());
					st.setString(32,name);
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(33,sqlDate);
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