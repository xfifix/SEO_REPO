package crawl4j.arbo.semantic;

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

import crawl4j.urlutilities.MultiSeedSemanticArboInfo;
import crawl4j.vsm.CorpusCache;
import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class SemanticArboController {
	public static String site_stub="http://www.cdiscount.com/";
	public static String[] multiple_seeds = {
		"http://www.cdiscount.com/",
		"http://www.amazon.fr/",
		"http://www.darty.com/",
		"http://www.rueducommerce.fr/",
		"http://www.delamaison.fr/",
		"http://www.lamaisonduconvertible.fr/",
		"http://www.habitat.fr/",
		"http://www.enviedemeubles.com/"
	};
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

		// First as a semantic crawler, we need to load in cache the semantic corpus 
		CorpusCache.load();
		
		System.setProperty("http.agent", "");
		System.out.println("Starting the crawl configuration");	
		String name = "Cdiscount";
		//String seed = "http://www.cdiscount.com/";
		String seed = "http://www.amazon.fr/";
		//String seed = "http://www.lamaisonduconvertible.fr/";
		//debugging seed
		//String seed = "http://www.delamaison.fr/rideau-tamisant-nouettes-lave-140x280cm-purete-p-162563.html";
				
		// we here launch just a few threads, enough for a shallow crawl
		// maximum twenty otherwise the concurrent update of the Map might get really too slow
		// and become a bottleneck rather than a 
		int numberOfCrawlers =  1;	
		// downsizing to test
		//int numberOfCrawlers =  1;

		if (args.length == 1) {
			seed = args[0];
		}
		
		if (args.length == 2) {
			seed = args[0];
			numberOfCrawlers=Integer.valueOf(args[1]);
		}	
		String rootFolder = "/home/sduprey/My_Data/My_Semantic_Arbo_Crawl4j";
		String user_agent_name = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
		CrawlConfig config = new CrawlConfig();
		config.setCrawlStorageFolder(rootFolder);
		config.setUserAgentString(user_agent_name);
		// Politeness delay : none by default
		config.setPolitenessDelay(0);
		// Unlimited number of pages can be crawled.
		config.setMaxPagesToFetch(-1);
		// we crawl up to depth 5
		// to get the navigation we only need to go up to depth 5
		int maxDepthOfCrawling =  1;        
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
		controller.start(SemanticArboCrawler.class, numberOfCrawlers);
		long estimatedTime = System.currentTimeMillis() - startTime;
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
		System.out.println("Aggregated Statistics:");
		System.out.println(" Processed Pages: " + totalProcessedPages);
		System.out.println(" Total Links found: " + totalLinks);
		System.out.println(" Total Text Size: " + totalTextSize);
		System.out.println(" Estimated time (ms): " + estimatedTime);

		// computing the number of inlinks per pages over the whole crawl
		System.out.println("Computing inlinks hashmap cache to the database");
		for (Object localData : crawlersLocalData) {
			SemanticArboCrawlDataCache stat = (SemanticArboCrawlDataCache) localData;
			Map<String, MultiSeedSemanticArboInfo> local_thread_cache = stat.getCrawledContent();
			updateInLinksThreadCache(local_thread_cache);
		}

		// saving results to the database
		System.out.println("Saving the whole crawl to the database");		
		System.out.println("Saving inlinks hashmap to the database");
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
	
	public static boolean isAllowedSiteforMultipleCrawl(String href){
		boolean found = false;
		for (String seed : multiple_seeds){
			if(href.startsWith(seed)){
				found=true;
			}
		}
		return found;
	}
}