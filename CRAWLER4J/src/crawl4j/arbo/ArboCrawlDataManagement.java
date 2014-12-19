package crawl4j.arbo;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import crawl4j.urlutilities.ArboInfo;

public class ArboCrawlDataManagement {
	// we here keep every thing in RAM memory because the inlinks cache updates each time.
	// we save everything just at the very end of the crawl
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";

	private static String insert_statement="INSERT INTO ARBOCRAWL_RESULTS (URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH,"
			+ " OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static String insert_statement_with_label="INSERT INTO ARBOCRAWL_RESULTS (URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH,"
			+ " OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, PAGE_TYPE, LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";


	private int totalProcessedPages;
	private long totalLinks;
	private long totalTextSize;
	private Connection con;

	// local cache which should contain the site up to depth 5
	// url arbo info cache ; common to all cache (static)
	private Map<String, ArboInfo> crawledContent = new HashMap<String, ArboInfo>();
	// in links cache common to all threads (static)
	private static Map<String, Set<String>> inlinks_cache = new HashMap<String, Set<String>>();

	public ArboCrawlDataManagement() {
		// Reading the property of our database
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
		try{
			con = DriverManager.getConnection(url, user, passwd);
		} catch (Exception e){
			System.out.println("Error instantiating either database or solr server");
			e.printStackTrace();
		}
	}

	public int getTotalProcessedPages() {
		return totalProcessedPages;
	}

	public void setTotalProcessedPages(int totalProcessedPages) {
		this.totalProcessedPages = totalProcessedPages;
	}

	public void incProcessedPages() {
		this.totalProcessedPages++;
	}

	public long getTotalLinks() {
		return totalLinks;
	}

	public void setTotalLinks(long totalLinks) {
		this.totalLinks = totalLinks;
	}

	public long getTotalTextSize() {
		return totalTextSize;
	}

	public void setTotalTextSize(long totalTextSize) {
		this.totalTextSize = totalTextSize;
	}

	public void incTotalLinks(int count) {
		this.totalLinks += count;
	}

	public void incTotalTextSize(int count) {
		this.totalTextSize += count;
	}

	public void saveDatabaseDataWithLabel(){
		try{
			Iterator<Entry<String, ArboInfo>> it = crawledContent.entrySet().iterator();
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_statement_with_label);
			int local_counter = 0;
			if (it.hasNext()){
				do {
					local_counter ++;
					//				PreparedStatement st = con.prepareStatement(insert_statement);
					Map.Entry<String, ArboInfo> pairs = (Map.Entry<String, ArboInfo>)it.next();
					String url=pairs.getKey();
					ArboInfo info = pairs.getValue();
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

	// old and brute force way : we insert all URLs
	public void saveDatabaseData(){
		try{
			Iterator<Entry<String, ArboInfo>> it = crawledContent.entrySet().iterator();
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_statement);
			int local_counter = 0;
			if (it.hasNext()){
				do {
					local_counter ++;
					//				PreparedStatement st = con.prepareStatement(insert_statement);
					Map.Entry<String, ArboInfo> pairs = (Map.Entry<String, ArboInfo>)it.next();
					String url=pairs.getKey();
					ArboInfo info = pairs.getValue();
					//(URL, WHOLE_TEXT, TITLE, H1, SHORT_DESCRIPTION, STATUS_CODE, DEPTH, OUTLINKS_SIZE, INLINKS_SIZE, NB_BREADCRUMBS, NB_AGGREGATED_RATINGS, NB_RATINGS_VALUES, NB_PRICES, NB_AVAILABILITIES, NB_REVIEWS, NB_REVIEWS_COUNT, NB_IMAGES, PAGE_TYPE, LAST_UPDATE)"
					//  1        2        3    4           5                6        7           8              9             10               11                      12            13              14             15            16             17         18         19
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
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(18,sqlDate);
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

	public void saveData(){
		saveDatabaseData();
		crawledContent.clear();
	}

	public void saveDataWithLabel(){
		saveDatabaseDataWithLabel();
		crawledContent.clear();
	}


	public void setCon(Connection con) {
		this.con = con;
	}

	public Connection getCon() {

		return con;
	}
	public Map<String, ArboInfo> getCrawledContent() {
		return crawledContent;
	}

	public void setCrawledContent(Map<String, ArboInfo> crawledContent) {
		this.crawledContent = crawledContent;
	}

	public Map<String, Set<String>> getInlinks_cache() {
		return inlinks_cache;
	}

	public void setInlinks_cache(Map<String, Set<String>> inlinks_cache) {
		this.inlinks_cache = inlinks_cache;
	}
}