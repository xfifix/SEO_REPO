package crawl4j.arbo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import crawl4j.urlutilities.ArboInfo;

public class ArboCrawlDataManagement {
	// we here keep every thing in RAM memory because the inlinks cache updates each time.
	// we save everything just at the very end of the crawl
	
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static String insert_statement="INSERT INTO CRAWL_RESULTS(URL,WHOLE_TEXT,TITLE,LINKS_SIZE,"
			+ "LINKS,H1,FOOTER_EXTRACT,ZTD_EXTRACT,SHORT_DESCRIPTION,VENDOR,ATTRIBUTES,NB_ATTRIBUTES,STATUS_CODE,HEADERS,DEPTH,PAGE_TYPE,MAGASIN,RAYON,PRODUIT,LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static String update_statement ="UPDATE CRAWL_RESULTS SET WHOLE_TEXT=?,TITLE=?,LINKS_SIZE=?,LINKS=?,H1=?,FOOTER_EXTRACT=?,ZTD_EXTRACT=?,SHORT_DESCRIPTION=?,VENDOR=?,ATTRIBUTES=?,NB_ATTRIBUTES=?,STATUS_CODE=?,HEADERS=?,DEPTH=?,PAGE_TYPE=?,MAGASIN=?,RAYON=?,PRODUIT=?,LAST_UPDATE=? WHERE URL=?";

	private int totalProcessedPages;
	private long totalLinks;
	private long totalTextSize;
	private Connection con;
	


	// local cache which should contain the site up to depth 5
	// url arbo info cache
	private Map<String, ArboInfo> crawledContent = new HashMap<String, ArboInfo>();
	// in links cache
	private Map<String, Set<String>> inlinks_cache = new HashMap<String, Set<String>>();

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

	public void updateDatabaseData(){
		try{
			Iterator<Entry<String, ArboInfo>> it = crawledContent.entrySet().iterator();
			int local_counter = 0;
			if (it.hasNext()){
				local_counter++;
				PreparedStatement st = con.prepareStatement(update_statement);
				do {
					local_counter ++;
					Map.Entry<String, ArboInfo> pairs = (Map.Entry<String, ArboInfo>)it.next();
					String url=pairs.getKey();
					ArboInfo info = pairs.getValue();
					// preparing the statement
					st.setString(1,info.getText());
					st.setString(2,info.getTitle());
					st.setInt(3,info.getLinks_size());
					st.setString(4,info.getOut_links());
					st.setString(5,info.getH1());
					st.setString(6,info.getFooter());
					st.setString(7,info.getZtd());
					st.setString(8,info.getShort_desc());
					st.setString(9,info.getVendor());
					st.setString(10,info.getAtt_desc());
					st.setInt(11,info.getAtt_number());
					st.setInt(12,info.getStatus_code());
					st.setString(13,info.getResponse_headers());		
					st.setInt(14,info.getDepth());
					st.setString(15, info.getPage_type());
					st.setString(16, info.getMagasin());
					st.setString(17, info.getRayon());
					st.setString(18, info.getProduit());
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(19,sqlDate);
					st.setString(20,url);
					int affected_row = st.executeUpdate();
					// if the row has not been updated, we have to insert it !
					if(affected_row == 0){
						PreparedStatement insert_st = con.prepareStatement(insert_statement);
						insert_st.setString(1,url);
						insert_st.setString(2,info.getText());
						insert_st.setString(3,info.getTitle());
						insert_st.setInt(4,info.getLinks_size());
						insert_st.setString(5,info.getOut_links());
						insert_st.setString(6,info.getH1());
						insert_st.setString(7,info.getFooter());
						insert_st.setString(8,info.getZtd());
						insert_st.setString(9,info.getShort_desc());
						insert_st.setString(10,info.getVendor());
						insert_st.setString(11,info.getAtt_desc());
						insert_st.setInt(12,info.getAtt_number());
						insert_st.setInt(13,info.getStatus_code());
						insert_st.setString(14,info.getResponse_headers());		
						insert_st.setInt(15,info.getDepth());
						insert_st.setString(16, info.getPage_type());
						insert_st.setString(17, info.getMagasin());
						insert_st.setString(18, info.getRayon());
						insert_st.setString(19, info.getProduit());
						insert_st.setDate(20,sqlDate);
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
					Logger lgr = Logger.getLogger(ArboCrawlDataManagement.class.getName());
					lgr.log(Level.ERROR, ex1.getMessage(), ex1);
				}
			}

			Logger lgr = Logger.getLogger(ArboCrawlDataManagement.class.getName());
			lgr.log(Level.ERROR, e.getMessage(), e);
		}	
	}
	// we here perform upsert to keep up to date our crawl referential
	public void saveData(){
		updateDatabaseData();
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


}