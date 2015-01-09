package crawl4j.continuous;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import crawl4j.urlutilities.URLinfo;

public class CrawlDataManagement {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static String insert_statement="INSERT INTO CRAWL_RESULTS(URL,WHOLE_TEXT,TITLE,LINKS_SIZE,"
			+ "LINKS,H1,FOOTER_EXTRACT,ZTD_EXTRACT,SHORT_DESCRIPTION,VENDOR,ATTRIBUTES,NB_ATTRIBUTES,STATUS_CODE,HEADERS,DEPTH,PAGE_TYPE,MAGASIN,RAYON,PRODUIT,LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static String insert_statement_with_oid="INSERT INTO CRAWL_RESULTS(URL,WHOLE_TEXT,TITLE,LINKS_SIZE,"
			+ "LINKS,H1,FOOTER_EXTRACT,ZTD_EXTRACT,SHORT_DESCRIPTION,VENDOR,ATTRIBUTES,NB_ATTRIBUTES,STATUS_CODE,HEADERS,DEPTH,PAGE_TYPE,MAGASIN,RAYON,PRODUIT,BLOBOID,LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static String update_statement ="UPDATE CRAWL_RESULTS SET WHOLE_TEXT=?,TITLE=?,LINKS_SIZE=?,LINKS=?,H1=?,FOOTER_EXTRACT=?,ZTD_EXTRACT=?,SHORT_DESCRIPTION=?,VENDOR=?,ATTRIBUTES=?,NB_ATTRIBUTES=?,STATUS_CODE=?,HEADERS=?,DEPTH=?,PAGE_TYPE=?,MAGASIN=?,RAYON=?,PRODUIT=?,LAST_UPDATE=? WHERE URL=?";
	private static String get_blob_oid = "SELECT BLOBOID FROM CRAWL_RESULTS WHERE URL = ?";
	private int totalProcessedPages;
	private long totalLinks;
	private long totalTextSize;
	private Connection con;
	private HttpSolrServer solr_server;
	private Map<String, URLinfo> crawledContent = new HashMap<String, URLinfo>();

	public CrawlDataManagement() {
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
			solr_server = new HttpSolrServer("http://localhost:8983/solr");
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

	public void updateSolrData() {
		try{
			Iterator<Entry<String, URLinfo>> it = crawledContent.entrySet().iterator();
			int local_counter = 0;
			if (it.hasNext()){
				local_counter++;
				do {
					local_counter ++;
					Map.Entry<String, URLinfo> pairs = (Map.Entry<String, URLinfo>)it.next();
					String url=pairs.getKey();
					URLinfo info =pairs.getValue();
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id",url.replace("http://www.cdiscount.com/",""));
					doc.addField("url",url);
					doc.addField("whole_text",info.getText());
					doc.addField("title",info.getTitle());
					doc.addField("links_size",info.getLinks_size());
					doc.addField("links",info.getOut_links());
					doc.addField("h1",info.getH1());
					doc.addField("footer_extract",info.getFooter());
					doc.addField("ztd_extract",info.getZtd());
					doc.addField("short_description",info.getShort_desc());		
					doc.addField("vendor",info.getVendor());
					doc.addField("attributes",info.getAtt_desc());
					doc.addField("nb_attributes",info.getAtt_number());
					doc.addField("status_code", info.getStatus_code());
					doc.addField("headers", info.getResponse_headers());
					doc.addField("depth", info.getDepth());
					doc.addField("page_type", info.getPage_type());
					doc.addField("magasin", info.getMagasin());
					doc.addField("rayon", info.getRayon());
					doc.addField("produit", info.getProduit());
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					doc.addField("last_update", sqlDate.toString());	
					try{
						solr_server.add(doc);
					}catch (Exception e){
						System.out.println("Trouble inserting : "+url);
						e.printStackTrace();  
					}
				}while (it.hasNext());	
				solr_server.commit();
				System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
			}
		} catch (Exception e){
			//System.out.println("Line already inserted : "+nb_lines);
			e.printStackTrace();  
		}
	}

	public int getBlobOID(String url){
		int oid = 0;
		try {
			PreparedStatement oid_query_statement = con.prepareStatement(get_blob_oid);
			oid_query_statement.setString(1, url);
			ResultSet oid_result = oid_query_statement.executeQuery();
			if (oid_result.next()){
				oid = oid_result.getInt(1);
			}		
		} catch (SQLException e) {
			System.out.println("Trouble fetching OID from the database");
			e.printStackTrace();
		}
		return oid;
	}

	public void update_url(URLinfo info, String url) throws SQLException, IOException{
		int oid = getBlobOID(url);
		// if oid not found, we insert a whole new line and we create a new blob
		if (oid == 0){
			// we create here the blob
			LargeObjectManager lobj = ((org.postgresql.PGConnection)con).getLargeObjectAPI();
			oid = lobj.create(LargeObjectManager.READ | LargeObjectManager.WRITE);
			// Open the large object for writing
			LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
			InputStream fis = new ByteArrayInputStream(info.getPage_source_code());
			// Copy the data from the file to the large object
			// 2048 is the buffer size, does not really matter
			byte buf[] = new byte[2048];
			int s, tl = 0;
			while ((s = fis.read(buf, 0, 2048)) > 0) {
				obj.write(buf, 0, s);
				tl += s;
			}
			System.out.println("BLOB byte size written : "+tl);
			obj.close();
			// once the blob has been written we insert the whole url line in crawl_results
			// we insert here the brand new url with its blob oid
			PreparedStatement insert_st = con.prepareStatement(insert_statement_with_oid);
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
			insert_st.setInt(20,oid);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			insert_st.setDate(21,sqlDate);
			insert_st.executeUpdate(); 	
		} else {
			// if oid found, we update the found line and we update the matching blob

		}
		con.commit();
	}

	public void updateDatabaseWithBlobData(){
		Iterator<Entry<String, URLinfo>> it = crawledContent.entrySet().iterator();

		while (it.hasNext()){
			Map.Entry<String, URLinfo> pairs = (Map.Entry<String, URLinfo>)it.next();
			String url=pairs.getKey();
			URLinfo info = pairs.getValue();
			try {
				con.setAutoCommit(false);
				update_url(info,url);

			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Trouble inserting URL  : " + url);
				e.printStackTrace();
			}
		}
	}

	public void updateDatabaseData(){
		try{
			Iterator<Entry<String, URLinfo>> it = crawledContent.entrySet().iterator();
			int local_counter = 0;
			if (it.hasNext()){
				local_counter++;
				PreparedStatement st = con.prepareStatement(update_statement);
				do {
					local_counter ++;
					Map.Entry<String, URLinfo> pairs = (Map.Entry<String, URLinfo>)it.next();
					String url=pairs.getKey();
					URLinfo info = pairs.getValue();
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
					ex1.printStackTrace();
				}
			}
			e.printStackTrace();
		}	
	}
	// we here perform upsert to keep up to date our crawl referential
	public void updateData(){
		// we here choose wether or not we store all the page source code
		if (ContinuousController.isBlobStored){
			updateDatabaseWithBlobData();
		} else {
			updateDatabaseData();
		}
		updateSolrData();
		// clear cache
		crawledContent.clear();
	}

	// old and brute force way : we insert all URLs
	public void saveData(){
		try{
			Iterator<Entry<String, URLinfo>> it = crawledContent.entrySet().iterator();
			int local_counter = 0;
			if (it.hasNext()){
				local_counter++;
				con.setAutoCommit(false);
				PreparedStatement st = con.prepareStatement(insert_statement);

				do {
					local_counter ++;
					Map.Entry<String, URLinfo> pairs = it.next();
					String url=pairs.getKey();
					URLinfo info = pairs.getValue();
					//					String prepared_string = "("+url+","
					//					                            +(String)list_values[0]+","
					//					                            +(String)list_values[1]+","
					//             					                +(String)list_values[1]+","
					//             					                +(int)list_values[2]+","
					//					                            +(String)list_values[3]+","
					//             					                +(String)list_values[4]+","
					//             					                +(String)list_values[5]+","
					//					                            +(String)list_values[6]+","
					//             					                +(String)list_values[7]+","
					//             					                +(String)list_values[8]+","
					//					                            +(String)list_values[9]+","
					//             					                +(int)list_values[10]+","
					//               					                +(int)list_values[11]+","
					//   					                            +(String)list_values[12]+")";
					st.setString(1,url);
					st.setString(2,info.getText());
					st.setString(3,info.getTitle());
					st.setInt(4,info.getLinks_size());
					st.setString(5,info.getOut_links());
					st.setString(6,info.getH1());
					st.setString(7,info.getFooter());
					st.setString(8,info.getZtd());
					st.setString(9,info.getShort_desc());
					st.setString(10,info.getVendor());
					st.setString(11,info.getAtt_desc());
					st.setInt(12,info.getAtt_number());
					st.setInt(13,info.getStatus_code());
					st.setString(14,info.getResponse_headers());		
					st.setInt(15,info.getDepth());
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(16,sqlDate);
					st.addBatch();
				}while (it.hasNext());	
				st.executeBatch();		 
				con.commit();
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
		}	
		crawledContent.clear();
	}

	public Connection getCon() {
		return con;
	}

	public void setCon(Connection con) {
		this.con = con;
	}

	public Map<String, URLinfo> getCrawledContent() {
		return crawledContent;
	}

	public void setCrawledContent(Map<String, URLinfo> crawledContent) {
		this.crawledContent = crawledContent;
	}
}