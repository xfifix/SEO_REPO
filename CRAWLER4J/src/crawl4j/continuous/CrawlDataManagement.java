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
import java.sql.Statement;
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
import crawl4j.xpathutility.XPathUtility;

public class CrawlDataManagement {
	public static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static String insert_statement="INSERT INTO CRAWL_RESULTS(URL,WHOLE_TEXT,TITLE,LINKS_SIZE,"
			+ "LINKS,H1,FOOTER_EXTRACT,ZTD_EXTRACT,SHORT_DESCRIPTION,XPATH1,XPATH2,XPATH3,XPATH4,XPATH5,XPATH6,XPATH7,XPATH8,XPATH9,XPATH10,CDISCOUNT_VENDOR,YOUTUBE_REFERENCED,ATTRIBUTES,NB_ATTRIBUTES,STATUS_CODE,HEADERS,DEPTH,PAGE_TYPE,MAGASIN,RAYON,PRODUIT,LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static String insert_statement_with_oid="INSERT INTO CRAWL_RESULTS(URL,WHOLE_TEXT,TITLE,LINKS_SIZE,"
			+ "LINKS,H1,FOOTER_EXTRACT,ZTD_EXTRACT,SHORT_DESCRIPTION,XPATH1,XPATH2,XPATH3,XPATH4,XPATH5,XPATH6,XPATH7,XPATH8,XPATH9,XPATH10,CDISCOUNT_VENDOR,YOUTUBE_REFERENCED,ATTRIBUTES,NB_ATTRIBUTES,STATUS_CODE,HEADERS,DEPTH,PAGE_TYPE,MAGASIN,RAYON,PRODUIT,BLOBOID,LAST_UPDATE)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static String update_statement ="UPDATE CRAWL_RESULTS SET WHOLE_TEXT=?,TITLE=?,LINKS_SIZE=?,LINKS=?,H1=?,FOOTER_EXTRACT=?,ZTD_EXTRACT=?,SHORT_DESCRIPTION=?,XPATH1=?,XPATH2=?,XPATH3=?,XPATH4=?,XPATH5=?,XPATH6=?,XPATH7=?,XPATH8=?,XPATH9=?,XPATH10=?,CDISCOUNT_VENDOR=?,YOUTUBE_REFERENCED=?,ATTRIBUTES=?,NB_ATTRIBUTES=?,STATUS_CODE=?,HEADERS=?,DEPTH=?,PAGE_TYPE=?,MAGASIN=?,RAYON=?,PRODUIT=?,LAST_UPDATE=? WHERE URL=?";
	private static String update_statement_with_oid ="UPDATE CRAWL_RESULTS SET WHOLE_TEXT=?,TITLE=?,LINKS_SIZE=?,LINKS=?,H1=?,FOOTER_EXTRACT=?,ZTD_EXTRACT=?,SHORT_DESCRIPTION=?,XPATH1=?,XPATH2=?,XPATH3=?,XPATH4=?,XPATH5=?,XPATH6=?,XPATH7=?,XPATH8=?,XPATH9=?,XPATH10=?,CDISCOUNT_VENDOR=?,YOUTUBE_REFERENCED=?,ATTRIBUTES=?,NB_ATTRIBUTES=?,STATUS_CODE=?,HEADERS=?,DEPTH=?,PAGE_TYPE=?,MAGASIN=?,RAYON=?,PRODUIT=?,BLOBOID=?,LAST_UPDATE=? WHERE URL=?";

	private static String get_blob_oid = "SELECT BLOBOID FROM CRAWL_RESULTS WHERE URL = ?";
	private int totalProcessedPages;
	private long totalLinks;
	private long totalTextSize;
	private Connection con;
	private HttpSolrServer solr_server;
	private Map<String, URLinfo> crawledContent = new HashMap<String, URLinfo>();
	private static String[] xpath_expression;

	public CrawlDataManagement() {
		// loading XPATH expression
		setXpath_expression(XPathUtility.loadXPATHConf());
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
					String[] XPATHRESULTS = info.getXPATH_results();
					if (XPATHRESULTS != null){	
						if (XPATHRESULTS[0] != null){
							doc.addField("xpath1",XPATHRESULTS[0]);	
						} else {
							doc.addField("xpath1","");
						}
						if (XPATHRESULTS[1] != null){
							doc.addField("xpath2",XPATHRESULTS[1]);
						} else {
							doc.addField("xpath2","");
						}
						if (XPATHRESULTS[2] != null){
							doc.addField("xpath3",XPATHRESULTS[2]);
						} else {
							doc.addField("xpath3","");
						}
						if (XPATHRESULTS[3] != null){
							doc.addField("xpath4",XPATHRESULTS[3]);
						} else {
							doc.addField("xpath4","");
						}
						if (XPATHRESULTS[4] != null){
							doc.addField("xpath5",XPATHRESULTS[4]);
						} else {
							doc.addField("xpath5","");
						}
						if (XPATHRESULTS[5] != null){
							doc.addField("xpath6",XPATHRESULTS[5]);
						} else {
							doc.addField("xpath6","");
						}
						if (XPATHRESULTS[6] != null){
							doc.addField("xpath7",XPATHRESULTS[6]);
						} else {
							doc.addField("xpath7","");
						}
						if (XPATHRESULTS[7] != null){
							doc.addField("xpath8",XPATHRESULTS[7]);
						} else {
							doc.addField("xpath8","");
						}
						if (XPATHRESULTS[8] != null){
							doc.addField("xpath9",XPATHRESULTS[8]);
						} else {
							doc.addField("xpath9","");
						}
						if (XPATHRESULTS[9] != null){
							doc.addField("xpath10",XPATHRESULTS[9]);
						} else {
							doc.addField("xpath10","");
						}
					}else {
						doc.addField("xpath1","");
						doc.addField("xpath2","");
						doc.addField("xpath3","");
						doc.addField("xpath4","");
						doc.addField("xpath5","");
						doc.addField("xpath6","");
						doc.addField("xpath7","");
						doc.addField("xpath8","");
						doc.addField("xpath9","");
						doc.addField("xpath10","");
					}
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
			oid_result.close();
			oid_query_statement.close();
		} catch (SQLException e) {
			System.out.println("Trouble fetching OID from the database");
			e.printStackTrace();
		}
		return oid;
	}

	@SuppressWarnings("deprecation")
	public void update_url_and_blob(URLinfo info, String url) throws SQLException, IOException{
		int oid = getBlobOID(url);
		LargeObjectManager lobj = ((org.postgresql.PGConnection)con).getLargeObjectAPI();
		// if oid not found, we insert a whole new line and we create a new blob
		if (oid == 0){
			// we create here the blob
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
			// we close the stream
			fis.close();
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


			String[] XPATHRESULTS = info.getXPATH_results();
			if (XPATHRESULTS != null){	
				if (XPATHRESULTS[0] != null){
					insert_st.setString(10, XPATHRESULTS[0]);
				} else {
					insert_st.setString(10, "");
				}
				if (XPATHRESULTS[1] != null){
					insert_st.setString(11, XPATHRESULTS[1]);
				} else {
					insert_st.setString(11, "");
				}
				if (XPATHRESULTS[2] != null){
					insert_st.setString(12, XPATHRESULTS[2]);
				} else {
					insert_st.setString(12, "");
				}
				if (XPATHRESULTS[3] != null){
					insert_st.setString(13, XPATHRESULTS[3]);
				} else {
					insert_st.setString(13, "");
				}
				if (XPATHRESULTS[4] != null){
					insert_st.setString(14, XPATHRESULTS[4]);
				} else {
					insert_st.setString(14, "");
				}
				if (XPATHRESULTS[5] != null){
					insert_st.setString(15, XPATHRESULTS[5]);
				} else {
					insert_st.setString(15, "");
				}
				if (XPATHRESULTS[6] != null){
					insert_st.setString(16, XPATHRESULTS[6]);
				} else {
					insert_st.setString(16, "");
				}
				if (XPATHRESULTS[7] != null){
					insert_st.setString(17, XPATHRESULTS[7]);
				} else {
					insert_st.setString(17, "");
				}
				if (XPATHRESULTS[8] != null){
					insert_st.setString(18, XPATHRESULTS[8]);
				} else {
					insert_st.setString(18, "");
				}
				if (XPATHRESULTS[9] != null){
					insert_st.setString(19, XPATHRESULTS[9]);
				} else {
					insert_st.setString(19, "");
				}
			}else {
				insert_st.setString(10, "");
				insert_st.setString(11, "");
				insert_st.setString(12, "");
				insert_st.setString(13, "");
				insert_st.setString(14, "");
				insert_st.setString(15, "");
				insert_st.setString(16, "");
				insert_st.setString(17, "");
				insert_st.setString(18, "");
				insert_st.setString(19, "");
			}
			insert_st.setBoolean(20,info.isCdiscountBestBid());
			insert_st.setBoolean(21,info.isYoutubeVideoReferenced());		
			insert_st.setString(22,info.getAtt_desc());
			insert_st.setInt(23,info.getAtt_number());
			insert_st.setInt(24,info.getStatus_code());
			insert_st.setString(25,info.getResponse_headers());		
			insert_st.setInt(26,info.getDepth());
			insert_st.setString(27, info.getPage_type());
			insert_st.setString(28, info.getMagasin());
			insert_st.setString(29, info.getRayon());
			insert_st.setString(30, info.getProduit());
			insert_st.setInt(31,oid);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			insert_st.setDate(32,sqlDate);
			insert_st.executeUpdate(); 	
			insert_st.close();
		} else {
			// if oid found not null, we update the found line and we update the matching blob
			// we don't create the object, we just open it
			LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
			// we write the new content to the BLOB
			InputStream fis = new ByteArrayInputStream(info.getPage_source_code());
			// Copy the data from the file to the large object
			// 2048 is the buffer size, does not really matter
			byte buf[] = new byte[2048];
			int s, tl = 0;
			while ((s = fis.read(buf, 0, 2048)) > 0) {
				obj.write(buf, 0, s);
				tl += s;
			}
			System.out.println("BLOB byte updated size : "+tl);
			// please here do not forget to truncate to the new size in case the previous one was bigger
			obj.truncate(tl);
			// Close the large object
			obj.close();
			// we close the stream
			fis.close();
			// once the BLOB object has been updated, we update the matching line
			PreparedStatement update_st = con.prepareStatement(update_statement_with_oid);
			update_st.setString(1,info.getText());
			update_st.setString(2,info.getTitle());
			update_st.setInt(3,info.getLinks_size());
			update_st.setString(4,info.getOut_links());
			update_st.setString(5,info.getH1());
			update_st.setString(6,info.getFooter());
			update_st.setString(7,info.getZtd());
			update_st.setString(8,info.getShort_desc());	

			String[] XPATHRESULTS = info.getXPATH_results();
			if (XPATHRESULTS != null){	
				if (XPATHRESULTS[0] != null){
					update_st.setString(9, XPATHRESULTS[0]);
				} else {
					update_st.setString(9, "");
				}
				if (XPATHRESULTS[1] != null){
					update_st.setString(10, XPATHRESULTS[1]);
				} else {
					update_st.setString(10, "");
				}
				if (XPATHRESULTS[2] != null){
					update_st.setString(11, XPATHRESULTS[2]);
				} else {
					update_st.setString(11, "");
				}
				if (XPATHRESULTS[3] != null){
					update_st.setString(12, XPATHRESULTS[3]);
				} else {
					update_st.setString(12, "");
				}
				if (XPATHRESULTS[4] != null){
					update_st.setString(13, XPATHRESULTS[4]);
				} else {
					update_st.setString(13, "");
				}
				if (XPATHRESULTS[5] != null){
					update_st.setString(14, XPATHRESULTS[5]);
				} else {
					update_st.setString(14, "");
				}
				if (XPATHRESULTS[6] != null){
					update_st.setString(15, XPATHRESULTS[6]);
				} else {
					update_st.setString(15, "");
				}
				if (XPATHRESULTS[7] != null){
					update_st.setString(16, XPATHRESULTS[7]);
				} else {
					update_st.setString(16, "");
				}
				if (XPATHRESULTS[8] != null){
					update_st.setString(17, XPATHRESULTS[8]);
				} else {
					update_st.setString(17, "");
				}
				if (XPATHRESULTS[9] != null){
					update_st.setString(18, XPATHRESULTS[9]);
				} else {
					update_st.setString(18, "");
				}
			}else {
				update_st.setString(9, "");
				update_st.setString(10, "");
				update_st.setString(11, "");
				update_st.setString(12, "");
				update_st.setString(13, "");
				update_st.setString(14, "");
				update_st.setString(15, "");
				update_st.setString(16, "");
				update_st.setString(17, "");
				update_st.setString(18, "");
			}
			update_st.setBoolean(19,info.isCdiscountBestBid());
			update_st.setBoolean(20,info.isYoutubeVideoReferenced());
			update_st.setString(21,info.getAtt_desc());
			update_st.setInt(22,info.getAtt_number());
			update_st.setInt(23,info.getStatus_code());
			update_st.setString(24,info.getResponse_headers());		
			update_st.setInt(25,info.getDepth());
			update_st.setString(26, info.getPage_type());
			update_st.setString(27, info.getMagasin());
			update_st.setString(28, info.getRayon());
			update_st.setString(29, info.getProduit());
			update_st.setInt(30, oid);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			update_st.setDate(31,sqlDate);
			update_st.setString(32,url);
			// we here don't care about wether or not the line has been found and updated
			// as we have found the blob oid, the line is present and should be updated
			//int affected_row = update_st.executeUpdate();
			update_st.executeUpdate();
			update_st.close();
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
				update_url_and_blob(info,url);
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
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(update_statement,Statement.RETURN_GENERATED_KEYS);
			while (it.hasNext()){
				local_counter++;
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
				String[] XPATHRESULTS = info.getXPATH_results();
				if (XPATHRESULTS != null){	
					if (XPATHRESULTS[0] != null){
						st.setString(9, XPATHRESULTS[0]);
					} else {
						st.setString(9, "");
					}
					if (XPATHRESULTS[1] != null){
						st.setString(10, XPATHRESULTS[1]);
					} else {
						st.setString(10, "");
					}
					if (XPATHRESULTS[2] != null){
						st.setString(11, XPATHRESULTS[2]);
					} else {
						st.setString(11, "");
					}
					if (XPATHRESULTS[3] != null){
						st.setString(12, XPATHRESULTS[3]);
					} else {
						st.setString(12, "");
					}
					if (XPATHRESULTS[4] != null){
						st.setString(13, XPATHRESULTS[4]);
					} else {
						st.setString(13, "");
					}
					if (XPATHRESULTS[5] != null){
						st.setString(14, XPATHRESULTS[5]);
					} else {
						st.setString(14, "");
					}
					if (XPATHRESULTS[6] != null){
						st.setString(15, XPATHRESULTS[6]);
					} else {
						st.setString(15, "");
					}
					if (XPATHRESULTS[7] != null){
						st.setString(16, XPATHRESULTS[7]);
					} else {
						st.setString(16, "");
					}
					if (XPATHRESULTS[8] != null){
						st.setString(17, XPATHRESULTS[8]);
					} else {
						st.setString(17, "");
					}
					if (XPATHRESULTS[9] != null){
						st.setString(18, XPATHRESULTS[9]);
					} else {
						st.setString(18, "");
					}
				}else {
					st.setString(9, "");
					st.setString(10, "");
					st.setString(11, "");
					st.setString(12, "");
					st.setString(13, "");
					st.setString(14, "");
					st.setString(15, "");
					st.setString(16, "");
					st.setString(17, "");
					st.setString(18, "");
				}
				st.setBoolean(19,info.isCdiscountBestBid());
				st.setBoolean(20,info.isYoutubeVideoReferenced());
				st.setString(21,info.getAtt_desc());
				st.setInt(22,info.getAtt_number());
				st.setInt(23,info.getStatus_code());
				st.setString(24,info.getResponse_headers());		
				st.setInt(25,info.getDepth());
				st.setString(26, info.getPage_type());
				st.setString(27, info.getMagasin());
				st.setString(28, info.getRayon());
				st.setString(29, info.getProduit());
				java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
				st.setDate(30,sqlDate);
				st.setString(31,url);
				st.addBatch();
			}
			st.executeBatch();
			con.commit();
			ResultSet rs = st.getGeneratedKeys();
			String inserted_keys="";
			while (rs != null && rs.next()) {
				inserted_keys = rs.getString(1);
				crawledContent.remove(inserted_keys);
			}
			st.close();
			// we loop over the update batch to check for missed updates results
			// missed update results were not present : we insert its
			Iterator<Entry<String, URLinfo>> batch_updates_missed_result_it = crawledContent.entrySet().iterator();
			con.setAutoCommit(false);
			PreparedStatement insert_st = con.prepareStatement(insert_statement);
			while (batch_updates_missed_result_it.hasNext()){
				Map.Entry<String, URLinfo> updates_pairs = (Map.Entry<String, URLinfo>)batch_updates_missed_result_it.next();
				String url=updates_pairs.getKey();
				URLinfo info = updates_pairs.getValue();
				insert_st.setString(1,url);
				insert_st.setString(2,info.getText());
				insert_st.setString(3,info.getTitle());
				insert_st.setInt(4,info.getLinks_size());
				insert_st.setString(5,info.getOut_links());
				insert_st.setString(6,info.getH1());
				insert_st.setString(7,info.getFooter());
				insert_st.setString(8,info.getZtd());
				insert_st.setString(9,info.getShort_desc());	
				String[] XPATHRESULTS = info.getXPATH_results();
				if (XPATHRESULTS != null){	
					if (XPATHRESULTS[0] != null){
						insert_st.setString(10, XPATHRESULTS[0]);
					} else {
						insert_st.setString(10, "");
					}
					if (XPATHRESULTS[1] != null){
						insert_st.setString(11, XPATHRESULTS[1]);
					} else {
						insert_st.setString(11, "");
					}
					if (XPATHRESULTS[2] != null){
						insert_st.setString(12, XPATHRESULTS[2]);
					} else {
						insert_st.setString(12, "");
					}
					if (XPATHRESULTS[3] != null){
						insert_st.setString(13, XPATHRESULTS[3]);
					} else {
						insert_st.setString(13, "");
					}
					if (XPATHRESULTS[4] != null){
						insert_st.setString(14, XPATHRESULTS[4]);
					} else {
						insert_st.setString(14, "");
					}
					if (XPATHRESULTS[5] != null){
						insert_st.setString(15, XPATHRESULTS[5]);
					} else {
						insert_st.setString(15, "");
					}
					if (XPATHRESULTS[6] != null){
						insert_st.setString(16, XPATHRESULTS[6]);
					} else {
						insert_st.setString(16, "");
					}
					if (XPATHRESULTS[7] != null){
						insert_st.setString(17, XPATHRESULTS[7]);
					} else {
						insert_st.setString(17, "");
					}
					if (XPATHRESULTS[8] != null){
						insert_st.setString(18, XPATHRESULTS[8]);
					} else {
						insert_st.setString(18, "");
					}
					if (XPATHRESULTS[9] != null){
						insert_st.setString(19, XPATHRESULTS[9]);
					} else {
						insert_st.setString(19, "");
					}
				}else {
					insert_st.setString(10, "");
					insert_st.setString(11, "");
					insert_st.setString(12, "");
					insert_st.setString(13, "");
					insert_st.setString(14, "");
					insert_st.setString(15, "");
					insert_st.setString(16, "");
					insert_st.setString(17, "");
					insert_st.setString(18, "");
					insert_st.setString(19, "");
				}
				insert_st.setBoolean(20,info.isCdiscountBestBid());
				insert_st.setBoolean(21,info.isYoutubeVideoReferenced());					
				insert_st.setString(22,info.getAtt_desc());
				insert_st.setInt(23,info.getAtt_number());
				insert_st.setInt(24,info.getStatus_code());
				insert_st.setString(25,info.getResponse_headers());		
				insert_st.setInt(26,info.getDepth());
				insert_st.setString(27, info.getPage_type());
				insert_st.setString(28, info.getMagasin());
				insert_st.setString(29, info.getRayon());
				insert_st.setString(30, info.getProduit());
				java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
				insert_st.setDate(31,sqlDate);
				insert_st.addBatch();
			}
			insert_st.executeBatch();
			con.commit();
			insert_st.close();
			System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
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


	public void updateDatabaseDataStepByStep(){
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
					String[] XPATHRESULTS = info.getXPATH_results();
					if (XPATHRESULTS != null){	
						if (XPATHRESULTS[0] != null){
							st.setString(9, XPATHRESULTS[0]);
						} else {
							st.setString(9, "");
						}
						if (XPATHRESULTS[1] != null){
							st.setString(10, XPATHRESULTS[1]);
						} else {
							st.setString(10, "");
						}
						if (XPATHRESULTS[2] != null){
							st.setString(11, XPATHRESULTS[2]);
						} else {
							st.setString(11, "");
						}
						if (XPATHRESULTS[3] != null){
							st.setString(12, XPATHRESULTS[3]);
						} else {
							st.setString(12, "");
						}
						if (XPATHRESULTS[4] != null){
							st.setString(13, XPATHRESULTS[4]);
						} else {
							st.setString(13, "");
						}
						if (XPATHRESULTS[5] != null){
							st.setString(14, XPATHRESULTS[5]);
						} else {
							st.setString(14, "");
						}
						if (XPATHRESULTS[6] != null){
							st.setString(15, XPATHRESULTS[6]);
						} else {
							st.setString(15, "");
						}
						if (XPATHRESULTS[7] != null){
							st.setString(16, XPATHRESULTS[7]);
						} else {
							st.setString(16, "");
						}
						if (XPATHRESULTS[8] != null){
							st.setString(17, XPATHRESULTS[8]);
						} else {
							st.setString(17, "");
						}
						if (XPATHRESULTS[9] != null){
							st.setString(18, XPATHRESULTS[9]);
						} else {
							st.setString(18, "");
						}
					}else {
						st.setString(9, "");
						st.setString(10, "");
						st.setString(11, "");
						st.setString(12, "");
						st.setString(13, "");
						st.setString(14, "");
						st.setString(15, "");
						st.setString(16, "");
						st.setString(17, "");
						st.setString(18, "");
					}
					st.setBoolean(19,info.isCdiscountBestBid());
					st.setBoolean(20,info.isYoutubeVideoReferenced());
					st.setString(21,info.getAtt_desc());
					st.setInt(22,info.getAtt_number());
					st.setInt(23,info.getStatus_code());
					st.setString(24,info.getResponse_headers());		
					st.setInt(25,info.getDepth());
					st.setString(26, info.getPage_type());
					st.setString(27, info.getMagasin());
					st.setString(28, info.getRayon());
					st.setString(29, info.getProduit());
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(30,sqlDate);
					st.setString(31,url);
					int affected_row = st.executeUpdate();
					st.close();
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
						if (XPATHRESULTS != null){	
							if (XPATHRESULTS[0] != null){
								insert_st.setString(10, XPATHRESULTS[0]);
							} else {
								insert_st.setString(10, "");
							}
							if (XPATHRESULTS[1] != null){
								insert_st.setString(11, XPATHRESULTS[1]);
							} else {
								insert_st.setString(11, "");
							}
							if (XPATHRESULTS[2] != null){
								insert_st.setString(12, XPATHRESULTS[2]);
							} else {
								insert_st.setString(12, "");
							}
							if (XPATHRESULTS[3] != null){
								insert_st.setString(13, XPATHRESULTS[3]);
							} else {
								insert_st.setString(13, "");
							}
							if (XPATHRESULTS[4] != null){
								insert_st.setString(14, XPATHRESULTS[4]);
							} else {
								insert_st.setString(14, "");
							}
							if (XPATHRESULTS[5] != null){
								insert_st.setString(15, XPATHRESULTS[5]);
							} else {
								insert_st.setString(15, "");
							}
							if (XPATHRESULTS[6] != null){
								insert_st.setString(16, XPATHRESULTS[6]);
							} else {
								insert_st.setString(16, "");
							}
							if (XPATHRESULTS[7] != null){
								insert_st.setString(17, XPATHRESULTS[7]);
							} else {
								insert_st.setString(17, "");
							}
							if (XPATHRESULTS[8] != null){
								insert_st.setString(18, XPATHRESULTS[8]);
							} else {
								insert_st.setString(18, "");
							}
							if (XPATHRESULTS[9] != null){
								insert_st.setString(19, XPATHRESULTS[9]);
							} else {
								insert_st.setString(19, "");
							}
						}else {
							insert_st.setString(10, "");
							insert_st.setString(11, "");
							insert_st.setString(12, "");
							insert_st.setString(13, "");
							insert_st.setString(14, "");
							insert_st.setString(15, "");
							insert_st.setString(16, "");
							insert_st.setString(17, "");
							insert_st.setString(18, "");
							insert_st.setString(19, "");
						}
						insert_st.setBoolean(20,info.isCdiscountBestBid());
						insert_st.setBoolean(21,info.isYoutubeVideoReferenced());					
						insert_st.setString(22,info.getAtt_desc());
						insert_st.setInt(23,info.getAtt_number());
						insert_st.setInt(24,info.getStatus_code());
						insert_st.setString(25,info.getResponse_headers());		
						insert_st.setInt(26,info.getDepth());
						insert_st.setString(27, info.getPage_type());
						insert_st.setString(28, info.getMagasin());
						insert_st.setString(29, info.getRayon());
						insert_st.setString(30, info.getProduit());
						insert_st.setDate(31,sqlDate);
						insert_st.executeUpdate();
						insert_st.close();
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
		// warning the upsert method in the PostgreSQL database will empty the cache
		// you have to update Solr first
		updateSolrData();
		// we here choose wether or not we store all the page source code
		if (ContinuousController.isBlobStored){
			updateDatabaseWithBlobData();
		} else {
			updateDatabaseData();
		}
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
					st.setString(1,url);
					st.setString(2,info.getText());
					st.setString(3,info.getTitle());
					st.setInt(4,info.getLinks_size());
					st.setString(5,info.getOut_links());
					st.setString(6,info.getH1());
					st.setString(7,info.getFooter());
					st.setString(8,info.getZtd());
					st.setString(9,info.getShort_desc());
					String[] XPATHRESULTS = info.getXPATH_results();		
					if (XPATHRESULTS != null){	
						if (XPATHRESULTS[0] != null){
							st.setString(10, XPATHRESULTS[0]);
						} else {
							st.setString(10, "");
						}
						if (XPATHRESULTS[1] != null){
							st.setString(11, XPATHRESULTS[1]);
						} else {
							st.setString(11, "");
						}
						if (XPATHRESULTS[2] != null){
							st.setString(12, XPATHRESULTS[2]);
						} else {
							st.setString(12, "");
						}
						if (XPATHRESULTS[3] != null){
							st.setString(13, XPATHRESULTS[3]);
						} else {
							st.setString(13, "");
						}
						if (XPATHRESULTS[4] != null){
							st.setString(14, XPATHRESULTS[4]);
						} else {
							st.setString(14, "");
						}
						if (XPATHRESULTS[5] != null){
							st.setString(15, XPATHRESULTS[5]);
						} else {
							st.setString(15, "");
						}
						if (XPATHRESULTS[6] != null){
							st.setString(16, XPATHRESULTS[6]);
						} else {
							st.setString(16, "");
						}
						if (XPATHRESULTS[7] != null){
							st.setString(17, XPATHRESULTS[7]);
						} else {
							st.setString(17, "");
						}
						if (XPATHRESULTS[8] != null){
							st.setString(18, XPATHRESULTS[8]);
						} else {
							st.setString(18, "");
						}
						if (XPATHRESULTS[9] != null){
							st.setString(19, XPATHRESULTS[9]);
						} else {
							st.setString(19, "");
						}
					}else {
						st.setString(10, "");
						st.setString(11, "");
						st.setString(12, "");
						st.setString(13, "");
						st.setString(14, "");
						st.setString(15, "");
						st.setString(16, "");
						st.setString(17, "");
						st.setString(18, "");
						st.setString(19, "");
					}
					st.setBoolean(20,info.isCdiscountBestBid());
					st.setBoolean(21,info.isYoutubeVideoReferenced());
					st.setString(22,info.getAtt_desc());
					st.setInt(23,info.getAtt_number());
					st.setInt(24,info.getStatus_code());
					st.setString(25,info.getResponse_headers());		
					st.setInt(26,info.getDepth());
					java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
					st.setDate(27,sqlDate);
					st.addBatch();
				}while (it.hasNext());	
				st.executeBatch();		 
				con.commit();
				st.close();
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

	public String[] getXpath_expression() {
		return xpath_expression;
	}

	public static void setXpath_expression(String[] xpath_expression) {
		CrawlDataManagement.xpath_expression = xpath_expression;
	}
}