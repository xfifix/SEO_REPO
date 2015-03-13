package crawl4j.nomatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

import crawl4j.continuous.ContinuousController;
import crawl4j.continuous.CrawlDataManagement;
import crawl4j.continuous.CrawlerUtility;
import crawl4j.facettesutility.FacettesInfo;
import crawl4j.facettesutility.FacettesUtility;
import crawl4j.urlutilities.URL_Utilities;
import crawl4j.urlutilities.URLinfo;
import crawl4j.xpathutility.XPathUtility;


public class NoMatchWorkerThread implements Runnable {
	public static int batch_size = 100;

	private static String nomatchUpdateStatement ="UPDATE NOMATCH SET TO_FETCH=FALSE WHERE ID=?";
	private static String nomatchSelectStatement ="SELECT URL, ID FROM NOMATCH WHERE TO_FETCH = TRUE and ID in ";

	private String user_agent;
	private List<ULRId> my_urls_to_fetch = new ArrayList<ULRId>();
	private CrawlDataManagement loc_data_manager;

	public NoMatchWorkerThread(CrawlDataManagement loc_crawl_data_manager ,List<Integer> to_fetch, String my_user_agent) throws SQLException{
		this.user_agent=my_user_agent;
		this.loc_data_manager = loc_crawl_data_manager;
		String my_url="";
		if (to_fetch.size()>0){
			try {
				PreparedStatement pst = null;
				my_url=nomatchSelectStatement+to_fetch.toString();
				my_url=my_url.replace("[", "(");
				my_url=my_url.replace("]", ")");
				pst = loc_data_manager.getCon().prepareStatement(my_url);
				ResultSet rs = null;
				rs = pst.executeQuery();
				while (rs.next()) {
					String loc_url = rs.getString(1);
					int id = rs.getInt(2);
					ULRId toadd = new ULRId();
					toadd.setId(id);
					toadd.setUrl(loc_url);
					my_urls_to_fetch.add(toadd); 
				}
				pst.close();
				System.out.println(Thread.currentThread()+" initialized with  : "+to_fetch.size() + " fetched URLs");

			}
			catch(SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with thread"+Thread.currentThread()+" and URL : "+my_url);
			}
		}
	}

	public void run() {
		List<ULRId> line_infos = new ArrayList<ULRId>();
		for (ULRId id :my_urls_to_fetch){
			line_infos.add(id);
			if (line_infos.size() !=0 && line_infos.size() % batch_size ==0) {
				runBatch(line_infos);	
				line_infos.clear();
				line_infos = new ArrayList<ULRId>();
			}
		}
		runBatch(line_infos);
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<ULRId> line_infos){
		List<ULRId> infos=processCommand(line_infos);
		updateStatus(infos);
		//updateStatusStepByStep(infos);
		System.out.println(Thread.currentThread().getName()+" End");
	}


	private void updateStatus(List<ULRId> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			loc_data_manager.getCon().setAutoCommit(false); 
			PreparedStatement st = loc_data_manager.getCon().prepareStatement(nomatchUpdateStatement);
			for (int i=0;i<infos.size();i++){
				ULRId local_info = infos.get(i);

				st.setInt(1, local_info.getId());
				st.addBatch();		
			}      
			//int counts[] = st.executeBatch();
			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
			st.executeBatch();
			loc_data_manager.getCon().commit();
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

	private void close_connection(){
		try {
			this.loc_data_manager.getCon().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private List<ULRId> processCommand(List<ULRId> line_infos) {
		List<ULRId> my_fetched_infos = new ArrayList<ULRId>();
		for(ULRId line_info : line_infos){
			String url_string = line_info.getUrl();
			HttpURLConnection connection = null;
			int status_code = 0;
			try{
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+line_info.getUrl());
				URL url = new URL(url_string);
				connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("User-Agent",this.user_agent);
				connection.setInstanceFollowRedirects(false);
				connection.setConnectTimeout(30000);
				connection.connect();
				// getting the status from the connection
				status_code = connection.getResponseCode();

				if (status_code == 301 || status_code == 400 || status_code == 404 || status_code == 503){
					my_fetched_infos.add(line_info);
				} else {
					// getting the content to parse
					InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
					BufferedReader buff = new BufferedReader(in);
					String content_line;
					StringBuilder builder=new StringBuilder();
					do {
						content_line = buff.readLine();
						builder.append(content_line);
					} while (content_line != null);
					String html = builder.toString();
					parseMyPage(html,status_code,url_string);
					// we managed to parse the URL and put it into cache
					my_fetched_infos.add(line_info);
				}
				if (connection != null){
					connection.disconnect();
				}
			} catch (Exception e){
				System.out.println("Error parsing with "+line_info+" status code "+status_code);
				e.printStackTrace();
			}

		}
		// updating the very last batch of our data
		System.out.println("Processed Pages: " + loc_data_manager.getTotalProcessedPages());
		System.out.println("Total Text Size: " + loc_data_manager.getTotalTextSize());
		loc_data_manager.updateData();

		return my_fetched_infos;
	}

	private void parseMyPage(String html, int status_code, String fullUrl){
		String url=URL_Utilities.clean(fullUrl);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);

		// the static Map cache is based upon the shortened and cleaned URL
		URLinfo info =loc_data_manager.getCrawledContent().get(url);
		if (info == null){
			info =new URLinfo();
		}	
		
		// filling up url parameters from list crawler
		info.setUrl(url);
		info.setStatus_code(status_code);
		
		// basic magasin, rayon, page type parsing
		info=CrawlerUtility.basicParsing(info,fullUrl);
		

		// we here can't have the depth in the list crawler
		info.setDepth(0);

		loc_data_manager.incProcessedPages();	

		// finding the appropriate vendor
		if (!"".equals(html)){
			info=CrawlerUtility.advancedTextParsing(info,html);
		}
		loc_data_manager.getCrawledContent().put(url,info);
	}

	public boolean is_cdiscount_best_vendor_from_page_source_code(String str_source_code){
		int cdiscount_index = str_source_code.indexOf("<p class='fpSellBy'>Vendu et expédié par <span class='logoCDS'>");
		if (cdiscount_index >0){
			return true;
		}else{
			return false;
		}
	}

	public boolean is_youtube_referenced_from_page_source_code(String str_source_code){
		int youtube_index = str_source_code.indexOf("http://www.youtube.com/");
		if (youtube_index >0){
			return true;
		}else{
			return false;
		}
	}
	class ULRId{
		private String url="";
		private int id;
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
	}

}
