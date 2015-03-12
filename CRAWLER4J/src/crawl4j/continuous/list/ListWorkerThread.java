package crawl4j.continuous.list;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import crawl4j.continuous.CrawlDataManagement;
import crawl4j.continuous.CrawlerUtility;
import crawl4j.urlutilities.URL_Utilities;
import crawl4j.urlutilities.URLinfo;


public class ListWorkerThread implements Runnable {
	public static int batch_size = 100;
	private String user_agent;
	private List<String> my_urls_to_fetch = new ArrayList<String>();
	private CrawlDataManagement loc_data_manager;

	public ListWorkerThread(CrawlDataManagement loc_crawl_data_manager ,List<String> to_fetch, String my_user_agent) throws SQLException{
		this.user_agent=my_user_agent;
		this.loc_data_manager = loc_crawl_data_manager;
		this.my_urls_to_fetch = to_fetch;
	}

	public void run() {
		List<String> line_infos = new ArrayList<String>();
		for (String id :my_urls_to_fetch){
			line_infos.add(id);
			if (line_infos.size() % batch_size ==0) {
				runBatch(line_infos);	
				line_infos.clear();
				line_infos = new ArrayList<String>();
			}
		}
		runBatch(line_infos);
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<String> line_infos){
		processCommand(line_infos);
		System.out.println(Thread.currentThread().getName()+" End");
		System.out.println(" Having processed :"+my_urls_to_fetch.size());
	}

	private void close_connection(){
		try {
			this.loc_data_manager.getCon().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processCommand(List<String> line_infos) {
		for(String url_string : line_infos){
			HttpURLConnection connection = null;
			int status_code = 503;
			String html = "";
			try{
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+url_string);
				URL url = new URL(url_string);
				connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("User-Agent",this.user_agent);
				connection.setInstanceFollowRedirects(false);
				connection.setConnectTimeout(30000);
				connection.connect();
				// getting the status from the connection
				status_code = connection.getResponseCode();
				
				if (!(status_code == 301 || status_code == 400 || status_code == 404 || status_code == 503)){
					// getting the content to parse
					InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
					BufferedReader buff = new BufferedReader(in);
					String content_line;
					StringBuilder builder=new StringBuilder();
					do {
						content_line = buff.readLine();
						builder.append(content_line);
					} while (content_line != null);
					html=builder.toString();
				} 
				parseMyPage(html,status_code,url_string);
				if (connection != null){
					connection.disconnect();
				}
			} catch (Exception e){
				System.out.println("Error parsing with "+url_string+" status code "+status_code);
				e.printStackTrace();
				parseMyPage(html,status_code,url_string);
			}
		}
		// updating the very last batch of our data
		System.out.println("Processed Pages: " + loc_data_manager.getTotalProcessedPages());
		System.out.println("Total Text Size: " + loc_data_manager.getTotalTextSize());
		loc_data_manager.updateData();
	}

	private void parseMyPage(String html, int status_code, String fullUrl){
		// we drop the unnecessary pieces of the URL

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
}
