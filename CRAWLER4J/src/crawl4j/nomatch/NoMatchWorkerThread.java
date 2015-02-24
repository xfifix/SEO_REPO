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

import crawl4j.continuous.CrawlDataManagement;
import crawl4j.continuous.CrawlerUtility;
import crawl4j.facettesutility.FacettesInfo;
import crawl4j.facettesutility.FacettesUtility;
import crawl4j.urlutilities.URL_Utilities;
import crawl4j.urlutilities.URLinfo;
import crawl4j.xpathutility.XPathUtility;


public class NoMatchWorkerThread implements Runnable {
	private int batch_size = 100;

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

	public void run(){
		List<ULRId> infos=processCommand(my_urls_to_fetch);
		updateStatus(infos);
		System.out.println(Thread.currentThread().getName()+" End");
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
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
			try{
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+line_info.getUrl());
				URL url = new URL(url_string);
				connection = (HttpURLConnection)url.openConnection();
				connection.setRequestMethod("GET");
				connection.setRequestProperty("User-Agent",this.user_agent);
				connection.setInstanceFollowRedirects(false);
				connection.setConnectTimeout(3000);
				connection.connect();
				// getting the status from the connection
				int status_code = connection.getResponseCode();

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
				if (connection != null){
					connection.disconnect();
				}
			} catch (Exception e){
				System.out.println("Error parsing with "+line_info);
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
		// we drop the unnecessary pieces of the URL
		String page_type = URL_Utilities.checkTypeFullUrl(fullUrl);
		String magasin = URL_Utilities.checkMagasinFullUrl(fullUrl);
		String rayon = URL_Utilities.checkRayonFullUrl(fullUrl);

		String url=URL_Utilities.clean(fullUrl);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);

		// the static Map cache is based upon the shortened and cleaned URL
		URLinfo info =loc_data_manager.getCrawledContent().get(url);
		if (info == null){
			info =new URLinfo();
		}		
		// filling up url regexp attributes
		info.setPage_type(page_type);
		info.setMagasin(magasin);
		info.setRayon(rayon);
		// filling up url parameters
		info.setUrl(url);
		info.setStatus_code(status_code);
		// we here can't have the depth
		info.setDepth(0);

		loc_data_manager.incProcessedPages();	


		// finding the appropriate vendor

		boolean vendor = is_cdiscount_best_vendor_from_page_source_code(html);
		info.setCdiscountBestBid(vendor);
		info.setVendor(vendor ? "Cdiscount" : "Market Place");
		boolean youtube = is_youtube_referenced_from_page_source_code(html);
		info.setYoutubeVideoReferenced(youtube);

		// XPATH parsing
		if (NoMatchThreadPool.isXPATHparsed){
			try {
				String[] xpathOutput = XPathUtility.parse_page_code_source(html,loc_data_manager.getXpath_expression());
				info.setXPATH_results(xpathOutput);
				info.setZtd(xpathOutput[8]);
				info.setFooter(xpathOutput[9]);
			} catch (XPathExpressionException | ParserConfigurationException
					| SAXException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Trouble parsing XPATH expressions for URL : "+url);
			}
		}

		// filling up entity to be cached with page source code
		if (NoMatchThreadPool.isBlobStored){
			byte[] compressedPageContent = CrawlerUtility.gzip_compress_byte_stream(html.getBytes());
			info.setPage_source_code(compressedPageContent);
		}

		Document doc = Jsoup.parse(html);
		info.setText(doc.text());
		Elements titleel = doc.select("title");
		info.setTitle(titleel.text());
		// fetching the H1 element
		Elements h1el = doc.select("h1");
		info.setH1(h1el.text());
		// finding the footer with jQuery
		//			Elements footerel = doc.select("div.ftMention");
		//			info.setFooter(footerel.text());
		// finding the ztd with jQuery
		//			Elements ztdunfolded = doc.select("p.scZtdTxt");
		//			Elements ztdfolded = doc.select("p.scZtdH");
		//			info.setZtd((ztdunfolded==null? "":ztdunfolded.text())+(ztdfolded==null? "":ztdfolded.text()));
		// finding the short description
		Elements short_desc_el = doc.select("p.fpMb");
		info.setShort_desc((short_desc_el==null? "":short_desc_el.text()));
		// finding the number of attributes
		if ("FicheProduit".equals(page_type)){		
			Elements attributes = doc.select(".fpDescTb tr");
			int nb_arguments = 0 ;
			StringBuilder arguments_text = new StringBuilder();
			for (Element tr_element : attributes){
				Elements td_elements = tr_element.select("td");
				if (td_elements.size() == 2){
					nb_arguments++;
					String category = td_elements.get(0).text();
					arguments_text.append(category+"|||");	
					String description = td_elements.get(1).text();                                    
					arguments_text.append(description);		
					arguments_text.append("@@");
					if (CrawlerUtility.category_name.equals(category)){
						info.setCategory(description);
					}
					if (CrawlerUtility.brand_name.equals(category)){
						info.setBrand(description);
					}
					if (CrawlerUtility.product_name.equals(category)){
						info.setProduit(description);
					}
				}
			}
			info.setAtt_number(nb_arguments);
			info.setAtt_desc(arguments_text.toString());
		}
		// parsing the facettes
		if (("ListeProduit".equals(page_type))|| ("ListeProduitFiltre".equals(page_type))){
			// finding the number of attributes
			List<FacettesInfo> list_facettes = new ArrayList<FacettesInfo>();
			FacettesInfo my_info = new FacettesInfo();
			Elements facette_elements = doc.select("div.mvFilter");			
			for (Element facette : facette_elements ){
				//System.out.println(e.toString());
				Elements facette_name = facette.select("div.mvFTit");
				my_info.setFacetteName(facette_name.text());
				Elements facette_values = facette.select("a");
				for (Element facette_value : facette_values){
					String categorie_value = facette_value.text();
					if ("".equals(categorie_value)){
						categorie_value = facette_value.attr("title");
					}
					Matcher matchPattern = CrawlerUtility.bracketPattern.matcher(categorie_value);
					String categorieCount ="";
					while (matchPattern.find()) {		
						categorieCount=matchPattern.group();
					}
					categorie_value=categorie_value.replace(categorieCount,"");
					categorieCount=categorieCount.replace("(", "");
					categorieCount=categorieCount.replace(")", "");	
					//System.out.println(categorie_value);
					try{
						my_info.setFacetteCount(Integer.valueOf(categorieCount));
						//System.out.println(Integer.valueOf(categorieCount));	
					} catch (NumberFormatException e){
						System.out.println("Trouble while formatting a facette");
						my_info.setFacetteCount(0);
					}
					my_info.setFacetteValue(categorie_value);
					list_facettes.add(my_info);
					my_info = new FacettesInfo();
					my_info.setFacetteName(facette_name.text());
				}		
			}
			String facette_json=FacettesUtility.getFacettesJSONStringToStore(list_facettes);
			info.setFacettes(facette_json);
		}

		loc_data_manager.getCrawledContent().put(url,info);
		// We save this crawler data after processing every bulk_sizes pages
		if (loc_data_manager.getTotalProcessedPages() % CrawlDataManagement.bulk_size == 0) {
			System.out.println("Processed Pages: " + loc_data_manager.getTotalProcessedPages());
			System.out.println("Total Text Size: " + loc_data_manager.getTotalTextSize());
			loc_data_manager.updateData();	
		}
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
