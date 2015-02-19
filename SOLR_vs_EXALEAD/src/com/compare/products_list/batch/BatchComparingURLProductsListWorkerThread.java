package com.compare.products_list.batch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import com.parsing.utility.ProductsListParseUtility;
import com.parsing.utility.URLComparisonListProductsInfo;

public class BatchComparingURLProductsListWorkerThread implements Runnable {
	private static int batch_size = 50;
	private static String selectStatement ="SELECT URL, ID FROM SOLR_VS_EXALEAD_PRODUCT_LIST WHERE TO_FETCH = TRUE and ID in ";
	private static String updateStatement ="UPDATE SOLR_VS_EXALEAD_PRODUCT_LIST SET STATUS_SOLR=?, MEILLEUR_VENTE_PRODUCTS_SOLR=?, PAGE_BODY_PRODUCTS_SOLR=?,PAGE2_BODY_PRODUCTS_SOLR=?,NUMBER_OF_PRODUCTS_SOLR=?,FACETTES_SOLR=?,GEOLOC_SOLR=?,STATUS_EXALEAD=?,MEILLEUR_VENTE_PRODUCTS_EXALEAD=?, PAGE_BODY_PRODUCTS_EXALEAD=?,PAGE2_BODY_PRODUCTS_EXALEAD=?,NUMBER_OF_PRODUCTS_EXALEAD=?,FACETTES_EXALEAD=?,GEOLOC_EXALEAD=?,STATUS_COMPARISON=?,MEILLEUR_VENTE_PRODUCTS_COMPARISON=?,PAGE_BODY_PRODUCTS_COMPARISON=?,PAGE2_BODY_PRODUCTS_COMPARISON=?,NUMBER_OF_PRODUCTS_COMPARISON=?,FACETTES_COMPARISON=?,GEOLOC_COMPARISON=?,TO_FETCH=FALSE WHERE ID=?";	
	private String user_agent;
	private List<ULRId> my_urls_to_fetch = new ArrayList<ULRId>();
	private Connection con;

	public BatchComparingURLProductsListWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent) throws SQLException{
		this.user_agent=my_user_agent;
		this.con = con;
		String my_url="";
		if (to_fetch.size()>0){
			try {
				PreparedStatement pst = null;
				my_url=selectStatement+to_fetch.toString();
				my_url=my_url.replace("[", "(");
				my_url=my_url.replace("]", ")");
				pst = con.prepareStatement(my_url);
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
				System.out.println(Thread.currentThread()+" fetched URL's IDs :"+to_fetch.toString());
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
		List<URLInfo> infos=processComparison(line_infos);
		updateStatus(infos);
		//updateStatusStepByStep(infos);
		System.out.println(Thread.currentThread().getName()+" End");
	}

	private void close_connection(){
		try {
			this.con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// step by step update
	private void updateStatusStepByStep(List<URLInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			PreparedStatement st = con.prepareStatement(updateStatement);
			for (int i=0;i<infos.size();i++){
				URLInfo local_info = infos.get(i);
				//UPDATE SOLR_VS_EXALEAD_PRODUCT_LIST SET STATUS_SOLR=?, MEILLEUR_VENTE_PRODUCTS_SOLR TEXT=?, PAGE_BODY_PRODUCTS_SOLR=?,PAGE2_BODY_PRODUCTS_SOLR=?,NUMBER_OF_PRODUCTS_SOLR=?,FACETTES_SOLR=?,GEOLOC_SOLR=?,STATUS_EXALEAD=?,MEILLEUR_VENTE_PRODUCTS_EXALEAD=?, PAGE_BODY_PRODUCTS_EXALEAD=?,PAGE2_BODY_PRODUCTS_EXALEAD=?,NUMBER_OF_PRODUCTS_EXALEAD=?,FACETTES_EXALEAD=?,GEOLOC_EXALEAD=?,STATUS_COMPARISON=?,MEILLEUR_VENTE_PRODUCTS_COMPARISON=?,PAGE_BODY_PRODUCTS_COMPARISON=?,PAGE2_BODY_PRODUCTS_COMPARISON=?,NUMBER_OF_PRODUCTS_COMPARISON=?,FACETTES_COMPARISON=?,GEOLOC_COMPARISON=?,TO_FETCH=FALSE WHERE ID=?";	
				//                                            1                            2                              3                           4                      5                     6               7               8                        9                               10                            11                           12                      13                  14              15                           16                               17                             18                                    19                     20                     21                                22
				st.setInt(1, local_info.getSolrOutput().getStatus());
				st.setString(2,local_info.getSolrOutput().getBest_sales_products());
				st.setString(3,local_info.getSolrOutput().getBody_page_products());
				st.setString(4,local_info.getSolrOutput().getBody_page_two_products());
				st.setString(5,local_info.getSolrOutput().getNumber_of_products());
				st.setString(6,local_info.getSolrOutput().getFacette_summary());
				st.setString(7,local_info.getSolrOutput().getGeoloc_selection());
				st.setInt(8, local_info.getExaleadOutput().getStatus());
				st.setString(9,local_info.getExaleadOutput().getBest_sales_products());
				st.setString(10,local_info.getExaleadOutput().getBody_page_products());
				st.setString(11,local_info.getExaleadOutput().getBody_page_two_products());
				st.setString(12,local_info.getExaleadOutput().getNumber_of_products());
				st.setString(13,local_info.getExaleadOutput().getFacette_summary());
				st.setString(14,local_info.getExaleadOutput().getGeoloc_selection());				
				// fill up if the strings match
				// status matching
				if (local_info.getSolrOutput().getStatus() == local_info.getExaleadOutput().getStatus()){
					st.setInt(15,1);
				}else{
					st.setInt(15,0);
				}
				// best sales products matching
				if (local_info.getSolrOutput().getBest_sales_products().equals(local_info.getExaleadOutput().getBest_sales_products())){
					st.setInt(16,1);
				}else{
					st.setInt(16,0);
				}
				// body products matching
				if (local_info.getSolrOutput().getBody_page_products().equals(local_info.getExaleadOutput().getBody_page_products())){
					st.setInt(17,1);
				}else{
					st.setInt(17,0);
				}
				// body page 2 products matching
				if (local_info.getSolrOutput().getBody_page_two_products().equals(local_info.getExaleadOutput().getBody_page_two_products())){
					st.setInt(18,1);
				}else{
					st.setInt(18,0);
				}
				// number of products matching
				if (local_info.getSolrOutput().getNumber_of_products().equals(local_info.getExaleadOutput().getNumber_of_products())){
					st.setInt(19,1);
				}else{
					st.setInt(19,0);
				}
				// summary facettes matching
				if (local_info.getSolrOutput().getFacette_summary().equals(local_info.getExaleadOutput().getFacette_summary())){
					st.setInt(20,1);
				}else{
					st.setInt(20,0);
				}
				//geoloc selection matching
				if (local_info.getSolrOutput().getGeoloc_selection().equals(local_info.getExaleadOutput().getGeoloc_selection())){
					st.setInt(21,1);
				}else{
					st.setInt(21,0);
				}
				st.setInt(22, local_info.getId());		
				st.executeUpdate();		
			}      
			//int counts[] = st.executeBatch();
			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}


	// batched update
	private void updateStatus(List<URLInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(updateStatement);
			for (int i=0;i<infos.size();i++){
				URLInfo local_info = infos.get(i);
				//UPDATE SOLR_VS_EXALEAD_PRODUCT_LIST SET STATUS_SOLR=?, MEILLEUR_VENTE_PRODUCTS_SOLR TEXT=?, PAGE_BODY_PRODUCTS_SOLR=?,PAGE2_BODY_PRODUCTS_SOLR=?,NUMBER_OF_PRODUCTS_SOLR=?,FACETTES_SOLR=?,GEOLOC_SOLR=?,STATUS_EXALEAD=?,MEILLEUR_VENTE_PRODUCTS_EXALEAD=?, PAGE_BODY_PRODUCTS_EXALEAD=?,PAGE2_BODY_PRODUCTS_EXALEAD=?,NUMBER_OF_PRODUCTS_EXALEAD=?,FACETTES_EXALEAD=?,GEOLOC_EXALEAD=?,STATUS_COMPARISON=?,MEILLEUR_VENTE_PRODUCTS_COMPARISON=?,PAGE_BODY_PRODUCTS_COMPARISON=?,PAGE2_BODY_PRODUCTS_COMPARISON=?,NUMBER_OF_PRODUCTS_COMPARISON=?,FACETTES_COMPARISON=?,GEOLOC_COMPARISON=?,TO_FETCH=FALSE WHERE ID=?";	
				//                                            1                            2                              3                           4                      5                     6               7               8                        9                               10                            11                           12                      13                  14              15                           16                               17                             18                                    19                     20                     21                                22
				st.setInt(1, local_info.getSolrOutput().getStatus());
				st.setString(2,local_info.getSolrOutput().getBest_sales_products());
				st.setString(3,local_info.getSolrOutput().getBody_page_products());
				st.setString(4,local_info.getSolrOutput().getBody_page_two_products());
				st.setString(5,local_info.getSolrOutput().getNumber_of_products());
				st.setString(6,local_info.getSolrOutput().getFacette_summary());
				st.setString(7,local_info.getSolrOutput().getGeoloc_selection());
				st.setInt(8, local_info.getExaleadOutput().getStatus());
				st.setString(9,local_info.getExaleadOutput().getBest_sales_products());
				st.setString(10,local_info.getExaleadOutput().getBody_page_products());
				st.setString(11,local_info.getExaleadOutput().getBody_page_two_products());
				st.setString(12,local_info.getExaleadOutput().getNumber_of_products());
				st.setString(13,local_info.getExaleadOutput().getFacette_summary());
				st.setString(14,local_info.getExaleadOutput().getGeoloc_selection());				
				// fill up if the strings match
				// status matching
				if (local_info.getSolrOutput().getStatus() == local_info.getExaleadOutput().getStatus()){
					st.setInt(15,1);
				}else{
					st.setInt(15,0);
				}
				// best sales products matching
				if (local_info.getSolrOutput().getBest_sales_products().equals(local_info.getExaleadOutput().getBest_sales_products())){
					st.setInt(16,1);
				}else{
					st.setInt(16,0);
				}
				// body products matching
				if (local_info.getSolrOutput().getBody_page_products().equals(local_info.getExaleadOutput().getBody_page_products())){
					st.setInt(17,1);
				}else{
					st.setInt(17,0);
				}
				// body page 2 products matching
				if (local_info.getSolrOutput().getBody_page_two_products().equals(local_info.getExaleadOutput().getBody_page_two_products())){
					st.setInt(18,1);
				}else{
					st.setInt(18,0);
				}
				// number of products matching
				if (local_info.getSolrOutput().getNumber_of_products().equals(local_info.getExaleadOutput().getNumber_of_products())){
					st.setInt(19,1);
				}else{
					st.setInt(19,0);
				}
				// summary facettes matching
				if (local_info.getSolrOutput().getFacette_summary().equals(local_info.getExaleadOutput().getFacette_summary())){
					st.setInt(20,1);
				}else{
					st.setInt(20,0);
				}
				//geoloc selection matching
				if (local_info.getSolrOutput().getGeoloc_selection().equals(local_info.getExaleadOutput().getGeoloc_selection())){
					st.setInt(21,1);
				}else{
					st.setInt(21,0);
				}
				st.setInt(22, local_info.getId());		
				st.addBatch();		
			}      
			//int counts[] = st.executeBatch();
			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
			st.executeBatch();
			con.commit();
			st.close();
			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting batch ");
		}
	}

	private List<URLInfo> processComparison(List<ULRId> line_infos) {
		List<URLInfo> my_fetched_infos = new ArrayList<URLInfo>();
		for(ULRId line_info : line_infos){
			// we here loop over each URL the thread has to handle
			URLInfo my_info = new URLInfo();
			// the URLComparisonInfo object will collect all the info
			// it will contain the id
			my_info.setId(line_info.getId());
			// getting the URL
			String url = line_info.getUrl();
			url=url.replace(" ", "%20");
			String page_two_url = url.replace(".html", "-2.html");
			try{
				String solrurl = url + "?b";
				String solrpage_two_url = page_two_url + "?b";
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+solrurl + " with cookie value to tap Solr");
				HttpGet getSolr = new HttpGet(solrurl);
				getSolr.setHeader("User-Agent", user_agent);
				HttpParams my_httpParams_solr = new BasicHttpParams();
				HttpClientParams.setRedirecting(my_httpParams_solr, false);
				HttpConnectionParams.setConnectionTimeout(my_httpParams_solr, 300000000);
				HttpConnectionParams.setSoTimeout(my_httpParams_solr, 1500000000);
				DefaultHttpClient clientSolr = new DefaultHttpClient(my_httpParams_solr);		
				clientSolr.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
				HttpContext HTTP_CONTEXT_SOLR = new BasicHttpContext();
				HTTP_CONTEXT_SOLR.setAttribute(CoreProtocolPNames.USER_AGENT, user_agent);
				//getSolr.setHeader("Referer", "http://www.google.com");
				getSolr.setHeader("User-Agent", user_agent);
				// set the cookies
				CookieStore cookieStoreSolr = new BasicCookieStore();
				BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "666.1");
				cookieSolr.setDomain("cdiscount.com");
				cookieSolr.setPath("/");
				cookieStoreSolr.addCookie(cookieSolr);    
				clientSolr.setCookieStore(cookieStoreSolr);
				// get the cookies
				HttpResponse responseSolr = clientSolr.execute(getSolr,HTTP_CONTEXT_SOLR);
				int solr_status = responseSolr.getStatusLine().getStatusCode();
				HttpEntity entitySolr = responseSolr.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page_source_codeSolr = EntityUtils.toString(entitySolr);
				EntityUtils.consume(entitySolr);
				// Getting the source code for pagination number 2
				HttpGet getPage2Solr = new HttpGet(solrpage_two_url);
				HttpResponse responsePage2Solr = clientSolr.execute(getPage2Solr,HTTP_CONTEXT_SOLR);        
				HttpEntity entityPage2Solr = responsePage2Solr.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page2_source_codeSolr = EntityUtils.toString(entityPage2Solr);
				EntityUtils.consume(entityPage2Solr);
				// closing the solr client
				clientSolr.close();

				// we now extract information from our page source code
				URLComparisonListProductsInfo solrOutput = ProductsListParseUtility.parse_page_source_code(page_source_codeSolr);
				ProductsListParseUtility.parse_page2_source_code(solrOutput,page2_source_codeSolr);
				solrOutput.setStatus(solr_status);
				// inspecting the output
				my_info.setSolrOutput(solrOutput);
				
				String exaleadurl = url + "?a";
				String exaleadpage_two_url = page_two_url + "?a";
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+exaleadurl + " with cookie value to tap Exalead");
				HttpGet getExalead = new HttpGet(exaleadurl);
				HttpContext HTTP_CONTEXT_EXALEAD = new BasicHttpContext();
				HTTP_CONTEXT_EXALEAD.setAttribute(CoreProtocolPNames.USER_AGENT, user_agent);
				//getSolr.setHeader("Referer", "http://www.google.com");
				getExalead.setHeader("User-Agent", user_agent);
				HttpParams my_httpParams_exalead = new BasicHttpParams();
				HttpClientParams.setRedirecting(my_httpParams_exalead, false);
				HttpConnectionParams.setConnectionTimeout(my_httpParams_exalead, 300000000);
				HttpConnectionParams.setSoTimeout(my_httpParams_exalead, 1500000000);
				DefaultHttpClient clientExalead = new DefaultHttpClient(my_httpParams_exalead);
				clientExalead.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, true);
				// set the cookies
				CookieStore cookieStoreExalead = new BasicCookieStore();
				BasicClientCookie cookieExalead = new BasicClientCookie("_$hidden", "666.0");
				cookieExalead.setDomain("cdiscount.com");
				cookieExalead.setPath("/");
				cookieStoreExalead.addCookie(cookieExalead);    
				clientExalead.setCookieStore(cookieStoreExalead);
				// get the cookies
				HttpResponse responseExalead = clientExalead.execute(getExalead,HTTP_CONTEXT_EXALEAD);
				int exalead_status = responseExalead.getStatusLine().getStatusCode();
				HttpEntity entityExalead = responseExalead.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page_source_codeExalead = EntityUtils.toString(entityExalead);
				EntityUtils.consume(entityExalead);

				// Getting the source code for pagination number 2
				HttpGet getPage2Exalead = new HttpGet(exaleadpage_two_url);
				HttpResponse responsePage2Exalead = clientExalead.execute(getPage2Exalead,HTTP_CONTEXT_SOLR);        
				HttpEntity entityPage2Exalead = responsePage2Exalead.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page2_source_codeExalead = EntityUtils.toString(entityPage2Exalead);
				EntityUtils.consume(entityPage2Exalead);
				clientExalead.close();
				// we now extract information from our page source code
				URLComparisonListProductsInfo exaleadOutput = ProductsListParseUtility.parse_page_source_code(page_source_codeExalead);
				ProductsListParseUtility.parse_page2_source_code(exaleadOutput,page2_source_codeExalead);
				exaleadOutput.setStatus(exalead_status);
				// inspecting the output
			    
				my_info.setExaleadOutput(exaleadOutput);
			} catch (Exception e){
				System.out.println("Trouble fetching URL : "+url);
				e.printStackTrace();
			}
			my_fetched_infos.add(my_info);
		}
		return my_fetched_infos;
	}

	class URLInfo{
		private int id;
		private URLComparisonListProductsInfo solrOutput;
		private URLComparisonListProductsInfo exaleadOutput;
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public URLComparisonListProductsInfo getSolrOutput() {
			return solrOutput;
		}
		public void setSolrOutput(URLComparisonListProductsInfo solrOutput) {
			this.solrOutput = solrOutput;
		}
		public URLComparisonListProductsInfo getExaleadOutput() {
			return exaleadOutput;
		}
		public void setExaleadOutput(URLComparisonListProductsInfo exaleadOutput) {
			this.exaleadOutput = exaleadOutput;
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
