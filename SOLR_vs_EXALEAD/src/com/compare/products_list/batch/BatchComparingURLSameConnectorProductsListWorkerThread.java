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
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import com.parsing.utility.ProductsListParseUtility;
import com.parsing.utility.URLComparisonListProductsInfo;

public class BatchComparingURLSameConnectorProductsListWorkerThread implements Runnable {
	private static int batch_size = 100;
	private static String selectStatement ="SELECT URL, ID FROM SOLR_VS_EXALEAD_PRODUCT_LIST WHERE TO_FETCH = TRUE and ID in ";
	private static String updateStatement ="UPDATE SOLR_VS_EXALEAD_PRODUCT_LIST SET STATUS=?, H1_SOLR=?, TITLE_SOLR=?, XPATH1_SOLR=?, XPATH2_SOLR=?, XPATH3_SOLR=?, XPATH4_SOLR=?, XPATH5_SOLR=?, XPATH6_SOLR=?, XPATH7_SOLR=?, XPATH8_SOLR=?, XPATH9_SOLR=?, XPATH10_SOLR=?, H1_EXALEAD=?, TITLE_EXALEAD=?, XPATH1_EXALEAD=?, XPATH2_EXALEAD=?, XPATH3_EXALEAD=?, XPATH4_EXALEAD=?, XPATH5_EXALEAD=?, XPATH6_EXALEAD=?, XPATH7_EXALEAD=?, XPATH8_EXALEAD=?, XPATH9_EXALEAD=?, XPATH10_EXALEAD=?, H1_COMPARISON=?, TITLE_COMPARISON=?, XPATH1_COMPARISON=?, XPATH2_COMPARISON=?, XPATH3_COMPARISON=?, XPATH4_COMPARISON=?, XPATH5_COMPARISON=?, XPATH6_COMPARISON=?, XPATH7_COMPARISON=?, XPATH8_COMPARISON=?, XPATH9_COMPARISON=?, XPATH10_COMPARISON=?, TO_FETCH=FALSE WHERE ID=?";
	private String user_agent;
	private List<ULRId> my_urls_to_fetch = new ArrayList<ULRId>();
	private Connection con;

	public BatchComparingURLSameConnectorProductsListWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent) throws SQLException{
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
		//updateStatus(infos);
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

	// batched update
	private void updateStatus(List<URLInfo> infos){
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(updateStatement);
			for (int i=0;i<infos.size();i++){
				URLInfo local_info = infos.get(i);

				//	st.setInt(1, local_info.getStatus());

				st.setInt(38, local_info.getId());
				//UPDATE HTTPINFOS_LIST SET STATUS=?, H1=?, TITLE=?, XPATH1=?, XPATH2=?, XPATH3=?, XPATH4=?, XPATH5=?, TO_FETCH=FALSE WHERE ID=?";
				//	String batch ="UPDATE HTTPINFOS_LIST SET STATUS="+infos.get(i).getStatus()+", H1='"+H1+"', TITLE='"+TITLE+ "',TO_FETCH=FALSE WHERE ID="+infos.get(i).getId();
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
				// fetching the solr version
				String solrurl = url + "?b";
				String solrpage_two_url = page_two_url + "?b";
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+solrurl + " with cookie value to tap Solr");
				HttpGet getSolr = new HttpGet(solrurl);
				getSolr.setHeader("User-Agent", user_agent);
				DefaultHttpClient clientSolr = new DefaultHttpClient();		
				HttpContext HTTP_CONTEXT_SOLR = new BasicHttpContext();
				HTTP_CONTEXT_SOLR.setAttribute(CoreProtocolPNames.USER_AGENT, user_agent);
				//getSolr.setHeader("Referer", "http://www.google.com");
				getSolr.setHeader("User-Agent", "CdiscountBot-crawler");
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
				my_info.setSolrOutput(solrOutput);
				String exaleadurl = url + "?a";
				String exaleadpage_two_url = page_two_url + "?a";
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+exaleadurl + " with cookie value to tap Exalead");
				HttpGet getExalead = new HttpGet(exaleadurl);
				HttpContext HTTP_CONTEXT_EXALEAD = new BasicHttpContext();
				HTTP_CONTEXT_EXALEAD.setAttribute(CoreProtocolPNames.USER_AGENT, user_agent);
				//getSolr.setHeader("Referer", "http://www.google.com");
				getExalead.setHeader("User-Agent", user_agent);
				DefaultHttpClient clientExalead = new DefaultHttpClient();
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
				HttpResponse responsePage2Exalead = clientExalead.execute(getPage2Exalead,HTTP_CONTEXT_EXALEAD);        
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
