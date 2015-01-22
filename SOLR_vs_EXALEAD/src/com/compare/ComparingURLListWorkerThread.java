package com.compare;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

import com.parsing.utility.XPathUtility;

public class ComparingURLListWorkerThread implements Runnable {
	private int batch_size = 100;

	private static String updateStatement ="UPDATE SOLR_VS_EXALEAD SET STATUS=?, H1_SOLR=?, TITLE_SOLR=?, XPATH1_SOLR=?, XPATH2_SOLR=?, XPATH3_SOLR=?, XPATH4_SOLR=?, XPATH5_SOLR=?, H1_EXALEAD=?, TITLE_EXALEAD=?, XPATH1_EXALEAD=?, XPATH2_EXALEAD=?, XPATH3_EXALEAD=?, XPATH4_EXALEAD=?, XPATH5_EXALEAD=?, H1_COMPARISON=?, TITLE_COMPARISON=?, XPATH1_COMPARISON=?, XPATH2_COMPARISON=?, XPATH3_COMPARISON=?, XPATH4_COMPARISON=?, XPATH5_COMPARISON=?, TO_FETCH=FALSE WHERE ID=?";
	private String[] xpathExpressions;
	private String user_agent;
	private List<ULRId> my_urls_to_fetch = new ArrayList<ULRId>();
	private Connection con;

	public ComparingURLListWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent, String[] xpathExpressions) throws SQLException{
		this.xpathExpressions = xpathExpressions;
		this.user_agent=my_user_agent;
		this.con = con;
		String my_url="";
		if (to_fetch.size()>0){
			try {
				PreparedStatement pst = null;
				my_url="SELECT URL, ID FROM SOLR_VS_EXALEAD WHERE TO_FETCH = TRUE and ID in "+to_fetch.toString();
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
		// running just a single batch
		runBatch(my_urls_to_fetch);
		// alternative : batch into batch
		//		List<ULRId> line_infos = new ArrayList<ULRId>();
		//		for (ULRId id :my_urls_to_fetch){
		//			line_infos.add(id);
		//			if (line_infos.size() !=0 && line_infos.size() % batch_size ==0) {
		//				runBatch(line_infos);	
		//				line_infos.clear();
		//				line_infos = new ArrayList<ULRId>();
		//			}
		//		}
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<ULRId> line_infos){
		List<URLComparisonInfo> infos=processComparison(line_infos);
		updateStatus(infos);
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
	private void updateStatus(List<URLComparisonInfo> infos){

		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(updateStatement);
			for (int i=0;i<infos.size();i++){
				URLComparisonInfo local_info = infos.get(i);
				ParsingOutput solrOutput = local_info.getSolrOutput();
				
				String H1_SOLR = solrOutput.getH1().replace("'", "");
				String TITLE_SOLR = solrOutput.getTitle().replace("'", "");
				String[] XPATHRESULTS_SOLR = solrOutput.getXpathResults();
				
				ParsingOutput exaleadOutput = local_info.getExaleadOutput();			
				String H1_EXALEAD = exaleadOutput.getH1().replace("'", "");
				String TITLE_EXALEAD = exaleadOutput.getTitle().replace("'", "");
				String[] XPATHRESULTS_EXALEAD = exaleadOutput.getXpathResults();
				
				st.setInt(1, local_info.getStatus());
				st.setString(2,H1_SOLR);
				st.setString(3, TITLE_SOLR);
				if (XPATHRESULTS_SOLR != null){
					st.setString(4, XPATHRESULTS_SOLR[0]);
					st.setString(5, XPATHRESULTS_SOLR[1]);
					st.setString(6, XPATHRESULTS_SOLR[2]);
					st.setString(7, XPATHRESULTS_SOLR[3]);
					st.setString(8, XPATHRESULTS_SOLR[4]);
				}else {
					st.setString(4, "");
					st.setString(5, "");
					st.setString(6, "");
					st.setString(7, "");
					st.setString(8, "");
				}
				st.setString(9,H1_EXALEAD);
				st.setString(10, TITLE_EXALEAD);
				if (XPATHRESULTS_EXALEAD != null){
					st.setString(11, XPATHRESULTS_EXALEAD[0]);
					st.setString(12, XPATHRESULTS_EXALEAD[1]);
					st.setString(13, XPATHRESULTS_EXALEAD[2]);
					st.setString(14, XPATHRESULTS_EXALEAD[3]);
					st.setString(15, XPATHRESULTS_EXALEAD[4]);
				}else {
					st.setString(11, "");
					st.setString(12, "");
					st.setString(13, "");
					st.setString(14, "");
					st.setString(15, "");
				}
				if (!H1_SOLR.equals(H1_EXALEAD)){
					st.setInt(16, 0);
				} else {
					st.setInt(16, 1);
				}
			    if (!TITLE_SOLR.equals(TITLE_EXALEAD)){
					st.setInt(17, 0);
				} else {
					st.setInt(17, 1);
				}

				if ((XPATHRESULTS_EXALEAD != null) && (XPATHRESULTS_SOLR != null)){
				    if (!XPATHRESULTS_SOLR[0].equals(XPATHRESULTS_EXALEAD[0])){
						st.setInt(18, 0);
					} else {
						st.setInt(18, 1);
					}
				    if (!XPATHRESULTS_SOLR[1].equals(XPATHRESULTS_EXALEAD[1])){
						st.setInt(19, 0);
					} else {
						st.setInt(19, 1);
					}
				    if (!XPATHRESULTS_SOLR[2].equals(XPATHRESULTS_EXALEAD[2])){
						st.setInt(20, 0);
					} else {
						st.setInt(20, 1);
					}
				    if (!XPATHRESULTS_SOLR[3].equals(XPATHRESULTS_EXALEAD[3])){
						st.setInt(21, 0);
					} else {
						st.setInt(21, 1);
					}
				    if (!XPATHRESULTS_SOLR[4].equals(XPATHRESULTS_EXALEAD[4])){
						st.setInt(22, 0);
					} else {
						st.setInt(22, 1);
					}
				}
	
				st.setInt(23, local_info.getId());
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

	// update step by step
	//	private void updateStatusStepByStep(List<URLInfo> infos){
	//		for (int i=0;i<infos.size();i++){
	//			String H1= infos.get(i).getH1().replace("'", "");
	//			String TITLE = infos.get(i).getTitle().replace("'", "");
	//			String batch ="UPDATE HTTPINFOS_LIST SET STATUS="+infos.get(i).getStatus()+", H1='"+H1+"', TITLE='"+TITLE+ "',TO_FETCH=FALSE WHERE ID="+thread_fetch_ids.get(i);
	//			try{
	//				PreparedStatement insert_st = con.prepareStatement(batch);
	//				insert_st.executeUpdate();
	//			} catch (SQLException e){
	//				System.out.println("Trouble inserting : "+batch);
	//				e.printStackTrace();
	//			}
	//
	//		}      
	//		System.out.println("Inserting : " + infos.size() + "ULRs into database");
	//	}

	private ParsingOutput parse_page_code_source(String page_source_code) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{
		ParsingOutput output = new ParsingOutput();
		org.jsoup.nodes.Document doc = Jsoup.parse(page_source_code);
		Elements h1s = doc.select("h1");
		String conc_h1="";
		for (Element h1 : h1s) {
			conc_h1=conc_h1+h1.text();
		}	
		output.setH1(conc_h1);
		Elements titles = doc.select("title");
		String conc_title="";
		for (Element title : titles) {
			conc_title=conc_title+title.text();
		}				
		output.setTitle(conc_title);
		String[] xpathResults = new String[5];
		int local_counter = 0;
		for (String xpath : xpathExpressions){
			String content = XPathUtility.parseContent(page_source_code, xpath);
			xpathResults[local_counter]=content;
			local_counter++;
		}
		output.setXpathResults(xpathResults);
		return output;
	}


	private List<URLComparisonInfo> processComparison(List<ULRId> line_infos) {
		List<URLComparisonInfo> my_fetched_infos = new ArrayList<URLComparisonInfo>();
		for(ULRId line_info : line_infos){
			// we here loop over each URL the thread has to handle
			URLComparisonInfo my_info = new URLComparisonInfo();
			// the URLComparisonInfo object will collect all the info
			// it will contain the id
			my_info.setId(line_info.getId());
			// getting the URL
			String url = line_info.getUrl();
			try{
				// fetching the solr version
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+url + " with cookie value to tap Solr");
				HttpGet getSolr = new HttpGet(url);
				DefaultHttpClient clientSolr = new DefaultHttpClient();		
				// set the cookies
				CookieStore cookieStoreSolr = new BasicCookieStore();
				BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "666.1");
				cookieSolr.setDomain("cdiscount.com");
				cookieSolr.setPath("/");
				cookieStoreSolr.addCookie(cookieSolr);    
				clientSolr.setCookieStore(cookieStoreSolr);
				// get the cookies
				HttpResponse responseSolr = clientSolr.execute(getSolr);
	
				System.out.println(responseSolr.getStatusLine());
				HttpEntity entitySolr = responseSolr.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page_source_codeSolr = EntityUtils.toString(entitySolr);
				EntityUtils.consume(entitySolr);
				clientSolr.close();
				ParsingOutput solrOutput = parse_page_code_source(page_source_codeSolr);
				my_info.setSolrOutput(solrOutput);
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+url + " with cookie value to tap Exalead");
				HttpGet getExalead = new HttpGet(url);
				DefaultHttpClient clientExalead = new DefaultHttpClient();
				// set the cookies
				CookieStore cookieStoreExalead = new BasicCookieStore();
				BasicClientCookie cookieExalead = new BasicClientCookie("_$hidden", "666.0");
				cookieExalead.setDomain("cdiscount.com");
				cookieExalead.setPath("/");
				cookieStoreExalead.addCookie(cookieExalead);    
				clientExalead.setCookieStore(cookieStoreExalead);
				// get the cookies
				HttpResponse responseExalead = clientExalead.execute(getExalead);
				System.out.println(responseExalead.getStatusLine());
				HttpEntity entityExalead = responseExalead.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page_source_codeExalead = EntityUtils.toString(entityExalead);
				System.out.println(page_source_codeExalead);
				EntityUtils.consume(entityExalead);
				clientExalead.close();
				ParsingOutput exaleadOutput = parse_page_code_source(page_source_codeExalead);
				my_info.setExaleadOutput(exaleadOutput);
			} catch (Exception e){
				System.out.println("Trouble fetching URL : "+url);
				e.printStackTrace();
			}
			my_fetched_infos.add(my_info);
			//			HttpURLConnection connection = null;
			//			try{
			//				
			//				URL url = new URL(line_info.getUrl());
			//				connection = (HttpURLConnection)url.openConnection();
			//				connection.setRequestMethod("GET");
			//				connection.setRequestProperty("User-Agent",this.user_agent);
			//				connection.setInstanceFollowRedirects(true);
			//				connection.setConnectTimeout(3000);
			//				connection.connect();
			//				// getting the status from the connection
			//				my_info.setStatus(connection.getResponseCode());
			//				// getting the content to parse
			//				InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
			//				BufferedReader buff = new BufferedReader(in);
			//				String content_line;
			//				StringBuilder builder=new StringBuilder();
			//				do {
			//					content_line = buff.readLine();
			//					builder.append(content_line);
			//				} while (content_line != null);
			//				String html = builder.toString();
			//				org.jsoup.nodes.Document doc = Jsoup.parse(html);
			//				Elements h1s = doc.select("h1");
			//				String conc_h1="";
			//				for (Element h1 : h1s) {
			//					conc_h1=conc_h1+h1.text();
			//				}	
			//				my_info.setH1(conc_h1);
			//				Elements titles = doc.select("title");
			//				String conc_title="";
			//				for (Element title : titles) {
			//					conc_title=conc_title+title.text();
			//				}				
			//				my_info.setTitle(conc_title);
			//				String[] xpathResults = new String[5];
			//				int local_counter = 0;
			//				for (String xpath : xpathExpressions){
			//					String content = XPathUtility.parseContent(html, xpath);
			//					xpathResults[local_counter]=content;
			//					local_counter++;
			//				}
			//				my_info.setXpathResults(xpathResults);
			//			} catch (Exception e){
			//				System.out.println("@@@@@@@@@@@@@@@@ Error with "+line_info);
			//				e.printStackTrace();
			//			}
			//
			//			if (connection != null){
			//				connection.disconnect();
			//			}

		}
		return my_fetched_infos;
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

	class ParsingOutput{
		private String[] xpathResults;
		private String h1="";
		private String title="";
		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public String getH1() {
			return h1;
		}
		public void setH1(String h1) {
			this.h1 = h1;
		}
		public String[] getXpathResults() {
			return xpathResults;
		}
		public void setXpathResults(String[] xpathResults) {
			this.xpathResults = xpathResults;
		}
	}

	class URLComparisonInfo{
		private int id;
		private int status=-1;
		private ParsingOutput solrOutput;
		private ParsingOutput exaleadOutput;
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public int getStatus() {
			return status;
		}
		public void setStatus(int status) {
			this.status = status;
		}
		public ParsingOutput getSolrOutput() {
			return solrOutput;
		}
		public void setSolrOutput(ParsingOutput solrOutput) {
			this.solrOutput = solrOutput;
		}
		public ParsingOutput getExaleadOutput() {
			return exaleadOutput;
		}
		public void setExaleadOutput(ParsingOutput exaleadOutput) {
			this.exaleadOutput = exaleadOutput;
		}
	}
}
