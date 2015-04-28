package com.compare.search.batch;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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

import com.parsing.utility.FacettesParsingOutput;
import com.parsing.utility.FacettesUtility;

public class BatchSearchComparingURLListWorkerThread implements Runnable {
	private static int batch_size = 100;
	private static String updateStatement ="UPDATE SOLR_VS_EXALEAD_SEARCH_LIST SET STATUS=?, H1_SOLR=?, TITLE_SOLR=?, XPATH1_SOLR=?, XPATH2_SOLR=?, XPATH3_SOLR=?, XPATH4_SOLR=?, XPATH5_SOLR=?, XPATH6_SOLR=?, XPATH7_SOLR=?, XPATH8_SOLR=?, XPATH9_SOLR=?, XPATH10_SOLR=?, H1_EXALEAD=?, TITLE_EXALEAD=?, XPATH1_EXALEAD=?, XPATH2_EXALEAD=?, XPATH3_EXALEAD=?, XPATH4_EXALEAD=?, XPATH5_EXALEAD=?, XPATH6_EXALEAD=?, XPATH7_EXALEAD=?, XPATH8_EXALEAD=?, XPATH9_EXALEAD=?, XPATH10_EXALEAD=?, H1_COMPARISON=?, TITLE_COMPARISON=?, XPATH1_COMPARISON=?, XPATH2_COMPARISON=?, XPATH3_COMPARISON=?, XPATH4_COMPARISON=?, XPATH5_COMPARISON=?, XPATH6_COMPARISON=?, XPATH7_COMPARISON=?, XPATH8_COMPARISON=?, XPATH9_COMPARISON=?, XPATH10_COMPARISON=?, TO_FETCH=FALSE WHERE ID=?";
	private String user_agent;
	private List<ExpressionId> my_expressions_to_fetch = new ArrayList<ExpressionId>();
	private Connection con;

	public BatchSearchComparingURLListWorkerThread(Connection con ,List<Integer> to_fetch, String my_user_agent) throws SQLException{
		this.user_agent=my_user_agent;
		this.con = con;
		String my_url="";
		if (to_fetch.size()>0){
			try {
				PreparedStatement pst = null;
				my_url="SELECT SEARCH_EXPRESSION, FACETTES_QUERY, ID FROM SOLR_VS_EXALEAD_SEARCH_LIST WHERE TO_FETCH = TRUE and ID in "+to_fetch.toString();
				my_url=my_url.replace("[", "(");
				my_url=my_url.replace("]", ")");
				pst = con.prepareStatement(my_url);
				ResultSet rs = null;
				rs = pst.executeQuery();
				while (rs.next()) {
					String loc_expression = rs.getString(1);
					String loc_facettes_query = rs.getString(2);					
					int id = rs.getInt(3);
					ExpressionId toadd = new ExpressionId();
					toadd.setId(id);
					toadd.setFacetteQuery(loc_facettes_query);
					toadd.setExpression(loc_expression);
					my_expressions_to_fetch.add(toadd); 
				}
				pst.close();
				System.out.println(Thread.currentThread()+" initialized with  : "+to_fetch.size() + " fetched URLs");
				System.out.println(Thread.currentThread()+" fetched expression's IDs :"+to_fetch.toString());
			}
			catch(SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with thread"+Thread.currentThread()+" and URL : "+my_url);
			}
		}
	}

	public void run() {
		List<ExpressionId> line_infos = new ArrayList<ExpressionId>();
		for (ExpressionId id :my_expressions_to_fetch){
			line_infos.add(id);
			if (line_infos.size() !=0 && line_infos.size() % batch_size ==0) {
				runBatch(line_infos);	
				line_infos.clear();
				line_infos = new ArrayList<ExpressionId>();
			}
		}
		runBatch(line_infos);
		close_connection();
		System.out.println(Thread.currentThread().getName()+" closed connection");
	}

	public void runBatch(List<ExpressionId> line_infos){
		List<FacettesURLComparisonInfo> infos=processComparison(line_infos);
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

	// batched update
	private void updateStatus(List<FacettesURLComparisonInfo> infos){
		//		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		//		try {
		//			//Statement st = con.createStatement();
		//			con.setAutoCommit(false); 
		//			PreparedStatement st = con.prepareStatement(updateStatement);
		//			for (int i=0;i<infos.size();i++){
		//				URLComparisonInfo local_info = infos.get(i);
		//				ParsingOutput solrOutput = local_info.getSolrOutput();
		//
		//				String H1_SOLR = solrOutput.getH1().replace("'", "");
		//				String TITLE_SOLR = solrOutput.getTitle().replace("'", "");
		//				String[] XPATHRESULTS_SOLR = solrOutput.getXpathResults();
		//
		//				ParsingOutput exaleadOutput = local_info.getExaleadOutput();			
		//				String H1_EXALEAD = exaleadOutput.getH1().replace("'", "");
		//				String TITLE_EXALEAD = exaleadOutput.getTitle().replace("'", "");
		//				String[] XPATHRESULTS_EXALEAD = exaleadOutput.getXpathResults();
		//
		//				st.setInt(1, local_info.getStatus());
		//				st.setString(2,H1_SOLR);
		//				st.setString(3, TITLE_SOLR);
		//				if (XPATHRESULTS_SOLR != null){	
		//					if (XPATHRESULTS_SOLR[0] != null){
		//						st.setString(4, XPATHRESULTS_SOLR[0]);
		//					} else {
		//						st.setString(4, "");
		//					}
		//					if (XPATHRESULTS_SOLR[1] != null){
		//						st.setString(5, XPATHRESULTS_SOLR[1]);
		//					} else {
		//						st.setString(5, "");
		//					}
		//					if (XPATHRESULTS_SOLR[2] != null){
		//						st.setString(6, XPATHRESULTS_SOLR[2]);
		//					} else {
		//						st.setString(6, "");
		//					}
		//					if (XPATHRESULTS_SOLR[3] != null){
		//						st.setString(7, XPATHRESULTS_SOLR[3]);
		//					} else {
		//						st.setString(7, "");
		//					}
		//					if (XPATHRESULTS_SOLR[4] != null){
		//						st.setString(8, XPATHRESULTS_SOLR[4]);
		//					} else {
		//						st.setString(8, "");
		//					}
		//					if (XPATHRESULTS_SOLR[5] != null){
		//						st.setString(9, XPATHRESULTS_SOLR[5]);
		//					} else {
		//						st.setString(9, "");
		//					}
		//					if (XPATHRESULTS_SOLR[6] != null){
		//						st.setString(10, XPATHRESULTS_SOLR[6]);
		//					} else {
		//						st.setString(10, "");
		//					}
		//					if (XPATHRESULTS_SOLR[7] != null){
		//						st.setString(11, XPATHRESULTS_SOLR[7]);
		//					} else {
		//						st.setString(11, "");
		//					}
		//					if (XPATHRESULTS_SOLR[8] != null){
		//						st.setString(12, XPATHRESULTS_SOLR[8]);
		//					} else {
		//						st.setString(12, "");
		//					}
		//					if (XPATHRESULTS_SOLR[9] != null){
		//						st.setString(13, XPATHRESULTS_SOLR[9]);
		//					} else {
		//						st.setString(13, "");
		//					}
		//				}else {
		//					st.setString(4, "");
		//					st.setString(5, "");
		//					st.setString(6, "");
		//					st.setString(7, "");
		//					st.setString(8, "");
		//					st.setString(9, "");
		//					st.setString(10, "");
		//					st.setString(11, "");
		//					st.setString(12, "");
		//					st.setString(13, "");
		//				}
		//				st.setString(14,H1_EXALEAD);
		//				st.setString(15, TITLE_EXALEAD);
		//				if (XPATHRESULTS_EXALEAD != null){
		//
		//					if (XPATHRESULTS_EXALEAD[0] != null){
		//						st.setString(16, XPATHRESULTS_EXALEAD[0]);
		//					} else {
		//						st.setString(16, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[1] != null){
		//						st.setString(17, XPATHRESULTS_EXALEAD[1]);
		//					} else {
		//						st.setString(17, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[2] != null){
		//						st.setString(18, XPATHRESULTS_EXALEAD[2]);
		//					} else {
		//						st.setString(18, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[3] != null){
		//						st.setString(19, XPATHRESULTS_EXALEAD[3]);
		//					} else {
		//						st.setString(19, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[4] != null){
		//						st.setString(20, XPATHRESULTS_EXALEAD[4]);
		//					} else {
		//						st.setString(20, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[5] != null){
		//						st.setString(21, XPATHRESULTS_EXALEAD[5]);
		//					} else {
		//						st.setString(21, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[6] != null){
		//						st.setString(22, XPATHRESULTS_EXALEAD[6]);
		//					} else {
		//						st.setString(22, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[7] != null){
		//						st.setString(23, XPATHRESULTS_EXALEAD[7]);
		//					} else {
		//						st.setString(23, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[8] != null){
		//						st.setString(24, XPATHRESULTS_EXALEAD[8]);
		//					} else {
		//						st.setString(24, "");
		//					}
		//					if (XPATHRESULTS_EXALEAD[9] != null){
		//						st.setString(25, XPATHRESULTS_EXALEAD[9]);
		//					} else {
		//						st.setString(25, "");
		//					}
		//				}else {
		//					st.setString(16, "");
		//					st.setString(17, "");
		//					st.setString(18, "");
		//					st.setString(19, "");
		//					st.setString(20, "");
		//					st.setString(21, "");
		//					st.setString(22, "");
		//					st.setString(23, "");
		//					st.setString(24, "");
		//					st.setString(25, "");
		//				}
		//				if (!H1_SOLR.equals(H1_EXALEAD)){
		//					st.setInt(26, 0);
		//				} else {
		//					st.setInt(26, 1);
		//				}
		//				if (!TITLE_SOLR.equals(TITLE_EXALEAD)){
		//					st.setInt(27, 0);
		//				} else {
		//					st.setInt(27, 1);
		//				}
		//
		//				if ((XPATHRESULTS_EXALEAD != null) && (XPATHRESULTS_SOLR != null)){
		//					if (!XPATHRESULTS_SOLR[0].equals(XPATHRESULTS_EXALEAD[0])){
		//						st.setInt(28, 0);
		//					} else {
		//						st.setInt(28, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[1].equals(XPATHRESULTS_EXALEAD[1])){
		//						st.setInt(29, 0);
		//					} else {
		//						st.setInt(29, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[2].equals(XPATHRESULTS_EXALEAD[2])){
		//						st.setInt(30, 0);
		//					} else {
		//						st.setInt(30, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[3].equals(XPATHRESULTS_EXALEAD[3])){
		//						st.setInt(31, 0);
		//					} else {
		//						st.setInt(31, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[4].equals(XPATHRESULTS_EXALEAD[4])){
		//						st.setInt(32, 0);
		//					} else {
		//						st.setInt(32, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[5].equals(XPATHRESULTS_EXALEAD[5])){
		//						st.setInt(33, 0);
		//					} else {
		//						st.setInt(33, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[6].equals(XPATHRESULTS_EXALEAD[6])){
		//						st.setInt(34, 0);
		//					} else {
		//						st.setInt(34, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[7].equals(XPATHRESULTS_EXALEAD[7])){
		//						st.setInt(35, 0);
		//					} else {
		//						st.setInt(35, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[8].equals(XPATHRESULTS_EXALEAD[8])){
		//						st.setInt(36, 0);
		//					} else {
		//						st.setInt(36, 1);
		//					}
		//					if (!XPATHRESULTS_SOLR[9].equals(XPATHRESULTS_EXALEAD[9])){
		//						st.setInt(37, 0);
		//					} else {
		//						st.setInt(37, 1);
		//					}
		//				}
		//
		//				st.setInt(38, local_info.getId());
		//				//UPDATE HTTPINFOS_LIST SET STATUS=?, H1=?, TITLE=?, XPATH1=?, XPATH2=?, XPATH3=?, XPATH4=?, XPATH5=?, TO_FETCH=FALSE WHERE ID=?";
		//				//	String batch ="UPDATE HTTPINFOS_LIST SET STATUS="+infos.get(i).getStatus()+", H1='"+H1+"', TITLE='"+TITLE+ "',TO_FETCH=FALSE WHERE ID="+infos.get(i).getId();
		//				st.addBatch();		
		//			}      
		//			//int counts[] = st.executeBatch();
		//			System.out.println("Beginning to insert : " + infos.size() + "ULRs into database");
		//			st.executeBatch();
		//			con.commit();
		//			st.close();
		//			System.out.println("Having inserted : " + infos.size() + "ULRs into database");
		//		} catch (SQLException e){
		//			e.printStackTrace();
		//			System.out.println("Trouble inserting batch ");
		//		}
	}

	private String construct_solr_url_search(ExpressionId expression){
		String expression_string = expression.getExpression().replace(" ","+");
		String facette_param = expression.getFacetteQuery();
		String url = "http://www.cdiscount.com/search/10/"+expression_string+".html";
		try {
			if (!"".equals(facette_param)){
				url=url+"?";
				String parameters = "NavigationForm.CurrentSelectedNavigationPath="+facette_param+"&b";
				url=url+URLEncoder.encode(parameters, "UTF-8");

			} else {
				url=url+"?b";
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return url;
	}

	private String construct_exalead_url_search(ExpressionId expression){
		String expression_string = expression.getExpression().replace(" ","+");
		String facette_param = expression.getFacetteQuery();
		String url = "http://www.cdiscount.com/search/10/"+expression_string+".html";
		try{
			if (!"".equals(facette_param)){
				String parameters = "NavigationForm.CurrentSelectedNavigationPath="+facette_param+"&a";
				url=url+URLEncoder.encode(parameters, "UTF-8");
			} else {
				url=url+"?a";
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return url;
	}


	private List<FacettesURLComparisonInfo> processComparison(List<ExpressionId> line_infos) {
		List<FacettesURLComparisonInfo> my_fetched_infos = new ArrayList<FacettesURLComparisonInfo>();
		for(ExpressionId line_info : line_infos){
			// we here loop over each URL the thread has to handle
			FacettesURLComparisonInfo my_info = new FacettesURLComparisonInfo();
			// the URLComparisonInfo object will collect all the info
			// it will contain the id
			my_info.setId(line_info.getId());
			// getting the URL
			String solrurl = construct_solr_url_search(line_info);
			String exaleadurl =construct_exalead_url_search(line_info);

			try{
				// fetching the solr version
				System.out.println(Thread.currentThread().getName()+" fetching URL : "+solrurl + " with no cookie value to tap Solr");
				HttpGet getSolr = new HttpGet(solrurl);
				getSolr.setHeader("User-Agent", user_agent);
				DefaultHttpClient clientSolr = new DefaultHttpClient();		
				HttpContext HTTP_CONTEXT_SOLR = new BasicHttpContext();
				HTTP_CONTEXT_SOLR.setAttribute(CoreProtocolPNames.USER_AGENT, user_agent);
				//getSolr.setHeader("Referer", "http://www.google.com");
				getSolr.setHeader("User-Agent", "CdiscountBot-crawler");
				// set the cookies
				CookieStore cookieStoreSolr = new BasicCookieStore();
				BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "670.1");
				cookieSolr.setDomain("cdiscount.com");
				cookieSolr.setPath("/");
				cookieStoreSolr.addCookie(cookieSolr);    
				clientSolr.setCookieStore(cookieStoreSolr);
				// get the cookies
				HttpResponse responseSolr = clientSolr.execute(getSolr,HTTP_CONTEXT_SOLR);

				System.out.println(responseSolr.getStatusLine());
				HttpEntity entitySolr = responseSolr.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page_source_codeSolr = EntityUtils.toString(entitySolr);
				EntityUtils.consume(entitySolr);
				clientSolr.close();

				FacettesParsingOutput solrOutput = FacettesUtility.parse_page_code_source(page_source_codeSolr);		
				my_info.setSolrOutput(solrOutput);
				my_info.setStatus(responseSolr.getStatusLine().getStatusCode());


				System.out.println(Thread.currentThread().getName()+" fetching URL : "+exaleadurl + " with cookie value to tap Exalead");
				HttpGet getExalead = new HttpGet(exaleadurl);
				HttpContext HTTP_CONTEXT_EXALEAD = new BasicHttpContext();
				HTTP_CONTEXT_EXALEAD.setAttribute(CoreProtocolPNames.USER_AGENT, user_agent);
				//getSolr.setHeader("Referer", "http://www.google.com");
				getExalead.setHeader("User-Agent", user_agent);
				DefaultHttpClient clientExalead = new DefaultHttpClient();
				// set the cookies
				CookieStore cookieStoreExalead = new BasicCookieStore();
				BasicClientCookie cookieExalead = new BasicClientCookie("_$hidden", "670.0");
				cookieExalead.setDomain("cdiscount.com");
				cookieExalead.setPath("/");
				cookieStoreExalead.addCookie(cookieExalead);    
				clientExalead.setCookieStore(cookieStoreExalead);
				// get the cookies
				HttpResponse responseExalead = clientExalead.execute(getExalead,HTTP_CONTEXT_EXALEAD);
				System.out.println(responseExalead.getStatusLine());
				HttpEntity entityExalead = responseExalead.getEntity();
				// do something useful with the response body
				// and ensure it is fully consumed
				String page_source_codeExalead = EntityUtils.toString(entityExalead);
				EntityUtils.consume(entityExalead);
				clientExalead.close();
				FacettesParsingOutput exaleadOutput = FacettesUtility.parse_page_code_source(page_source_codeExalead);
				my_info.setExaleadOutput(exaleadOutput);
			} catch (Exception e){
				System.out.println("Trouble fetching expression : "+solrurl);
				e.printStackTrace();
			}
			my_fetched_infos.add(my_info);
		}
		return my_fetched_infos;
	}

	class ExpressionId{
		private String expression="";
		private String facetteQuery="";
		private int id;
		public String getExpression() {
			return expression;
		}
		public void setExpression(String expression) {
			this.expression = expression;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public String getFacetteQuery() {
			return facetteQuery;
		}
		public void setFacetteQuery(String facetteQuery) {
			this.facetteQuery = facetteQuery;
		}
	}

	class FacettesURLComparisonInfo{
		private int id;
		private int status=-1;
		private FacettesParsingOutput solrOutput;
		private FacettesParsingOutput exaleadOutput;
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
		public FacettesParsingOutput getSolrOutput() {
			return solrOutput;
		}
		public void setSolrOutput(FacettesParsingOutput solrOutput) {
			this.solrOutput = solrOutput;
		}
		public FacettesParsingOutput getExaleadOutput() {
			return exaleadOutput;
		}
		public void setExaleadOutput(FacettesParsingOutput exaleadOutput) {
			this.exaleadOutput = exaleadOutput;
		}
	}
}
