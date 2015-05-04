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
	//private static int batch_size = 100;
	private static int batch_size = 10;	
	private static String updateStatement ="UPDATE SOLR_VS_EXALEAD_SEARCH_LIST SET STATUS_SOLR=?, NB_PRODUCTS_SOLR=?, FACETTES_SOLR=?, LISTSKUS_SOLR=?, STATUS_EXALEAD=?, NB_PRODUCTS_EXALEAD=?, FACETTES_EXALEAD=?, LISTSKUS_EXALEAD=?, FACETTE_EQUALITY=?, LISTSKUS_EQUALITY=?,  TO_FETCH=FALSE WHERE ID=?";
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
		System.out.println("Adding to batch : " + infos.size() + "ULRs into database");
		try {
			//Statement st = con.createStatement();
			con.setAutoCommit(false); 
			PreparedStatement st = con.prepareStatement(updateStatement);
			for (int i=0;i<infos.size();i++){
				FacettesURLComparisonInfo local_info = infos.get(i);
				FacettesParsingOutput solrOutput = local_info.getSolrOutput();

				String FACETTES_SOLR = solrOutput.getFacettes();
				String NBPRODUCTS_SOLR = solrOutput.getNb_products();
				String SKULIST_SOLR = solrOutput.getOrderedSkusList().toString();
				
				FacettesParsingOutput exaleadOutput = local_info.getExaleadOutput();		
				String FACETTES_EXALEAD = exaleadOutput.getFacettes();
				String NBPRODUCTS_EXALEAD = exaleadOutput.getNb_products();
				String SKULIST_EXALEAD = exaleadOutput.getOrderedSkusList().toString();
				
				boolean facette_equality = FACETTES_EXALEAD.equals(FACETTES_SOLR);
				boolean skulist_equality = SKULIST_SOLR.equals(SKULIST_EXALEAD);
				//UPDATE SOLR_VS_EXALEAD_SEARCH_LIST SET STATUS_SOLR=?, NB_PRODUCTS_SOLR=?, FACETTES_SOLR=?,NB_PRODUCTS_EXALEAD=?, FACETTES_EXALEAD=?, TO_FETCH=FALSE WHERE ID=?";

				st.setInt(1, local_info.getSolrstatus());
				st.setString(2, NBPRODUCTS_SOLR);
				st.setString(3, FACETTES_SOLR);
				st.setString(4, SKULIST_SOLR);
				st.setInt(5, local_info.getExaleadstatus());
				st.setString(6, NBPRODUCTS_EXALEAD);
				st.setString(7, FACETTES_EXALEAD);
				st.setString(8, SKULIST_EXALEAD);
				st.setBoolean(9,facette_equality);
				st.setBoolean(10,skulist_equality);
				st.setInt(11, local_info.getId());
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

	private String construct_solr_url_search(ExpressionId expression){
		String expression_string = expression.getExpression().replace(" ","+");
		String facette_param = expression.getFacetteQuery();
		String url = "http://www.cdiscount.com/search/10/"+expression_string+".html";
		try {
			if (!"".equals(facette_param)){
				url=url+"?";
				String parameters = "?NavigationForm.CurrentSelectedNavigationPath="+facette_param+"&b";
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
				url=url+"?";
				String parameters = "?NavigationForm.CurrentSelectedNavigationPath="+facette_param+"&a";
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
				List<String> my_solr_ordered_skus = FacettesUtility.parse_page_code_source_for_ordered_skus(page_source_codeSolr);
				solrOutput.setOrderedSkusList(my_solr_ordered_skus);
				
				my_info.setSolrOutput(solrOutput);
				my_info.setSolrstatus(responseSolr.getStatusLine().getStatusCode());


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
				my_info.setExaleadstatus(responseExalead.getStatusLine().getStatusCode());
				List<String> my_exalead_ordered_skus = FacettesUtility.parse_page_code_source_for_ordered_skus(page_source_codeExalead);
				exaleadOutput.setOrderedSkusList(my_exalead_ordered_skus);

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
		private int solrstatus=-1;
		private int exaleadstatus=-1;
		private FacettesParsingOutput solrOutput;
		private FacettesParsingOutput exaleadOutput;
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public int getSolrstatus() {
			return solrstatus;
		}
		public void setSolrstatus(int solrstatus) {
			this.solrstatus = solrstatus;
		}
		public int getExaleadstatus() {
			return exaleadstatus;
		}
		public void setExaleadstatus(int exaleadstatus) {
			this.exaleadstatus = exaleadstatus;
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
