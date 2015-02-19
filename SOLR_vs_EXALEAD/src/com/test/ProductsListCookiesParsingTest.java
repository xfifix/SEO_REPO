package com.test;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

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
import org.xml.sax.SAXException;

import com.parsing.utility.ProductsListParseUtility;
import com.parsing.utility.URLComparisonListProductsInfo;

public class ProductsListCookiesParsingTest {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{
		//String url = "http://www.cdiscount.com/bagages/homme/sacs-de-sport/l-1432004.html";
		//String url = "http://www.cdiscount.com/electromenager/refrigerateur-congelateur/achat-refrigerateur/refrigerateur-encastrable/refrigerateur-2-portes/l-11003090502.html";
		// testing a redirection
		String url="http://www.cdiscount.com/cdiscount pro/equipement-du-bureau/porte-manteaux-porte-parapluies/porte-manteaux/l-115851401.html";
		url=url.replace(" ", "%20");
		String page_two_url = url.replace(".html", "-2.html");
		String user_agent= "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
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
			System.out.println(exaleadOutput);
			System.out.println(exaleadOutput.getStatus());
			System.out.println(solrOutput.getStatus());
		} catch (Exception e){
			System.out.println("Trouble fetching URL : "+url);
			e.printStackTrace();
		}
	}
}
