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

import com.parsing.utility.FacettesParsingOutput;
import com.parsing.utility.FacettesUtility;
import com.parsing.utility.ProductsListParseUtility;
import com.parsing.utility.URLComparisonListProductsInfo;

public class FacettesParsingTest {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{		
		String url="http://www.cdiscount.com/search/10/tondeuse+sans+fil.html";
		String user_agent= "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
		try{
			String solrurl = url + "?b";
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
			// closing the solr client
			clientSolr.close();
			// we now extract information from our page source code
			FacettesParsingOutput solrOutput = FacettesUtility.parse_page_code_source(page_source_codeSolr);
			// inspecting the output
			System.out.println(solrOutput);
			
			// inspecting the output
			String exaleadurl = url + "?a";
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
			clientExalead.close();
			// we now extract information from our page source code
			FacettesParsingOutput exaleadOutput = FacettesUtility.parse_page_code_source(page_source_codeExalead);
			// inspecting the output
			System.out.println(exaleadOutput);
		} catch (Exception e){
			System.out.println("Trouble fetching URL : "+url);
			e.printStackTrace();
		}
	}
}
