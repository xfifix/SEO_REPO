package com.test;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

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
import org.xml.sax.SAXException;

import com.parsing.utility.ParsingOutput;
import com.parsing.utility.XPathUtility;

public class CookiesParsingTest {
	public static void main(String[] args) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{
		String url = "http://www.recette-cdiscount.com/juniors/figurines/dc-comics-new-52-terre-2-superman-action-figure/f-1206740-dcd0761941320083.html";
		System.out.println(Thread.currentThread().getName()+" fetching URL : "+url + " with cookie value to tap Solr");
		HttpGet getSolr = new HttpGet(url);
		DefaultHttpClient clientSolr = new DefaultHttpClient();		
		HttpContext HTTP_CONTEXT_SOLR = new BasicHttpContext();
		//String useragent = "CdiscountBot-crawler";
		String useragent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
		HTTP_CONTEXT_SOLR.setAttribute(CoreProtocolPNames.USER_AGENT,useragent);
		//getSolr.setHeader("Referer", "http://www.google.com");
		getSolr.setHeader("User-Agent", useragent);
		// set the cookies
		CookieStore cookieStoreSolr = new BasicCookieStore();
		BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "666.1");
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
		ParsingOutput solrOutput = XPathUtility.parse_page_code_source(page_source_codeSolr,XPathUtility.loadXPATHConf());

		System.out.println(Thread.currentThread().getName()+" fetching URL : "+url + " with cookie value to tap Exalead");
		HttpGet getExalead = new HttpGet(url);
		HttpContext HTTP_CONTEXT_EXALEAD = new BasicHttpContext();
		HTTP_CONTEXT_EXALEAD.setAttribute(CoreProtocolPNames.USER_AGENT, useragent);
		//getSolr.setHeader("Referer", "http://www.google.com");
		getExalead.setHeader("User-Agent", useragent);
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
		System.out.println(responseExalead.getStatusLine());
		HttpEntity entityExalead = responseExalead.getEntity();
		// do something useful with the response body
		// and ensure it is fully consumed
		String page_source_codeExalead = EntityUtils.toString(entityExalead);
		EntityUtils.consume(entityExalead);
		clientExalead.close();
		ParsingOutput exaleadOutput = XPathUtility.parse_page_code_source(page_source_codeExalead,XPathUtility.loadXPATHConf());

	}
}
