package com.test;

import java.io.IOException;
import java.io.PrintWriter;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpGet;
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

import com.parsing.utility.ParsingOutput;
import com.parsing.utility.XPathUtility;


public class CdiscountIntraCookiesParsingTest {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException{

		for (int i=1;i<2;i++){
			//String url = "http://www.recette-cdiscount.com/juniors/figurines/toute-l-offre-figurines/l-1206740.html#_his_";
			String url = "";

			if (i%2 ==0){
				url = "http://www.cdiscount.com/livres-bd/livres-sciences-humaines/vetement-histoire-archeologie/f-10515490302-9782863770894.html?a";
			} else {
				url = "http://www.cdiscount.com/maison/sanitaire/tete-interchangeable-urinoir-presto-60-60-tc/f-1174503-pre3535770010098.html?a";
			}

			//String url = "http://www.recette-cdiscount.com/juniors/figurines/dc-comics-new-52-terre-2-superman-action-figure/f-1206740-dcd0761941320083.html";
			//String url = "http://www.recette-cdiscount.com/jardin-animalerie/v-163-1.html";
			System.out.println(Thread.currentThread().getName()+" fetching URL : "+url + " with cookie value to tap Solr");
			HttpGet getSolr = new HttpGet(url);
			HttpParams my_httpParams_solr = new BasicHttpParams();

			HttpConnectionParams.setConnectionTimeout(my_httpParams_solr, 300000000);
			DefaultHttpClient clientSolr = new DefaultHttpClient(my_httpParams_solr);


			HttpContext HTTP_CONTEXT_SOLR = new BasicHttpContext();
			//String useragent = "CdiscountBot-crawler";
			//String useragent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
			String useragent = "CdiscountBot-crawler";

			HTTP_CONTEXT_SOLR.setAttribute(CoreProtocolPNames.USER_AGENT,useragent);
			//getSolr.setHeader("Referer", "http://www.google.com");
			getSolr.setHeader("User-Agent", useragent);
			// set the cookies
			CookieStore cookieStoreSolr = new BasicCookieStore();
			BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "668.1");
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
			String solr_path_to_write = "D:\\My_Data\\My_Solr_vs_Exalead\\SolrVersion.txt";
			PrintWriter solr_out = new PrintWriter(solr_path_to_write);
			solr_out.println(page_source_codeSolr);
			System.out.println(Thread.currentThread().getName()+" fetching URL : "+url + " with cookie value to tap Exalead");
			HttpGet getExalead = new HttpGet(url);
			HttpContext HTTP_CONTEXT_EXALEAD = new BasicHttpContext();
			HTTP_CONTEXT_EXALEAD.setAttribute(CoreProtocolPNames.USER_AGENT, useragent);
			//getSolr.setHeader("Referer", "http://www.google.com");
			getExalead.setHeader("User-Agent", useragent);
			HttpParams my_httpParams_exalead = new BasicHttpParams();

			HttpConnectionParams.setConnectionTimeout(my_httpParams_exalead, 300000000);
			HttpConnectionParams.setSoTimeout(my_httpParams_exalead, 1500000000);

			DefaultHttpClient clientExalead = new DefaultHttpClient(my_httpParams_exalead);
			// set the cookies
			CookieStore cookieStoreExalead = new BasicCookieStore();
			BasicClientCookie cookieExalead = new BasicClientCookie("_$hidden", "668.0");
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
			
			String exalead_path_to_write = "D:\\My_Data\\My_Solr_vs_Exalead\\ExaleadVersion.txt";
			ParsingOutput exaleadOutput = XPathUtility.parse_page_code_source(page_source_codeExalead,XPathUtility.loadXPATHConf());
			PrintWriter exalead_out = new PrintWriter(exalead_path_to_write);
			exalead_out.println(page_source_codeExalead);
			System.out.println("Comparing URLs :"+url);
			System.out.println(solrOutput);
			System.out.println(exaleadOutput);
			if (!solrOutput.getH1().equals(exaleadOutput.getH1())){
				System.out.println("H1 Not good");
			} else {
				System.out.println("H1 Good");
			}
			
			if (!solrOutput.getTitle().equals(exaleadOutput.getTitle())){
				System.out.println("Title Not good");
			} else {
				System.out.println("Title Good");
			}
		
			for (int k=0;k<5;k++){
				if (!solrOutput.getXpathResults()[k].equals(exaleadOutput.getXpathResults()[k])){
					System.out.println("XPATH Title Not good");
				} else {
					System.out.println("XPATH Title Good");
				}
			}
		}
	}
}