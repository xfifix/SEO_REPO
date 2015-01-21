package com.test;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.util.EntityUtils;

public class TestingCookies {

	@SuppressWarnings({ "resource", "deprecation" })
	public static void main(String[] args) throws ClientProtocolException, IOException{

		String pathSolr = "/home/sduprey/My_Data/My_Solr_vs_Exalead/SolrVersion.txt";
		// requesting Solr
		System.out.println("Requesting Solr");
		String fiche_produit_url = "http://www.cdiscount.com/juniors/figurines/dc-comics-new-52-terre-2-superman-action-figure/f-1206740-dcd0761941320083.html";
		HttpGet getSolr = new HttpGet(fiche_produit_url);
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
		System.out.println(page_source_codeSolr);
		EntityUtils.consume(entitySolr);
		// writing the file to the disk
		PrintWriter outSolr = new PrintWriter(pathSolr);
		outSolr.println(page_source_codeSolr);
		outSolr.close();

		// requesting Exalead
		String pathExalead = "/home/sduprey/My_Data/My_Solr_vs_Exalead/ExaleadVersion.txt";
		System.out.println("Requesting Exalead");
		HttpGet getExalead = new HttpGet(fiche_produit_url);
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
		// printing the file to disk
		PrintWriter outExalead = new PrintWriter(pathExalead);
		outExalead.println(page_source_codeExalead);
		outExalead.close();
		
		// just a little warning
		if (!page_source_codeExalead.equals(page_source_codeSolr)){
			System.out.println("We do have a problem");
		}
	}
}
