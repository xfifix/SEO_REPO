package com.utility;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CrawlingUtility {
	
	public static List<String> parseStandardDepartmentList(String pageSourceCode){
		List<String> to_return = new ArrayList<String>();
		Document doc = Jsoup.parse(pageSourceCode,"UTF-8");
		Elements attributes = doc.select(".zg_title");
		Elements department_links = attributes.select("a");		
		for(Element link : department_links){
			System.out.println("Title : "+link.text());
			System.out.println("URL " +link.attributes());
			to_return.add(link.attributes().toString());
		}
		return to_return;
	}
	
	public static List<String> parseEntryMenu(String pageSourceCode){
		List<String> to_return = new ArrayList<String>();
		Document doc = Jsoup.parse(pageSourceCode,"UTF-8");
		Elements attributes = doc.select("#zg_browseRoot");
		Elements department_links = attributes.select("a");		
		for(Element link : department_links){
			System.out.println("Department : "+link.text());
			System.out.println("Attributes " +link.attributes().get("href"));
			to_return.add(link.attributes().get("href"));
		}
		return to_return;
	}
		
	public static String getPSCode(String url_string) throws ClientProtocolException, IOException{
		HttpGet getSolr = new HttpGet(url_string);
		String userAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
		getSolr.setHeader("User-Agent", userAgent);
		DefaultHttpClient clientSolr = new DefaultHttpClient();		
		HttpContext HTTP_CONTEXT_SOLR = new BasicHttpContext();
		HTTP_CONTEXT_SOLR.setAttribute(CoreProtocolPNames.USER_AGENT, userAgent);
		//getSolr.setHeader("Referer", "http://www.google.com");
		getSolr.setHeader("User-Agent",userAgent);
//		// set the cookies
//		CookieStore cookieStoreSolr = new BasicCookieStore();
//		BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "670.1");
//		cookieSolr.setDomain("cdiscount.com");
//		cookieSolr.setPath("/");
//		cookieStoreSolr.addCookie(cookieSolr);    
//		clientSolr.setCookieStore(cookieStoreSolr);
		// get the cookies
		HttpResponse responseSolr = clientSolr.execute(getSolr,HTTP_CONTEXT_SOLR);

		System.out.println(responseSolr.getStatusLine());
		HttpEntity entitySolr = responseSolr.getEntity();
		
		String page_source_codeSolr = EntityUtils.toString(entitySolr);
		EntityUtils.consume(entitySolr);
		clientSolr.close();
		return page_source_codeSolr;
	}
	
	public static String getPageSourceCode(String url_string){
		System.out.println("Processing URL : "+url_string);
		System.out.println(Thread.currentThread().getName()+" fetching SKU : "+url_string);
		HttpURLConnection connection = null;
		String html="";
		try{
			//System.out.println(Thread.currentThread().getName()+" fetching URL : "+line_info.getUrl());
			URL url = new URL(url_string);
			connection = (HttpURLConnection)url.openConnection();
			connection.setRequestProperty("Accept-Charset", "UTF-8");
			connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
			connection.setRequestMethod("GET");
			String userAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)";
			connection.setRequestProperty("User-Agent",userAgent);
			connection.setInstanceFollowRedirects(false);
			connection.setConnectTimeout(30000);
			connection.connect();

			// getting the status from the connection
			int status=connection.getResponseCode();
			StringBuilder strB = new StringBuilder();
			try {
			    BufferedReader input = new BufferedReader(
			            new InputStreamReader(connection.getInputStream(), "UTF-8")); 
			    
			    String str;
			    while (null != (str = input.readLine())) {
			        strB.append(str); 
			    }
			    input.close();
			} catch (IOException e) {
			    e.printStackTrace();
			}
//			
//			// getting the content to parse
//			BufferedReader buff = new BufferedReader(new InputStreamReader((InputStream) connection.getContent(), "utf-8"));
//			String content_line;
//			StringBuilder builder=new StringBuilder();
//			do {
//				content_line = buff.readLine();
//				builder.append(content_line);
//			} while (content_line != null);
//			html = builder.toString();
			html=strB.toString();
			html = new String(html.getBytes(),"UTF-8");
			System.out.println(Thread.currentThread().getName()+" Status " +status+ " fetching URL : "+url_string);
		} catch (Exception e){
			System.out.println("Error with : "+url_string);
			e.printStackTrace();
		}
		return html;
	}
}
