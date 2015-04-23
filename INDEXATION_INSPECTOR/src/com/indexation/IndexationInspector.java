package com.indexation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class IndexationInspector {
	//	private static int min_number_of_wait_times = 40;
	//	private static int max_number_of_wait_times = 60;
	private static int min_number_of_wait_times = 20;
	private static int max_number_of_wait_times = 25;
	private static int bunch_size = 30;
	private static List<String> user_agents = new ArrayList<String>();
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/url_list_status.properties";
	private static String user_agent_path = "/home/sduprey/My_Data/My_User_Agents/user-agent.txt";
	private static String fetch_url_to_check = "SELECT URL FROM URL_TO_CHECK_LIST WHERE TO_FETCH = TRUE";
	private static String update_url = "UPDATE URL_TO_CHECK_LIST set IN_INDEX=? WHERE URL=?";
	
	public static void main(String[] args){

		if (args.length == 1 ){
			try{
				min_number_of_wait_times = Integer.parseInt(args[0]);
				max_number_of_wait_times = min_number_of_wait_times + 5;
			} catch (NumberFormatException e){
				e.printStackTrace();
				System.out.println("Minimum number of waiting times : " + min_number_of_wait_times);
				System.out.println("Maximum number of waiting times : " + max_number_of_wait_times);
			}
		}

		System.out.println("Minimum number of waiting times : " + min_number_of_wait_times);
		System.out.println("Maximum number of waiting times : " + max_number_of_wait_times);

		System.setProperty("http.agent", "");
		try {
			loadUserAgents();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("Trouble loading all user-agents");
			// we'll do with just one user agent
			user_agents.add("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)");
		}
		// getting the database connection properties
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_con_path);
			props.load(in);
		} catch (IOException ex) {
			System.out.println("Trouble fetching database configuration");
			ex.printStackTrace();

		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				System.out.println("Trouble fetching database configuration");
				ex.printStackTrace();
			}
		}
		//the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		System.out.println("You are connected to the postgresql HTTPSTATUS_LIST database as "+user);

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// getting the number of URLs to fetch
			pst = con.prepareStatement(fetch_url_to_check);
			rs = pst.executeQuery();
			while (rs.next()) {
				String url_to_test = rs.getString(1);
				boolean is_index = proxy_in_index_url(url_to_test);
				PreparedStatement update_pst = con.prepareStatement(update_url);
				update_pst.setBoolean(1, is_index);
				update_pst.setString(2, url_to_test);
				update_pst.executeUpdate();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
				System.out.println("Trouble with the database");
			}
		}
	}


	private static void loadUserAgents() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(user_agent_path));
		String line="";	
		while ((line = br.readLine()) != null) {
			user_agents.add(line);
		}
		br.close();
	}

	private static String randomUserAgent(){
		int list_size = user_agents.size();
		return user_agents.get(randInt(0,list_size-1));
	}

	public static boolean proxy_in_index_url(String url){
		// we here fetch up to three paginations
		long startTimeMs = System.currentTimeMillis( );
		org.jsoup.nodes.Document doc;
		int my_rank=30;
		int nb_results=0;

		String my_url = "";
		boolean found = false;
		try{
			// we wait between x and xx seconds
			Thread.sleep(randInt(min_number_of_wait_times,max_number_of_wait_times)*1000);
			System.out.println("Fetching a new page");
			String constructed_url ="https://www.google.fr/search?q="+url+"&num="+bunch_size;

			// adding a fake cookie
			CookieStore cookieStoreSolr = new BasicCookieStore();
			BasicClientCookie cookieSolr = new BasicClientCookie("_$hidden", "230.1");
			cookieSolr.setDomain("cdiscount.com");
			cookieSolr.setPath("/");
			cookieStoreSolr.addCookie(cookieSolr);    
			CloseableHttpClient httpclient = HttpClients.custom()
					.setDefaultCookieStore(cookieStoreSolr)
					.build();
			//HttpClientContext context = HttpClientContext.create();
			HttpContext context = new BasicHttpContext();
			String randomAgent = randomUserAgent();
			context.setAttribute(CoreProtocolPNames.USER_AGENT,randomAgent);
			// request.setHeader("Referer", "http://www.google.com");

			HttpGet getSERPrequest = new HttpGet(constructed_url);
			// we here use our properly configured squid proxy on port 3128 on localhost

			HttpHost squid_proxy = new HttpHost("localhost", 3128, "http");

			RequestConfig config = RequestConfig.custom()
					.setProxy(squid_proxy)
					.build();
			getSERPrequest.setConfig(config);

			CloseableHttpResponse responseSERP = httpclient.execute(getSERPrequest,context);

			// and ensure it is fully consumed
			String pageString = EntityUtils.toString(responseSERP.getEntity());
			EntityUtils.consume(responseSERP.getEntity());

			doc =  Jsoup.parse(pageString);
			Elements serps = doc.select("h3[class=r]");
			for (Element serp : serps) {
				Element link = serp.getElementsByTag("a").first();
				if (link != null){
					String linkref = link.attr("href");
					if (linkref.startsWith("/url?q=") || linkref.startsWith("http://")){
						nb_results++;
						if (linkref.startsWith("/url?q=")){
							linkref = linkref.substring(7,linkref.indexOf("&"));
						} else {
							if (linkref.indexOf("&") != -1){
								linkref = linkref.substring(0,linkref.indexOf("&"));
							}
						}
					}
					//					if (linkref.contains(targe_name) && !found){
					//						my_rank=nb_results;
					//						my_url=linkref;
					//						found=true;
					//					}			
				}
			}
			if (nb_results == 0){
				System.out.println("Warning captcha");
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}catch (InterruptedException e){
			e.printStackTrace();
		}
		long taskTimeMs  = System.currentTimeMillis( ) - startTimeMs;
		System.out.println("Overall google requesting time : "+taskTimeMs);

		if (nb_results == 0){
			System.out.println("Warning captcha");
		}else {
			System.out.println("Number of links read in the pages : "+nb_results);
		}
		//		System.out.println("My rank : "+my_rank+" for keyword : "+keyword);
		//		System.out.println("My URL : "+my_url+" for keyword : "+keyword);
		return true;
	}

	public static int randInt(int min, int max) {
		// NOTE: Usually this should be a field rather than a method
		// variable so that it is not re-seeded every call.
		Random rand = new Random();
		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
}