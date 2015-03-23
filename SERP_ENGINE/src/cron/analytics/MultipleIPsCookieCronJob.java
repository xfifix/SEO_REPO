package cron.analytics;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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

public class MultipleIPsCookieCronJob {
	//	private static int min_number_of_wait_times = 40;
	//	private static int max_number_of_wait_times = 60;
	private static int min_number_of_wait_times = 20;
	private static int max_number_of_wait_times = 25;
	private static int bunch_size = 30;
	private static List<String> user_agents = new ArrayList<String>();
	private static String user_agent_path = "/home/sduprey/My_Data/My_User_Agents/user-agent.txt";
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
		try{	
			DataBaseManagement.instantiante_connection();
			int counter = DataBaseManagement.check_alive_run();
			if (counter >= 1){
				System.out.println("Cron : Fatal, another job is running");
				return;
			}
			java.sql.Date current_Date = new java.sql.Date(System.currentTimeMillis());
			long generated_run_id = DataBaseManagement.insertRunById(current_Date);
			String runId = Long.toString(generated_run_id);
			System.out.println("Generated run id for the cron job" + runId);

			// getting our groups
			ResultSet group_resultSet = DataBaseManagement.search_group(args);

			// we here loop over each group 
			while (group_resultSet.next()) {
				String idGroup = group_resultSet.getString("idGroup");
				String name = group_resultSet.getString("name");
				String module = group_resultSet.getString("module");
				String options = group_resultSet.getString("options");
				System.out.println("idGroup: " + idGroup);
				System.out.println("module: " + module);
				System.out.println("name: " + name);
				System.out.println("options: " + options);
				// Getting the targeted site to SERP
				// for the moment we only target cdiscount !
				ResultSet target_resultSet = DataBaseManagement.search_target(idGroup);
				String target = null;
				String idTarget = null;
				while (target_resultSet.next()) {
					target=target_resultSet.getString("name");
					idTarget=target_resultSet.getString("idTarget");
					System.out.println("target: " + target);
					System.out.println("idTarget: " + idTarget);
				}

				// inserting a check row in the check table
				long idCheck =DataBaseManagement.insertCheckById(current_Date,idGroup,Long.toString(generated_run_id));
				String checkId = Long.toString(idCheck);
				System.out.println("Generated check id for the cron job" + checkId);
				// select the keywords to update
				ResultSet keyword_resultSet = DataBaseManagement.search_keywords(idGroup);
				while (keyword_resultSet.next()) {
					String idKeyword = keyword_resultSet.getString("idKeyword");			
					String keyword_name = keyword_resultSet.getString("name");
					//System.out.println("idKeyword: " + idKeyword);
					System.out.println("Launching keyword: " + keyword_name);

					// asynchronous launch
					//GoogleSearchSaveTask beep=new GoogleSearchSaveTask(checkId, idTarget,  idKeyword,keyword_name);

					// Synchronous launch but waiting after
					RankInfo loc_info = proxy_ranking_keyword(keyword_name,target);
					DataBaseManagement.insertKeyword(checkId, idTarget,  idKeyword,loc_info.getPosition(), loc_info.getUrl()); 
				}

				// closing the run by inserting a stopping date !
				DataBaseManagement.close_current_run();
			}	
		} catch (SQLException e){
			e.printStackTrace();
		} finally{
			DataBaseManagement.close();
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

	public static RankInfo proxy_ranking_keyword(String keyword, String targe_name){
		RankInfo info= new RankInfo();
		keyword=keyword.replace(" ","%20");
		info.setKeyword(keyword);
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
			String constructed_url ="https://www.google.fr/search?q="+keyword+"&num="+bunch_size;

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
					if (linkref.contains(targe_name) && !found){
						my_rank=nb_results;
						my_url=linkref;
						found=true;
					}			
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
		info.setPosition(my_rank);
		info.setUrl(my_url);
		if (nb_results == 0){
			System.out.println("Warning captcha");
		}else {
			System.out.println("Number of links read in the pages : "+nb_results);
		}
		System.out.println("My rank : "+my_rank+" for keyword : "+keyword);
		System.out.println("My URL : "+my_url+" for keyword : "+keyword);
		return info;
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
