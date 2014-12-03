package proxy.testing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cron.analytics.RankInfo;

public class GoogleProxyTesting {

	private static int max_number_of_wait_times = 50;

	private static List<String> user_agents = new ArrayList<String>();
	private static String user_agent_path = "/home/sduprey/My_Data/My_User_Agents/user-agent.txt";
	public static void main(String[] args){
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
		//RankInfo info=ranking_keyword("sportswear", "cdiscount.com");
		RankInfo info=proxy_ranking_keyword("accessoires lumia 625 pas cher", "cdiscount.com");
		System.out.println("Keyword : "+info.getKeyword());
		System.out.println("Keyword : "+info.getPosition());
		System.out.println("Keyword : "+info.getUrl());
	}

	private static void loadUserAgents() throws IOException{
		BufferedReader br = new BufferedReader(new FileReader(user_agent_path));
		String line="";	
		while ((line = br.readLine()) != null) {
			user_agents.add(line);
		}
	}

	public static RankInfo proxy_ranking_keyword(String keyword, String targe_name){
		RankInfo info= new RankInfo();
		keyword=keyword.replace(" ","%20");
		info.setKeyword(keyword);
		// we here fetch up to five paginations
		int nb_depth = 5;
		long startTimeMs = System.currentTimeMillis( );
		org.jsoup.nodes.Document doc;
		int depth=0;
		int nb_results=0;
		int my_rank=50;
		String my_url = "";
		boolean found = false;
		while (depth<nb_depth && !found){
			try{
				// we wait between x and xx seconds
				//Thread.sleep(max_number_of_wait_times*1000);

				System.out.println("Fetching a new page");
				String constructed_url ="https://www.google.fr/search?q="+keyword+"&start="+Integer.toString(depth*10);

				Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 3128));
				URL url = new URL(constructed_url);
				HttpURLConnection connection = (HttpURLConnection)url.openConnection(proxy);
				String randomAgent = randomUserAgent();
				connection.setRequestProperty("User-Agent",randomAgent);
				connection.connect();
				InputStreamReader in = new InputStreamReader((InputStream) connection.getContent());
				BufferedReader buff = new BufferedReader(in);
				StringBuilder builder = new StringBuilder();
				String line;
				do {
					line = buff.readLine();
					builder.append(line);
				} while (line != null);
				String pageString = builder.toString();
				connection.disconnect();		

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
				depth++;
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		long taskTimeMs  = System.currentTimeMillis( ) - startTimeMs;
		//System.out.println(taskTimeMs);
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

	private static String randomUserAgent(){
		int list_size = user_agents.size();
		return user_agents.get(randInt(0,list_size-1));
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
