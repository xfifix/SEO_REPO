package proxy.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cron.analytics.RankInfo;

public class GoogleProxyTesting {

	private static int max_number_of_wait_times = 120;
	
	public static void main(String[] args) throws InterruptedException, IOException{
		
		RankInfo info=ranking_keyword("sportswear", "cdiscount.com");
		System.out.println("Keyword : "+info.getKeyword());
		System.out.println("Keyword : "+info.getPosition());
		System.out.println("Keyword : "+info.getUrl());
	}

	
	public static RankInfo ranking_keyword(String keyword, String targe_name){
		RankInfo info= new RankInfo();
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
				Thread.sleep(max_number_of_wait_times*1000);
				
				System.out.println("Fetching a new page");
				String constructed_url ="https://www.google.fr/search?q="+keyword+"&start="+Integer.toString(depth*10);
				
				Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 3128));
				URL url = new URL(constructed_url);
				HttpURLConnection connection = (HttpURLConnection)url.openConnection(proxy);
				connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2");
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
					String linkref = link.attr("href");
					if (linkref.startsWith("/url?q=")){
						nb_results++;
						linkref = linkref.substring(7,linkref.indexOf("&"));
					}
					if (linkref.contains(targe_name)){
						my_rank=nb_results;
						my_url=linkref;
						found=true;
					}			
				}
				if (nb_results == 0){
					System.out.println("Warning captcha");
				}
				depth++;
			}
			catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
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
			System.out.println("Number of links : "+nb_results);
		}
		System.out.println("My rank : "+my_rank+" for keyword : "+keyword);
		System.out.println("My URL : "+my_url+" for keyword : "+keyword);
		return info;
	}
	
}
