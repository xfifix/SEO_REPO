package com.marketplace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.crawljax.browser.EmbeddedBrowser.BrowserType;
import com.crawljax.core.CrawlerContext;
import com.crawljax.core.CrawljaxRunner;
import com.crawljax.core.configuration.BrowserConfiguration;
import com.crawljax.core.configuration.CrawljaxConfiguration;
import com.crawljax.core.configuration.CrawljaxConfiguration.CrawljaxConfigurationBuilder;
import com.crawljax.core.plugin.OnUrlLoadPlugin;


public class GoogleNavigatorCrawler {

	private static int min_number_of_wait_times = 50;
	private static int max_number_of_wait_times = 80;
	private static String target_name = "cdiscount.com";

	private static CrawljaxRunner crawljax;
	private static Map<String,RankInfo> crawl_infos = new HashMap<String,RankInfo>();

	public static void main(String[] args) throws InterruptedException{
		// setting the right path for the phantomjs binaries
		Properties props = System.getProperties();
		props.setProperty("phantomjs.binary.path", "/home/sduprey/My_Programs/phantomjs-1.9.8-linux-x86_64/bin/phantomjs");
		props.setProperty("webdriver.chrome.driver", "/usr/bin/google-chrome");

		String[] keywords = {"karcher","sportswear","senseo","cocotte"};

		//first_methodology(keywords);
		second_methodology(keywords);

		// stopping everything afterwhile :
		crawljax.stop();
	}


	public static void first_methodology(String[] keywords){
		for (String keyword : keywords){
			// checking with the former results
			//			RankInfo info = standard_ranking_keyword(keyword);
			//			System.out.println(info.getKeyword() + info.getPosition() + info.getUrl());
			ajax_ranking_crawler(keyword);	
		}

		Iterator<Entry<String, RankInfo>> it = crawl_infos.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, RankInfo> pairs = it.next();
			String keyword =pairs.getKey();
			RankInfo rankinfo = pairs.getValue();
			System.out.println("@@@@@@@@@@@@@@@");
			System.out.println(keyword+"@@"+rankinfo.getPosition()+"@@"+rankinfo.getUrl()+"\n");
			System.out.println("@@@@@@@@@@@@@@@");
		}
	}

	public static void second_methodology(String[] keywords){
		List<RankInfo> results = new ArrayList<RankInfo>();
		List<String> immediate_results = new ArrayList<String>();
		for (String keyword : keywords){
			// checking with the former results
			//			RankInfo info = standard_ranking_keyword(keyword);
			//			System.out.println(info.getKeyword() + info.getPosition() + info.getUrl());
			RankInfo fetched_info = ajax_ranking(keyword);	
			System.out.println("#########");
			immediate_results.add(fetched_info.getKeyword()+"##"+fetched_info.getPosition()+"##"+fetched_info.getUrl()+"\n");
			//System.out.println(fetched_info.getKeyword()+"##"+fetched_info.getPosition()+"##"+fetched_info.getUrl()+"\n");
			System.out.println("########");
			results.add(fetched_info);
		}

		for (RankInfo fetchedInfo : results){
			System.out.println("@@@@@@@@@@@@@@@");
			System.out.println(fetchedInfo.getKeyword()+"@@"+fetchedInfo.getPosition()+"@@"+fetchedInfo.getUrl()+"\n");
			System.out.println("@@@@@@@@@@@@@@@");
		}

		for (String fetchedString : immediate_results){
			System.out.println("#########");
			System.out.println(fetchedString);
			System.out.println("#########");
		}

	}

	public static RankInfo ajax_ranking(String keyword) {
		final RankInfo info= new RankInfo();
		info.setKeyword(keyword);

		// we must set the geolocalisation for phantomJS
		String my_url = "https://www.google.fr/search?q="+keyword+"&num=15&hl=FR&lr=lang_FR";

		// if you choose not to get geolocalized
		//String my_url = "https://www.google.fr/search?q="+keyword+"&num=15";

		// if you choose to get less results 
		//String my_url = "https://www.google.fr/search?q="+keyword;

		//Thread.sleep(randInt(min_number_of_wait_times,max_number_of_wait_times)*1000);

		CrawljaxConfigurationBuilder builder = CrawljaxConfiguration.builderFor(my_url);
		// we will follow some of the results
		builder.crawlRules().click("h3").withAttribute("class","r");
		builder.crawlRules().insertRandomDataInInputForms(true);
		builder.setMaximumDepth(1);
		BrowserConfiguration browserConf = new BrowserConfiguration(BrowserType.PHANTOMJS, 1);
		builder.setBrowserConfig(browserConf);
		// we give ourselves time (fetching additional resellers might be long)
		//builder.crawlRules().waitAfterReloadUrl(waiting_times, TimeUnit.SECONDS);
		builder.addPlugin(new OnUrlLoadPlugin() {
			@Override
			public void onUrlLoad(CrawlerContext context) {
				// TODO Auto-generated method stub
				String content_to_parse = context.getBrowser().getStrippedDom();
				org.jsoup.nodes.Document doc = Jsoup.parse(content_to_parse);
				int nb_results = 0;
				int my_rank = 0;
				String my_url="";
				// we do that if the page is a google page
				Elements serps = doc.select("h3[class=r]");
				for (Element serp : serps) {
					Element link = serp.getElementsByTag("a").first();
					//String refclass = link.attr("data-href");
					String linkref = link.attr("href");
					String lclass = link.attr("class");
					if ((!( lclass.equals("l")))&& (!( lclass.equals("sla")))){				
						nb_results++;
						if (linkref.contains(target_name)){		
							my_rank=nb_results;
							my_url=linkref;
							info.setPosition(my_rank);
							info.setUrl(my_url);
						}			
					}
				}
			}
			@Override
			public String toString() {
				return "Market Place Plugin";
			}
		});


		crawljax = new CrawljaxRunner(builder.build());

		crawljax.call();

		return info;

	}

	public static void ajax_ranking_crawler(String keyword) {		
		final RankInfo info= new RankInfo();
		info.setKeyword(keyword);


		// we must set the geolocalisation for phantomJS
		String my_url = "https://www.google.fr/search?q="+keyword+"&num=15&hl=FR&lr=lang_FR";

		// if you choose not to get geolocalized
		//String my_url = "https://www.google.fr/search?q="+keyword+"&num=15";

		// if you choose to get less results 
		//String my_url = "https://www.google.fr/search?q="+keyword;

		//Thread.sleep(randInt(min_number_of_wait_times,max_number_of_wait_times)*1000);

		CrawljaxConfigurationBuilder builder = CrawljaxConfiguration.builderFor(my_url);
		// we will follow some of the results
		builder.crawlRules().click("h3").withAttribute("class","r");
		builder.crawlRules().insertRandomDataInInputForms(false);
		builder.setMaximumDepth(1);
		BrowserConfiguration browserConf = new BrowserConfiguration(BrowserType.PHANTOMJS, 1);
		builder.setBrowserConfig(browserConf);
		// we give ourselves time (fetching additional resellers might be long)
		//builder.crawlRules().waitAfterReloadUrl(waiting_times, TimeUnit.SECONDS);
		builder.addPlugin(new OnUrlLoadPlugin() {
			@Override
			public void onUrlLoad(CrawlerContext context) {
				// TODO Auto-generated method stub
				String content_to_parse = context.getBrowser().getStrippedDom();
				org.jsoup.nodes.Document doc = Jsoup.parse(content_to_parse);
				int nb_results = 0;
				int my_rank = 0;
				String my_url="";
				// we do that if the page is a google page
				Elements serps = doc.select("h3[class=r]");
				for (Element serp : serps) {
					Element link = serp.getElementsByTag("a").first();
					String refclass = link.attr("data-href");
					String linkref = link.attr("href");
					String lclass = link.attr("class");
					if ((!( lclass.equals("l")))&& (!( lclass.equals("sla")))){				
						nb_results++;
						if (linkref.contains(target_name)){		
							my_rank=nb_results;
							my_url=linkref;
							String keywordpriced = info.getKeyword();
							RankInfo tostore = new RankInfo();
							tostore.setKeyword(keywordpriced);
							tostore.setPosition(my_rank);
							tostore.setUrl(my_url);
							crawl_infos.put(keywordpriced,tostore);
						}			
					}
				}
			}
			@Override
			public String toString() {
				return "Market Place Plugin";
			}
		});


		crawljax = new CrawljaxRunner(builder.build());

		crawljax.call();
	}

	public static String getting_geolocalisation(){
		String location = "Nantes,Pays de la Loire,France";
		int my_length = location.length();
		KeysCodec.populate();
		String codec = KeysCodec.getCodecs().get(my_length);
		// encode data on your side using BASE64
		byte[]  bytesEncoded = Base64.encodeBase64(location.getBytes());
		System.out.println("ecncoded value is " + new String(bytesEncoded ));
		String encoded_location=new String(bytesEncoded);
		// Decode data on other side, by processing encoded data
		byte[] valueDecoded= Base64.decodeBase64(bytesEncoded );
		System.out.println("Decoded value is " + new String(valueDecoded));
		String final_string = KeysCodec.stub+codec+encoded_location;
		return final_string;
	}

	public static RankInfo standard_ranking_keyword(String keyword){
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
				Thread.sleep(randInt(min_number_of_wait_times,max_number_of_wait_times)*1000);
				System.out.println("Fetching a new page");
				doc =  Jsoup.connect(
						"https://www.google.fr/search?q="+keyword+"&start="+Integer.toString(depth*10))
						.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
						.ignoreHttpErrors(true)
						.timeout(0)
						.get();
				Elements serps = doc.select("h3[class=r]");
				for (Element serp : serps) {
					Element link = serp.getElementsByTag("a").first();
					String linkref = link.attr("href");
					if (linkref.startsWith("/url?q=")){
						nb_results++;
						linkref = linkref.substring(7,linkref.indexOf("&"));
					}
					if (linkref.contains(target_name)){
						my_rank=nb_results;
						my_url=linkref;
						found=true;
					}			
					//					System.out.println("Link ref: "+linkref);
					//					System.out.println("Title: "+serp.text());
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