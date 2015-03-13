package crawl4j.parser.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import crawl4j.continuous.CrawlerUtility;
import crawl4j.urlutilities.URLinfo;
import crawl4j.xpathutility.XPathUtility;

public class YoutubeVideoParsingTest {
	public static void main(String[] args){
		XPathUtility.loadXPATHConf();
		// link present
		//String my_url_to_fetch = "http://www.cdiscount.com/electromenager/lavage-sechage/bosch-wak24160ff-lave-linge-frontal-8-kg/f-11001040406-bos4242002781082.html";		
		// link not present
		String my_url_to_fetch = "http://www.cdiscount.com/electromenager/tous-nos-accessoires/joint-hublot-d-30-30-cm/f-11029-ind3662734065501.html";		
		try{
			// we here don't even use JQuery or XPath
			URL page = new URL(my_url_to_fetch);
			HttpURLConnection conn = (HttpURLConnection) page.openConnection();
			conn.connect();
			InputStreamReader in = new InputStreamReader((InputStream) conn.getContent());
			BufferedReader buff = new BufferedReader(in);
			String line;
			StringBuilder contentbuilder = new StringBuilder();
			do {
				line = buff.readLine();
				contentbuilder.append(line);
			} while (line != null);
			String code_source = contentbuilder.toString();
			System.out.println(code_source);
			int youtube_index = code_source.indexOf("http://www.youtube.com/");
			if (youtube_index >0){
				System.out.println("Youtube link present");
			}else{
				System.out.println("Youtube link not present");
			}
			
			URLinfo info =new URLinfo();		
			// basic magasin, rayon, page type parsing
			// info has to be instantiated
			info=CrawlerUtility.basicParsing(info,my_url_to_fetch);
			info=CrawlerUtility.advancedTextParsing(info,code_source);
			
			System.out.println("Is Youtube referenced ? : "+info.isYoutubeVideoReferenced());
			
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
