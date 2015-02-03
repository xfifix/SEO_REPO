package crawl4j.parser.testing;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class YoutubeVideoParsingTest {
	public static void main(String[] args){
		String my_url_to_fetch = "http://www.cdiscount.com/electromenager/lavage-sechage/bosch-wak24160ff-lave-linge-frontal-8-kg/f-11001040406-bos4242002781082.html";
		// fetching data using jQuery
		org.jsoup.nodes.Document doc;
		try{
			// we wait between 30 and 70 seconds
			doc =  Jsoup.connect(my_url_to_fetch)
					.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
					.ignoreHttpErrors(true)
					.timeout(0)
					.get();
			System.out.println(doc.toString());
			Elements resellers = doc.select(".fpSellBy");
			StringBuilder resellerBuilder = new StringBuilder();
			for (Element reseller : resellers){
				if(reseller.getElementsByTag("a") != null){
					resellerBuilder.append(reseller.getElementsByTag("a").text());
				}
			}
			String vendor = resellerBuilder.toString();
			System.out.println(vendor);
		}
		catch (IOException e) {
			e.printStackTrace();
		} 

	}

}
