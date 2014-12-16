package crawl4j.parser.testing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.jsoup.Jsoup;
import org.jsoup.select.Elements;

public class ArboParsingTestingClass {

	public static void main(String[] args){

		//String my_url_to_fetch = "http://www.cdiscount.com/electromenager/tous-nos-accessoires/joint-hublot-d-30-30-cm/f-11029-ind3662734065501.html#mpos=2|mp";
		//String my_url_to_fetch = "http://www.cdiscount.com/le-sport/vetements-de-sport/kappa-survetement-armor-homme/f-121020526-3025ej0005.html#mpos=1|cd";
		//String my_url_to_fetch = "http://www.cdiscount.com/animalerie/chiens/lot-de-3-sofas-pour-chien/f-1621004-ifd19945rouge.html";
		//String my_url_to_fetch = "http://www.cdiscount.com/telephonie/r-housse+guidon.html#_his_";
		//String my_url_to_fetch = "http://www.cdiscount.com/maison/tapis/rio-tapis-shaggy-anthracite-30-mm-160x230-cm/f-1172512-r252an160230.html";
		//String my_url_to_fetch = "http://www.cdiscount.com/maison/canape-canapes/v-11701-11701.html";
		String my_url_to_fetch = "http://www.cdiscount.com/informatique/tablettes-tactiles-ebooks/polaroid-platinium-10-1-16go-blanc/f-10798010225-mid4710p112.html";
		// fetching data using Jsoup and jQuery
		org.jsoup.nodes.Document doc;
		try{
			doc =  Jsoup.connect(my_url_to_fetch)
					.userAgent("Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)")
					.ignoreHttpErrors(true)
					.timeout(0)
					.get();
			// number of inlinks
			// presence of breadcrumbs, 
			// price occurences,
			// number of images,
			// presence of order button,
			// content volume,
			// rich snippet avis 
			// we can add the following as last resort :
			// url, pattern in the url 
			// but the classifier should guess without it 
			// size of the in memory cache per thread (200 default value)

			// we don't just parse the html
			// we here look for Bread Crumbs
			Elements breadCrumbs = doc.getElementsByAttributeValue("itemtype", "http://data-vocabulary.org/Breadcrumb");
			System.out.println("Number of breadcrumbs : "+breadCrumbs.size());
			System.out.println("Breadcrumbs text : "  + breadCrumbs.text());


			Elements aggregateRatings = doc.getElementsByAttributeValue("itemprop", "aggregateRating");
			System.out.println("Number of aggregateRatings : "+aggregateRatings.size());
			System.out.println("aggregateRatings text : "  + aggregateRatings.text());
			
			Elements ratingValues = doc.getElementsByAttributeValue("itemprop", "ratingValue");
			System.out.println("Number of ratingValues : "+ratingValues.size());
			System.out.println("ratingValues text : "  + ratingValues.text());
			
			Elements prices = doc.getElementsByAttributeValue("itemprop", "price");
			System.out.println("Number of prices : "+prices.size());
			System.out.println("prices text : "  + prices.text());
			
			Elements priceCurrencies = doc.getElementsByAttributeValue("itemprop", "priceCurrency");
			System.out.println("Number of price currencies : "+priceCurrencies.size());
			System.out.println("price currencies text : "  + priceCurrencies.text());
			
			Elements availabilities = doc.getElementsByAttributeValue("itemprop", "availability");
			System.out.println("Number of availability : "+availabilities.size());
			System.out.println("availability text : "  + availabilities.text());
			
			Elements descriptions = doc.getElementsByAttributeValue("itemprop", "description");
			System.out.println("Number of description : "+descriptions.size());
			System.out.println("description text : "  + descriptions.text());

			Elements names = doc.getElementsByAttributeValue("itemprop", "name");
			System.out.println("Number of names : "+names.size());
			System.out.println("names text : "  + names.text());
			Elements reviews = doc.getElementsByAttributeValue("itemprop", "review");
			System.out.println("Number of reviews : "+reviews.size());
			System.out.println("reviews text : "  + reviews.text());
			Elements reviewRatings = doc.getElementsByAttributeValue("itemprop", "reviewRating");
			System.out.println("Number of reviewRating : "+reviewRatings.size());
			System.out.println("reviewRating text : "  + reviewRatings.text());
			Elements worstRatings = doc.getElementsByAttributeValue("itemprop", "worstRating");
			System.out.println("Number of worstRatings : "+worstRatings.size());
			System.out.println("worstRatings text : "  + worstRatings.text());
			Elements bestRatings = doc.getElementsByAttributeValue("itemprop", "bestRating");
			System.out.println("Number of bestRatings : "+bestRatings.size());
			System.out.println("bestRatings text : "  + bestRatings.text());
			Elements reviewCounts = doc.getElementsByAttributeValue("itemprop", "reviewCount");
			System.out.println("Number of reviewCounts : "+reviewCounts.size());
			System.out.println("reviewCounts text : "  + reviewCounts.text());		
			Elements images = doc.getElementsByAttributeValue("itemprop", "image");
			System.out.println("Number of images : "+images.size());
			System.out.println("images text : "  + images.text());	
            // getting all the javascript code to parse it if some data is contained in it
			URL url = new URL(my_url_to_fetch);
			HttpURLConnection connection = (HttpURLConnection)url.openConnection();
			connection.setRequestProperty("Accept-Charset", "UTF-8"); 
			connection.setRequestProperty("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)");
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
			System.out.println(pageString.contains("http://data-vocabulary.org/Breadcrumb"));
			System.out.println(pageString.contains("itemprop=\"aggregateRating\""));
			System.out.println(pageString.contains("itemprop=\"ratingValue\""));		
			System.out.println(pageString.contains("itemprop=\"price\""));
			System.out.println(pageString.contains("itemprop=\"priceCurrency\""));
			System.out.println(pageString.contains("itemprop=\"availability\""));
			System.out.println(pageString.contains("itemprop=\"description\""));
			System.out.println(pageString.contains("itemprop=\"name\""));
			System.out.println(pageString.contains("itemprop=\"review\""));
			System.out.println(pageString.contains("itemprop=\"reviewRating\""));
			System.out.println(pageString.contains("itemprop=\"worstRating\""));
			System.out.println(pageString.contains("itemprop=\"bestRating\""));
			System.out.println(pageString.contains("itemprop=\"reviewCount\""));
			System.out.println(pageString.contains("itemprop=\"image\""));
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
	}
}
