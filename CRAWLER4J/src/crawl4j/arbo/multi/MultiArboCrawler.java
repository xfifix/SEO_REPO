package crawl4j.arbo.multi;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import crawl4j.urlutilities.ArboInfo;
import crawl4j.urlutilities.URL_Utilities;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class MultiArboCrawler extends WebCrawler {

//	number of breadcrumbs int [0,+infinity[
//	number of aggregated rating boolean int [0, +infinity[
//	number of product rating values int [0, +infinity[
//	number of product prices int [0, +infinity[
//	number of product availabilities int [0, +infinity[
//	number of reviews int [0, +infinity[
//	number of reviews ratings int [0, +infinity[
//	number of review counts 
//	number of images int [0, +infinity[
//	number of price dans le texte
//	number of symbole euro dans le code
//	editorial text length	
//	vue épurée retranché du noyau commun aux pages de profondeur 0 et 1 (retirer le négatif)
//  nombre d'images ajoutées quand on a retranché la charte graphique (noyau depth 1 2 )
	
	// size of the in memory cache per thread (200 default value)

	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg" + "|png|tiff|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	MultiArboCrawlDataManagement myCrawlDataManager;

	public MultiArboCrawler() {
		myCrawlDataManager = new MultiArboCrawlDataManagement();
	}

	// we don't visit media URLs and we keep inside Cdiscount
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return !filters.matcher(href).matches() && href.startsWith("http://www.cdiscount.com/");
	}

	@Override
	public void visit(Page page) {
		// we here parse the html to fill up the cache with the following information
		String url = page.getWebURL().getURL();
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);
		ArboInfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new ArboInfo();
		}		
		info.setUrl(url);
		info.setDepth((int)page.getWebURL().getDepth());
		myCrawlDataManager.incProcessedPages();	
		List<WebURL> links = null;
		if (page.getParseData() instanceof HtmlParseData) {
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
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			info.setText(htmlParseData.getText());
			String html = htmlParseData.getHtml();
			links = htmlParseData.getOutgoingUrls();
			myCrawlDataManager.incTotalLinks(links.size());
			myCrawlDataManager.incTotalTextSize(htmlParseData.getText().length());	
			// we here filter the outlinks we want to keep (they must be internal and they must respect the robot.txt
			Set<String> filtered_links = filter_out_links(links);
			// managing the count of the inlinks here
			updateInLinksCache(filtered_links, url);

			info.setLinks_size(filtered_links.size());
			// parsing the document to get the predictor of our model			
			Document doc = Jsoup.parse(html);	
			Elements breadCrumbs = doc.getElementsByAttributeValue("itemtype", "http://data-vocabulary.org/Breadcrumb");
			info.setNb_breadcrumbs(breadCrumbs.size());
			Elements aggregateRatings = doc.getElementsByAttributeValue("itemprop", "aggregateRating");
			info.setNb_aggregated_rating(aggregateRatings.size());
			Elements ratingValues = doc.getElementsByAttributeValue("itemprop", "ratingValue");
			info.setNb_ratings(ratingValues.size());
			Elements prices = doc.getElementsByAttributeValue("itemprop", "price");
			info.setNb_prices(prices.size());
			Elements availabilities = doc.getElementsByAttributeValue("itemprop", "availability");
			info.setNb_availabilities(availabilities.size());	
			Elements reviews = doc.getElementsByAttributeValue("itemprop", "review");
			info.setNb_reviews(reviews.size());
			Elements reviewCounts = doc.getElementsByAttributeValue("itemprop", "reviewCount");
			info.setNb_reviews_count(reviewCounts.size());
			Elements images = doc.getElementsByAttributeValue("itemprop", "image");
			info.setNb_images(images.size());
			// end of predictor parsing
		}
		myCrawlDataManager.getCrawledContent().put(url,info);
	}

	public Set<String> filter_out_links(List<WebURL> links){
		Set<String> outputSet = new HashSet<String>();
		for (WebURL url_out : links){
			if ((shouldVisit(url_out)) && (getMyController().getRobotstxtServer().allows(url_out))){
				String final_link = URL_Utilities.drop_parameters(url_out.getURL());
				outputSet.add(final_link);
			}
		}
		return outputSet;
	}

	public void updateInLinksCache(Set<String> outputSet, String sourceURL){
		for (String targetURL : outputSet){
			Set<String> inLinks = getMyLocalData().getInlinks_cache().get(targetURL);
			if (inLinks == null){
				inLinks= new HashSet<String>();
			}
			inLinks.add(sourceURL);
			getMyLocalData().getInlinks_cache().put(targetURL,inLinks);
		}
	}

	// This function is called by controller to get the local data of this
	// crawler when job is finished
	@Override
	public MultiArboCrawlDataManagement getMyLocalData() {
		return myCrawlDataManager;
	}

	@Override
	protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
		String url = webUrl.getURL();
		ArboInfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new ArboInfo();
		}	
		info.setStatus_code(statusCode);
		myCrawlDataManager.getCrawledContent().put(url,info);
	}

}