package crawl4j.continuous;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.http.Header;

import crawl4j.urlutilities.URL_Utilities;
import crawl4j.urlutilities.URLinfo;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class ContinuousCrawler extends WebCrawler {

	// size of the in memory cache per thread (200 default value)
	// depending wether or not you store the whole page source code
	// this cache size is important
	// if you don't save blob and store the page source code you can go up to 200
	// blob cache size

	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg|jpg" + "|png|tiff|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	CrawlDataManagement myCrawlDataManager;

	public ContinuousCrawler() {
		myCrawlDataManager = new CrawlDataManagement();
	}

	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return !filters.matcher(href).matches() && href.startsWith(ContinuousController.crawl_restrainer_start_with);
	}

	@Override
	public void visit(Page page) {		
		String fullUrl = page.getWebURL().getURL();
		// we drop the unnecessary pieces of the URL
		String url=URL_Utilities.clean(fullUrl);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);
		// the static Map cache is based upon the shortened and cleaned URL
		URLinfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new URLinfo();
		}			
		// filling up url parameters
		info.setUrl(url);	
		info.setDepth((int)page.getWebURL().getDepth());

		// basic magasin, rayon, page type parsing
		info=CrawlerUtility.basicParsing(info,fullUrl);

		myCrawlDataManager.incProcessedPages();	
		// finding the appropriate vendor
		// advanced parsing
		List<WebURL> links = null;

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			// the html string is here the page source code
			String html = htmlParseData.getHtml();
			// Outgoing links from CRAWL4J
			links = htmlParseData.getOutgoingUrls();
			myCrawlDataManager.incTotalLinks(links.size());
			myCrawlDataManager.incTotalTextSize(htmlParseData.getText().length());	
			// we here filter the outlinks we want to keep (they must be internal and they must respect the robot.txt
			Set<String> filtered_links = continuous_crawl_filter_out_links(links);
			info.setLinks_size(filtered_links.size());
			info.setOut_links(CrawlerUtility.linksSettoJSON(filtered_links));
			// usual common html text parsing
			info=CrawlerUtility.advancedTextParsing(info,html);
		}

		// page header fetching from crawl4j
		Header[] responseHeaders = page.getFetchResponseHeaders();
		StringBuilder conc = new StringBuilder();
		if (responseHeaders != null) {
			for (Header header : responseHeaders) {
				conc.append( header.getName() + ": " + header.getValue()  + "@");
			}
			info.setResponse_headers(conc.toString());	
		}
		myCrawlDataManager.getCrawledContent().put(url,info);
		// We save this crawler data after processing every bulk_sizes pages
		if (myCrawlDataManager.getTotalProcessedPages() % CrawlDataManagement.bulk_size == 0) {
			saveData();
		}
	}

	public Set<String> continuous_crawl_filter_out_links(List<WebURL> links){
		Set<String> outputSet = new HashSet<String>();
		for (WebURL url_out : links){
			// the should visit is done upon the full URL
			if ((shouldVisit(url_out)) && (getMyController().getRobotstxtServer().allows(url_out))){
				String final_link = URL_Utilities.clean(url_out.getURL());
				outputSet.add(final_link);
			}
		}
		return outputSet;
	}

	// This function is called by controller to get the local data of this
	// crawler when job is finished
	@Override
	public Object getMyLocalData() {
		return myCrawlDataManager;
	}

	// This function is called by controller before finishing the job.
	// You can put whatever stuff you need here.
	@Override
	public void onBeforeExit() {
		saveData();
		try {
			myCrawlDataManager.getCon().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
		String url = webUrl.getURL();
		url = URL_Utilities.clean(url);
		URLinfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new URLinfo();
		}	
		// very important to override the previously good 200 URLs when changed to 301
		info.setUrl(url);
		info.setStatus_code(statusCode);
		myCrawlDataManager.getCrawledContent().put(url,info);
		//		if (statusCode != HttpStatus.SC_OK) {
		//			if (statusCode == HttpStatus.SC_NOT_FOUND) {
		//				System.out.println("Broken link: " + webUrl.getURL() + ", this link was found in page with docid: " + webUrl.getParentDocid());
		//			} else {
		//				System.out.println("Non success status for link: " + webUrl.getURL() + ", status code: " + statusCode + ", description: " + statusDescription);
		//			}
		//		}
	}

	public void saveData(){
		int id = getMyId();
		// This is just an example. Therefore I print on screen. You may
		// probably want to write in a text file.
		System.out.println("Crawler " + id + "> Processed Pages: " + myCrawlDataManager.getTotalProcessedPages());
		System.out.println("Crawler " + id + "> Total Links Found: " + myCrawlDataManager.getTotalLinks());
		System.out.println("Crawler " + id + "> Total Text Size: " + myCrawlDataManager.getTotalTextSize());
		myCrawlDataManager.updateData();	
	}
}