package crawler4j.benchmarking;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;


public class BenchmarkingCrawler extends WebCrawler {
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" 
			+ "|png|tiff?|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|pdf" 
			+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private int status_code;

	/**
	 * You should implement this function to specify whether
	 * the given url should be crawled or not (based on your
	 * crawling logic).
	 */
	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return !FILTERS.matcher(href).matches() && href.startsWith("http://www.cdiscount.com/");
	}
	/**
	 * This function is called when a page is fetched and ready 
	 * to be processed by your program.
	 */
	@Override
	public void visit(Page page) {          
		String url = page.getWebURL().getURL();
		//System.out.println("URL: " + url);
		//int docid = page.getWebURL().getDocid();
		String domain = page.getWebURL().getDomain();
		String path = page.getWebURL().getPath();
		String subDomain = page.getWebURL().getSubDomain();
		String parentUrl = page.getWebURL().getParentUrl();
		String anchor = page.getWebURL().getAnchor();
		//		System.out.println("Docid: " + docid);
		//		System.out.println("URL: " + url);
		//		System.out.println("Domain: '" + domain + "'");
		//		System.out.println("Sub-domain: '" + subDomain + "'");
		//		System.out.println("Path: '" + path + "'");
		//		System.out.println("Parent page: " + parentUrl);
		//		System.out.println("Anchor text: " + anchor);
		String text ="";
		List<WebURL> links = null;
		String title = "";
		String h1 = "";
		String footer = "";
		String ztd = "";
		String short_desc="";
		String vendor = "";
		String att_desc="";
		int att_number=0;
		String response_headers = "";
		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			text = htmlParseData.getText();
			String html = htmlParseData.getHtml();
			links = htmlParseData.getOutgoingUrls();
			title = htmlParseData.getTitle();
			//			System.out.println("Text length: " + text.length());
			//			System.out.println("Html length: " + html.length());
			//			System.out.println("Number of outgoing links: " + links.size());
			//			System.out.println("Title : " +title);	
			Document doc = Jsoup.parse(html);
			// fetching the H1 element
			Elements h1el = doc.select("h1");
			h1 = h1el.text();
			//	        System.out.println("h1");
			//	        System.out.println(h1el.text());
			// finding the footer
			Elements footerel = doc.select("div.ftMention");
			//	        System.out.println("footer"); 
			//	        System.out.println(footerel.text());  
			footer=footerel.text();
			// finding the ztd
			Elements ztdunfolded = doc.select("p.scZtdTxt");
			Elements ztdfolded = doc.select("p.scZtdH");
			//	        System.out.println("ZTD");
			//	        System.out.println(ztdunfolded==null? "":ztdunfolded.text());
			//	        System.out.println(ztdfolded==null? "":ztdfolded.text());
			ztd = (ztdunfolded==null? "":ztdunfolded.text())+(ztdfolded==null? "":ztdfolded.text());
			// finding the short description
			Elements short_desc_el = doc.select("p.fpMb");
			//	        System.out.println("Short description");
			//	        System.out.println(short_desc_el==null? "":short_desc_el.text());
			short_desc = (short_desc_el==null? "":short_desc_el.text());
			// finding the vendor
			Elements vendorel = doc.select(".logoCDS");
			//	        System.out.println("Vendor");
			//	        System.out.println(vendorel==null? "":vendorel.text());
			vendor = (vendorel==null? "":vendorel.text());
			// finding the number of attributes
			Elements attributes = doc.select(".fpDescTb tbody");
			//	        System.out.println("Attribute description");
			//	        System.out.println(attributes==null? "":attributes.text());
			//	        System.out.println("Number of attributes"+attributes.size());
			att_number = attributes.size();
			att_desc = (attributes==null? "":attributes.text());
		}

		Header[] responseHeaders = page.getFetchResponseHeaders();
		StringBuilder conc = new StringBuilder();
		if (responseHeaders != null) {
			//System.out.println("Response headers:");
			for (Header header : responseHeaders) {
				conc.append( header.getName() + ": " + header.getValue()  + "@");
				//System.out.println("\t" + header.getName() + ": " + header.getValue());
			}
			response_headers = conc.toString();
			//System.out.println("=============");		
		}
		BenchmarkingController.insert_parse_result(url,text,title,links.size(),links.toString(),h1,footer,ztd,short_desc,vendor, att_desc, att_number, this.status_code, response_headers);
	}

	@Override
	protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
		this.setStatus_code(statusCode);
		//		if (statusCode != HttpStatus.SC_OK) {
		//			if (statusCode == HttpStatus.SC_NOT_FOUND) {
		//				System.out.println("Broken link: " + webUrl.getURL() + ", this link was found in page with docid: " + webUrl.getParentDocid());
		//			} else {
		//				System.out.println("Non success status for link: " + webUrl.getURL() + ", status code: " + statusCode + ", description: " + statusDescription);
		//			}
		//		}
	}

	public int getStatus_code() {
		return status_code;
	}
	public void setStatus_code(int status_code) {
		this.status_code = status_code;
	}

}