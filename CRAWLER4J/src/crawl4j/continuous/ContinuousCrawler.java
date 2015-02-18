package crawl4j.continuous;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.Charsets;
import org.apache.http.Header;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

import crawl4j.facettesutility.FacettesInfo;
import crawl4j.facettesutility.FacettesUtility;
import crawl4j.urlutilities.URL_Utilities;
import crawl4j.urlutilities.URLinfo;
import crawl4j.xpathutility.XPathUtility;
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
	public static int bulk_size = 100;	
	private static Pattern bracketPattern = Pattern.compile("\\(.*?\\)");
	private static String category_name = "Catégorie";
	private static String product_name = "Nom du produit";
	private static String brand_name = "Marque";

	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg|jpg" + "|png|tiff|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	CrawlDataManagement myCrawlDataManager;

	public ContinuousCrawler() {
		myCrawlDataManager = new CrawlDataManagement();
	}

	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return !filters.matcher(href).matches() && href.startsWith(ContinuousController.seed);
	}

	@Override
	public void visit(Page page) {		
		String fullUrl = page.getWebURL().getURL();
		// we drop the unnecessary pieces of the URL
		String page_type = URL_Utilities.checkTypeFullUrl(fullUrl);
		String magasin = URL_Utilities.checkMagasinFullUrl(fullUrl);
		String rayon = URL_Utilities.checkRayonFullUrl(fullUrl);

		String url=URL_Utilities.clean(fullUrl);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);
		// the static Map cache is based upon the shortened and cleaned URL
		URLinfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new URLinfo();
		}		
		// filling up url regexp attributes
		info.setPage_type(page_type);
		info.setMagasin(magasin);
		info.setRayon(rayon);
		// filling up url parameters
		info.setUrl(url);
		info.setDepth((int)page.getWebURL().getDepth());

		myCrawlDataManager.incProcessedPages();	

		List<WebURL> links = null;

		// finding the appropriate vendor
		String str_source_code = new String(page.getContentData(), Charsets.UTF_8);
		boolean vendor = is_cdiscount_best_vendor_from_page_source_code(str_source_code);
		info.setCdiscountBestBid(vendor);
		info.setVendor(vendor ? "Cdiscount" : "Market Place");
		boolean youtube = is_youtube_referenced_from_page_source_code(str_source_code);
		info.setYoutubeVideoReferenced(youtube);

		// XPATH parsing
		if (ContinuousController.isXPATHparsed){
			try {
				String[] xpathOutput = XPathUtility.parse_page_code_source(str_source_code,myCrawlDataManager.getXpath_expression());
				info.setXPATH_results(xpathOutput);
				info.setZtd(xpathOutput[8]);
				info.setFooter(xpathOutput[9]);
			} catch (XPathExpressionException | ParserConfigurationException
					| SAXException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Trouble parsing XPATH expressions for URL : "+url);
			}
		}

		// filling up entity to be cached with page source code
		if (ContinuousController.isBlobStored){
			byte[] compressedPageContent = gzip_compress_byte_stream(page.getContentData());
			info.setPage_source_code(compressedPageContent);
		}

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
			info.setText(htmlParseData.getText());
			String html = htmlParseData.getHtml();
			links = htmlParseData.getOutgoingUrls();
			myCrawlDataManager.incTotalLinks(links.size());
			myCrawlDataManager.incTotalTextSize(htmlParseData.getText().length());	

			// we here filter the outlinks we want to keep (they must be internal and they must respect the robot.txt
			Set<String> filtered_links = filter_out_links(links);
			info.setLinks_size(filtered_links.size());
			info.setOut_links(filtered_links.toString());

			Document doc = Jsoup.parse(html);
			Elements titleel = doc.select("title");
			info.setTitle(titleel.text());
			// fetching the H1 element
			Elements h1el = doc.select("h1");
			info.setH1(h1el.text());
			// finding the footer with jQuery
			//			Elements footerel = doc.select("div.ftMention");
			//			info.setFooter(footerel.text());
			// finding the ztd with jQuery
			//			Elements ztdunfolded = doc.select("p.scZtdTxt");
			//			Elements ztdfolded = doc.select("p.scZtdH");
			//			info.setZtd((ztdunfolded==null? "":ztdunfolded.text())+(ztdfolded==null? "":ztdfolded.text()));
			// finding the short description
			Elements short_desc_el = doc.select("p.fpMb");
			info.setShort_desc((short_desc_el==null? "":short_desc_el.text()));
			// finding the number of attributes
			if ("FicheProduit".equals(page_type)){		
				Elements attributes = doc.select(".fpDescTb tr");
				int nb_arguments = 0 ;
				StringBuilder arguments_text = new StringBuilder();
				for (Element tr_element : attributes){
					Elements td_elements = tr_element.select("td");
					if (td_elements.size() == 2){
						nb_arguments++;
						String category = td_elements.get(0).text();
						arguments_text.append(category+"|||");	
						String description = td_elements.get(1).text();                                    
						arguments_text.append(description);		
						arguments_text.append("@@");
						if (category_name.equals(category)){
							info.setCategory(description);
						}
						if (brand_name.equals(category)){
							info.setBrand(description);
						}
						if (product_name.equals(category)){
							info.setProduit(description);
						}
					}
				}
				info.setAtt_number(nb_arguments);
				info.setAtt_desc(arguments_text.toString());
			}
			// parsing the facettes
			if (("ListeProduit".equals(page_type))|| ("ListeProduitFiltre".equals(page_type))){
				// finding the number of attributes
				List<FacettesInfo> list_facettes = new ArrayList<FacettesInfo>();
				FacettesInfo my_info = new FacettesInfo();
				Elements facette_elements = doc.select("div.mvFilter");			
				for (Element facette : facette_elements ){
					//System.out.println(e.toString());
					Elements facette_name = facette.select("div.mvFTit");
					my_info.setFacetteName(facette_name.text());
					Elements facette_values = facette.select("a");
					for (Element facette_value : facette_values){
						String categorie_value = facette_value.text();
						if ("".equals(categorie_value)){
							categorie_value = facette_value.attr("title");
						}
						Matcher matchPattern = bracketPattern.matcher(categorie_value);
						String categorieCount ="";
						while (matchPattern.find()) {		
							categorieCount=matchPattern.group();
						}
						categorie_value=categorie_value.replace(categorieCount,"");
						categorieCount=categorieCount.replace("(", "");
						categorieCount=categorieCount.replace(")", "");	
						//System.out.println(categorie_value);
						try{
							my_info.setFacetteCount(Integer.valueOf(categorieCount));
							//System.out.println(Integer.valueOf(categorieCount));	
						} catch (NumberFormatException e){
							e.printStackTrace();
							System.out.println("Trouble while formatting a facette");
						}
						my_info.setFacetteValue(categorie_value);
						list_facettes.add(my_info);
						my_info = new FacettesInfo();
						my_info.setFacetteName(facette_name.text());
					}		
				}
				String facette_json=FacettesUtility.getFacettesJSONStringToStore(list_facettes);
				info.setFacettes(facette_json);
			}
		}

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
		if (myCrawlDataManager.getTotalProcessedPages() % bulk_size == 0) {
			saveData();
		}
	}

	public boolean is_cdiscount_best_vendor_from_page_source_code(String str_source_code){
		int cdiscount_index = str_source_code.indexOf("<p class='fpSellBy'>Vendu et expédié par <span class='logoCDS'>");
		if (cdiscount_index >0){
			return true;
		}else{
			return false;
		}
	}

	public boolean is_youtube_referenced_from_page_source_code(String str_source_code){
		int youtube_index = str_source_code.indexOf("http://www.youtube.com/");
		if (youtube_index >0){
			return true;
		}else{
			return false;
		}
	}

	// compressing the byte stream
	public byte[] gzip_compress_byte_stream(byte[] dataToCompress ){
		ByteArrayOutputStream byteStream = null;
		try{
			byteStream =
					new ByteArrayOutputStream(dataToCompress.length);
			GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
			try
			{
				zipStream.write(dataToCompress);
			}
			finally
			{
				zipStream.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			if (byteStream != null){
				try {
					byteStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		byte[] compressedData = new byte[0];
		if (byteStream != null){
			compressedData = byteStream.toByteArray();
		}	
		return compressedData;
	}

	public Set<String> filter_out_links(List<WebURL> links){
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