package crawl4j.corpus.wikipedia.fr;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import crawl4j.vsm.VectorStateSpringRepresentation;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class CorpusCrawler extends WebCrawler {

	// size of the in memory cache per thread (200 default value)
	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg" + "|png|tiff?|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	WikipediaFRCorpusCrawlDataManagement myCrawlDataManager;

	public CorpusCrawler() {
		myCrawlDataManager = new WikipediaFRCorpusCrawlDataManagement();
	}

	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return !filters.matcher(href).matches() && href.startsWith(WikipediaFRCorpusController.crawler_seed);
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		System.out.println(url);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();			
			String html = htmlParseData.getHtml();
			Elements elements = Jsoup.parse(html).body().select("*");
			StringBuilder textBuilder = new StringBuilder();
			for (Element element : elements) {
				textBuilder.append(" "+element.text()+" ");
			}
			VectorStateSpringRepresentation vector_rep = new VectorStateSpringRepresentation(textBuilder.toString());
			Map<String, Integer> word_map = vector_rep.getWordFrequencies();

			Iterator<Map.Entry<String, Integer>> it = word_map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
				String word=pairs.getKey();
				// we here don't want any number
				if (!word.matches(".*\\d+.*")){
					word=word.replace("l'", "");
					word=word.replace("n'", "");
					word=word.replace("d'", "");
					word=word.replace("m'", "");
					System.out.println("Word to add to the corpus : "+word);
					myCrawlDataManager.updateWord(word, url);
				}
			}	
		}
	}

	public Set<String> filter_out_links(List<WebURL> links){
		Set<String> outputSet = new HashSet<String>();
		for (WebURL url_out : links){
			if ((shouldVisit(url_out)) && (getMyController().getRobotstxtServer().allows(url_out))){
				outputSet.add(url_out.getURL());
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
		//		saveData();
		//		try {
		//			myCrawlDataManager.getCon().close();
		//		} catch (SQLException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
	}



	// we just don't care about the status code
	//	@Override
	//	protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
	//		String url = webUrl.getURL();
	//		CORPUSinfo info =myCrawlDataManager.getCrawledContent().get(url);
	//		if (info == null){
	//			info =new CORPUSinfo();
	//		}	
	//		//info.setStatus_code(statusCode);
	//		myCrawlDataManager.getCrawledContent().put(url,info);
	//		//		if (statusCode != HttpStatus.SC_OK) {
	//		//			if (statusCode == HttpStatus.SC_NOT_FOUND) {
	//		//				System.out.println("Broken link: " + webUrl.getURL() + ", this link was found in page with docid: " + webUrl.getParentDocid());
	//		//			} else {
	//		//				System.out.println("Non success status for link: " + webUrl.getURL() + ", status code: " + statusCode + ", description: " + statusDescription);
	//		//			}
	//		//		}
	//	}

	public void saveData(){
		int id = getMyId();
		// This is just an example. Therefore I print on screen. You may
		// probably want to write in a text file.
		System.out.println("Crawler " + id + "> Processed Pages: " + myCrawlDataManager.getTotalProcessedPages());
		System.out.println("Crawler " + id + "> Total Text Size: " + myCrawlDataManager.getTotalTextSize());
	//	myCrawlDataManager.updateData();	
	}

}