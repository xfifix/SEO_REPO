package crawl4j.corpus;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import crawl4j.vsm.CorpusCache;
import crawl4j.vsm.VectorStateSpringRepresentation;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class CorpusCrawler extends WebCrawler {

	// size of the in memory cache per thread (200 default value)
	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg|jpg" + "|png|tiff?|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	CorpusCrawlDataManagement myCrawlDataManager;

	public CorpusCrawler() {
		myCrawlDataManager = new CorpusCrawlDataManagement();
	}

	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return ((!filters.matcher(href).matches()) && CorpusController.isAllowedSiteforMultipleCrawl(href));
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		System.out.println(url);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);

		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();			
			String semantic_text = CorpusCache.preprocessSemanticText(htmlParseData.getText());
			VectorStateSpringRepresentation vector_rep = new VectorStateSpringRepresentation(semantic_text);
			Map<String, Integer> word_map = vector_rep.getWordFrequencies();
			Iterator<Map.Entry<String, Integer>> it = word_map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
				String word=pairs.getKey();
				// we here don't want any number
				if (!word.matches(".*\\d+.*")){
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

	public void saveData(){
		int id = getMyId();
		// This is just an example. Therefore I print on screen. You may
		// probably want to write in a text file.
		System.out.println("Crawler " + id + "> Processed Pages: " + myCrawlDataManager.getTotalProcessedPages());
		System.out.println("Crawler " + id + "> Total Text Size: " + myCrawlDataManager.getTotalTextSize());
		//	myCrawlDataManager.updateData();	
	}

}