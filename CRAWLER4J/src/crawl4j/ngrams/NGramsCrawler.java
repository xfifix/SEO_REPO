package crawl4j.ngrams;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

import crawl4j.vsm.NGramsAnalyzer;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class NGramsCrawler extends WebCrawler {

	// size of the in memory cache per thread (200 default value)
	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg|jpg" + "|png|tiff?|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	NGramsCrawlDataManagement myCrawlDataManager;

	public NGramsCrawler() {
		myCrawlDataManager = new NGramsCrawlDataManagement();
	}

	@Override
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return ((!filters.matcher(href).matches()) && NGramsController.isAllowedSiteforMultipleCrawl(href));
	}

	@Override
	public void visit(Page page) {
		String url = page.getWebURL().getURL();
		System.out.println(Thread.currentThread()+": Computing n-grams URL : "+url);
		if (page.getParseData() instanceof HtmlParseData) {
			HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();	
			String text_to_parse = htmlParseData.getText();
			// n-grams analyzing 
			try {
				Analyzer ngram_analyzer = new NGramsAnalyzer();
				TokenStream stream = ngram_analyzer.tokenStream("content", new StringReader(text_to_parse));
				Token token = new Token();
				while ((token = stream.next(token)) != null){
					String ngram = token.term();
					System.out.println("Updating n-gram : "+ngram + " for URL : "+url);
					myCrawlDataManager.updateNGrams(ngram, url);
				}
			} catch (IOException e) {
				System.out.println("Trouble parsing n-grams for URLs : "+url);
				e.printStackTrace();
			}
		}
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