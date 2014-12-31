package crawl4j.arbo.multi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import crawl4j.urlutilities.MultiArboInfo;

public class MultiArboCrawlDataCache {
	// we here keep every thing in RAM memory because the inlinks cache updates each time.
	// we save everything just at the very end of the crawl

	private int totalProcessedPages;
	private long totalLinks;
	private long totalTextSize;
	

	// local cache for each thread which has crawled his own URLs
	private Map<String, MultiArboInfo> crawledContent = new ConcurrentHashMap<String, MultiArboInfo>();
	
	public MultiArboCrawlDataCache() {
	}
	
	public int getTotalProcessedPages() {
		return totalProcessedPages;
	}

	public void setTotalProcessedPages(int totalProcessedPages) {
		this.totalProcessedPages = totalProcessedPages;
	}

	public void incProcessedPages() {
		this.totalProcessedPages++;
	}

	public long getTotalLinks() {
		return totalLinks;
	}

	public void setTotalLinks(long totalLinks) {
		this.totalLinks = totalLinks;
	}

	public long getTotalTextSize() {
		return totalTextSize;
	}

	public void setTotalTextSize(long totalTextSize) {
		this.totalTextSize = totalTextSize;
	}

	public void incTotalLinks(int count) {
		this.totalLinks += count;
	}

	public void incTotalTextSize(int count) {
		this.totalTextSize += count;
	}

	public Map<String, MultiArboInfo> getCrawledContent() {
		return crawledContent;
	}

	public void setCrawledContent(Map<String, MultiArboInfo> crawledContent) {
		this.crawledContent = crawledContent;
	}

}