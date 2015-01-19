package crawl4j.arbo.semantic;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import crawl4j.urlutilities.MultiSeedSemanticArboInfo;
import crawl4j.urlutilities.URL_Utilities;
import crawl4j.vsm.CorpusCache;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class SemanticArboCrawler extends WebCrawler {
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
	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpeg|jpg" + "|png|tiff|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|ico|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");

	SemanticArboCrawlDataCache myCrawlDataManager;

	public SemanticArboCrawler() {
		myCrawlDataManager = new SemanticArboCrawlDataCache();
	}

	// we don't visit media URLs and we keep inside Cdiscount
	// we don't visit media URLs and we keep inside Cdiscount
	public boolean shouldVisit(WebURL url) {
		String href = url.getURL().toLowerCase();
		return (!filters.matcher(href).matches() && SemanticArboController.isAllowedSiteforMultipleCrawl(href));
	}

	@Override
	public void visit(Page page) {
		// we here parse the html to fill up the cache with the following information
		String fullUrl = page.getWebURL().getURL();
		String url = URL_Utilities.drop_parameters(fullUrl);
		System.out.println(Thread.currentThread()+": Visiting URL : "+url);
		MultiSeedSemanticArboInfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new MultiSeedSemanticArboInfo();
		}		
		info.setUrl(url);
		info.setDepth((int)page.getWebURL().getDepth());

		String page_type = URL_Utilities.checkTypeFullUrl(url);
		info.setPage_type(page_type);

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
			String title = htmlParseData.getTitle();
			info.setTitle(title);

			Map<String, Integer> title_tfMap = CorpusCache.computePageTFVector(title);
			String title_semantic = CorpusCache.formatTFMap(title_tfMap);
			info.setTitle_semantic(title_semantic);
			info.setText(htmlParseData.getText());
			String html = htmlParseData.getHtml();
			links = htmlParseData.getOutgoingUrls();
			myCrawlDataManager.incTotalLinks(links.size());
			myCrawlDataManager.incTotalTextSize(htmlParseData.getText().length());	

			Set<LinkInfo> filtered_links = filter_out_links(links);
			info.setOutgoingLinks(filtered_links);
			info.setLinks_size(filtered_links.size());
			// parsing the document to get the predictor of our model			
			Document doc = Jsoup.parse(html);	
			Elements h1el = doc.select("h1");
			info.setH1(h1el.text());
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
			// end of rich snippet predictor parsing
			String text_to_parse = doc.text();
			// beginning of url built parameters
			int nb_search_in_url = StringUtils.countMatches(text_to_parse, "search")+StringUtils.countMatches(text_to_parse, "recherche");
			info.setNb_search_in_url(nb_search_in_url);
			// beginning of text built parameters
			int nb_add  = StringUtils.countMatches(text_to_parse, "ajout")+StringUtils.countMatches(text_to_parse, "ajouter")+StringUtils.countMatches(text_to_parse, "Ajout")+StringUtils.countMatches(text_to_parse, "Ajouter");
			info.setNb_add_in_text(nb_add);
			int nb_filter  = StringUtils.countMatches(text_to_parse, "filtre")+StringUtils.countMatches(text_to_parse, "facette");
			info.setNb_filter_in_text(nb_filter);
			int nb_search  = StringUtils.countMatches(text_to_parse, "Ma recherche")+StringUtils.countMatches(text_to_parse, "Votre recherche")+StringUtils.countMatches(text_to_parse, "résultats pour")+StringUtils.countMatches(text_to_parse, "résultats associés");
			info.setNb_search_in_text(nb_search);

			int nb_guide_achat = StringUtils.countMatches(text_to_parse, "search");
			info.setNb_guide_achat_in_text(nb_guide_achat);
			int nb_product_info = StringUtils.countMatches(text_to_parse, "caractéristique")+StringUtils.countMatches(text_to_parse, "Caractéristique")+StringUtils.countMatches(text_to_parse, "descriptif")+StringUtils.countMatches(text_to_parse, "Descriptif")+StringUtils.countMatches(text_to_parse, "information")+StringUtils.countMatches(text_to_parse, "Information");	    		
			info.setNb_product_info_in_text(nb_product_info);
			int nb_livraison = StringUtils.countMatches(text_to_parse, "livraison") +StringUtils.countMatches(text_to_parse, "frais de port")+StringUtils.countMatches(text_to_parse, "Frais de port") ;
			info.setNb_livraison_in_text(nb_livraison);
			int nb_garanties = StringUtils.countMatches(text_to_parse, "garantie")+StringUtils.countMatches(text_to_parse, "Garantie")+StringUtils.countMatches(text_to_parse, "Assurance")+StringUtils.countMatches(text_to_parse, "assurance");    
			info.setNb_garanties_in_text(nb_garanties);
			int nb_produits_similaires = StringUtils.countMatches(text_to_parse, "Produits Similaires")+StringUtils.countMatches(text_to_parse, "produits similaires")+StringUtils.countMatches(text_to_parse, "Meilleures Ventes")+StringUtils.countMatches(text_to_parse, "meilleures ventes")+StringUtils.countMatches(text_to_parse, "Meilleures ventes")+StringUtils.countMatches(text_to_parse, "Nouveautés")+StringUtils.countMatches(text_to_parse, "nouveautés");
			info.setNb_produits_similaires_in_text(nb_produits_similaires);
			Elements imageElements = doc.getElementsByTag("img");
			int nb_total_images = imageElements.size();
			info.setNb_total_images(nb_total_images);

			// average height
			Elements heights  = doc.getElementsByAttribute("height");
			double height_result = 0;
			double height_count = 0;
			double width_result = 0;
			double width_count = 0;

			for (Element height : heights){
				String heightstring = height.attr("height");
				if (!(heightstring.contains("%") || heightstring.contains("px"))){
					double heightvalue = 0;
					try{
						heightvalue = Double.valueOf(heightstring);
					} catch (NumberFormatException e){
						System.out.println("Trouble parsing width, height average size");
						e.printStackTrace();
					}
					height_count++;
					height_result=height_result+heightvalue;
				}
			}
			//average width
			Elements widths = doc.getElementsByAttribute("width");
			for (Element width : widths){
				String widthstring = width.attr("width");
				if (!(widthstring.contains("%") || widthstring.contains("px"))){
					double widthvalue = 0;
					try{
						widthvalue = Double.valueOf(widthstring);
					} catch (NumberFormatException e){
						System.out.println("Trouble parsing width, height average size");
						e.printStackTrace();
					}
					width_count++;
					width_result=width_result+widthvalue;
				}
			}

			info.setHeight_average(height_result/height_count);
			info.setWidth_average(width_result/width_count);	
			// extracting the semantic most relevant words with TF/IDF indicators
			// this step needs to put the semantics corpus frequency in cache at the crawling set up				
			Map<String, Double> tfIdfMap = CorpusCache.computePageTFIDFVector(text_to_parse);
			String semantics_hit_to_store = CorpusCache.formatTFIDFMapBestHits(tfIdfMap);
			info.setSemantics_hit(semantics_hit_to_store);
		}
		myCrawlDataManager.getCrawledContent().put(url,info);
	}

	public Set<LinkInfo> filter_out_links(List<WebURL> links){
		Set<LinkInfo> outputSet = new HashSet<LinkInfo>();
		for (WebURL url_out : links){
			if ((shouldVisit(url_out)) && (getMyController().getRobotstxtServer().allows(url_out))){
				LinkInfo info = new LinkInfo();
				info.setAnchor(url_out.getAnchor());
				String final_link = URL_Utilities.drop_parameters(url_out.getURL());
				info.setUrl(final_link);
				outputSet.add(info);
			}
		}
		return outputSet;
	}

	// This function is called by controller to get the local data of this
	// crawler when job is finished
	@Override
	public SemanticArboCrawlDataCache getMyLocalData() {
		return myCrawlDataManager;
	}

	@Override
	protected void handlePageStatusCode(WebURL webUrl, int statusCode, String statusDescription) {
		String fullUrl = webUrl.getURL();
		String url = URL_Utilities.drop_parameters(fullUrl);

		MultiSeedSemanticArboInfo info =myCrawlDataManager.getCrawledContent().get(url);
		if (info == null){
			info =new MultiSeedSemanticArboInfo();
		}	
		info.setStatus_code(statusCode);
		myCrawlDataManager.getCrawledContent().put(url,info);
	}

}