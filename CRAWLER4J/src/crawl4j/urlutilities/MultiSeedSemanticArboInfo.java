package crawl4j.urlutilities;

import java.util.Set;

public class MultiSeedSemanticArboInfo {
	// text built predictors
	private int nb_add_in_text;
	private int nb_filter_in_text;
	private int nb_search_in_text;
	private int nb_guide_achat_in_text;
	private int nb_product_info_in_text;
	private int nb_livraison_in_text;
	private int nb_garanties_in_text;
	private int nb_produits_similaires_in_text;
	private int nb_total_images;
	private double height_average;
	private double width_average;
    // url built predictors
	private int nb_search_in_url;
    // standard predictors
	private String url="";
	private String text="";
	private String title="";
	private String h1="";
	private String short_desc="";
	private String page_type="Unknown";
	private int status_code=0;
	private int depth=0;
	private int links_size=0;
	// rich snippet predictors
	private int nb_breadcrumbs=0;
	private int nb_ratings=0;
	private int nb_aggregated_rating=0;
	private int nb_prices=0;
	private int nb_availabilities=0;
	private int nb_reviews=0;
    private Set<String> outgoingLinks;
    private String semantics_hit;
    private String title_semantic;
    
	public int getNb_total_images() {
		return nb_total_images;
	}
	public void setNb_total_images(int nb_total_images) {
		this.nb_total_images = nb_total_images;
	}
	public double getHeight_average() {
		return height_average;
	}
	public void setHeight_average(double height_average) {
		this.height_average = height_average;
	}
	public double getWidth_average() {
		return width_average;
	}
	public void setWidth_average(double width_average) {
		this.width_average = width_average;
	}
	public int getNb_breadcrumbs() {
		return nb_breadcrumbs;
	}
	public void setNb_breadcrumbs(int nb_breadcrumbs) {
		this.nb_breadcrumbs = nb_breadcrumbs;
	}
	public int getNb_ratings() {
		return nb_ratings;
	}
	public void setNb_ratings(int nb_ratings) {
		this.nb_ratings = nb_ratings;
	}
	public int getNb_aggregated_rating() {
		return nb_aggregated_rating;
	}
	public void setNb_aggregated_rating(int nb_aggregated_rating) {
		this.nb_aggregated_rating = nb_aggregated_rating;
	}
	public int getNb_prices() {
		return nb_prices;
	}
	public void setNb_prices(int nb_prices) {
		this.nb_prices = nb_prices;
	}
	public int getNb_availabilities() {
		return nb_availabilities;
	}
	public void setNb_availabilities(int nb_availabilities) {
		this.nb_availabilities = nb_availabilities;
	}
	public int getNb_reviews() {
		return nb_reviews;
	}
	public void setNb_reviews(int nb_reviews) {
		this.nb_reviews = nb_reviews;
	}
	public int getNb_reviews_count() {
		return nb_reviews_count;
	}
	public void setNb_reviews_count(int nb_reviews_count) {
		this.nb_reviews_count = nb_reviews_count;
	}
	public int getNb_images() {
		return nb_images;
	}
	public void setNb_images(int nb_images) {
		this.nb_images = nb_images;
	}
	private int nb_reviews_count=0;
	private int nb_images=0;

	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public int getLinks_size() {
		return links_size;
	}
	public void setLinks_size(int links_size) {
		this.links_size = links_size;
	}
	public String getH1() {
		return h1;
	}
	public void setH1(String h1) {
		this.h1 = h1;
	}
	public String getShort_desc() {
		return short_desc;
	}
	public void setShort_desc(String short_desc) {
		this.short_desc = short_desc;
	}
	public int getStatus_code() {
		return status_code;
	}
	public void setStatus_code(int status_code) {
		this.status_code = status_code;
	}
	public int getDepth() {
		return depth;
	}
	public void setDepth(int depth) {
		this.depth = depth;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getPage_type() {
		return page_type;
	}
	public void setPage_type(String page_type) {
		this.page_type = page_type;
	}
	public Set<String> getOutgoingLinks() {
		return outgoingLinks;
	}
	public void setOutgoingLinks(Set<String> outgoingLinks) {
		this.outgoingLinks = outgoingLinks;
	}
	public int getNb_add_in_text() {
		return nb_add_in_text;
	}
	public void setNb_add_in_text(int nb_add_in_text) {
		this.nb_add_in_text = nb_add_in_text;
	}
	public int getNb_filter_in_text() {
		return nb_filter_in_text;
	}
	public void setNb_filter_in_text(int nb_filter_in_text) {
		this.nb_filter_in_text = nb_filter_in_text;
	}
	public int getNb_search_in_text() {
		return nb_search_in_text;
	}
	public void setNb_search_in_text(int nb_search_in_text) {
		this.nb_search_in_text = nb_search_in_text;
	}
	public int getNb_search_in_url() {
		return nb_search_in_url;
	}
	public void setNb_search_in_url(int nb_search_in_url) {
		this.nb_search_in_url = nb_search_in_url;
	}
	public int getNb_guide_achat_in_text() {
		return nb_guide_achat_in_text;
	}
	public void setNb_guide_achat_in_text(int nb_guide_achat_in_text) {
		this.nb_guide_achat_in_text = nb_guide_achat_in_text;
	}
	public int getNb_product_info_in_text() {
		return nb_product_info_in_text;
	}
	public void setNb_product_info_in_text(int nb_product_info_in_text) {
		this.nb_product_info_in_text = nb_product_info_in_text;
	}
	public int getNb_livraison_in_text() {
		return nb_livraison_in_text;
	}
	public void setNb_livraison_in_text(int nb_livraison_in_text) {
		this.nb_livraison_in_text = nb_livraison_in_text;
	}
	public int getNb_garanties_in_text() {
		return nb_garanties_in_text;
	}
	public void setNb_garanties_in_text(int nb_garanties_in_text) {
		this.nb_garanties_in_text = nb_garanties_in_text;
	}
	public int getNb_produits_similaires_in_text() {
		return nb_produits_similaires_in_text;
	}
	public void setNb_produits_similaires_in_text(int nb_produits_similaires_in_text) {
		this.nb_produits_similaires_in_text = nb_produits_similaires_in_text;
	}
	public String getSemantics_hit() {
		return semantics_hit;
	}
	public void setSemantics_hit(String semantics_hit) {
		this.semantics_hit = semantics_hit;
	}
	public String getTitle_semantic() {
		return title_semantic;
	}
	public void setTitle_semantic(String title_semantic) {
		this.title_semantic = title_semantic;
	}
}
