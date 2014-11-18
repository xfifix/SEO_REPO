package crawl4j.urlutilities;


public class URLinfo {
	private String url="";
	private String text="";
	private String title="";
	private int links_size=0;
	private String out_links="";
	private String h1="";
	private String footer="";
	private String ztd="";
	private String short_desc="";
	private String vendor="";
	private String att_desc="";
	private int att_number=0;
	private int status_code=0;
	private String response_headers="";;
	private int depth=0;
	private String page_type="";
	private String magasin="";
	private String rayon="";
	private String produit="";
	
	public String getPage_type() {
		return page_type;
	}
	public void setPage_type(String page_type) {
		this.page_type = page_type;
	}
	public String getMagasin() {
		return magasin;
	}
	public void setMagasin(String magasin) {
		this.magasin = magasin;
	}
	public String getRayon() {
		return rayon;
	}
	public void setRayon(String rayon) {
		this.rayon = rayon;
	}
	public String getProduit() {
		return produit;
	}
	public void setProduit(String produit) {
		this.produit = produit;
	}
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
	public String getOut_links() {
		return out_links;
	}
	public void setOut_links(String out_links) {
		this.out_links = out_links;
	}
	public String getH1() {
		return h1;
	}
	public void setH1(String h1) {
		this.h1 = h1;
	}
	public String getFooter() {
		return footer;
	}
	public void setFooter(String footer) {
		this.footer = footer;
	}
	public String getZtd() {
		return ztd;
	}
	public void setZtd(String ztd) {
		this.ztd = ztd;
	}
	public String getShort_desc() {
		return short_desc;
	}
	public void setShort_desc(String short_desc) {
		this.short_desc = short_desc;
	}
	public String getVendor() {
		return vendor;
	}
	public void setVendor(String vendor) {
		this.vendor = vendor;
	}
	public String getAtt_desc() {
		return att_desc;
	}
	public void setAtt_desc(String att_desc) {
		this.att_desc = att_desc;
	}
	public int getAtt_number() {
		return att_number;
	}
	public void setAtt_number(int att_number) {
		this.att_number = att_number;
	}
	public int getStatus_code() {
		return status_code;
	}
	public void setStatus_code(int status_code) {
		this.status_code = status_code;
	}
	public String getResponse_headers() {
		return response_headers;
	}
	public void setResponse_headers(String response_headers) {
		this.response_headers = response_headers;
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
		this.setPage_type(URL_Utilities.checkType(url));
		this.setMagasin(URL_Utilities.checkMagasin(url));
		this.setProduit(URL_Utilities.checkProduit(url));
		this.setRayon(URL_Utilities.checkRayon(url));
	}
}		
