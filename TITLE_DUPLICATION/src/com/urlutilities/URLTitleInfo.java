package com.urlutilities;


public class URLTitleInfo {
	private String url="";
	private String title="";
	private String vendor="";
	private String page_type="";
	private String magasin="";
	private String rayon="";

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

	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getVendor() {
		return vendor;
	}
	public void setVendor(String vendor) {
		this.vendor = vendor;
	}
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
}		
