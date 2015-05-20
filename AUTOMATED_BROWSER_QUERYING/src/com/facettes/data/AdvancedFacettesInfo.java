package com.facettes.data;

public class AdvancedFacettesInfo {
	private int id;
	private String url;
	private String facetteName;
	private String facetteValue;
	private int facetteCount;
	private int marketPlaceFacetteCount;
	private int products_size;
	private boolean is_opened=false;
	private String openedURL;
	private double market_place_quote_part;
	public boolean isIs_opened() {
		return is_opened;
	}
	public void setIs_opened(boolean is_opened) {
		this.is_opened = is_opened;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getFacetteName() {
		return facetteName;
	}
	public void setFacetteName(String facetteName) {
		this.facetteName = facetteName;
	}
	public String getFacetteValue() {
		return facetteValue;
	}
	public void setFacetteValue(String facetteValue) {
		this.facetteValue = facetteValue;
	}
	public int getFacetteCount() {
		return facetteCount;
	}
	public void setFacetteCount(int facetteCount) {
		this.facetteCount = facetteCount;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getProducts_size() {
		return products_size;
	}
	public void setProducts_size(int products_size) {
		this.products_size = products_size;
	}
	public double getMarket_place_quote_part() {
		return market_place_quote_part;
	}
	public void setMarket_place_quote_part(double market_place_quote_part) {
		this.market_place_quote_part = market_place_quote_part;
	}
	public int getMarketPlaceFacetteCount() {
		return marketPlaceFacetteCount;
	}
	public void setMarketPlaceFacetteCount(int marketPlaceFacetteCount) {
		this.marketPlaceFacetteCount = marketPlaceFacetteCount;
	}
	public String getOpenedURL() {
		return openedURL;
	}
	public void setOpenedURL(String openedURL) {
		this.openedURL = openedURL;
	}
}
