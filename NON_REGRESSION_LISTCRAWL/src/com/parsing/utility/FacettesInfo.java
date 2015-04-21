package com.parsing.utility;

public class FacettesInfo{
	private int id;
	private String url;
	private String facetteName;
	private String facetteValue;
	private int facetteCount;
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
}