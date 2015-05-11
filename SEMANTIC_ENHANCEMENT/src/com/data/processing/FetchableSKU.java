package com.data.processing;

public class FetchableSKU {
	private String SKU;
	private String jsonAttributs;
	private boolean to_fetch;
	public boolean isTo_fetch() {
		return to_fetch;
	}
	public void setTo_fetch(boolean to_fetch) {
		this.to_fetch = to_fetch;
	}
	public String getSKU() {
		return SKU;
	}
	public void setSKU(String sKU) {
		SKU = sKU;
	}
	public String getJsonAttributs() {
		return jsonAttributs;
	}
	public void setJsonAttributs(String jsonAttributs) {
		this.jsonAttributs = jsonAttributs;
	}
}
