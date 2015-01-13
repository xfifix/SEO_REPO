package com.offer.matching;

public class OfferMatchingRequestResults {
	private String request;
	private int volume;
	private int search_postion;
	private boolean isRelevant;
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public int getVolume() {
		return volume;
	}
	public void setVolume(int volume) {
		this.volume = volume;
	}
	public int getSearch_postion() {
		return search_postion;
	}
	public void setSearch_postion(int search_postion) {
		this.search_postion = search_postion;
	}
	public boolean isRelevant() {
		return isRelevant;
	}
	public void setRelevant(boolean isRelevant) {
		this.isRelevant = isRelevant;
	}
}
