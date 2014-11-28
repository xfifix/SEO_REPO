package com.keywords.processing;

import java.util.List;

import com.keywords.processing.ExaleadKeywordsRequestingWorkerThread.ULRLineToInsert;

public class RequestResults {
	private String request;
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

	public List<ULRLineToInsert> getResult() {
		return result;
	}
	public void setResult(List<ULRLineToInsert> result) {
		this.result = result;
	}

	private int volume;
	private List<ULRLineToInsert> result;

}
