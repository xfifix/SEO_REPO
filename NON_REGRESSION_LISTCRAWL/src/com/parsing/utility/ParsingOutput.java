package com.parsing.utility;

public class ParsingOutput {
	private String[] xpathResults;
	private String h1="";
	private String title="";
	private long exalead_time;
	private long solr_time;
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getH1() {
		return h1;
	}
	public void setH1(String h1) {
		this.h1 = h1;
	}
	public String[] getXpathResults() {
		return xpathResults;
	}
	public void setXpathResults(String[] xpathResults) {
		this.xpathResults = xpathResults;
	}
	public long getExalead_time() {
		return exalead_time;
	}
	public void setExalead_time(long exalead_time) {
		this.exalead_time = exalead_time;
	}
	public long getSolr_time() {
		return solr_time;
	}
	public void setSolr_time(long solr_time) {
		this.solr_time = solr_time;
	}
}
