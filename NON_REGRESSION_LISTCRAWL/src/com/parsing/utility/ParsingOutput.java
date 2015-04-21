package com.parsing.utility;

public class ParsingOutput {
	private String[] xpathResults;
	private String h1="";
	private String title="";
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
}
