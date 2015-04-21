package com.parsing.utility;

public class URLComparisonListProductsInfo{
	private int id;
	private int status=-1;
    private String best_sales_products;
    private String body_page_products;
    private String body_page_two_products;
    private String number_of_products;
    private String facette_summary;
    private String geoloc_selection;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getBest_sales_products() {
		return best_sales_products;
	}
	public void setBest_sales_products(String best_sales_products) {
		this.best_sales_products = best_sales_products;
	}
	public String getBody_page_products() {
		return body_page_products;
	}
	public void setBody_page_products(String body_page_products) {
		this.body_page_products = body_page_products;
	}
	public String getBody_page_two_products() {
		return body_page_two_products;
	}
	public void setBody_page_two_products(String body_page_two_products) {
		this.body_page_two_products = body_page_two_products;
	}
	public String getNumber_of_products() {
		return number_of_products;
	}
	public void setNumber_of_products(String number_of_products) {
		this.number_of_products = number_of_products;
	}
	public String getFacette_summary() {
		return facette_summary;
	}
	public void setFacette_summary(String facette_summary) {
		this.facette_summary = facette_summary;
	}
	public String getGeoloc_selection() {
		return geoloc_selection;
	}
	public void setGeoloc_selection(String geoloc_selection) {
		this.geoloc_selection = geoloc_selection;
	}
}