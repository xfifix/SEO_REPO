package com.parsing.utility;

import java.util.List;

public class FacettesParsingOutput {
	private List<String> orderedSkusList;
	private String nb_products;
	private String Facettes;
	public String getNb_products() {
		return nb_products;
	}
	public void setNb_products(String nb_products) {
		this.nb_products = nb_products;
	}
	public String getFacettes() {
		return Facettes;
	}
	public void setFacettes(String facettes) {
		Facettes = facettes;
	}
	public List<String> getOrderedSkusList() {
		return orderedSkusList;
	}
	public void setOrderedSkusList(List<String> orderedSkusList) {
		this.orderedSkusList = orderedSkusList;
	}
}
