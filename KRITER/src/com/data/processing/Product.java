package com.data.processing;

import java.util.ArrayList;
import java.util.List;

public class Product {
	private Sku sku = new Sku();
	private List<Sku> similar_skus = new ArrayList<Sku>();

	public Sku getSku() {
		return sku;
	}
	public void setSku(Sku sku) {
		this.sku = sku;
	}
	public List<Sku> getSimilar_skus() {
		return similar_skus;
	}
	public void setSimilar_skus(List<Sku> similar_skus) {
		this.similar_skus = similar_skus;
	}
}
