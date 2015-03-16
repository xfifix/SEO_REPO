package com.statistics.processing;

public class StatisticsMeasures {
	private int nb_distinct_category5;
	private String distinct_category5;
	private int nb_distinct_category4;
	private String distinct_category4;
	private int nb_distinct_brands;
	private String distinct_brands;
	private Double[] tf_distances_libelle = new Double[0];
	private Double[] tf_idf_distances_libelle = new Double[0];
	private Double[] levenshtein_distances_libelle = new Double[0];
	private Double[] tf_description80 = new Double[0];
	private Double[] tf_idf_description80 = new Double[0];
	private Double[] levenshtein_description80 = new Double[0];
	private String currentSku;
	private String currentVendor;
	private String state;
	private String magasin;
	private String rayon;
	
	public Double[] getLevenshtein_distances_libelle() {
		return levenshtein_distances_libelle;
	}
	public void setLevenshtein_distances_libelle(
			Double[] levenshtein_distances_libelle) {
		this.levenshtein_distances_libelle = levenshtein_distances_libelle;
	}
	public Double[] getLevenshtein_description80() {
		return levenshtein_description80;
	}
	public void setLevenshtein_description80(Double[] levenshtein_description80) {
		this.levenshtein_description80 = levenshtein_description80;
	}

	public int getNb_distinct_category5() {
		return nb_distinct_category5;
	}
	public void setNb_distinct_category5(int nb_distinct_category5) {
		this.nb_distinct_category5 = nb_distinct_category5;
	}
	public String getDistinct_category5() {
		return distinct_category5;
	}
	public void setDistinct_category5(String distinct_category5) {
		this.distinct_category5 = distinct_category5;
	}
	public int getNb_distinct_category4() {
		return nb_distinct_category4;
	}
	public void setNb_distinct_category4(int nb_distinct_category4) {
		this.nb_distinct_category4 = nb_distinct_category4;
	}
	public String getDistinct_category4() {
		return distinct_category4;
	}
	public void setDistinct_category4(String distinct_category4) {
		this.distinct_category4 = distinct_category4;
	}
	public int getNb_distinct_brands() {
		return nb_distinct_brands;
	}
	public void setNb_distinct_brands(int nb_distinct_brands) {
		this.nb_distinct_brands = nb_distinct_brands;
	}
	public String getDistinct_brands() {
		return distinct_brands;
	}
	public void setDistinct_brands(String distinct_brands) {
		this.distinct_brands = distinct_brands;
	}
	public Double[] getTf_distances_libelle() {
		return tf_distances_libelle;
	}
	public void setTf_distances_libelle(Double[] tf_distances_libelle) {
		this.tf_distances_libelle = tf_distances_libelle;
	}
	public Double[] getTf_idf_distances_libelle() {
		return tf_idf_distances_libelle;
	}
	public void setTf_idf_distances_libelle(Double[] tf_idf_distances_libelle) {
		this.tf_idf_distances_libelle = tf_idf_distances_libelle;
	}
	public Double[] getTf_description80() {
		return tf_description80;
	}
	public void setTf_description80(Double[] tf_description80) {
		this.tf_description80 = tf_description80;
	}
	public Double[] getTf_idf_description80() {
		return tf_idf_description80;
	}
	public void setTf_idf_description80(Double[] tf_idf_description80) {
		this.tf_idf_description80 = tf_idf_description80;
	}
	public String getCurrentVendor() {
		return currentVendor;
	}
	public void setCurrentVendor(String currentVendor) {
		this.currentVendor = currentVendor;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getMagasin() {
		return magasin;
	}
	public void setMagasin(String magasin) {
		this.magasin = magasin;
	}
	public String getRayon() {
		return rayon;
	}
	public void setRayon(String rayon) {
		this.rayon = rayon;
	}
	public String getCurrentSku() {
		return currentSku;
	}
	public void setCurrentSku(String currentSku) {
		this.currentSku = currentSku;
	}
}
