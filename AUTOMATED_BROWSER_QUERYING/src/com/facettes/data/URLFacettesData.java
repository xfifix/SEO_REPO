package com.facettes.data;

public class URLFacettesData {
	
	private int id;
	private String url;
	private String magasin;
	private String level_two;
	private String level_three;
	private int productlist_count;
	private boolean is_facette_opened;
	private boolean is_value_opened;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getMagasin() {
		return magasin;
	}
	public void setMagasin(String magasin) {
		this.magasin = magasin;
	}
	public String getLevel_two() {
		return level_two;
	}
	public void setLevel_two(String level_two) {
		this.level_two = level_two;
	}
	public String getLevel_three() {
		return level_three;
	}
	public void setLevel_three(String level_three) {
		this.level_three = level_three;
	}
	public int getProductlist_count() {
		return productlist_count;
	}
	public void setProductlist_count(int productlist_count) {
		this.productlist_count = productlist_count;
	}
	public boolean isIs_facette_opened() {
		return is_facette_opened;
	}
	public void setIs_facette_opened(boolean is_facette_opened) {
		this.is_facette_opened = is_facette_opened;
	}
	public boolean isIs_value_opened() {
		return is_value_opened;
	}
	public void setIs_value_opened(boolean is_value_opened) {
		this.is_value_opened = is_value_opened;
	}

}
