package com.test;

import com.utility.AttributsUtility;

public class SkuTestingClass {
	public static void main(String[] args){
		String sku_tot_test = "MP00234469";
		String json = AttributsUtility.crawl_sku(sku_tot_test);
		System.out.println("");
	}
}
