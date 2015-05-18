package com.crawling;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.client.ClientProtocolException;

import com.data.MenuDataItem;
import com.data.ProductDataItem;
import com.utility.CrawlingUtility;

public class StartingPoint {

	public static void main(String[] args) throws ClientProtocolException, IOException{
		Map<String, List<ProductDataItem>> productsResult = new HashMap<String, List<ProductDataItem>>();
		String startingURL = "http://www.amazon.fr/gp/bestsellers/ref=zg_bs_unv_t_0_t_1";
		String html = CrawlingUtility.getPSCode(startingURL);
		// we here parse the very entry menu using the id="zg_browseRoot" #zg_browseRoot
		List<MenuDataItem> my_departments = CrawlingUtility.parseEntryMenu(html);
		for (MenuDataItem departmentURL : my_departments){
			// for each department we have to make a recursive crawl as we don't know the depth
			String depPSCode = CrawlingUtility.getPSCode(departmentURL.getUrl());
			List<ProductDataItem> products =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode);			
			// adding the zero level results
			productsResult.put(departmentURL.getLabel(),products);			
			// going down into the depth 1
			List<MenuDataItem> under_one_Menu = CrawlingUtility.parseDepthMenu(depPSCode);
			for (MenuDataItem under_one : under_one_Menu){
				String depPSCode_under_one = CrawlingUtility.getPSCode(under_one.getUrl());
				List<ProductDataItem> products_under_one =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_one);			
				productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel(),products_under_one);			
				List<MenuDataItem> under_two_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_one);
				for (MenuDataItem under_two : under_two_Menu){
					String depPSCode_under_two = CrawlingUtility.getPSCode(under_two.getUrl());
					List<ProductDataItem> products_under_two =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_two);			
					productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel(),products_under_two);			
					List<MenuDataItem> under_three_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_two);
					for (MenuDataItem under_three : under_three_Menu){
						String depPSCode_under_three = CrawlingUtility.getPSCode(under_three.getUrl());
						List<ProductDataItem> products_under_three =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_three);			
						productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel(),products_under_three);			
						
						List<MenuDataItem> under_four_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_three);
						for (MenuDataItem under_four : under_four_Menu){
							String depPSCode_under_four = CrawlingUtility.getPSCode(under_four.getUrl());
							List<ProductDataItem> products_under_four =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_four);			
							productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel()+"|"+under_four.getLabel(),products_under_four);			
							List<MenuDataItem> under_five_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_four);
							for (MenuDataItem under_five : under_five_Menu){
								String depPSCode_under_five = CrawlingUtility.getPSCode(under_five.getUrl());
								List<ProductDataItem> products_under_five =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_five);			
								productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel()+"|"+under_four.getLabel()+"|"+under_five.getLabel(),products_under_five);											
							}
						}
					}
				}
			}
			System.out.println("Finishing Department : "+departmentURL);
		}
	}
}
