package com.test;

import java.io.IOException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;

import com.data.MenuDataItem;
import com.utility.CrawlingUtility;

public class TestingDepthMenu {
	public static void main(String[] args) throws ClientProtocolException, IOException{
		String departmentURL = "http://www.amazon.fr/gp/bestsellers/pet-supplies/ref=zg_bs_nav_0/275-4480584-2580118";
		String sourceCodeDepartmentId = CrawlingUtility.getPSCode(departmentURL);
		List<MenuDataItem> menuList = CrawlingUtility.parseDepthMenu(sourceCodeDepartmentId);
		System.out.println(menuList);
	}
	
}
