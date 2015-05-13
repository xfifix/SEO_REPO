package com.crawling;

import java.io.IOException;
import java.util.List;

import org.apache.http.client.ClientProtocolException;

import com.utility.CrawlingUtility;

public class StartingPoint {

	public static void main(String[] args) throws ClientProtocolException, IOException{
		String startingURL = "http://www.amazon.fr/gp/bestsellers/ref=zg_bs_unv_t_0_t_1";
		String html = CrawlingUtility.getPSCode(startingURL);
		List<String> my_departments = CrawlingUtility.parseEntryMenu(html);
		for (String departmentURL : my_departments){
			String depPSCode = CrawlingUtility.getPSCode(departmentURL);
			List<String> products =  CrawlingUtility.parseStandardDepartmentList(depPSCode);
			System.out.println("Department : "+departmentURL);
			System.out.println("Number of product found : "+products.size());
		}
	}
}
