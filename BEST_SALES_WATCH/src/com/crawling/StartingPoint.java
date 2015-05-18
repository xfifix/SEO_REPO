package com.crawling;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.http.client.ClientProtocolException;

import com.data.MenuDataItem;
import com.data.ProductDataItem;
import com.utility.CrawlingUtility;

public class StartingPoint {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/concurrency_sales.properties";
	private static String insertStatement ="INSERT INTO AMAZON_CONCURRENCY_BEST_SALES (ARBO_LABEL,PRODUCT_TITLE,URL) VALUES(?, ?, ?)";

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
			// sorting everything and updating database
			Map<String, List<ProductDataItem>> treeMap = new TreeMap<String, List<ProductDataItem>>(productsResult);
		    updateDatabase(treeMap);
			productsResult.clear();

			// going down into the depth 1
			List<MenuDataItem> under_one_Menu = CrawlingUtility.parseDepthMenu(depPSCode);
			for (MenuDataItem under_one : under_one_Menu){
				String depPSCode_under_one = CrawlingUtility.getPSCode(under_one.getUrl());
				List<ProductDataItem> products_under_one =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_one);			
				productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel(),products_under_one);	
				// sorting everything and updating database
				Map<String, List<ProductDataItem>> treeMapOne = new TreeMap<String, List<ProductDataItem>>(productsResult);
			    updateDatabase(treeMapOne);
				productsResult.clear();

				List<MenuDataItem> under_two_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_one);
				for (MenuDataItem under_two : under_two_Menu){
					String depPSCode_under_two = CrawlingUtility.getPSCode(under_two.getUrl());
					List<ProductDataItem> products_under_two =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_two);			
					productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel(),products_under_two);	
					// sorting everything and updating database
					Map<String, List<ProductDataItem>> treeMapTwo = new TreeMap<String, List<ProductDataItem>>(productsResult);
				    updateDatabase(treeMapTwo);
					productsResult.clear();

					List<MenuDataItem> under_three_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_two);
					for (MenuDataItem under_three : under_three_Menu){
						String depPSCode_under_three = CrawlingUtility.getPSCode(under_three.getUrl());
						List<ProductDataItem> products_under_three =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_three);			
						productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel(),products_under_three);			
						// sorting everything and updating database
						Map<String, List<ProductDataItem>> treeMapThree = new TreeMap<String, List<ProductDataItem>>(productsResult);
					    updateDatabase(treeMapThree);
						productsResult.clear();

						List<MenuDataItem> under_four_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_three);
						for (MenuDataItem under_four : under_four_Menu){
							String depPSCode_under_four = CrawlingUtility.getPSCode(under_four.getUrl());
							List<ProductDataItem> products_under_four =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_four);			
							productsResult.put(departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel()+"|"+under_four.getLabel(),products_under_four);			
							// sorting everything and updating database
							Map<String, List<ProductDataItem>> treeMapFour = new TreeMap<String, List<ProductDataItem>>(productsResult);
						    updateDatabase(treeMapFour);
							productsResult.clear();

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
	
	public static void updateDatabase(Map<String, List<ProductDataItem>> treeMap){
//		Getting the database property
			Properties props = new Properties();
			FileInputStream in = null;      
			try {
				in = new FileInputStream(database_con_path);
				props.load(in);
			} catch (IOException ex) {
				System.out.println("Trouble fetching database configuration");
				ex.printStackTrace();

			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException ex) {
					System.out.println("Trouble fetching database configuration");
					ex.printStackTrace();
				}
			}
			//the following properties have been identified
			String url = props.getProperty("db.url");
			String user = props.getProperty("db.user");
			String passwd = props.getProperty("db.passwd");



			System.out.println("You are connected to the postgresql CONCURRENCYSALESDB database as "+user);

			// The database connection
			Connection con = null;
			PreparedStatement pst = null;
			ResultSet rs = null;

			try {  
				con = DriverManager.getConnection(url, user, passwd);
				//con.setAutoCommit(false); 
				PreparedStatement st = con.prepareStatement(insertStatement);
				//inserting every thing
				for (Map.Entry<String, List<ProductDataItem>> entry : treeMap.entrySet()) {
					String label =  entry.getKey();
					List<ProductDataItem> productsList = entry.getValue();
					for (ProductDataItem item : productsList){
						//Statement st = con.createStatement();	
						try {
							st.setString(1, label);
							st.setString(2, item.getLabel());
							st.setString(3, item.getUrl());					
							//st.addBatch();
							st.executeUpdate();
						}catch (SQLException ex) {
							ex.printStackTrace();
						} 
					}
				}

				//			st.executeBatch();
				//			con.commit();
				st.close();
				System.out.println("Batch insertion succeeded into database");
			} catch (SQLException ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (rs != null) {
						rs.close();
					}
					if (pst != null) {
						pst.close();
					}
					if (con != null) {
						con.close();
					}

				} catch (SQLException ex) {
					ex.printStackTrace();
				}
			}
	}
	
	
}
