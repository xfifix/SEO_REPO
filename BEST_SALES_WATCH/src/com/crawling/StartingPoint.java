package com.crawling;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.http.client.ClientProtocolException;

import com.data.MenuDataItem;
import com.data.ProductDataItem;
import com.utility.CrawlingUtility;

public class StartingPoint {


	private static Set<String> alreadyDealtWithArbo = new HashSet<String>();
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/concurrency_sales.properties";
	private static String insertStatement ="INSERT INTO AMAZON_CONCURRENCY_BEST_SALES (ARBO_LABEL,PRODUCT_TITLE,URL) VALUES(?, ?, ?)";

	public static void main(String[] args) throws ClientProtocolException, IOException{
		Map<String, List<ProductDataItem>> productsResult = new HashMap<String, List<ProductDataItem>>();
		String startingURL = "http://www.amazon.fr/gp/bestsellers/ref=zg_bs_unv_t_0_t_1";
		String html = CrawlingUtility.getPSCode(startingURL);
		// we here parse the very entry menu using the id="zg_browseRoot" #zg_browseRoot
		List<MenuDataItem> my_departments = CrawlingUtility.parseEntryMenu(html);
		Set<String> item_under_zero_Menu = getMenuItems(my_departments);
		for (MenuDataItem departmentURL : my_departments){
			String labelZero = departmentURL.getLabel();
			String depPSCode = "";
			if (checkLabel(labelZero)){
				depPSCode = CrawlingUtility.getPSCode(departmentURL.getUrl());
				// for each department we have to make a recursive crawl as we don't know the depth
				List<ProductDataItem> products =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode);			
				// adding the zero level results
				productsResult.put(labelZero,products);		
				alreadyDealtWithArbo.add(labelZero);
			}

			// sorting everything and updating database
			Map<String, List<ProductDataItem>> treeMap = new TreeMap<String, List<ProductDataItem>>(productsResult);
			updateDatabase(treeMap);
			productsResult.clear();

			// going down into the depth 1
			List<MenuDataItem> under_one_Menu = CrawlingUtility.parseDepthMenu(depPSCode);
			Set<String> item_under_one_Menu = getMenuItems(under_one_Menu);
			for (MenuDataItem under_one : under_one_Menu){

				String depPSCode_under_one = "";
				if (!item_under_zero_Menu.contains(under_one.getLabel())){
					String labelOne = departmentURL.getLabel()+"|"+under_one.getLabel();

					if (checkLabel(labelOne)){
						System.out.println("Label 1 : "+labelOne);
						depPSCode_under_one = CrawlingUtility.getPSCode(under_one.getUrl());
						List<ProductDataItem> products_under_one =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_one);			
						productsResult.put(labelOne,products_under_one);	
						alreadyDealtWithArbo.add(labelOne);
					}
					// sorting everything and updating database
					Map<String, List<ProductDataItem>> treeMapOne = new TreeMap<String, List<ProductDataItem>>(productsResult);
					updateDatabase(treeMapOne);
					productsResult.clear();
				}
				List<MenuDataItem> under_two_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_one);
				Set<String> item_under_two_Menu = getMenuItems(under_two_Menu);
				for (MenuDataItem under_two : under_two_Menu){
					String depPSCode_under_two = "";
					if (!item_under_zero_Menu.contains(under_two.getLabel()) && !item_under_one_Menu.contains(under_two.getLabel())){
						String labelTwo = departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel();


						if (checkLabel(labelTwo)){
							System.out.println("Label 2 : "+labelTwo);
							depPSCode_under_two = CrawlingUtility.getPSCode(under_two.getUrl());
							List<ProductDataItem> products_under_two =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_two);			
							productsResult.put(labelTwo,products_under_two);	
							alreadyDealtWithArbo.add(labelTwo);
						}
						// sorting everything and updating database
						Map<String, List<ProductDataItem>> treeMapTwo = new TreeMap<String, List<ProductDataItem>>(productsResult);
						updateDatabase(treeMapTwo);
						productsResult.clear();
					}
					List<MenuDataItem> under_three_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_two);
					Set<String> item_under_three_Menu = getMenuItems(under_three_Menu);
					String depPSCode_under_three = "";
					for (MenuDataItem under_three : under_three_Menu){
						if (!item_under_zero_Menu.contains(under_three.getLabel()) && !item_under_one_Menu.contains(under_three.getLabel()) && !item_under_two_Menu.contains(under_three.getLabel())){
	
							String labelThree = departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel();
						
							if (checkLabel(labelThree)){
								System.out.println("Label 3 : "+labelThree);
								depPSCode_under_three = CrawlingUtility.getPSCode(under_three.getUrl());
								List<ProductDataItem> products_under_three =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_three);			

								productsResult.put(labelThree,products_under_three);			
								alreadyDealtWithArbo.add(labelThree);
							}
							// sorting everything and updating database
							Map<String, List<ProductDataItem>> treeMapThree = new TreeMap<String, List<ProductDataItem>>(productsResult);
							updateDatabase(treeMapThree);
							productsResult.clear();
						}

						List<MenuDataItem> under_four_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_three);
						Set<String> item_under_four_Menu = getMenuItems(under_four_Menu);
						for (MenuDataItem under_four : under_four_Menu){
							String depPSCode_under_four = "";
							if (!item_under_zero_Menu.contains(under_four.getLabel()) &&  !item_under_one_Menu.contains(under_four.getLabel()) && !item_under_two_Menu.contains(under_four.getLabel())&& !item_under_three_Menu.contains(under_four.getLabel())){
							
								String labelFour = departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel()+"|"+under_four.getLabel();
								if (checkLabel(labelFour)){
									System.out.println("Label 4 : "+labelFour);

									depPSCode_under_four = CrawlingUtility.getPSCode(under_four.getUrl());
									List<ProductDataItem> products_under_four =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_four);			
									productsResult.put(labelFour,products_under_four);
									alreadyDealtWithArbo.add(labelFour);
								}
								// sorting everything and updating database
								Map<String, List<ProductDataItem>> treeMapFour = new TreeMap<String, List<ProductDataItem>>(productsResult);
								updateDatabase(treeMapFour);
								productsResult.clear();
							}
							List<MenuDataItem> under_five_Menu = CrawlingUtility.parseDepthMenu(depPSCode_under_four);
							for (MenuDataItem under_five : under_five_Menu){
								String depPSCode_under_five = "";
								if (!item_under_zero_Menu.contains(under_five.getLabel()) && !item_under_one_Menu.contains(under_five.getLabel()) && !item_under_two_Menu.contains(under_five.getLabel())&& !item_under_three_Menu.contains(under_five.getLabel())&& !item_under_four_Menu.contains(under_five.getLabel())){
									String labelFive = departmentURL.getLabel()+"|"+under_one.getLabel()+"|"+under_two.getLabel()+"|"+under_three.getLabel()+"|"+under_four.getLabel()+"|"+under_five.getLabel();
									if (checkLabel(labelFive)){
										System.out.println("Label 5 : "+labelFive);

										depPSCode_under_five = CrawlingUtility.getPSCode(under_five.getUrl());
										List<ProductDataItem> products_under_five =  CrawlingUtility.parseStandardDepartmentProductsFromPaginationList(depPSCode_under_five);			
										productsResult.put(labelFive,products_under_five);		
										alreadyDealtWithArbo.add(labelFive);
									}
									Map<String, List<ProductDataItem>> treeMapFive = new TreeMap<String, List<ProductDataItem>>(productsResult);
									updateDatabase(treeMapFive);
									productsResult.clear();
								}
							}
						}
					}
				}
			}
			System.out.println("Finishing Department : "+departmentURL);
		}







	}

	public static Set<String> getMenuItems(List<MenuDataItem> menu){
		Set<String> to_Return = new HashSet<String>();
		for (MenuDataItem menuDataItem : menu){
			to_Return.add(menuDataItem.getLabel());
		}
		return to_Return;
	}
	
	public static boolean checkLabel(String label){
		if (alreadyDealtWithArbo.contains(label)){
			return false;
		}

		return true;

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
