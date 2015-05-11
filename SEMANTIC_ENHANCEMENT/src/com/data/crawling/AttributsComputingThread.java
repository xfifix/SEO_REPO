package com.data.crawling;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.data.processing.FetchableSKU;
import com.utility.AttributsUtility;

public class AttributsComputingThread implements Runnable {
	private Connection con;
	private Map<String, List<FetchableSKU>> my_categories_to_compute  = new HashMap<String, List<FetchableSKU>>();
	// beware static shared global cache for unfetched skus
	
	//private static String insert_cds_statement = "INSERT INTO CDS_SIMILAR_PRODUCTS(SKU,SKU1,SKU2,SKU3,SKU4,SKU5,SKU6) VALUES(?,?,?,?,?,?,?)";
	private static String update_catalog_statement = "UPDATE CATALOG SET SKU1=?,SKU2=?,SKU3=?,SKU4=?,SKU5=?,SKU6=?,TO_FETCH=false where SKU=?";

	public AttributsComputingThread(Connection con, Map<String, List<FetchableSKU>>  to_fetch) throws SQLException{
		this.con = con;
		this.my_categories_to_compute = to_fetch;
	}

	public void run() {
		String category_to_debug="";
		try {  
			Iterator<Entry<String, List<FetchableSKU>>> it = my_categories_to_compute.entrySet().iterator();
			// dispatching to threads
			while (it.hasNext()){	
				Map.Entry<String, List<FetchableSKU>> pairs = (Map.Entry<String, List<FetchableSKU>>)it.next();
				String category=pairs.getKey();
				List<FetchableSKU> my_data = pairs.getValue();
				category_to_debug=category;
				System.out.println(Thread.currentThread()+" Dealing with category : "+category);
				System.out.println(Thread.currentThread()+" Category skus all fetched for data : "+category);
				List<FetchableSKU> my_tofetch_data = filterToFetchData(my_data);
				computeSKUsList(my_tofetch_data);

			}		
			close_connection();
		} catch (Exception ex) {
			System.out.println("Trouble with category : "+category_to_debug);
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	public void computeSKUsList(List<FetchableSKU> my_data){
		for (FetchableSKU my_sku : my_data){
			if (my_sku.isTo_fetch()){
				String attributsJSON = AttributsUtility.crawl_sku(my_sku.getSKU());
			}
			
		}
	}
	

	
	public List<FetchableSKU> filterToFetchData(List<FetchableSKU> my_data){
		List<FetchableSKU> filtered_List = new ArrayList<FetchableSKU>();
		for (FetchableSKU entry : my_data){
			if (entry.isTo_fetch()) { 
				filtered_List.add(entry);
			}
		}
		return filtered_List; 
	}

	private void close_connection(){
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
}
