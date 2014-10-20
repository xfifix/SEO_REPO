package com.cdiscount.majestic;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import com.majesticseo.external.rpc.APIService;
import com.majesticseo.external.rpc.DataTable;
import com.majesticseo.external.rpc.Response;

public class CdiscountGetHistoryBackLinks {

	public static void main(String[] args) {

		//Majestic parameter
		String endpoint = "http://enterprise.majesticseo.com/api_command";

		String app_api_key="322BBDCD5638D138192E9D783EAB5EA0";
		String itemToQuery="cdiscount.com";
		APIService apiService = new APIService(app_api_key, endpoint);
		// set up parameters	

		Map<String, String> parameters = new LinkedHashMap<String, String>();
		parameters.put("Count", "100000000");
		parameters.put("item", itemToQuery);
		//     parameters.put("Mode", "0");
		parameters.put("datasource", "historic");

		// requesting MAJESTIC
		//  Response response = apiService.executeCommand("GetBackLinkData", parameters);
		Response response = apiService.executeCommand("GetBackLinksHistory", parameters);            

		// check the response code
		if (response.isOK()) {
			// print the URL table
			Map<String, DataTable> my_tables = response.getTables();
			Iterator it = my_tables.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry)it.next();
				String table_name=(String)pairs.getKey();
				System.out.println(table_name);
			}

			DataTable results = response.getTableForName("Results");
			int counter = 0;
			for(Map<String, String> row : results.getTableRows()) {
				counter ++;
				System.out.println("Link number : "+counter);
				TreeSet<String> keys = new TreeSet<String>(row.keySet());
				for(String key : keys) {   
					String value = row.get(key);
					System.out.println(" " + key + " ... " + value);
				}
			}
			//                for(Map<String, String> row : results.getTableRows()) {
			//                    System.out.println("\nURL: " + row.get("SourceURL"));
			//                    System.out.println("ACRank: " + row.get("ACRank"));
			//                }
		} else {
			System.out.println("\nERROR MESSAGE:");
			System.out.println(response.getErrorMessage());

			System.out.println("\n\n***********************************************************"
					+ "*****************");

			System.out.println("\nDebugging Info:");
			System.out.println("\n  Endpoint: \t" + endpoint);
			System.out.println("  API Key: \t" + app_api_key);

			if("http://enterprise.majesticseo.com/api_command".equals(endpoint)) {
				System.out.println("\n  Is this API Key valid for this Endpoint?");

				System.out.println("\n  This program is hard-wired to the Enterprise API.");

				System.out.println("\n  If you do not have access to the Enterprise API, "
						+ "change the endpoint to: \n  http://developer.majesticseo.com/api_command.");
			}

			System.out.println("\n***********************************************************"
					+ "*****************");
		}


	}
}
