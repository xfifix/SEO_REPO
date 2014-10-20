package com.cdiscount.majestic;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import com.majesticseo.external.rpc.APIService;
import com.majesticseo.external.rpc.DataTable;
import com.majesticseo.external.rpc.Response;

public class OneDayGetNewLostBackLinks {

	public static void main(String[] args) {
		// BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		String endpoint = "http://enterprise.majesticseo.com/api_command";

		System.out.println("\n***********************************************************"
				+ "*****************");

		System.out.println("\nEndpoint: " + endpoint);

		if("http://enterprise.majesticseo.com/api_command".equals(endpoint)) {
			System.out.println("\nThis program is hard-wired to the Enterprise API.");

			System.out.println("\nIf you do not have access to the Enterprise API, "
					+ "change the endpoint to: \nhttp://developer.majesticseo.com/api_command.");
		} else {
			System.out.println("\nThis program is hard-wired to the Developer API "
					+ "and hence the subset of data \nreturned will be substantially "
					+ "smaller than that which will be returned from \neither the "
					+ "Enterprise API or the Majestic SEO website.");

			System.out.println("\nTo make this program use the Enterprise API, change "
					+ "the endpoint to: \nhttp://enterprise.majesticseo.com/api_command.");
		}

		//        System.out.println("\n***********************************************************"
		//                    + "*****************");
		//
		//        System.out.println(
		//                "\n\nThis example program will return the top backlinks for any URL, domain "
		//                + "\nor subdomain."
		//                + "\n\nThe following must be provided in order to run this program: "
		//                + "\n1. API key "
		//                + "\n2. A URL, domain or subdomain to query"
		//                + "\n\nPlease enter your API key:");

		//     try {
		//String app_api_key = br.readLine();
		String app_api_key="322BBDCD5638D138192E9D783EAB5EA0";


		//            System.out.println("\nPlease enter a URL, domain or subdomain to query:");
		//            String itemToQuery = br.readLine();
		String itemToQuery="cdiscount.com";
		// set up parameters
		Map<String, String> parameters = new LinkedHashMap<String, String>();
		parameters.put("Count", "10000000");
		parameters.put("item", itemToQuery);
		//     parameters.put("Mode", "0");
		parameters.put("datasource", "fresh");
		parameters.put("datefrom", "2014-09-05");
		parameters.put("dateto", "2014-09-06");
		
		//            parameters.put("datasource", "historic");

		APIService apiService = new APIService(app_api_key, endpoint);
		//  Response response = apiService.executeCommand("GetBackLinkData", parameters);
		Response response = apiService.executeCommand("GetNewLostBackLinks", parameters);            


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


			DataTable results = response.getTableForName("BackLinks");
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

			if("http://developer.majesticseo.com/api_command".equals(endpoint)) {
				System.out.println("\n***********************************************************"
						+ "*****************");

				System.out.println("\nEndpoint: " + endpoint);

				System.out.println("\nThis program is hard-wired to the Developer API "
						+ "and hence the subset of data \nreturned will be substantially "
						+ "smaller than that which will be returned from \neither the "
						+ "Enterprise API or the Majestic SEO website.");

				System.out.println("\nTo make this program use the Enterprise API, change "
						+ "the endpoint to: \nhttp://enterprise.majesticseo.com/api_command.");

				System.out.println("\n***********************************************************"
						+ "*****************");
			}
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

		//br.close();
		//        } catch (IOException ioe) {
		//            System.out.println(
		//                    "IO error trying to read either the api key entered or the item to "
		//                    + "query.\n");
		//            throw new RuntimeException(ioe);
		//        }
	}
}
