package com.cdiscount.majestic;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import com.majesticseo.external.rpc.APIService;
import com.majesticseo.external.rpc.DataTable;
import com.majesticseo.external.rpc.Response;

public class CdiscountLoopingGetNewLostBackLinks {

	public static void main(String[] args) {
		// BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		long oneDayMilSec = 86400000; // number of milliseconds in one day
		long twoWeeksMilSec = 14*oneDayMilSec; // number of milliseconds in one day
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		try {
			//date parameter
		    Date startDate = sdf.parse("2013-01-01");
		    Date endDate = sdf.parse("2014-09-01");
		    long startDateMilSec = startDate.getTime();
		    long endDateMilSec = endDate.getTime();

		    //Majestic parameter
			String endpoint = "http://enterprise.majesticseo.com/api_command";

			String app_api_key="322BBDCD5638D138192E9D783EAB5EA0";
			String itemToQuery="cdiscount.com";
			APIService apiService = new APIService(app_api_key, endpoint);
			// set up parameters	
		    for(long d=startDateMilSec; d<=endDateMilSec; d=d+twoWeeksMilSec){
		    	Date localDate = new Date(d);
		    	String beg_formatted_Date = sdf.format(localDate);
		    	System.out.println("Beginning date "+beg_formatted_Date);
		    	String end_formatted_Date = sdf.format(new Date(d+twoWeeksMilSec));
		    	System.out.println("Beginning date "+end_formatted_Date);
		    	
				Map<String, String> parameters = new LinkedHashMap<String, String>();
				parameters.put("Count", "100000000");
				parameters.put("item", itemToQuery);
				//     parameters.put("Mode", "0");
				parameters.put("datasource", "historic");
				parameters.put("datefrom", beg_formatted_Date);
				parameters.put("dateto", end_formatted_Date);

				// requesting MAJESTIC
				//  Response response = apiService.executeCommand("GetBackLinkData", parameters);
				Response response = apiService.executeCommand("GetNewLostBackLinks", parameters);            


				// check the response code
				if (response.isOK()) {
					// print the URL table

//					Map<String, DataTable> my_tables = response.getTables();
//					Iterator it = my_tables.entrySet().iterator();
//					while (it.hasNext()) {
//						Map.Entry pairs = (Map.Entry)it.next();
//						String table_name=(String)pairs.getKey();
//						System.out.println(table_name);
//					}

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
		} catch (ParseException e) {
		    e.printStackTrace();
		}
	
	}
}
