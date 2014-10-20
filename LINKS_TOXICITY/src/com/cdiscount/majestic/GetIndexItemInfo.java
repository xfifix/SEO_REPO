/*
 * Version 0.9.3
 *
 * Copyright (c) 2011, Majestic-12 Ltd
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *   2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *   3. Neither the name of the Majestic-12 Ltd nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL Majestic-12 Ltd BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

/* NOTE: The code below is specifically for the GetIndexItemInfo API command.
 *       For other API commands, the arguments required may differ.
 *       Please refer to the Majestic SEO Developer Wiki for more information
 *       regarding other API commands and their arguments.
 */

package com.cdiscount.majestic;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

import com.majesticseo.external.rpc.APIService;
import com.majesticseo.external.rpc.DataTable;
import com.majesticseo.external.rpc.Response;

public class GetIndexItemInfo {

    public static void main(String[] args) {
        //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

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

        System.out.println("\n***********************************************************"
                    + "*****************");

        System.out.println(
                "\n\nThis example program will return key information about \"index items\"."
                + "\n\nThe following must be provided in order to run this program: "
                + "\n1. API key "
                + "\n2. List of items to query"
                + "\n\nPlease enter your API key:");

      //  try {
            //String app_api_key = br.readLine();
            String app_api_key="322BBDCD5638D138192E9D783EAB5EA0";
            System.out.println(
                    "\nPlease enter the list of items you wish to query seperated by commas: "
                    + "\n(e.g. majesticseo.com, majestic12.co.uk)");

 //           String itemsToQuery = br.readLine();
            String itemsToQuery = "http://www.cdiscount.com";
            String[] items = itemsToQuery.split(", ");

            /* create a Map from the resulting array with the key being
             * "item0 => first item to query, item1 => second item to query" etc
             */
            Map<String, String> parameters = new LinkedHashMap<String, String>();
            for (int i = 0; i < items.length; i++) {
                parameters.put("item" + i, items[i]);
            }

            // add the total number of items to the Dictionary with the key being "items"
            parameters.put("items", String.valueOf(items.length));
            parameters.put("datasource", "fresh");

            APIService apiService = new APIService(app_api_key, endpoint);
            Response response = apiService.executeCommand("GetIndexItemInfo", parameters);

            // check the response code
            if (response.isOK()) {
                // print the results table
                DataTable results = response.getTableForName("Results");

                for(Map<String, String> row : results.getTableRows()) {
                    String item = row.get("Item");
                    System.out.println("\n<" + item + ">");

                    TreeSet<String> keys = new TreeSet<String>(row.keySet());
                    for(String key : keys) {
                        if(!key.equals("Item")) {
                            String value = row.get(key);
                            System.out.println(" " + key + " ... " + value);
                        }
                    }
                }

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

         //   br.close();
//        } catch (IOException ioe) {
//            System.out.println(
//                    "IO error trying to read either the api key entered or the items to "
//                    + "query.\n");
//            throw new RuntimeException(ioe);
//        }
    }
}
