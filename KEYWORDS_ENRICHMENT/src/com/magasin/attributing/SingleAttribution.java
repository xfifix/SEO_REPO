package com.magasin.attributing;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class SingleAttribution {
	private static String request_url = "http://www.cdiscount.com/sa-10/";
	private static String seed = "botte+moto";

	public static void main(String[] args) throws IOException{
		//  building the URL to request
		String my_url = request_url+seed+".html";
		
		URL url = new URL(my_url);
		HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)");
		// we here want to be redirected to the proper magasin
		connection.setInstanceFollowRedirects(true);
		connection.connect();	
		System.out.println(connection.getResponseCode());	
		String redirected_url =connection.getURL().toString(); 
		System.out.println(redirected_url);
		String magasin =URL_Utilities.checkMagasin(redirected_url);
		System.out.println(magasin);
		String rayon =URL_Utilities.checkRayon(redirected_url);
		System.out.println(rayon);
		String produit =URL_Utilities.checkProduit(redirected_url);
		System.out.println(produit);
	}

}
