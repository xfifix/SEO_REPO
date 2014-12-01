package com.populating;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class DatabasePopulating {


	public static void main(String[] args) throws IOException {
		String csvFile="D:\\test\\top100k_new.csv";

		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = ";";
		int nb_line=1;
		// last error
		br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));	
		// we skip the first line : the headers
		header = br.readLine();
		column_names= header.split(cvsSplitBy);
		while ((line = br.readLine()) != null) {



			String[] splitted_line = line.split(cvsSplitBy);
			System.out.println(splitted_line[0]);
			System.out.println(splitted_line[1]);
			

//			System.out.println("Title : "+title);
//			System.out.println("counter : "+counter);
//			System.out.println("URL : "+splitted_line[1]);
//			System.out.println("Magasin : "+magasin);
//			System.out.println("Rayon : "+rayon);
//			System.out.println("Produit : "+produit);


			nb_line++;
		}
	}
}

