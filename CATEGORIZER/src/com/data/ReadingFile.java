package com.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ReadingFile {
	private static String input_file_path = "/home/sduprey/My_Data/My_Cdiscount_Challenge/training.csv";
	private static int counter = 0;
	public static void main(String[] args){
		insert_new_Catalog();
		System.out.println("New DATA inserted from csv file : "+input_file_path);
	}

	private static void insert_new_Catalog(){
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = ";";
		BufferedReader br = null;
		try{
			// preparing the database for insertion
			br = new BufferedReader(new InputStreamReader(new FileInputStream(input_file_path), "UTF-8"));	
			// we skip the first line : the headers
			header = br.readLine();
			column_names= header.split(cvsSplitBy);
			System.out.println("Column names headers : "+Arrays.toString(column_names));
			int nb_line=1;
			System.out.println("We do not insert the first  : " + nb_line + " lines ");
			while ((line = br.readLine()) != null) {
				if (nb_line >= counter){
					System.out.println("Reading line number : "+nb_line);
					nb_line++;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}

			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

}
