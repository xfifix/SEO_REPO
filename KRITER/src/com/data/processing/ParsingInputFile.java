package com.data.processing;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ParsingInputFile {
	private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/Catalogue_KryterFull.csv";
	public static void main(String[] args) throws IOException {
		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = "\\u0001";
		// we read the file
		br = new BufferedReader(new InputStreamReader(new FileInputStream(input_file_path), "UTF-8"));	
		// we skip the first line : the headers
		header = br.readLine();
		column_names= header.split(cvsSplitBy);
		int nb_line=0;
		System.out.println("Column names headers : "+Arrays.toString(column_names));
		while ((line = br.readLine()) != null) {
			nb_line++;
			System.out.println("Line number : "+nb_line);
			String[] fields= line.split(cvsSplitBy);
			System.out.println("Fields : "+Arrays.toString(fields));;
		}
		br.close();
		System.out.println("Having found : "+nb_line+" products");
	}
}
