package google.extract;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class GWTURLsExtractor {
    private static String beginning_pattern = "http";
    //private static String ending_pattern = "\"\"";
    private static String ending_pattern = "\"\"";
	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException{
		String csvFile = "/home/sduprey/My_Data/My_GWT_Extracts/URL_GWT_KO_CC.csv";
		String outputCsvFile = "/home/sduprey/My_Data/My_GWT_Extracts/ORDAINED_URL_GWT_KO_CC.csv";
		PrintWriter writer = new PrintWriter(outputCsvFile, "UTF-8");
		BufferedReader br = null;
		String line = "";
		@SuppressWarnings("unused")
		String header = "";
		br = new BufferedReader(new FileReader(csvFile));
		// we skip the first line : the headers
		header = br.readLine();
		// we write the first line : add balise
		while ((line = br.readLine()) != null) {
			int beginning = line.indexOf(beginning_pattern);
			if (beginning>0){
				line=line.substring(beginning,line.length());	
				int ending = line.indexOf(ending_pattern);
				if (ending>0){
					line=line.substring(0,ending);
				}
				String[] urls = line.split("\\|");
				for (String url : urls){
					System.out.println(url.trim());
					writer.println(url.trim());
				}
			}
		}
		br.close();
		writer.close();
	}
}