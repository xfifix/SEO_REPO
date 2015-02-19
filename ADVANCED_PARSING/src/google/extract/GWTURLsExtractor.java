package google.extract;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class GWTURLsExtractor {

	@SuppressWarnings("resource")
	public static void main(String[] args) throws IOException{
		String csvFile = "D:\\My_Data\\My_GWT_Datas\\COMMON_URLS_METADESCR_TITLES\\GWT_Meta_en_double.csv";
		BufferedReader br = null;
		String line = "";
		@SuppressWarnings("unused")
		String header = "";
		br = new BufferedReader(new FileReader(csvFile));
		// we skip the first line : the headers
		header = br.readLine();
		// we write the first line : add balise
		while ((line = br.readLine()) != null) {
			int beginning = line.indexOf("http");
			if (beginning>0){
				line=line.substring(beginning,line.length());	
				int ending = line.indexOf(";");
				if (ending>0){
					line=line.substring(0,ending);
				}
				String[] urls = line.split("\\|");
				for (String url : urls){
					System.out.println(url.trim());
				}
			}
		}
	}
}