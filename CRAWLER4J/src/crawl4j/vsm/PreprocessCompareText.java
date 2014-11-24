package crawl4j.vsm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PreprocessCompareText {
	public static Properties properties;
	public  static File stop_words;
	private static Map<String,String> urls = new HashMap<String,String>();
	private static List<String> url_file = new ArrayList<String>();
	private static ArrayList<String> ztd_content = new ArrayList<String>();
	// storage to improve : the matrix is symmetric
	private static Double[][] distanceMatrix;
	public static void main(String[] args) {
		loadProperties();
		stop_words = new File(properties.getProperty("config.stop_words_path"));
//		Document.setDocumentProperties(properties.getProperty("config.stop_words_path"),
//				properties.getProperty("config.concept_level"),
//				new Double(properties.getProperty("config.concept_alpha")), 
//				properties.getProperty("config.wsd_type"),
//				new Integer(properties.getProperty("config.wsd_context")),
//				"true".equals(properties.getProperty("config.senses_log")),
//				properties.getProperty("config.senses_log_dir"));
		PreprocessCompareText obj = new PreprocessCompareText();
		obj.parse();
		obj.buildComparisonMatrix();
		System.out.println(distanceMatrix);
	}

	public void parse() {
		String csvFile = properties.getProperty("data.csv_file_path");
		String ztdStorePath = properties.getProperty("data.ztd_store_path");
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ";";
		try {
			int i=1;
			br = new BufferedReader(new FileReader(csvFile));
			// we skip the first line
			line = br.readLine();
			while ((line = br.readLine()) != null) {
				// use comma as separator
				String[] splitted_line = line.split(cvsSplitBy);
				System.out.println("Line number" + i);
				if (splitted_line.length >= 2){
					System.out.println("Country [URL= " + splitted_line[0] 
							+ " , ZTD length=" + splitted_line[1] + "]");
				}
				if (splitted_line.length >= 3){
					System.out.println("ZTD content= " + splitted_line[2]);
					String proper_file_url_name = splitted_line[0].replace("/","");
					proper_file_url_name = proper_file_url_name.replace(".","");
					proper_file_url_name = proper_file_url_name.replace(":","");
					proper_file_url_name = proper_file_url_name.replace("-","");
					proper_file_url_name = proper_file_url_name.replace("+","");
					proper_file_url_name = proper_file_url_name.replace("%","");
					proper_file_url_name = proper_file_url_name.replace("\"","");
					urls.put(splitted_line[0],proper_file_url_name);
					url_file.add(ztdStorePath+proper_file_url_name);
					ztd_content.add(splitted_line[2]);
					createfile(ztdStorePath, proper_file_url_name, splitted_line[2]);
				}	
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("Done");
	}

	private void createfile(String ztdStorePath, String url, String ztd_text){
		PrintWriter writer;
		try {
			writer = new PrintWriter(ztdStorePath+url);
			writer.println(ztd_text);
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File("config/properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}

	public void buildComparisonMatrix(){
		System.out.println("Computing similarity distance");
		int nb_urls = url_file.size();
		distanceMatrix = new Double[nb_urls][nb_urls]; 
	
		for (int i=0; i< urls.size(); i++){
			distanceMatrix[i][i]=(double) 1;
			for (int j=i+1; j<urls.size(); j++){
				distanceMatrix[i][j] = computeSimilarity(url_file.get(i),url_file.get(j));
				distanceMatrix[j][i] = distanceMatrix[i][j];
				System.out.println("i +"+i+" j + "+j+" = "+distanceMatrix[i][j]);
			}
		}
	}

	private Double computeSimilarity(String ztd1, String ztd2) {
		VectorStateSpringRepresentation vs1 =new VectorStateSpringRepresentation(ztd1);
		VectorStateSpringRepresentation vs2 =new VectorStateSpringRepresentation(ztd2);
		return cosine_similarity(vs1.getWordFrequencies() , vs2.getWordFrequencies());
	}
	
	private double cosine_similarity(Map<String, Integer> v1, Map<String, Integer> v2) {
        Set<String> both = new HashSet<String>(v1.keySet());
        both.retainAll(v2.keySet());
        double sclar = 0, norm1 = 0, norm2 = 0;
        for (String k : both) sclar += v1.get(k) * v2.get(k);
        for (String k : v1.keySet()) norm1 += v1.get(k) * v1.get(k);
        for (String k : v2.keySet()) norm2 += v2.get(k) * v2.get(k);
        return sclar / Math.sqrt(norm1 * norm2);
    }

	public String getProperty(String key){
		return properties.getProperty(key);
	}
	public Properties getProperties(){
		return properties;
	}
}