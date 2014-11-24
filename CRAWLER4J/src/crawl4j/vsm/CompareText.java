package crawl4j.vsm;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class CompareText {
	private static Properties properties;
	private static ArrayList<Document>allDocuments = new ArrayList<Document>();
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		loadProperties();
		Document.setDocumentProperties(properties.getProperty("config.stop_words_path"),
				properties.getProperty("config.concept_level"),
				new Double(properties.getProperty("config.concept_alpha")), 
				properties.getProperty("config.wsd_type"),
				new Integer(properties.getProperty("config.wsd_context")),
				"true".equals(properties.getProperty("config.senses_log")),
				properties.getProperty("config.senses_log_dir"));
		String path = properties.getProperty("data.collection_path");
		Iterator<File> iterator = Arrays.asList(new File(path).listFiles()).iterator();
		int filesCounter=0;

		while ( iterator.hasNext() ){
			filesCounter ++;
			System.out.println("loading doc:" + filesCounter);
			allDocuments.add(new Document(iterator.next()));
		}
		
		// we here actually compute the similarity metric for each document
		for (int i=0; i< allDocuments.size(); i++){
			for (int j=i+1; j<allDocuments.size(); j++){		
				// Get current time
				long start = System.currentTimeMillis();
				// Do something ...
				System.out.println(cosine_similarity(allDocuments.get(i).getWordFrequencies(),allDocuments.get(j).getWordFrequencies()));				
				// Get elapsed time in milliseconds
				long elapsedTimeMillis = System.currentTimeMillis()-start;
				// Get elapsed time in seconds
				float elapsedTimeSec = elapsedTimeMillis/1000F;
				System.out.println("Elapsed time in seconds " + elapsedTimeSec);
			}
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

	public String getProperty(String key){
		return properties.getProperty(key);
	}

	public Properties getProperties(){
		return properties;
	}
	
	static double cosine_similarity(Map<String, Integer> v1, Map<String, Integer> v2) {
        Set<String> both = new HashSet<String>(v1.keySet());
        both.retainAll(v2.keySet());
        double sclar = 0, norm1 = 0, norm2 = 0;
        for (String k : both) sclar += v1.get(k) * v2.get(k);
        for (String k : v1.keySet()) norm1 += v1.get(k) * v1.get(k);
        for (String k : v2.keySet()) norm2 += v2.get(k) * v2.get(k);
        return sclar / Math.sqrt(norm1 * norm2);
    }
	
}
