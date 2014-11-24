package crawl4j.vsm;

import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;



public class TfIdfTest {
	private static String config_path = "/home/sduprey/My_Code/My_Java_Workspace/CRAWLER4J/similarity_config/";
	public static Properties properties;
	public  static File stop_words;
	
	public static void main(String[] args){
		loadProperties();
		// getting the french stop words 
		stop_words = new File(properties.getProperty("config.stop_words_path"));

		String text1 = "le chat sort du jardin et va dans la forêt";
		String text2 = "le loup rôde dans le jardin et cherche des lapins";
		computeSimilarity(text1, text2);
		
		//magasin_to_filter=properties.getProperty("data.magasintofilter");
//		String url = properties.getProperty("db.url");
//		String user = properties.getProperty("db.user");
//		String mdp = properties.getProperty("db.passwd");    
//		// database variables
//		try {
//			con = DriverManager.getConnection(url, user, mdp);
//			//yafetch_distinct_magasin_and_rayon();
//			//fetch_rayon();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.out.println("Trouble with the database");
//			System.exit(0);
//		}
	}
	
	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(config_path+"properties")));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}
	
	private static Double computeSimilarity(String text1, String text2) {
		VectorStateSpringRepresentation vs1 =new VectorStateSpringRepresentation(text1);
		VectorStateSpringRepresentation vs2 =new VectorStateSpringRepresentation(text2);
		return cosine_similarity(vs1.getWordFrequencies() , vs2.getWordFrequencies());
	}

	private static double cosine_similarity(Map<String, Integer> v1, Map<String, Integer> v2) {
		Set<String> both = new HashSet<String>(v1.keySet());
		both.retainAll(v2.keySet());
		double sclar = 0, norm1 = 0, norm2 = 0;
		for (String k : both) sclar += v1.get(k) * v2.get(k);
		for (String k : v1.keySet()) norm1 += v1.get(k) * v1.get(k);
		for (String k : v2.keySet()) norm2 += v2.get(k) * v2.get(k);
		return sclar / Math.sqrt(norm1 * norm2);
	}
}
