package crawl4j.vsm;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RemoveStopWordsUtility {


	private static List<String> stopWords=new ArrayList<String>();
	private static String french_stopword_path = "/home/sduprey/My_Data/My_Semantics_Data/stopwords_fr.txt";

	public static void loadFrenchStopWords(){
		//Read File Line By Line
		try {
			FileInputStream fstream = new FileInputStream(french_stopword_path);
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String strLine;
			while ((strLine = br.readLine()) != null)   {
				stopWords.add(strLine);
			}
			//Close the input stream
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
     Remove Stop Word in String
	 */
	public static Map<String, Integer> removeStopWords(String rowValue) {
		Pattern pattern = Pattern.compile("[\\wàáâãäçèéêëœìíîïñòóôõöùúûüÿÁÀÂÄÈÉÊËÎÏÔŒÙÛÜŸÇŒ]+");
		Map<String, Integer> tf_map = new HashMap<String, Integer>();
		//Pattern pattern = Pattern.compile("[\\w]+");
		Matcher matcher = pattern.matcher(rowValue);
		//Récupération des mots de la chaine
		while (matcher.find()) {
			String word = matcher.group();
			// we don't add stop words
			if (!stopWords.contains(word)){
				Integer counter = tf_map.get(word);
				if (counter == null){
					tf_map.put(word, new Integer(1));
				}else{
					tf_map.put(word, ++counter);
				}
			}
		}
		return tf_map;
	}
}