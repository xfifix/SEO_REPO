package crawl4j.vsm;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class VectorStateSpringRepresentation {

	private HashMap<String, Integer>  wordFrequencies =  new HashMap<String, Integer>() ;
	private static StandardAnalyzer analyzer;
	private  static File stop_words;
	private static String stopword_path = "/home/sduprey/My_Data/My_Semantics_Data/stopwords_fr.txt";

	static{
		try{
			stop_words = new File(stopword_path);
			analyzer = new StandardAnalyzer(stop_words);
		} catch (IOException e){
			e.printStackTrace();
		}
	}

	public HashMap<String, Integer> getWordFrequencies() {
		return wordFrequencies;
	}

	public void setWordFrequencies(HashMap<String, Integer> wordFrequencies) {
		this.wordFrequencies = wordFrequencies;
	}

	public VectorStateSpringRepresentation(String ztd_content){
		// TODO Auto-generated method stub
		try{
			TokenStream stream = analyzer.tokenStream("text", new StringReader(ztd_content));
			Token token = stream.next();
			while ( token != null ){
				// @todo : to optimize, we do the same methodology for word/frequency
				Integer wordFrequency = wordFrequencies.get(token.term());
				if ( wordFrequency == null ){
					wordFrequencies.put(token.term(), 1);
				}else{
					wordFrequencies.put(token.term(), wordFrequency + 1);
				}
				token = stream.next();
			}

			//		} catch (FileNotFoundException e) {
			//			System.out.println("Failed to load document: " + file.getName());
			//			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("Error tokenizing document");
			e.printStackTrace();
		}
	}
}
