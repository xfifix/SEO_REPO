package vsm;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class VectorStateSpringRepresentation {

	private HashMap<String, Integer>  wordFrequencies =  new HashMap<String, Integer>() ;

	public HashMap<String, Integer> getWordFrequencies() {
		return wordFrequencies;
	}

	public void setWordFrequencies(HashMap<String, Integer> wordFrequencies) {
		this.wordFrequencies = wordFrequencies;
	}

	public VectorStateSpringRepresentation(String ztd_content){
		// TODO Auto-generated method stub
		try{
			StandardAnalyzer analyzer = new StandardAnalyzer(ProcessInsideMagasinSimilarity.stop_words);
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
		} catch (IOException e) {
			System.out.println("Error tokenizing document");
			e.printStackTrace();
		}
	}
}
