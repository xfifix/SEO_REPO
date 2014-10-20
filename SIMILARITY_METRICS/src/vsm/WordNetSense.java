package vsm;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

@SuppressWarnings("deprecation")
public class WordNetSense {
	
	private static HashMap<Integer, WordNetSense> allSenses = new HashMap<Integer, WordNetSense>();
	
	private int senseId;
	private String pos;
	private List<Document> documents;
	private String gloss;
	private List<String> examples;
	private List<String> glossTerms;
	private List<String> exampleTerms;
	
	private WordNetSense(int senseId, String pos){
		this.senseId = senseId;
		this.pos = pos;
		this.documents = new ArrayList<Document>();
	}
	
	public int getSenseId(){
		return senseId;
	}
	
	public String getGloss(){
		if (gloss == null){
			gloss = WordNetUtils.wordnet.getGloss(senseId);
		}
		return gloss;
	}
	
	public List<String> getExamples(){
		if (examples == null){
			String[] senseExamples = WordNetUtils.wordnet.getExamples(senseId);
			if (senseExamples == null){
				examples = new ArrayList<String>();
			} else {
				examples = Arrays.asList(senseExamples);
			}
		}
		return examples;
	}
	
	public List<String> getGlossTerms(){
		if (glossTerms == null){
			List<String> terms = new ArrayList<String>();
			try {
				StandardAnalyzer analyzer = new StandardAnalyzer(Document.stop_words);
				TokenStream stream = analyzer.tokenStream("text", new StringReader(getGloss()));
				Token token = stream.next();
				while ( token != null ){
					terms.add(WordNetUtils.getStem(token.term()));
					token = stream.next();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			glossTerms = terms;
		}
		return glossTerms;
	}
	
	public List<String> getExampleTerms(){
		if (exampleTerms == null){
			List<String> terms = new ArrayList<String>();
			try {
				StandardAnalyzer analyzer = new StandardAnalyzer(Document.stop_words);
				for (String example : getExamples()){
					TokenStream stream = analyzer.tokenStream("text", new StringReader(example));
					Token token = stream.next();
					while ( token != null ){
						terms.add(WordNetUtils.getStem(token.term()));
						token = stream.next();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			exampleTerms = terms;
		}
		return exampleTerms;
	}
	
	
	public static HashMap<WordNetSense, Double> allSensesZeroScores(){
		HashMap<WordNetSense, Double> zeroScores = new HashMap<WordNetSense, Double>();
		Iterator<WordNetSense> sensesIterator = allSenses.values().iterator();
		while ( sensesIterator.hasNext() ){
			zeroScores.put(sensesIterator.next(), 0.0);
		}
		return zeroScores;
	}
	
	public int documentFrequency(){
		return documents.size();
	}
	
	public void addDocument(Document document){
		documents.add(document);
	}
	
	public static WordNetSense getSense(Integer senseId){
		Iterator<Integer> iterator = allSenses.keySet().iterator();
		while ( iterator.hasNext() ){
			Integer id = iterator.next();
			if ( id.equals(senseId) ){
				return allSenses.get(id);
			}
		}
		WordNetSense sense = new WordNetSense(senseId, WordNetUtils.wordnet.getPos(senseId));
		allSenses.put(senseId, sense);
		return sense;
	}
	
	public static List<WordNetSense> getSenses(String term, String pos){
		List<WordNetSense> senses = new ArrayList<WordNetSense>();
		int[] newIds = null;
		try{
			 newIds = WordNetUtils.wordnet.getSenseIds(term, pos);
		} catch (Exception e){
			return senses;
		}
		for (int i=0; i<newIds.length; i++){
			WordNetSense newSense = allSenses.get(newIds[i]);
			if (newSense == null){
				newSense = new WordNetSense(newIds[i], pos);
				allSenses.put(newSense.senseId, newSense);
			}
			senses.add(newSense);
		}
		return senses;
	}
	
	public static List<WordNetSense> getSenses(String term){
		List<WordNetSense> senses = new ArrayList<WordNetSense>();
		for (String pos : WordNetUtils.poss){
			senses.addAll(getSenses(term, pos));
		}
		return senses;
	}
	
	public static String allToString(){
		Iterator<WordNetSense> sensesIterator = allSenses.values().iterator();
		StringBuilder result = new StringBuilder("");
		WordNetSense sense;
		while ( sensesIterator.hasNext()){
			sense = sensesIterator.next();
			result.append(sense.getSenseId()).append(": ").append(sense.documentFrequency()).append("\n");
		}
		return result.toString();
	}

	public static Collection<WordNetSense> getAllSenses() {
		return allSenses.values();
	}

	public String getPos() {
		return pos;
	}
	
	public List<WordNetSense> getHyperHypoSenses(){
		List<WordNetSense> senses = new ArrayList<WordNetSense>();
		String[] hypernyms = null;
		String[] hyponyms = null;
		try{
			hypernyms = WordNetUtils.wordnet.getHypernyms(this.senseId);
		} catch (Exception e){}
		try{
			hyponyms = WordNetUtils.wordnet.getHyponyms(this.senseId);
		} catch (Exception e){}
		if (hypernyms != null){
			for (String hypernym : hypernyms){
				senses.addAll(getSenses(hypernym, this.pos));
			}
		}
		if (hyponyms != null){
			for (String hyponym : hyponyms){
				senses.addAll(getSenses(hyponym, this.pos));
			}
		}
		return senses;
	}

}
