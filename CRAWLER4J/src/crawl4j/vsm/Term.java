package crawl4j.vsm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Term {
	
	private static List<Term> allTerms = new ArrayList<Term>();
	
	private String value;
	private List<Document> documents;
	
	private Term(String value){
		this.value = value;
		this.documents = new ArrayList<Document>();
	}
	
	public String getValue(){
		return value;
	}
	
	public static HashMap<Term, Double> allTermsZeroScores(){
		HashMap<Term, Double> zeroScores = new HashMap<Term, Double>();
		Iterator<Term> termsIterator = allTerms.iterator();
		while ( termsIterator.hasNext() ){
			zeroScores.put(termsIterator.next(), 0.0);
		}
		return zeroScores;
	}
	
	public int documentFrequency(){
		return documents.size();
	}
	
	public void addDocument(Document document){
		documents.add(document);
	}
	
	public static Term getTerm(String value, Boolean isStem){
		String stem = null;
		if (isStem){
			stem = value;
		} else {
			// beware in french the stem action is totally different
			stem = WordNetUtils.getStem(value);
		}
		Iterator<Term> iterator = allTerms.iterator();
		Term term;
		while ( iterator.hasNext() ){
			term = iterator.next();
			if ( term.value.equals(stem) ){
				return term;
			}
		}
		term = new Term(stem);
		allTerms.add(term);
		return term;
	}
	
	public static String allToString(){
		Iterator<Term> termsIterator = allTerms.iterator();
		StringBuilder result = new StringBuilder("");
		Term term;
		while ( termsIterator.hasNext()){
			term = termsIterator.next();
			result.append(term.getValue()).append(": ").append(term.documentFrequency()).append("\n");
		}
		return result.toString();
	}

	public static List<Term> getAllTerms() {
		return allTerms;
	}

}
