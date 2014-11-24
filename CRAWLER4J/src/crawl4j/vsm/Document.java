package crawl4j.vsm;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

@SuppressWarnings("deprecation")
public class Document {

	private File file;
	private int termSize;
	private HashMap<String, Integer>  wordFrequencies;
	private HashMap<Term, Integer> termFrequencies;
	private HashMap<Term, Double> termScores;
	private int senseSize;
	private HashMap<WordNetSense, Integer> senseFrequencies;
	private HashMap<WordNetSense, Double> senseScores;
	//	private HashMap<Document, Double> similarities;
	private HashMap<Document, Double> termSimilarities;
	private HashMap<Document, Double> senseSimilarities;
	private List<Term> orderedTerms;
	private Node node;
	private HashMap<Double, Integer> neighborhoodSizes;

	private static List<Document> allDocuments;
	public static File stop_words;
	private static String conceptLevel;
	private static Double conceptAlpha;
	private static String wsdType;
	private static Integer wsdContext;
	private static Boolean sensesLog;
	private static String sensesLogPath;
	private static OutputStreamWriter sensesLogger;

	public static Boolean windowContext(){
		return (wsdContext > 0);
	}

	public static boolean includeSynonyms(){
		return("synonyms".equals(conceptLevel) || "hypernyms".equals(conceptLevel));
	}

	public static boolean includeHypernyms(){
		return("hypernyms".equals(conceptLevel));
	}

	public static boolean includeWsd(){
		return (!"none".equals(wsdType));
	}

	public static boolean wsdPrimitive(){
		return ("primitive".equals(wsdType) || "expanded_primitive".equals(wsdType)) || "simplified_lesk".equals(wsdType) || ("simplified_lesk_plus".equals(wsdType));
	}

	public static boolean wsdAdaptedLesk(){
		return ("adapted_lesk".equals(wsdType) || "adapted_lesk_plus".equals(wsdType));
	}

	public static boolean wsdAdaptedLeskPlus(){
		return ("adapted_lesk_plus".equals(wsdType));
	}

	public static boolean wsdExpandedPrimitive(){
		return ("expanded_primitive".equals(wsdType));
	}

	public static boolean wsdSimplifiedLesk(){
		return ("simplified_lesk".equals(wsdType)) || ("simplified_lesk_plus".equals(wsdType));
	}

	public static boolean wsdSimplifiedLeskPlus(){
		return ("simplified_lesk_plus".equals(wsdType));
	}

	public Document(File file, Boolean isDump){
		this.file = file;
		this.termSize = 0;
		termFrequencies = new HashMap<Term, Integer>();
		wordFrequencies = new HashMap<String, Integer>();
		termScores = new HashMap<Term, Double>();
		termSimilarities = new HashMap<Document, Double>();
		if (includeSynonyms()){
			this.senseSize = 0;
			senseFrequencies = new HashMap<WordNetSense, Integer>();
			senseScores = new HashMap<WordNetSense, Double>();
			senseSimilarities = new HashMap<Document, Double>();
		}
		try {
			BufferedReader docReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(file))));
			String line;
			while((line = docReader.readLine()).length() > 0){
				String[] lineParts = line.split(":");
				addTerm(lineParts[0], new Integer(lineParts[lineParts.length - 1]));
			}
			while((line = docReader.readLine()) != null){
				String[] lineParts = line.split(":");
				addSense(new Integer(lineParts[0]), new Integer(lineParts[1]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public Document(File file){
		this.file = file;
		this.termSize = 0;
		termFrequencies = new HashMap<Term, Integer>();
		wordFrequencies = new HashMap<String, Integer>();
		termScores = new HashMap<Term, Double>();
		//		similarities = new HashMap<Document, Double>();
		termSimilarities = new HashMap<Document, Double>();
		orderedTerms = new ArrayList<Term>();
		if (includeSynonyms()){
			this.senseSize = 0;
			senseFrequencies = new HashMap<WordNetSense, Integer>();
			senseScores = new HashMap<WordNetSense, Double>();
			senseSimilarities = new HashMap<Document, Double>();
		}
		node = new Node();
		neighborhoodSizes = new HashMap<Double, Integer>();

		if (sensesLog){
			try {
				sensesLogger = new OutputStreamWriter(new FileOutputStream(new File(sensesLogPath + "/" + getFileName())));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		analyze();

		if (sensesLog){
			try {
				sensesLogger.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public HashMap<String, Integer> getWordFrequencies() {
		return wordFrequencies;
	}

	public void setWordFrequencies(HashMap<String, Integer> wordFrequencies) {
		this.wordFrequencies = wordFrequencies;
	}

	private Document(List<Document> aggregate){
		termScores = new HashMap<Term, Double>();
		//		similarities = new HashMap<Document, Double>();
		termSimilarities = new HashMap<Document, Double>();
		orderedTerms = new ArrayList<Term>();
		this.termSize = 0;
		Iterator<Term> termsIterator = Term.getAllTerms().iterator();
		Term term;
		Double scoresSum;
		Iterator<Document> docsIterator;
		while (termsIterator.hasNext()){
			term = termsIterator.next();
			docsIterator = aggregate.iterator();
			scoresSum = 0.0;
			while (docsIterator.hasNext()){
				scoresSum += docsIterator.next().getScore(term);
			}
			if (scoresSum > 0.0001){
				termSize ++;
				termScores.put(term, scoresSum/aggregate.size());
			}
		}
		if (includeSynonyms()){
			senseFrequencies = new HashMap<WordNetSense, Integer>();
			senseScores = new HashMap<WordNetSense, Double>();
			senseSimilarities = new HashMap<Document, Double>();
			this.senseSize = 0;
			Iterator<WordNetSense> sensesIterator = WordNetSense.getAllSenses().iterator();
			WordNetSense sense;
			while (sensesIterator.hasNext()){
				sense = sensesIterator.next();
				docsIterator = aggregate.iterator();
				scoresSum = 0.0;
				while (docsIterator.hasNext()){
					scoresSum += docsIterator.next().getScore(sense);
				}
				if (scoresSum > 0.0001){
					senseSize ++;
					senseScores.put(sense, scoresSum/aggregate.size());
				}
			}
		}
	}

	public String getFileName(){
		return file.getName();
	}

	public File getFile(){
		return file;
	}

	public static List<Document> getAllDocuments(){
		return allDocuments;
	}

	public Double getScore(Term term){
		Double score = termScores.get(term);
		if (score == null){
			return 0.0;
		}
		return score;
	}

	public Double getScore(WordNetSense sense){
		Double score = senseScores.get(sense);
		if (score == null){
			return 0.0;
		}
		return score;
	}

	private void analyze(){
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer(stop_words);
			TokenStream stream = analyzer.tokenStream("text", new FileReader(file));
			Token token = stream.next();
			while ( token != null ){
				addToken(token);
				token = stream.next();
			}
			addSensesIfIncluded(orderedTerms);
		} catch (FileNotFoundException e) {
			System.out.println("Failed to load document: " + file.getName());
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Error tokenizing document: " + file.getName());
			e.printStackTrace();
		}
	}

	private Integer getOverlap(List<String> senseGlossTerms, Term docTerm, WordNetSense docTermSense){
		Integer overlap = 0;
		//for each gloss term of the tested sense
		for (String glossTerm : senseGlossTerms){
			// match: context term == a gloss term of the tested sense
			if (docTerm.getValue().equals(glossTerm)){
				if (wsdSimplifiedLeskPlus()){
					overlap += 4;
				} else {
					overlap ++;
				}
			}
			List<String> docTermSenseGlossTerms = docTermSense.getGlossTerms();
			// for each term of the sense of the context term
			for (String docTermSenseGlossTerm : docTermSenseGlossTerms){
				// match: a gloss term of a sense of a context term == a gloss term of the tested sense
				if (docTermSenseGlossTerm.equals(glossTerm)){
					overlap ++;
				}
			}
		}
		return overlap;
	}

	private Integer getAdaptedLeskPlusOverlap(WordNetSense sense1, WordNetSense sense2){
		if (sense1 == null || sense2 == null){
			return 0;
		}
		List<List<Integer[]>> subsequences = new ArrayList<List<Integer[]>>();
		List<String> gloss1 = sense1.getGlossTerms();
		List<String> gloss2 = sense2.getGlossTerms();
		for (int i=0; i<gloss1.size(); i++){
			for (int j=0; j<gloss2.size(); j++){
				if (gloss1.get(i).equals(gloss2.get(j))){
					//search for a prefix
					Boolean added = false;
					Integer[] match = {i,j};
					for (int k=0; k<subsequences.size(); k++){
						List<Integer[]> subsequence = subsequences.get(k);
						Integer[] tail = subsequence.get(subsequence.size()-1);
						if (tail[0].equals(i-1) && tail[1].equals(j-1)){
							subsequence.add(match);
							added = true;
							break;
						}
					}
					if (!added){
						List<Integer[]> newSubsequence = new ArrayList<Integer[]>();
						newSubsequence.add(match);
						subsequences.add(newSubsequence);
					}
				}
			}
		}
		Integer overlap = 0;
		for (List<Integer[]> subsequence : subsequences){
			Integer subSize = subsequence.size();
			overlap += (subSize * subSize);
		}
		return overlap;
	}

	private Integer getAdaptedLeskOverlap(WordNetSense sense1, WordNetSense sense2){
		if (sense1 == null || sense2 == null){
			return 0;
		}
		Integer overlap = 0;
		for (String sense1GlossTerm : sense1.getGlossTerms()){
			for (String sense2GlossTerm : sense2.getGlossTerms()){
				if (sense1GlossTerm.equals(sense2GlossTerm)){
					overlap ++;
				}
			}
		}

		return overlap;
	}

	private static void logSenses(Term term, List<WordNetSense> senses){
		if (sensesLog){
			try {
				sensesLogger.write(term.getValue() + "\n");
				for (WordNetSense sense : senses){
					sensesLogger.write(sense.getPos() + ": " + sense.getGloss() + "\n");
				}
				sensesLogger.write("\n\n");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private List<WordNetSense> refineAdaptedLesk(Integer termContextIndex, List<Term> context, HashMap<Term, List<WordNetSense>> termSenses){
		if (!windowContext()){
			throw new UnsupportedOperationException("wsd_type: adapted_lesk must be used with wsd_context > 0");
		}
		List<WordNetSense> maxSenses = new ArrayList<WordNetSense>();

		// Get lists of context senses 
		List<List<WordNetSense>> contextSenses = new ArrayList<List<WordNetSense>>();
		List<Integer> contextIndexes = new ArrayList<Integer>();
		for (Term neighbor : context){
			contextSenses.add(termSenses.get(neighbor));
			contextIndexes.add(0);
		}

		// Loop on all combinations
		Boolean done = false;
		Integer maxScore = -1;
		while (!done){
			// combination available at contextSenses.get(i).get(contextIndexes.get(i)) for all i
			List<WordNetSense> combination = new ArrayList<WordNetSense>();
			for (int i=0; i<context.size(); i++){
				List<WordNetSense> neighborSenses = contextSenses.get(i);
				if (neighborSenses.size() > 0){
					combination.add(neighborSenses.get(contextIndexes.get(i)));
				} else { // term with 0 senses
					combination.add(null);
				}
			}

			Integer combinationScore = 0;
			// Looping on combination pairs i & j
			for (int i=0; i<combination.size(); i++){
				for (int j=i+1; j<combination.size(); j++){
					if (wsdAdaptedLesk()){
						combinationScore += getAdaptedLeskOverlap(combination.get(i), combination.get(j));
					} else {
						combinationScore += getAdaptedLeskPlusOverlap(combination.get(i), combination.get(j));
					}
				}
			}

			// check for max?
			WordNetSense candidate = combination.get(termContextIndex);
			if (maxScore == combinationScore && candidate != null && !maxSenses.contains(candidate)){
				maxSenses.add(candidate);
			} else if (maxScore < combinationScore && candidate != null){
				maxScore = combinationScore;
				maxSenses = new ArrayList<WordNetSense>();
				maxSenses.add(candidate);
			}

			// start advancing indexes until one is not at its end
			for (int i=0; i<contextIndexes.size(); i++){
				Integer index = contextIndexes.get(i);
				index ++;
				if (index < contextSenses.get(i).size()){ // still within range: set and break
					contextIndexes.set(i, index);
					break;
				} else { // end of its range: reset and continue to next index
					contextIndexes.set(i, 0);
					if (i == (contextIndexes.size() - 1)){ // resetting highest significant index: terminate
						done = true;
					}
				}
			}
		}

		return maxSenses;
	}

	private List<WordNetSense> refineSenses(Integer termContextIndex, List<Term> context, HashMap<Term, List<WordNetSense>> termSenses){
		Term term = context.get(termContextIndex);
		List<WordNetSense> senses = termSenses.get(term);
		List<WordNetSense> selectedSenses = new ArrayList<WordNetSense>();
		if (wsdPrimitive()){
			Integer maxMatch = -1;
			// for each sense of these to be filtered
			for (WordNetSense sense : senses){
				List<String> glossTerms = sense.getGlossTerms();
				Integer match = 0;
				// for each term in the context
				for (Term contextTerm : context){
					// ignore congruent terms
					if (term.getValue().equals(contextTerm.getValue())){
						continue;
					}
					if (wsdSimplifiedLesk()){
						Integer maxOverlap = 0;
						// for each sense of the context term
						for (WordNetSense docTermSense : termSenses.get(contextTerm)){
							Integer overlap = getOverlap(glossTerms, contextTerm, docTermSense);
							if (overlap > maxOverlap){
								maxOverlap = overlap;
							}
						}
						match += maxOverlap;
					} else {
						//for each gloss term of the tested sense
						for (String glossTerm : glossTerms){
							// match: context term == a gloss term of the tested sense
							if (contextTerm.getValue().equals(glossTerm)){
								match ++;
							}
							if (wsdExpandedPrimitive()){
								// for each sense of the context term
								for (WordNetSense docTermSense : termSenses.get(contextTerm)){
									List<String> docTermSenseGlossTerms = docTermSense.getGlossTerms();
									// for each term of the sense of the context term
									for (String docTermSenseGlossTerm : docTermSenseGlossTerms){
										// match: a gloss term of a sense of a context term == a gloss term of the tested sense
										if (docTermSenseGlossTerm.equals(glossTerm)){
											match ++;
										}
									}
								}
							}
						}
					}
				}
				if (match == maxMatch){
					selectedSenses.add(sense);
				} else if (match > maxMatch){
					maxMatch = match;
					selectedSenses = new ArrayList<WordNetSense>();
					selectedSenses.add(sense);
				}
			}
		} else if (wsdAdaptedLesk()){
			selectedSenses = refineAdaptedLesk(termContextIndex, context, termSenses);
		}
		return selectedSenses;
	}

	private void addSensesIfIncluded(List<Term> docTerms){
		if (includeSynonyms()){
			HashMap<Term, List<WordNetSense>> termSenses = new HashMap<Term, List<WordNetSense>>();
			for (Term term : docTerms){
				termSenses.put(term, WordNetSense.getSenses(term.getValue()));
			}
			for (int i=0; i<docTerms.size(); i++){
				Term term = docTerms.get(i);

				List<Term> context = null;
				Integer termContextIndex = null;
				if (windowContext()){
					//extract context sublist
					//start is inclusive
					Integer start = i - wsdContext;
					termContextIndex = wsdContext;
					if (start < 0){
						start = 0;
						termContextIndex = i;
					}
					//end is exclusive
					Integer end = i + wsdContext + 1;
					if (end > docTerms.size()){
						end = docTerms.size();
					}
					context = docTerms.subList(start, end);
				} else {
					termContextIndex = i;
					context = docTerms;
				}

				List<WordNetSense> senses = termSenses.get(term);

				System.out.print(term.getValue() + " ");

				if (includeWsd()){
					senses = refineSenses(termContextIndex, context, termSenses);
				}

				senseSize += senses.size();
				for (WordNetSense sense : senses){
					Integer senseFrequency = senseFrequencies.get(sense);
					if ( senseFrequency == null ){
						senseFrequencies.put(sense, 1);
						sense.addDocument(this);
					}else{
						senseFrequencies.put(sense, senseFrequency + 1);
					}
					if (includeHypernyms()){
						for (WordNetSense hyperHypoSense : sense.getHyperHypoSenses()){
							senseFrequency = senseFrequencies.get(hyperHypoSense);
							if ( senseFrequency == null ){
								senseFrequencies.put(hyperHypoSense, 1);
								hyperHypoSense.addDocument(this);
							}else{
								senseFrequencies.put(hyperHypoSense, senseFrequency + 1);
							}
						}
					}
				}
				logSenses(term, senses);
			}
			System.out.println();
		}
	}

	private void addToken(Token token){
		termSize ++;
		// in english we choose to keep up just the stem
		// Term term = Term.getTerm(token.term(), false);
		// in french we bypass it (verb != adjective != adverb != noun)
		Term term = Term.getTerm(token.term(), true);
		orderedTerms.add(term);
		Integer termFrequency = termFrequencies.get(term);
		if ( termFrequency == null ){
			termFrequencies.put(term, 1);
			term.addDocument(this);
		}else{
			termFrequencies.put(term, termFrequency + 1);
		}
		// @todo : to optimize, we do the same methodology for word/frequency
		Integer wordFrequency = wordFrequencies.get(token.term());
		if ( wordFrequency == null ){
			wordFrequencies.put(token.term(), 1);
		}else{
			wordFrequencies.put(token.term(), wordFrequency + 1);
		}
		
	}

	private void addSense(Integer senseId, Integer frequency){
		senseSize += frequency;
		WordNetSense sense = WordNetSense.getSense(senseId);
		sense.addDocument(this);
		senseFrequencies.put(sense, frequency);
	}

	private void addTerm(String stem, Integer frequency){
		termSize += frequency;
		Term term = Term.getTerm(stem, true);
		term.addDocument(this);
		termFrequencies.put(term, frequency);
	}

	private Double tfIdf(Term term){
		Integer frequency = termFrequencies.get(term);
		Double tf = new Double(frequency) / termSize;
		Double idf = Math.log(new Double(1+allDocuments.size()) / (1+term.documentFrequency()));
		return tf * idf;
	}

	private Double tfIdf(WordNetSense sense){
		Integer frequency = senseFrequencies.get(sense);
		Double tf = new Double(frequency) / senseSize;
		Double idf = Math.log(new Double(1+allDocuments.size()) / (1+sense.documentFrequency()));
		return tf * idf;
	}

	private Double cosineSimilarity(Document doc){
		Double sim;
		//		sim = similarities.get(doc);
		//		if (sim == null){
		if (includeSynonyms()){
			sim = conceptAlpha*sensesCosineSimilarity(doc) + (1.0 - conceptAlpha)*termsCosineSimilarity(doc);
		} else {
			sim = termsCosineSimilarity(doc);
		}
		//			similarities.put(doc, sim);
		//			doc.similarities.put(this, sim);
		//		}
		return sim;
	}

	private Double termsCosineSimilarity(Document doc){
		Double sim;
		sim = termSimilarities.get(doc);
		if (sim == null){
			sim = this.termsDotProduct(doc) / (this.termsMagnitude() * doc.termsMagnitude());
			termSimilarities.put(doc, sim);
			doc.termSimilarities.put(this, sim);
		}
		return sim;
	}

	private Double sensesCosineSimilarity(Document doc){
		Double sim;
		sim = senseSimilarities.get(doc);
		if (sim == null){
			sim = this.sensesDotProduct(doc) / (this.sensesMagnitude() * doc.sensesMagnitude());
			senseSimilarities.put(doc, sim);
			doc.senseSimilarities.put(this, sim);
		}
		return sim;
	}

	private Double termsDotProduct(Document doc){
		Iterator<Term> termsIterator = Term.getAllTerms().iterator();
		Double product = 0.0;
		while ( termsIterator.hasNext() ){
			Term term = termsIterator.next();
			product = product + this.getScore(term) * doc.getScore(term);
		}
		return product;
	}

	private Double sensesDotProduct(Document doc){
		Iterator<WordNetSense> sensesIterator = WordNetSense.getAllSenses().iterator();
		Double product = 0.0;
		while ( sensesIterator.hasNext() ){
			WordNetSense sense = sensesIterator.next();
			product = product + this.getScore(sense) * doc.getScore(sense);
		}
		return product;
	}

	private Double termsMagnitude(){
		return Math.sqrt(termsDotProduct(this));
	}

	private Double sensesMagnitude(){
		return Math.sqrt(sensesDotProduct(this));
	}

	public Double magnitude(){
		return termsMagnitude();
	}

	private void calculateScores(){
		Iterator<Term> termsIterator = termFrequencies.keySet().iterator();
		while ( termsIterator.hasNext() ){
			Term term = termsIterator.next();
			termScores.put(term, tfIdf(term));
		}
		if (includeSynonyms()){
			Iterator<WordNetSense> sensesIterator = senseFrequencies.keySet().iterator();
			while ( sensesIterator.hasNext() ){
				WordNetSense sense = sensesIterator.next();
				senseScores.put(sense, tfIdf(sense));
			}
		}
	}

	public void saveRepresentation(String saveDir){
		try {
			new File(saveDir).mkdir();
			OutputStreamWriter docWriter = new OutputStreamWriter(new FileOutputStream(new File(saveDir + "/" + getFileName())));
			for (Term term : termFrequencies.keySet()){
				docWriter.write(term.getValue() + ":" + termFrequencies.get(term).toString() + "\n");
			}
			docWriter.write("\n");
			for (WordNetSense sense : senseFrequencies.keySet()){
				docWriter.write(sense.getSenseId() + ":" + senseFrequencies.get(sense).toString() + "\n");
			}
			docWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Document loadDocument(String docPath,
			String stopWordsPath,
			String desiredConceptLevel,
			String desiredWsdType,
			Integer desiredWsdContext,
			Boolean doSensesLog,
			String senseLogDir){
		stop_words = new File(stopWordsPath);
		conceptLevel = desiredConceptLevel;
		wsdType = desiredWsdType;
		wsdContext = desiredWsdContext;
		sensesLog = doSensesLog;
		if (sensesLog){
			sensesLogPath = senseLogDir + "/" + conceptLevel + " - " + wsdType + " - " + wsdContext;
			new File(sensesLogPath).mkdir();
		}
		System.out.println("Logging: " + sensesLog.toString());
		System.out.println("Windowing: " + windowContext().toString() + " " + wsdContext.toString());
		return new Document(new File(docPath));
	}

	public static void loadFromDump(String dumpPath, Double desiredConceptAlpha, String desiredConceptLevel){
		allDocuments = new ArrayList<Document>();
		conceptLevel = desiredConceptLevel;
		conceptAlpha = desiredConceptAlpha;
		Iterator<File> iterator = Arrays.asList(new File(dumpPath).listFiles()).iterator();
		int filesCounter=0;
		while ( iterator.hasNext() ){
			filesCounter ++;
			System.out.println("loading doc:" + filesCounter);
			allDocuments.add(new Document(iterator.next(), true));
		}
		calculateAllScores();
		System.out.println("Documents loaded");
	}

	public static void setDocumentProperties(
			String stopWordsPath,
			String desiredConceptLevel,
			Double desiredConceptAlpha,
			String desiredWsdType,
			Integer desiredWsdContext,
			Boolean doSensesLog,
			String senseLogDir
			){
		stop_words = new File(stopWordsPath);
		allDocuments = new ArrayList<Document>();
		conceptLevel = desiredConceptLevel;
		conceptAlpha = desiredConceptAlpha;
		wsdType = desiredWsdType;
		wsdContext = desiredWsdContext;
		sensesLog = doSensesLog;
		if (sensesLog){
			sensesLogPath = senseLogDir + "/" + conceptLevel + " - " + wsdType + " - " + wsdContext;
			new File(sensesLogPath).mkdir();
		}
		System.out.println("Logging: " + sensesLog.toString());
		System.out.println("Windowing: " + windowContext().toString() + " " + wsdContext.toString());
		System.out.println("Documents properties set");
	}

	public static void loadDocuments(String path,
			String stopWordsPath,
			String desiredConceptLevel,
			Double desiredConceptAlpha,
			String desiredWsdType,
			Integer desiredWsdContext,
			Boolean doSensesLog,
			String senseLogDir
			){
		stop_words = new File(stopWordsPath);
		allDocuments = new ArrayList<Document>();
		conceptLevel = desiredConceptLevel;
		conceptAlpha = desiredConceptAlpha;
		wsdType = desiredWsdType;
		wsdContext = desiredWsdContext;
		sensesLog = doSensesLog;
		Iterator<File> iterator = Arrays.asList(new File(path).listFiles()).iterator();
		int filesCounter=0;
		if (sensesLog){
			sensesLogPath = senseLogDir + "/" + conceptLevel + " - " + wsdType + " - " + wsdContext;
			new File(sensesLogPath).mkdir();
		}
		System.out.println("Logging: " + sensesLog.toString());
		System.out.println("Windowing: " + windowContext().toString() + " " + wsdContext.toString());
		while ( iterator.hasNext() ){
			filesCounter ++;
			System.out.println("loading doc:" + filesCounter);
			allDocuments.add(new Document(iterator.next()));
		}
		calculateAllScores();
		System.out.println("Documents loaded");
	}

	private static void calculateAllScores(){
		Iterator<Document> docsIterator = allDocuments.iterator();
		while ( docsIterator.hasNext() ){
			docsIterator.next().calculateScores();
		}
	}

	public Double getSimilarity(Document doc){
		return cosineSimilarity(doc);
	}

	public boolean isEquivalent(Document doc){
		return this.termsCosineSimilarity(doc) > 0.9999 && this.termsMagnitude() - doc.termsMagnitude() < 0.0001;
	}

	public static Document mean(List<Document> documents){
		return new Document(documents);
	}

	public static void resetAllSimilarities(Double desiredConceptAlpha){
		resetConceptSetting(desiredConceptAlpha);
		for (Document doc : allDocuments){
			doc.resetSimilarities();
		}
	}

	public static void resetConceptSetting(Double desiredConceptAlpha){
		conceptAlpha = desiredConceptAlpha;
		//		conceptLevel = desiredConceptLevel;
	}

	public void resetSimilarities(){
		termSimilarities = new HashMap<Document, Double>();
		senseSimilarities = new HashMap<Document, Double>();
		//		similarities = new HashMap<Document, Double>();
		neighborhoodSizes = new HashMap<Double, Integer>();
	}

	public void visit(){
		node.visited = true;
	}

	public void markAsNoise(){
		node.noise = true;
	}

	public void markAsClustered(){
		node.clustered = true;
	}

	public boolean isVisited(){
		return node.visited;
	}

	public boolean isNoise(){
		return node.noise;
	}

	public boolean isClustered(){
		return node.clustered;
	}

	public Integer getNeighborHoodSize(Double neighborhood){
		Integer size = neighborhoodSizes.get(neighborhood);
		if (size == null){
			size = calculateNeighborhoodSize(neighborhood);
			neighborhoodSizes.put(neighborhood, size);
		}
		return size;
	}


	private Integer calculateNeighborhoodSize(Double neighborhood) {
		Integer size = 0;
		for (Iterator<Document> docsIterator = allDocuments.iterator(); docsIterator.hasNext();){
			Document doc = docsIterator.next();
			if (doc == this)
				continue;
			if (getSimilarity(doc) > neighborhood)
				size ++;
		}
		return size;
	}

	public String toString(){
		Iterator<Term> termsIterator = termFrequencies.keySet().iterator();
		StringBuilder result = new StringBuilder(file.getName()).append(": ").append(termSize).append(" words\n");
		Term term;
		while ( termsIterator.hasNext() ){
			term = termsIterator.next();
			result.append(term.getValue()).append(": frequency(").append(termFrequencies.get(term).toString()).append("), score(").append(termScores.get(term).toString()).append(")\n");
		}
		return result.append("\n").toString();
	}

	public static String allToString(){
		Iterator<Document> docsIterator = allDocuments.iterator();
		StringBuilder result = new StringBuilder("");
		while ( docsIterator.hasNext() ){
			result.append(docsIterator.next());
		}
		return result.toString();
	}

	public static void printAllSimilarities(){
		Iterator<Document> docsIterator = allDocuments.iterator();
		while ( docsIterator.hasNext() ){
			Document doc = docsIterator.next();
			System.out.print(doc.file.getName() + ": ");
			Iterator<Document> othersIterator = allDocuments.iterator();
			while (othersIterator.hasNext() ){
				Document other = othersIterator.next();
				if ( doc == other ){ continue; }
				System.out.print("(" + other.file.getName() + ": " + doc.getSimilarity(other) + ")");
			}
			System.out.println();
		}
	}

	public void resetNode() {
		node = new Node();
	}

	public static Double getConceptAlpha() {
		return conceptAlpha;
	}

}
