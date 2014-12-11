package crawl4j.vsm;

public class TfIdfTest {
	//http://www.seo-camp.org/wp-content/uploads/2009/02/apport_semantique.pdf
	public static void main(String[] args){
		CorpusCache.load();
		String text1 = "le chat sort du jardin et va dans la forêt";
		String text2 = "le loup rôde dans le jardin et cherche des lapins";
		System.out.println("TF similarity" + CorpusCache.computeTFSimilarity(text1, text2));
		System.out.println("TFIDF similarity" + CorpusCache.computeTFSIDFimilarity(text1, text2));
	}
}
		

