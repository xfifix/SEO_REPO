package crawl4j.vsm;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class NgramsTest {

	public static void main(String[] args) {
		try {

			
			String str = "le loup rôde dans le jardin et cherche des lapins. Il a faim, plus que faim et aimerait en manger un ou deux";

			Analyzer two_analyzer = new NGramsAnalyzer();

			TokenStream two_stream = two_analyzer.tokenStream("content", new StringReader(str));
			Token two_token = new Token();
			while ((two_token = two_stream.next(two_token)) != null){
				System.out.println(two_token.term());
			}
		} catch (IOException ie) {
			System.out.println("IO Error " + ie.getMessage());
		}
	}
}
