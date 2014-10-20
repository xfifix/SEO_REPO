package vsm;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

@SuppressWarnings("deprecation")
public class EnglishAnalyzer extends StandardAnalyzer {
	
	public final TokenStream tokenStream(String fieldName, Reader reader){
		return new PorterStemFilter(super.tokenStream(fieldName, reader));
	}
	
	public EnglishAnalyzer(File stopwords) throws IOException{
		super(stopwords);
	}
	
}
