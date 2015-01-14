package crawl4j.vsm;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopAnalyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleMatrixFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class NgramAnalyzer extends Analyzer {
	@Override
	public TokenStream tokenStream(String arg0, Reader arg1) {
		// TODO Auto-generated method stub
		return new StopFilter(new LowerCaseFilter(new ShingleMatrixFilter(new StandardTokenizer(arg1),2,2,' ')),
				StopAnalyzer.ENGLISH_STOP_WORDS);

	}
}
