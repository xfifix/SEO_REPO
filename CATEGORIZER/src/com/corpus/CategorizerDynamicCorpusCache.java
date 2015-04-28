package com.corpus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CategorizerDynamicCorpusCache {
	private static String select_corpus_frequency_word_statement="select nb_documents from CATEGORIZER_CORPUS_WORDS where word=?";
	private static String select_totalcount_statement="select count(*) from DATA";
	
	private Connection con;
	
	public CategorizerDynamicCorpusCache(String url, String user, String passwd) throws SQLException{
		con = DriverManager.getConnection(url, user, passwd);
	}

	public Map<String, Double> convert_to_tfidf(Map<String, Integer> tf_bag_of_words) throws SQLException{
		Map<String, Double> normalized_tf_idf_bag_of_words = new HashMap<String,Double>();
		Map<String, Double> tf_idf_bag_of_words = new HashMap<String,Double>();
		Integer nb_total =  get_total_documents_number();
		Iterator<Map.Entry<String, Integer>> it = tf_bag_of_words.entrySet().iterator();
		Double square_sum = new Double(0);
		while (it.hasNext()) {
			Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
			String word=pairs.getKey();
			Integer tf_count=pairs.getValue();
			Integer corpus_frequency = get_corpus_frequency(word);
			Double idf = Math.log10((double)nb_total/(double)corpus_frequency);
			Double numerator = (double)tf_count*idf;
			square_sum=square_sum+numerator*numerator;
			tf_idf_bag_of_words.put(word,numerator);
		}
		tf_bag_of_words.clear();
		Iterator<Map.Entry<String, Double>> tfifd_it = tf_idf_bag_of_words.entrySet().iterator();
		while (tfifd_it.hasNext()) {
			Map.Entry<String, Double> pairs = (Map.Entry<String, Double>)tfifd_it.next();
			String word=pairs.getKey();
			Double tfidf=pairs.getValue();
			normalized_tf_idf_bag_of_words.put(word, tfidf/Math.sqrt(square_sum));
		}
		tf_idf_bag_of_words.clear();
		return normalized_tf_idf_bag_of_words;
	}

	public Integer get_total_documents_number() throws SQLException{
		Integer nb_total_documents=null;
		PreparedStatement pst_total = con.prepareStatement(select_totalcount_statement);
		ResultSet rs_total = pst_total.executeQuery();

		if (rs_total.next()) {
			nb_total_documents = rs_total.getInt(1);	
			System.out.println("Total number of documents found : "+nb_total_documents);
		}	
		pst_total.close();
		return nb_total_documents;
	}

	public Integer get_corpus_frequency(String word) throws SQLException{
		Integer nb_document=1;
		// getting all words from the corpus table
		PreparedStatement pst = con.prepareStatement(select_corpus_frequency_word_statement);
		pst.setString(1, word);
		ResultSet rs = pst.executeQuery();
		if (rs.next()) {
			nb_document = rs.getInt(1);

		}	
		pst.close();
		rs.close();
		return nb_document;
	}
}
