package com.corpus;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class KriterDatabaseCorpusCache {
	private static String select_corpus_frequency_word_statement="select nb_documents from KRITER_CORPUS_WORDS where word=?";
	private static String select_totalcount_statement="select count(*) from CATALOG";

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private Connection con;

	public KriterDatabaseCorpusCache() {
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_con_path);
			props.load(in);
			// the following properties have been identified
			String url = props.getProperty("db.url");
			String user = props.getProperty("db.user");
			String passwd = props.getProperty("db.passwd");
			con = DriverManager.getConnection(url, user, passwd);
		} catch (IOException | SQLException ex) {
			System.out.println("Trouble fetching database configuration");
			ex.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				System.out.println("Trouble fetching database configuration");
				ex.printStackTrace();
			}
		}
	}

	public KriterDatabaseCorpusCache(String url, String user, String passwd) throws SQLException{
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

	
	public Map<String, Integer> computeVectorRepresentation(String text){
		return RemoveStopWordsUtility.removeStopWords(text);
	}

	public  Double computeTFSimilarity(String text1, String text2) {
		if (("".equals(text1))&&("".equals(text2))){
			return (double) 1;
		}
		if (("".equals(text1))&&(!"".equals(text2))){
			return (double) 0;
		}
		if ((!"".equals(text1))&&("".equals(text2))){
			return (double) 0;
		}
		Map<String, Integer> vector1 = computeVectorRepresentation(text1);
		Map<String, Integer> vector2 = computeVectorRepresentation(text2);

		if ((vector1.size() == 0) && vector2.size() ==0){
			return (double) 1;
		}
		if ((vector1.size() != 0) && vector2.size() ==0){
			return (double) 0;
		}
		if ((vector1.size() == 0) && vector2.size() !=0){
			return (double) 0;
		}
		return cosine_tfsimilarity(vector1 , vector2);
	}
	
	public static double cosine_tfsimilarity(Map<String, Integer> v1, Map<String, Integer> v2) {
		Set<String> both = new HashSet<String>(v1.keySet());
		both.retainAll(v2.keySet());
		double sclar = 0, norm1 = 0, norm2 = 0;
		for (String k : both) sclar += v1.get(k) * v2.get(k);
		for (String k : v1.keySet()) norm1 += v1.get(k) * v1.get(k);
		for (String k : v2.keySet()) norm2 += v2.get(k) * v2.get(k);
		return sclar / Math.sqrt(norm1 * norm2);
	}
	
	public  Double computeAlgoWeightedDistance(String text1,String text2){
		Double tf_distance = computeTFSimilarity(text1, text2);
		Integer levenshteing_distance = StringUtils.getLevenshteinDistance(text1, text2);
		return tf_distance+(double)levenshteing_distance;
	}
}
