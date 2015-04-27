package com.predictors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.data.DataEntry;

import crawl4j.vsm.CorpusCache;
import crawl4j.vsm.VectorStateSpringRepresentation;

public class CorpusManager {

	private static Connection con;
	
	private static String find_statement="select DOC_LIST from CATEGORIZER_CORPUS_WORDS where WORD=?";
	private static String insert_statement="INSERT INTO CATEGORIZER_CORPUS_WORDS(WORD,NB_DOCUMENTS,DOC_LIST) values(?,?,?)";
	private static String update_statement="UPDATE CATEGORIZER_CORPUS_WORDS SET NB_DOCUMENTS=?,DOC_LIST=? WHERE WORD=?";

	public CorpusManager(String url,String user,String passwd) throws SQLException{
		con = DriverManager.getConnection(url, user, passwd);
	}

	public void updateEntry(DataEntry entry){
		this.updateText(entry.getLIBELLE_PRODUIT(),entry.getSKU());
	}
	
	public void updateText(String text, String currentUrl){
		// we don't need that as we have updated our stopwords with single letters
		//String semantic_text = CorpusCache.preprocessSemanticText(text);
		String semantic_text = text;
		VectorStateSpringRepresentation vector_rep = new VectorStateSpringRepresentation(semantic_text);
		Map<String, Integer> word_map = vector_rep.getWordFrequencies();
		Iterator<Map.Entry<String, Integer>> it = word_map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
			String word=pairs.getKey();
			// we here don't want any number
			if (!word.matches(".*\\d+.*")){
				System.out.println("Word to add to the corpus : "+word);
				this.updateWord(word, currentUrl);
			}
		}	
	}

	public void updateWord(String word, String currentUrl){
		try{
			// finding if the world is already present in our database dictionary
			PreparedStatement select_st = con.prepareStatement(find_statement);
			select_st.setString(1,word);
			ResultSet rs = select_st.executeQuery();
			boolean found = false;
			String document_list="";
			if (rs.next()) {
				document_list=rs.getString(1);
				found = true;
			}
			select_st.close();
			if (found){	
				// we found it
				// we check if the current url is already in the list
				Set<String> links_set = parse_nodes_out_links(document_list);
				boolean isAlreadyPresent=links_set.contains(currentUrl);

				// if the document is already listed, the frequency is up to date, we do nothing
				if (!isAlreadyPresent){
					// else if the document is not present, we must update the database row by adding the document
					links_set.add(currentUrl);
					PreparedStatement update_st = con.prepareStatement(update_statement);
					//NB_DOCUMENTS=?,DOCUMENTS=? WHERE WORD=?
					update_st.setInt(1,links_set.size());
					update_st.setString(2,links_set.toString());
					update_st.setString(3,word);
					update_st.executeUpdate();
				}
			}else{
				// we did not find it	
				// we create the new document set
				Set<String> documents_set = new HashSet<String>();
				documents_set.add(currentUrl);
				// we have to add it 
				PreparedStatement insert_st = con.prepareStatement(insert_statement);
				//(WORD,NB_DOCUMENTS,DOCUMENTS) values(?,?,?)
				insert_st.setString(1,word);
				insert_st.setInt(2,documents_set.size());
				insert_st.setString(3,documents_set.toString());
				insert_st.executeUpdate();
			}
			System.out.println(Thread.currentThread()+"Committed " +  " updates");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting "+word + " for URL : "+currentUrl);
		}	
	}
	
	public Set<String> parse_nodes_out_links(String output_links) {
		output_links = output_links.replace("[", "");
		output_links = output_links.replace("]", "");
		String[] url_outs = output_links.split(",");
		Set<String> outputSet = new HashSet<String>();
		for (String url_out : url_outs){
			url_out=url_out.trim();
			outputSet.add(url_out);
		}
		return outputSet;
	}
}
