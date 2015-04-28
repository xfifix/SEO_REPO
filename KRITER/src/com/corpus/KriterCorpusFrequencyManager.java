package com.corpus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.statistics.processing.CatalogEntry;

public class KriterCorpusFrequencyManager {

	private Connection con;

	private static String find_statement="select DOC_LIST from KRITER_CORPUS_WORDS where WORD=?";
	private static String insert_statement="INSERT INTO KRITER_CORPUS_WORDS(WORD,NB_DOCUMENTS,DOC_LIST) values(?,?,?)";
	private static String update_statement="UPDATE KRITER_CORPUS_WORDS SET NB_DOCUMENTS=?,DOC_LIST=? WHERE WORD=?";

	public KriterCorpusFrequencyManager(String url,String user,String passwd) throws SQLException{
		con = DriverManager.getConnection(url, user, passwd);
		RemoveStopWordsUtility.loadFrenchStopWords();
	}

	public void updateEntry(CatalogEntry entry){
		this.updateText(entry.getLIBELLE_PRODUIT().toLowerCase()+" "+entry.getDESCRIPTION_LONGUEUR80().toLowerCase(),entry.getSKU());
	}

	public void updateText(String text, String SKU){
		// we don't need that as we have updated our stopwords with single letters
		//String semantic_text = CorpusCache.preprocessSemanticText(text);
		Map<String,Integer> word_map = RemoveStopWordsUtility.removeStopWords(text);

		Iterator<Map.Entry<String, Integer>> it = word_map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
			String word=pairs.getKey();
			Integer count=pairs.getValue();
			// we here don't want any number
			System.out.println("Word to add to the corpus : "+word);
			this.updateSKU(word, SKU,count);
		}	
	}

	public void updateSKU(String word, String SKU, Integer counter){
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
				Map<String,Integer> skus_map = parse_skulist_links(document_list);
			    // we update the map with the new SKU
				skus_map.put(SKU,counter);
				Integer total = count_map(skus_map);
				PreparedStatement update_st = con.prepareStatement(update_statement);
				//NB_DOCUMENTS=?,DOCUMENTS=? WHERE WORD=?
				update_st.setInt(1,total);
				update_st.setString(2,skus_map.toString());
				update_st.setString(3,word);
				update_st.executeUpdate();
			}else{
				// we did not find it	
				// we create the map from scratch
				Map<String,Integer> documents_map = new HashMap<String,Integer>();
				documents_map.put(SKU,counter);
				// we have to add it 
				PreparedStatement insert_st = con.prepareStatement(insert_statement);
				//(WORD,NB_DOCUMENTS,DOCUMENTS) values(?,?,?)
				insert_st.setString(1,word);
				insert_st.setInt(2,counter);
				insert_st.setString(3,documents_map.toString());
				insert_st.executeUpdate();
			}
			System.out.println(Thread.currentThread()+"Committed one update");
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting "+word + " for URL : "+SKU);
		}	
	}


    public Integer count_map(Map<String,Integer> tocount){
    	Integer total = 0;
		Iterator<Entry<String, Integer>> it = tocount.entrySet().iterator();
		// dispatching to threads
		while (it.hasNext()){	
			Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
			Integer local_count = pairs.getValue();
			total=total+local_count;
		}
    	return total;
    }
    

	public Map<String,Integer> parse_skulist_links(String output_links) {
		output_links = output_links.replace("{", "");
		output_links = output_links.replace("}", "");
		String[] url_outs = output_links.split(",");
		Map<String,Integer> outputSet = new HashMap<String,Integer>();
		for (String url_out : url_outs){
			url_out=url_out.trim();
			String[] skuData = url_out.split("=");
			outputSet.put(skuData[0],Integer.valueOf(skuData[1]));
		}
		return outputSet;
	}
}
