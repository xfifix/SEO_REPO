package com.predictors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import com.corpus.CategorizerDynamicCorpusCache;
import com.corpus.RemoveStopWordsUtility;
import com.data.DataEntry;

public class CategoryBagOfWordsManager {
	private Connection con;
	
	private CategorizerDynamicCorpusCache corpus_cache;

	// BLOB storing requests
	private static String get_blob_oid = "SELECT BLOBOID FROM CATEGORY_BAG_OF_WORDS WHERE CATEGORY = ?";
	private static String update_blob="UPDATE CATEGORY_BAG_OF_WORDS SET BLOBOID=?,LAST_UPDATE=? WHERE CATEGORY=?";
	private static String insert_blob="INSERT INTO CATEGORY_BAG_OF_WORDS (CATEGORY,BLOBOID,LAST_UPDATE) VALUES (?,?,?)";

	public CategoryBagOfWordsManager(String url,String user,String passwd) throws SQLException{
		con = DriverManager.getConnection(url, user, passwd);
		corpus_cache=new CategorizerDynamicCorpusCache(url, user, passwd);
		RemoveStopWordsUtility.loadFrenchStopWords();
	}

	public void updateCategoryEntry(String category, List<DataEntry> category_data){
		Map<String, Integer> tf_bag_of_words = new HashMap<String, Integer>();
		for (DataEntry my_entry : category_data){
			Map<String,Integer> word_map = RemoveStopWordsUtility.removeStopWords(my_entry.getLIBELLE().toLowerCase()+" "+my_entry.getDESCRIPTION().toLowerCase()+" "+my_entry.getMARQUE());
			Iterator<Map.Entry<String, Integer>> it = word_map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
				String word=pairs.getKey();
				Integer count=pairs.getValue();
				//System.out.println("Word to add to the category corpus : "+word);
				Integer counter = tf_bag_of_words.get(word);
				if (counter == null){
					tf_bag_of_words.put(word,count);
				}else{
					tf_bag_of_words.put(word,count+counter);
				}	
			}	
		}

		// we compute the tf/idf vector for the current category 
		try {
			Map<String, Double> tf_idf_bag_of_words = corpus_cache.convert_to_tfidf(tf_bag_of_words);
			System.out.println("Updating category : "+category);
			con.setAutoCommit(false);
			updateCategoryBlob(category,tf_idf_bag_of_words);
			con.setAutoCommit(true);
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble updating the BLOB bag of words for category : "+category);
		}
	}

	public int getBlobOID(String category){
		int oid = 0;
		try {
			PreparedStatement oid_query_statement = con.prepareStatement(get_blob_oid);
			oid_query_statement.setString(1, category);
			ResultSet oid_result = oid_query_statement.executeQuery();
			if (oid_result.next()){
				oid = oid_result.getInt(1);
			}	
			oid_result.close();
			oid_query_statement.close();
		} catch (SQLException e) {
			System.out.println("Trouble fetching OID from the database");
			e.printStackTrace();
		}
		return oid;
	}

	public void updateCategoryBlob(String category, Map<String, Double> to_serialize) throws SQLException, IOException{
		int oid = getBlobOID(category);
		LargeObjectManager lobj = ((org.postgresql.PGConnection)con).getLargeObjectAPI();
		// if oid not found, we insert a whole new line and we create a new blob
		if (oid == 0){
			// we create here the blob
			oid = lobj.create(LargeObjectManager.READ | LargeObjectManager.WRITE);
			// Open the large object for writing
			LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(to_serialize);
			objOut.close();

			InputStream fis = new ByteArrayInputStream(out.toByteArray());

			// Copy the data from the file to the large object
			// 2048 is the buffer size, does not really matter
			byte buf[] = new byte[2048];
			int s, tl = 0;
			while ((s = fis.read(buf, 0, 2048)) > 0) {
				obj.write(buf, 0, s);
				tl += s;
			}
			System.out.println("BLOB byte size written : "+tl);
			obj.close();
			// we close the stream
			fis.close();
			// once the blob has been written we insert the whole url line in crawl_results
			// we insert here the brand new url with its blob oid
			PreparedStatement insert_st = con.prepareStatement(insert_blob);
			insert_st.setString(1,category);			
			insert_st.setInt(2,oid);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			insert_st.setDate(3,sqlDate);
			insert_st.executeUpdate(); 	
			insert_st.close();
		} else {
			// if oid found not null, we update the found line and we update the matching blob
			// we don't create the object, we just open it
			LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
			// we write the new content to the BLOB
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream objOut = new ObjectOutputStream(out);
			objOut.writeObject(to_serialize);
			objOut.close();

			InputStream fis = new ByteArrayInputStream(out.toByteArray());
			// Copy the data from the file to the large object
			// 2048 is the buffer size, does not really matter
			byte buf[] = new byte[2048];
			int s, tl = 0;
			while ((s = fis.read(buf, 0, 2048)) > 0) {
				obj.write(buf, 0, s);
				tl += s;
			}
			System.out.println("BLOB byte updated size : "+tl);
			// please here do not forget to truncate to the new size in case the previous one was bigger
			obj.truncate(tl);
			// Close the large object
			obj.close();
			// we close the stream
			fis.close();
			// once the BLOB object has been updated, we update the matching line
			PreparedStatement update_st = con.prepareStatement(update_blob);
			update_st.setInt(1, oid);
			java.sql.Date sqlDate = new java.sql.Date(System.currentTimeMillis());
			update_st.setDate(2,sqlDate);
			update_st.setString(3,category);
			// we here don't care about wether or not the line has been found and updated
			// as we have found the blob oid, the line is present and should be updated
			//int affected_row = update_st.executeUpdate();
			update_st.executeUpdate();
			update_st.close();
		}
		con.commit();
	}
}
