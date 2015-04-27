package com.predictors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import com.data.DataEntry;

public class CategoryBagOfWordsManager {
	private static Connection con;
	public CategoryBagOfWordsManager(String url,String user,String passwd) throws SQLException{
		con = DriverManager.getConnection(url, user, passwd);
		RemoveStopWordsUtility.loadFrenchStopWords();
	}

	public void updateCategoryEntry(String category, List<DataEntry> category_data){
		
		
	}
}
