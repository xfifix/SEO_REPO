package com.similarity.computing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.statistics.processing.CatalogEntry;
import com.statistics.processing.StatisticsUtility;

public class SimilarityComputingWorkerThread implements Runnable {
	private Connection con;
	private List<String> my_categories_to_compute = new ArrayList<String>();
	private static String select_entry_from_category = " select SKU, MAGASIN, RAYON, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4, CATEGORIE_NIVEAU_5, LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, URL, LIEN_IMAGE, VENDEUR, ETAT FROM CATALOG where CATEGORIE_NIVEAU_4=?";
	private static Map<String,List<String>> matching_skus = new HashMap<String,List<String>>();

	public SimilarityComputingWorkerThread(Connection con, List<String> to_fetch) throws SQLException{
		this.con = con;
		this.my_categories_to_compute = to_fetch;
	}

	public void run() {
		try {  
			for (String category : my_categories_to_compute){
				System.out.println("Dealing with category : "+category);
				List<CatalogEntry> my_data = fetch_category_data(category);
				Double[] symmetric_matrix = computeDistanceMatrix(my_data);
				find_similar(symmetric_matrix,my_data);
				close_connection();
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	public void find_similar(Double[] symmetric_matrix,List<CatalogEntry> entries){
		int size_list = entries.size();
		Double[] vector_list = new Double[size_list]; 
		for (int i=0;i<size_list;i++){
			System.out.println("i"+i);
			for (int j= 0;j<size_list;j++){
				vector_list[j] = symmetric_matrix[fromMatrixToVector(i,j,size_list)];
			}
			Arrays.sort(vector_list);
		}
	}

	public List<CatalogEntry> fetch_category_data(String category) throws SQLException{
		List<CatalogEntry> my_entries = new ArrayList<CatalogEntry>();
		PreparedStatement select_statement = con.prepareStatement(select_entry_from_category);
		select_statement.setString(1, category);
		ResultSet rs = select_statement.executeQuery();
		while (rs.next()) {
			CatalogEntry entry = new CatalogEntry();
			String sku = rs.getString(1);
			entry.setSKU(sku);
			String MAGASIN = rs.getString(2);
			entry.setMAGASIN(MAGASIN);
			String RAYON = rs.getString(3);
			entry.setRAYON(RAYON);
			String CATEGORIE_NIVEAU_1 = rs.getString(4);
			entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
			String CATEGORIE_NIVEAU_2 = rs.getString(5);
			entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
			String CATEGORIE_NIVEAU_3 = rs.getString(6);
			entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
			String CATEGORIE_NIVEAU_4 = rs.getString(7);
			entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
			String CATEGORIE_NIVEAU_5 = rs.getString(8);
			entry.setCATEGORIE_NIVEAU_5(CATEGORIE_NIVEAU_5);
			String  LIBELLE_PRODUIT = rs.getString(9);
			entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
			String MARQUE = rs.getString(10);
			entry.setMARQUE(MARQUE);
			String  DESCRIPTION_LONGUEUR80 = rs.getString(11);
			entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
			String URL = rs.getString(12);
			entry.setURL(URL);
			String LIEN_IMAGE = rs.getString(13);
			entry.setLIEN_IMAGE(LIEN_IMAGE);
			String VENDEUR = rs.getString(14);
			entry.setVENDEUR(VENDEUR);
			String ETAT = rs.getString(15);
			entry.setETAT(ETAT);
			my_entries.add(entry);
		}
		select_statement.close();
		return my_entries;
	}

	public Double[] computeDistanceMatrix(List<CatalogEntry> entries){
		int size_list = entries.size();
		Double[] to_return = new Double[size_list*(size_list+1)/2];
		for (int i=0;i<size_list;i++){
			System.out.println("i"+i);
			for (int j=i;j<size_list;j++){
				CatalogEntry entryi = entries.get(i);
				CatalogEntry entryj = entries.get(j);
				Double distone = StatisticsUtility.computeTFdistance(entryi.getLIBELLE_PRODUIT(), entryj.getLIBELLE_PRODUIT());
				Double disttwo = StatisticsUtility.computeTFdistance(entryi.getDESCRIPTION_LONGUEUR80(), entryj.getDESCRIPTION_LONGUEUR80());
				to_return[fromMatrixToVector(i,j,size_list)] = distone + disttwo;
			}
		}
		return to_return;
	}

	public int fromMatrixToVector(int i, int j, int N)
	{
		int my_index;
		if (i <= j)
			my_index = i * N - (i - 1) * i / 2 + j - i;
		else
			my_index = j * N - (j - 1) * j / 2 + i - j;
		
		return my_index;
	}
	private void close_connection(){
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}
}
