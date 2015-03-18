package com.similarity.computing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import com.statistics.processing.CatalogEntry;
import com.statistics.processing.StatisticsUtility;

public class SimilarityComputingWorkerThread implements Runnable {
	private Connection con;
	private List<String> my_categories_to_compute = new ArrayList<String>();
	private static String select_entry_from_category = " select SKU, MAGASIN, RAYON, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4, CATEGORIE_NIVEAU_5, LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, URL, LIEN_IMAGE, VENDEUR, ETAT FROM CATALOG where CATEGORIE_NIVEAU_4=?";
	private static String insert_cds_statement = "INSERT INTO CDS_SIMILAR_PRODUCTS(SKU,SKU1,SKU2,SKU3,SKU4,SKU5,SKU6) VALUES(?,?,?,?,?,?,?)";
	private Map<String,List<String>> matching_skus = new HashMap<String,List<String>>();
	private static int kriter_threshold =6;
	private static int max_list_size = 10000; 

	public SimilarityComputingWorkerThread(Connection con, List<String> to_fetch) throws SQLException{
		this.con = con;
		this.my_categories_to_compute = to_fetch;
	}

	public void run() {
		String category_to_debug="";
		try {  
			for (String category : my_categories_to_compute){
				category_to_debug=category;
				System.out.println("Dealing with category : "+category);
				List<CatalogEntry> my_data = fetch_category_data(category);
				if (my_data.size() >= kriter_threshold && my_data.size()<= max_list_size){
					// we do it the standard way
					Double[] symmetric_matrix = computeDistanceMatrix(my_data);
					find_similar(symmetric_matrix,my_data);
				} else if (my_data.size() < kriter_threshold) {
					List<String> similars = new ArrayList<String>();
					for (CatalogEntry to_add : my_data){
						similars.add(to_add.getSKU());
					}
					for (CatalogEntry to_process : my_data){
						matching_skus.put(to_process.getSKU(),similars);
					}
					// we here have to fetch lower category
				}else if (my_data.size() > max_list_size) {
					// we here have to restrain ourselves
					// we do it randomly
					// but we should get a more proper criteria (business value, clicking trend)
					List<CatalogEntry> randomSet = shrink(my_data);
					System.out.println(randomSet.size());
					Double[][] distance_matrix = computeRestrictedDistanceMatrix(my_data,randomSet);
					find_restricted_similar(distance_matrix,my_data);
				}
				saving_similar();
			}
			close_connection();

		} catch (Exception ex) {
			System.out.println("Trouble with category : "+category_to_debug);
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

	public List<CatalogEntry> shrink(List<CatalogEntry> my_list){
		Set<CatalogEntry> to_return = new HashSet<CatalogEntry>();
		Random my_rand = new Random();
		while (to_return.size() < max_list_size){
			to_return.add(my_list.get(my_rand.nextInt(my_list.size())));
		}
		return new ArrayList<CatalogEntry>(to_return);
	}

	public void saving_similar(){
		System.out.println("Inserting the batch "+matching_skus.size());
		try{

			Iterator<Entry<String, List<String>>> it = matching_skus.entrySet().iterator();
			int local_counter = 0;
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_cds_statement);
			while (it.hasNext()){
				local_counter++;
				Map.Entry<String, List<String>> pairs = (Map.Entry<String, List<String>>)it.next();
				String current_sku=pairs.getKey();
				List<String> similars =pairs.getValue();
				System.out.println("Current Sku :" + current_sku + similars);
				// preparing the statement
				st.setString(1,current_sku);
				st.setString(2,similars.get(0));
				st.setString(3,similars.get(1));
				st.setString(4,similars.get(2));
				st.setString(5,similars.get(3));
				st.setString(6,similars.get(4));
				st.setString(7,similars.get(5));
				st.addBatch();
			}
			st.executeBatch();
			con.commit();
			st.close();
			System.out.println(Thread.currentThread()+"Committed " + local_counter + " updates");
		} catch (SQLException e){
			//System.out.println("Line already inserted : "+nb_lines);
			e.printStackTrace();  
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException ex1) {
					ex1.printStackTrace();
				}
			}
			e.printStackTrace();
		}	
	}

	public void find_restricted_similar(Double[][] distance_matrix, List<CatalogEntry> entries){
		int size_list = entries.size();
		Double[] vector_list = new Double[size_list]; 
		for (int i=0;i<size_list;i++){
			CatalogEntry current_entry = entries.get(i);
			//System.out.println("i"+i);
			entries.get(i);
			for (int j= 0;j<max_list_size;j++){
				vector_list[j] = distance_matrix[i][j];
			}
			// sorting the array and keeping the indexes
			ArrayIndexComparator comparator = new ArrayIndexComparator(vector_list);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);

			List<String> similars = new ArrayList<String>();
			// adding the 6 first closest skus
			similars.add(entries.get(0).getSKU());
			similars.add(entries.get(1).getSKU());
			similars.add(entries.get(2).getSKU());
			similars.add(entries.get(3).getSKU());
			similars.add(entries.get(4).getSKU());
			similars.add(entries.get(5).getSKU());
			matching_skus.put(current_entry.getSKU(),similars);
		}
	}

	public void find_similar(Double[] symmetric_matrix,List<CatalogEntry> entries){
		int size_list = entries.size();
		Double[] vector_list = new Double[size_list]; 
		for (int i=0;i<size_list;i++){
			CatalogEntry current_entry = entries.get(i);
			//System.out.println("i"+i);
			entries.get(i);
			for (int j= 0;j<size_list;j++){
				vector_list[j] = symmetric_matrix[fromMatrixToVector(i,j,size_list)];
			}
			// sorting the array and keeping the indexes
			ArrayIndexComparator comparator = new ArrayIndexComparator(vector_list);
			Integer[] indexes = comparator.createIndexArray();
			Arrays.sort(indexes, comparator);

			List<String> similars = new ArrayList<String>();
			// adding the 6 first closest skus
			similars.add(entries.get(0).getSKU());
			similars.add(entries.get(1).getSKU());
			similars.add(entries.get(2).getSKU());
			similars.add(entries.get(3).getSKU());
			similars.add(entries.get(4).getSKU());
			similars.add(entries.get(5).getSKU());
			matching_skus.put(current_entry.getSKU(),similars);
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


	public Double[][] computeRestrictedDistanceMatrix(List<CatalogEntry> entries,List<CatalogEntry> restrictedPanel){
		int size_list = entries.size();
		int restricted_size_list = restrictedPanel.size();

		Double[][] to_return = new Double[size_list][restricted_size_list];
		for (int i=0;i<size_list;i++){
			if (i%1000 == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix"+i+" from "+size_list);
			}
			for (int j=0;j<restricted_size_list;j++){
				CatalogEntry entryi = entries.get(i);
				CatalogEntry entryj = restrictedPanel.get(j);
				Double distone = StatisticsUtility.computeTFdistance(entryi.getLIBELLE_PRODUIT(), entryj.getLIBELLE_PRODUIT());
				Double disttwo = StatisticsUtility.computeTFdistance(entryi.getDESCRIPTION_LONGUEUR80(), entryj.getDESCRIPTION_LONGUEUR80());
				to_return[i][j] = distone + disttwo;
			}
		}
		return to_return;
	}

	public Double[] computeDistanceMatrix(List<CatalogEntry> entries){
		int size_list = entries.size();
		long vector_size_list = size_list*(size_list+1)/2;
		Double[] to_return = new Double[(int)vector_size_list];
		for (int i=0;i<size_list;i++){
			if (i%1000 == 0){
				System.out.println(Thread.currentThread() +" Having computed distance matrix"+i+" from "+size_list);
			}
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
