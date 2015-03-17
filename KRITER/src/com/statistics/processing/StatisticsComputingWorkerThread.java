package com.statistics.processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticsComputingWorkerThread implements Runnable {
	private Connection con;
	private List<String[]> my_skus_to_fetch = new ArrayList<String[]>();
	private static String select_parameters = " select MAGASIN, RAYON, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4, CATEGORIE_NIVEAU_5, LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, URL, LIEN_IMAGE, VENDEUR, ETAT FROM CATALOG where SKU=?";
	private static String update_statement = "UPDATE CATALOG SET NB_DISTINCT_CAT5=?, NB_DISTINCT_CAT4=?, NB_DISTINCT_BRAND=?, NB_DISTINCT_BRAND_WITHOUT_DEFAULT=?, DISTINCT_CAT5=?, DISTINCT_CAT4=?, DISTINCT_BRAND=?, TF_DISTANCE_LIBELLE=?, TF_IDF_DISTANCE_LIBELLE=?, LEVENSHTEIN_DISTANCE_LIBELLE=?, TF_DISTANCE_DESCRIPTION80=?, TF_IDF_DISTANCE_DESCRIPTION80=?, LEVENSHTEIN_DISTANCE_DESCRIPTION80=? where SKU=?";
	public StatisticsComputingWorkerThread(Connection con, List<String[]> to_fetch) throws SQLException{
		this.con = con;
		this.my_skus_to_fetch = to_fetch;
	}

	public void run() {
		for (String[] skus : my_skus_to_fetch){
			System.out.println("Fetching " + Arrays.toString(skus));
			Map<String, CatalogEntry> filled_up = new HashMap<String, CatalogEntry>();
			for (String sku : skus){
				PreparedStatement select_statement;
				try {
					select_statement = con.prepareStatement(select_parameters);
					select_statement.setString(1, sku);
					ResultSet rs = select_statement.executeQuery();
					if (rs.next()) {
						CatalogEntry entry = new CatalogEntry();
						entry.setSKU(sku);
						String MAGASIN = rs.getString(1);
						entry.setMAGASIN(MAGASIN);
						String RAYON = rs.getString(2);
						entry.setRAYON(RAYON);
						String CATEGORIE_NIVEAU_1 = rs.getString(3);
						entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
						String CATEGORIE_NIVEAU_2 = rs.getString(4);
						entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
						String CATEGORIE_NIVEAU_3 = rs.getString(5);
						entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
						String CATEGORIE_NIVEAU_4 = rs.getString(6);
						entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
						String CATEGORIE_NIVEAU_5 = rs.getString(7);
						entry.setCATEGORIE_NIVEAU_5(CATEGORIE_NIVEAU_5);
						String  LIBELLE_PRODUIT = rs.getString(8);
						entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
						String MARQUE = rs.getString(9);
						entry.setMARQUE(MARQUE);
						String  DESCRIPTION_LONGUEUR80 = rs.getString(10);
						entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
						String URL = rs.getString(11);
						entry.setURL(URL);
						String LIEN_IMAGE = rs.getString(12);
						entry.setLIEN_IMAGE(LIEN_IMAGE);
						String VENDEUR = rs.getString(13);
						entry.setVENDEUR(VENDEUR);
						String ETAT = rs.getString(14);
						entry.setETAT(ETAT);
						filled_up.put(sku,entry);
					}
					select_statement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			StatisticsMeasures measures = StatisticsUtility.computeStatistics(skus,filled_up);
			try {
				update_statistics(measures);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Trouble updating sku "+skus[0]);
			}
			System.out.println(Thread.currentThread().getName()+" updating sku "+skus[0]);
		}
		close_connection();
	}

	public void update_statistics(StatisticsMeasures measures) throws SQLException{
		PreparedStatement update_st = con.prepareStatement(update_statement);
		// udpate set NB_DISTINCT_CAT5=?, NB_DISTINCT_CAT4=?, NB_DISTINCT_BRAND=?, DISTINCT_CAT5=?, DISTINCT_CAT4=?, DISTINCT_BRAND=?, TF_DISTANCE_LIBELLE=?, TF_IDF_DISTANCE_LIBELLE=?, LEVENSHTEIN_DISTANCE_LIBELLE=?, TF_DISTANCE_DESCRIPTION80=?, TF_IDF_DISTANCE_DESCRIPTION80=?, LEVENSHTEIN_DISTANCE_DESCRIPTION80=? where SKU=?";
		update_st.setInt(1, measures.getNb_distinct_category5());
		update_st.setInt(2, measures.getNb_distinct_category4());
		update_st.setInt(3, measures.getNb_distinct_brands());
		update_st.setInt(4, measures.getNb_distinct_brands_without_default());
		update_st.setString(5, measures.getDistinct_category5());
		update_st.setString(6, measures.getDistinct_category4());
		update_st.setString(7, measures.getDistinct_brands());
		update_st.setString(8,Arrays.toString(measures.getTf_distances_libelle()));
		update_st.setString(9,Arrays.toString(measures.getTf_idf_distances_libelle()));
		update_st.setString(10,Arrays.toString(measures.getLevenshtein_distances_libelle()));
		update_st.setString(11,Arrays.toString(measures.getTf_description80()));
		update_st.setString(12,Arrays.toString(measures.getTf_idf_description80()));
		update_st.setString(13,Arrays.toString(measures.getLevenshtein_description80()));
		update_st.setString(14,measures.getCurrentSku());
		update_st.executeUpdate();
		update_st.close();
	}

	private void close_connection(){
		try {
			if (con != null) {
				con.close();
			}
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(StatisticsComputingThreadPool.class.getName());
			lgr.log(Level.WARNING, ex.getMessage(), ex);
		}
	}
}
