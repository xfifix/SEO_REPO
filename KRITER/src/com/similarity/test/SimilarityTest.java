package com.similarity.test;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.similarity.computing.SimilarityComputingNoFetchWorkerThread;
import com.similarity.parameter.KriterParameter;
import com.statistics.processing.CatalogEntry;

public class SimilarityTest {
	public static String kriter_conf_path = "/home/sduprey/My_Data/My_Kriter_Conf/kriter.conf";
	public static Properties properties;
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private static String select_entry_from_specific_category4 = " select SKU, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4,  LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR80, VENDEUR, ETAT, RAYON FROM CATALOG WHERE CATEGORIE_NIVEAU_4=?";

	public static void main(String[] args) throws SQLException{
		System.out.println("Reading the configuration files : "+kriter_conf_path);
		try{
			loadProperties();
			KriterParameter.database_con_path=properties.getProperty("kriter.database_con_path");
			KriterParameter.small_list_pool_size =Integer.valueOf(properties.getProperty("kriter.small_list_pool_size")); 
			KriterParameter.small_list_size_bucket =Integer.valueOf(properties.getProperty("kriter.small_list_size_bucket")); 
			KriterParameter.big_list_pool_size =Integer.valueOf(properties.getProperty("kriter.big_list_pool_size")); 
			KriterParameter.big_list_size_bucket =Integer.valueOf(properties.getProperty("kriter.big_list_size_bucket")); 
			KriterParameter.max_list_size_separator_string=properties.getProperty("kriter.max_list_size_separator_string");
			KriterParameter.recreate_table=Boolean.parseBoolean(properties.getProperty("kriter.recreate_table"));
			KriterParameter.compute_optimal_parameters=Boolean.parseBoolean(properties.getProperty("kriter.compute_optimal_parameters"));
			KriterParameter.kriter_threshold =Integer.valueOf(properties.getProperty("kriter.kriter_threshold")); 
			KriterParameter.computing_max_list_size =Integer.valueOf(properties.getProperty("kriter.computing_max_list_size"));
			KriterParameter.batch_size =Integer.valueOf(properties.getProperty("kriter.batch_size"));
			KriterParameter.displaying_threshold =Integer.valueOf(properties.getProperty("kriter.displaying_threshold"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble getting the configuration : unable to launch the crawler");
			System.exit(0);
		}
		//		// Getting the database property
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_con_path);
			props.load(in);
		} catch (IOException ex) {
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
		//the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		System.out.println("You'll connect to the postgresql KRITERDB database as "+user);

		//List<String> categories = new ArrayList<String>();
		//      categories.add("TARTE");
		//		categories.add("CARTE TUNER TV");
		//		categories.add("PANES - CORDON BLEUS");
		//		categories.add("FANION DE SIGNALISATION");	
		//		categories.add("TIGE A URETRE");
		//		categories.add("COQUE - HOUSSE");
		//      categories.add("SALADE");
		//      categories.add("CABINE D'ESSAYAGE - MIROIR D'ESSAYAGE - RIDEAU DE CABINE");
		//     categories.add("COQUE - BUMPER - FACADE TELEPHONE");
		//      categories.add("XBOX 360");
		//      categories.add("SAC A MAIN");

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		Map<String, List<CatalogEntry>> my_entries = new HashMap<String, List<CatalogEntry>>();
		try {  
			con = DriverManager.getConnection(url, user, passwd);
			System.out.println("Cleaning up and building the category_following table");

			// getting the number of URLs to fetch
			System.out.println("Requesting all distinct categories");
			pst = con.prepareStatement(select_entry_from_specific_category4);
			pst.setString(1,"SAC A MAIN");
			rs = pst.executeQuery();
			while (rs.next()) {
				// fetching all
				CatalogEntry entry = new CatalogEntry();
				String sku = rs.getString(1);
				entry.setSKU(sku);
				// category fetching
				String CATEGORIE_NIVEAU_1 = rs.getString(2);
				entry.setCATEGORIE_NIVEAU_1(CATEGORIE_NIVEAU_1);
				String CATEGORIE_NIVEAU_2 = rs.getString(3);
				entry.setCATEGORIE_NIVEAU_2(CATEGORIE_NIVEAU_2);
				String CATEGORIE_NIVEAU_3 = rs.getString(4);
				entry.setCATEGORIE_NIVEAU_3(CATEGORIE_NIVEAU_3);
				String CATEGORIE_NIVEAU_4 = rs.getString(5);
				entry.setCATEGORIE_NIVEAU_4(CATEGORIE_NIVEAU_4);
				// product libelle
				String  LIBELLE_PRODUIT = rs.getString(6);
				entry.setLIBELLE_PRODUIT(LIBELLE_PRODUIT);
				String MARQUE = rs.getString(7);
				entry.setMARQUE(MARQUE);
				// brand description
				String  DESCRIPTION_LONGUEUR80 = rs.getString(8);
				entry.setDESCRIPTION_LONGUEUR80(DESCRIPTION_LONGUEUR80);
				// vendor and state (available or not)
				String VENDEUR = rs.getString(9);
				entry.setVENDEUR(VENDEUR);
				String ETAT = rs.getString(9);
				entry.setETAT(ETAT);
				List<CatalogEntry> toprocess = my_entries.get(CATEGORIE_NIVEAU_4);
				if (toprocess == null){
					toprocess = new ArrayList<CatalogEntry>();
					my_entries.put(CATEGORIE_NIVEAU_4, toprocess);
				}
				toprocess.add(entry);
			}
		} catch (SQLException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
		long startTime = System.currentTimeMillis();

		Runnable worker = new SimilarityComputingNoFetchWorkerThread(con,my_entries);
		worker.run();

		long endTime = System.currentTimeMillis();

		long timeneeded = (endTime-startTime)/(1000*60);
		System.out.println("Needed time in minutes : "+timeneeded);
	}
	

	private static void loadProperties(){
		properties = new Properties();
		try {
			properties.load(new FileReader(new File(kriter_conf_path)));
		} catch (Exception e) {
			System.out.println("Failed to load properties file!!");
			e.printStackTrace();
		}
	}
}
