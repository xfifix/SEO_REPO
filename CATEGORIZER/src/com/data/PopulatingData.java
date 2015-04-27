package com.data;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public class PopulatingData {
	private static String database_categorizer_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/categorizer.properties";
	private static String drop_CURRENT_DATA_table = "DROP TABLE IF EXISTS DATA";
	private static String create_CURRENT_DATA_table = "CREATE TABLE IF NOT EXISTS DATA (MAGASIN VARCHAR(100),RAYON VARCHAR(100), CATEGORIE_NIVEAU_1 TEXT, CATEGORIE_NIVEAU_2 TEXT, CATEGORIE_NIVEAU_3 TEXT,CATEGORIE_NIVEAU_4 TEXT,CATEGORIE_NIVEAU_5 TEXT,SKU VARCHAR(100),LIBELLE_PRODUIT TEXT,MARQUE VARCHAR(100),DESCRIPTION_LONGUEUR50 TEXT,DESCRIPTION_LONGUEUR80 TEXT,URL TEXT,LIEN_IMAGE TEXT,VENDEUR VARCHAR(100),ETAT VARCHAR(100),TO_FETCH BOOLEAN) TABLESPACE mydbspace";
	private static String insert_statement = "INSERT INTO DATA(MAGASIN, RAYON, CATEGORIE_NIVEAU_1, CATEGORIE_NIVEAU_2, CATEGORIE_NIVEAU_3, CATEGORIE_NIVEAU_4, CATEGORIE_NIVEAU_5, SKU, LIBELLE_PRODUIT, MARQUE, DESCRIPTION_LONGUEUR50, DESCRIPTION_LONGUEUR80, URL, LIEN_IMAGE, VENDEUR, ETAT, TO_FETCH)  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	private static Connection con;
	//private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/Catalogue_KryterFull.csv";
	private static String input_file_path = "/home/sduprey/My_Data/My_Kriter_Data/Catalogue_KryterFull_17_04_2015_10_37_30_utf8.csv";
	private static int counter = 0;
	private static int batch_size = 10000;
	public static void main(String[] args){
		// Reading the property of our database for the continuous crawl
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_categorizer_con_path);
			props.load(in);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");
		// we will here insert all the entries from the catalog csv file
		try {
			con = DriverManager.getConnection(url, user, passwd);
			cleaning_database();
			System.out.println("DATA table dropped and recreated");
			insert_new_Catalog();
			System.out.println("New DATA inserted from csv file : "+input_file_path);
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}
	}

	private static void insert_new_Catalog(){
		ResultSet rs = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = "\\u0001";
		PreparedStatement pst = null;
		BufferedReader br = null;
		try{
			// preparing the database for insertion
			con.setAutoCommit(false);
			pst = con.prepareStatement(insert_statement);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(input_file_path), "UTF-8"));	
			// we skip the first line : the headers
			header = br.readLine();
			column_names= header.split(cvsSplitBy);
			System.out.println("Column names headers : "+Arrays.toString(column_names));
			int nb_line=1;
			int batch_current_size=1;
			System.out.println("We do not insert the first  : " + nb_line + " lines ");
			while ((line = br.readLine()) != null) {
				if (nb_line >= counter){
					System.out.println("Inserting line number : "+nb_line);
					String[] fields= line.split(cvsSplitBy);
					//System.out.println("Fields to insert : "+Arrays.toString(fields));								
					pst.setString(1,fields[0]);
					pst.setString(2,fields[1]);
					pst.setString(3,fields[2]);
					pst.setString(4,fields[3]);
					pst.setString(5,fields[4]);
					pst.setString(6,fields[5]);
					pst.setString(7,fields[6]);
					pst.setString(8,fields[7]);
					pst.setString(9,fields[8]);
					pst.setString(10,fields[9]);
					pst.setString(11,fields[10]);
					pst.setString(12,fields[11]);
					pst.setString(13,fields[12]);
					pst.setString(14,fields[13]);
					pst.setString(15,fields[14]);
					pst.setString(16,fields[15]);
					pst.setBoolean(17,true);
					pst.addBatch();
					batch_current_size++;
					if (batch_current_size == batch_size){
						System.out.println("Inserting a "+batch_size+" batch");
						pst.executeBatch();		 
						con.commit();
						batch_current_size=0;
					}
					nb_line++;
				}
			}
			System.out.println("Inserting the last batch");
			pst.executeBatch();		 
			con.commit();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}
				if (br != null) {
					br.close();
				}

			} catch (SQLException | IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private static void cleaning_database() throws SQLException{
		PreparedStatement drop_catalog_table_st = con.prepareStatement(drop_CURRENT_DATA_table);
		drop_catalog_table_st.executeUpdate();
		System.out.println("Dropping the old DATA table");

		PreparedStatement create_catalog_table_st = con.prepareStatement(create_CURRENT_DATA_table);
		create_catalog_table_st.executeUpdate();
		System.out.println("Creating the new DATA table");
	}
}
