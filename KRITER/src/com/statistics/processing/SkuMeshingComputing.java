package com.statistics.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SkuMeshingComputing {
	private static int nb_similar_skus = 6;
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/kriter.properties";
	private static String select_all_distinct_skus = "SELECT SKU,CATEGORIE_NIVEAU_4,MAGASIN FROM CATALOG";
	private static String select_all_linked_skus = "SELECT SKU,SKU1,SKU2,SKU3,SKU4,SKU5,SKU6 FROM CATALOG";
	private static String insert_cds_nodes_statement = "INSERT INTO NODES (ID,SKU,CATEGORIE_LEVEL_4,MAGASIN) values (?,?,?,?)";
	private static String insert_cds_edges_statement = "INSERT INTO EDGES (SOURCE, TARGET) values (?,?)";
	//private static String update_catalog_statement = "UPDATE CATALOG SET COUNTER=? where SKU=?";
	private static String drop_CDS_MESHING_NODES_SIMILAR_PRODUCTS_table = "DROP TABLE IF EXISTS NODES";
	private static String create_CDS_MESHING_NODES_SIMILAR_PRODUCTS_table = "CREATE TABLE IF NOT EXISTS NODES (ID SERIAL PRIMARY KEY NOT NULL, SKU VARCHAR(100), CATEGORIE_LEVEL_4 TEXT, MAGASIN VARCHAR(100)) TABLESPACE mydbspace";
	private static String drop_CDS_MESHING_EDGES_SIMILAR_PRODUCTS_table = "DROP TABLE IF EXISTS EDGES";
	private static String create_CDS_MESHING_EDGES_SIMILAR_PRODUCTS_table = "CREATE TABLE IF NOT EXISTS EDGES (SOURCE INT,TARGET INT) TABLESPACE mydbspace";

	private static Map<String,LightSKU> linked_skus_ids_cache = new HashMap<String,LightSKU>();
	public static void main(String[] args){
		// it would be best to use a property file to store MD5 password
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
		// The database connection
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		try {  
			con = DriverManager.getConnection(url, user, passwd);
			// cleaning and recreating from scratch the previous LINKING SKUS table
			cleaning_nodes_edges_database(con);
			// getting the number of URLs to fetch
			System.out.println("Requesting all distinct SKUs as a similar product");
			pst = con.prepareStatement(select_all_distinct_skus);
			rs = pst.executeQuery();
			// dispatching to threads
			int sku_id_counter=1;
			while (rs.next()) {
				// sku 1
				if (sku_id_counter%500 == 0){
					System.out.println(Thread.currentThread() +" Having processed "+sku_id_counter+" SKUs");
				}
				LightSKU newSku = new LightSKU();
				// the very first sku is not linked to
				String current_sku=rs.getString(1);
				String category_level_4=rs.getString(2);
				String magasin=rs.getString(3);
				newSku.setCategory_level_4(category_level_4);
				newSku.setMagasin(magasin);
				newSku.setId(sku_id_counter);
				// if the sku is not in the map, we put it with no link to it
				linked_skus_ids_cache.put(current_sku, newSku);	
				sku_id_counter++;
			}
			rs.close();
			pst.close();
			System.out.println("We have fetched " +sku_id_counter + " linked SKUs according to the KRITER database \n");
			int local_counter = 0;
			try{
				con.setAutoCommit(false);
				PreparedStatement st = con.prepareStatement(insert_cds_nodes_statement);
				//PreparedStatement st = con.prepareStatement(update_catalog_statement);
				Iterator<Entry<String, LightSKU>> it = linked_skus_ids_cache.entrySet().iterator();
				while (it.hasNext()){	
					local_counter++;
					if (local_counter%500 == 0){
						System.out.println(Thread.currentThread() +" Having inserted "+local_counter+" SKUs");
					}
					Map.Entry<String, LightSKU> pairs = (Map.Entry<String, LightSKU>)it.next();
					String current_sku=pairs.getKey();
					LightSKU light_skus = pairs.getValue();
					st.setInt(1,light_skus.getId());
					st.setString(2,current_sku);
					st.setString(3,light_skus.getCategory_level_4());
					st.setString(4,light_skus.getMagasin());
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
			System.out.println("Having inserted "+local_counter+" SKUs in the NODE database");


			System.out.println("Requesting all linking SKUs as a similar product");
			pst = con.prepareStatement(select_all_linked_skus);
			con.setAutoCommit(false);
			PreparedStatement edges_st = con.prepareStatement(insert_cds_edges_statement);
			rs = pst.executeQuery();
			// dispatching to threads
			int edges_counter=1;
			while (rs.next()) {
				// sku 1
				if (edges_counter%500 == 0){
					System.out.println(Thread.currentThread() +" Having processed the linking of : "+edges_counter+" SKUs");
				}

				// the very first sku is not linked to
				String current_sku=rs.getString(1);
				LightSKU currentId = linked_skus_ids_cache.get(current_sku);	

				for (int nb_sku=2;nb_sku<(nb_similar_skus+2);nb_sku++){
					String current_linked_skus = rs.getString(nb_sku);
					LightSKU linkedId = linked_skus_ids_cache.get(current_linked_skus);	
					// we insert only if the SKU is null
					if (linkedId != null){
						edges_st.setInt(1,currentId.getId());
						edges_st.setInt(2,linkedId.getId());
						edges_st.addBatch();
					} 
				}
				edges_counter++;
			}
			edges_st.executeBatch();		 
			con.commit();
			edges_st.close();
			
			rs.close();
			pst.close();

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(StatisticsComputingThreadPool.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
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
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(StatisticsComputingThreadPool.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		System.out.println("Finished all threads");
	}

	private static void cleaning_nodes_edges_database(Connection con) throws SQLException{
		PreparedStatement drop_node_table_st = con.prepareStatement(drop_CDS_MESHING_NODES_SIMILAR_PRODUCTS_table);
		drop_node_table_st.executeUpdate();
		System.out.println("Dropping the old NODES table");
		drop_node_table_st.close();
		PreparedStatement create_node_table_st = con.prepareStatement(create_CDS_MESHING_NODES_SIMILAR_PRODUCTS_table);
		create_node_table_st.executeUpdate();
		System.out.println("Creating the new NODES table");
		create_node_table_st.close();

		PreparedStatement drop_edges_table_st = con.prepareStatement(drop_CDS_MESHING_EDGES_SIMILAR_PRODUCTS_table);
		drop_edges_table_st.executeUpdate();
		System.out.println("Dropping the old EDGES table");
		drop_edges_table_st.close();
		PreparedStatement create_edges_table_st = con.prepareStatement(create_CDS_MESHING_EDGES_SIMILAR_PRODUCTS_table);
		create_edges_table_st.executeUpdate();
		System.out.println("Creating the new EDGES table");
		create_edges_table_st.close();
	}
}
