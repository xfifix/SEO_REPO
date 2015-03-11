package com.concurrentcrawl;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.urlutilities.URLTitleInfo;

public class ProcessContinuousCrawl {
	private static Connection con;
	private static String database_duplication_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/title_duplication.properties";
	private static String database_crawl4j_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static int batch_size = 10000;
	private static String insert_statement = "INSERT INTO CURRENT_CONTINUOUS_DUPLICATES(TITLE,NB_URLS,URLS,DUPLICATE_TIME,MAGASIN,RAYON)"
			+ " VALUES(?,?,?,?,?,?)";
	private static String select_statement = "select title, url, magasin, rayon, cdiscount_vendor, page_type from crawl_results  where now()-last_update > INTERVAL '10 days'";
	private static Map<String, List<URLTitleInfo>> titles_data = new HashMap<String, List<URLTitleInfo>>();
	private static String drop_CURRENT_CONTINUOUS_DUPLICATES_table = "DROP TABLE IF EXISTS CURRENT_CONTINUOUS_DUPLICATES";
	private static String create_CURRENT_CONTINUOUS_DUPLICATESs_table = "CREATE TABLE IF NOT EXISTS CURRENT_CONTINUOUS_DUPLICATES (TITLE TEXT,NB_URLS INT, URLS TEXT, DUPLICATE_TIME DATE, MAGASIN TEXT, RAYON TEXT) TABLESPACE mydbspace";
	public static void main(String[] args) {
		// Reading the property of our database for the continuous crawl
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_crawl4j_con_path);
			props.load(in);
		} catch (IOException ex) {
			Logger lgr = Logger.getLogger(ProcessContinuousCrawl.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				Logger lgr = Logger.getLogger(ProcessContinuousCrawl.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}

		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		// we will here fetch all the titles from the continuous crawl database
		try {
			con = DriverManager.getConnection(url, user, passwd);
			fetch_titles_from_continuous_crawl();
			System.out.println("Titles fetched");
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}

		// Reading the property of our database for the duplicated titles storage
		props = new Properties();
		in = null;      
		try {
			in = new FileInputStream(database_duplication_con_path);
			props.load(in);
		} catch (IOException ex) {
			Logger lgr = Logger.getLogger(ProcessContinuousCrawl.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				Logger lgr = Logger.getLogger(ProcessContinuousCrawl.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}
		// the following properties have been identified
		url = props.getProperty("db.url");
		user = props.getProperty("db.user");
		passwd = props.getProperty("db.passwd");
		// we will here sabe all metrics to the duplicated title database
		try {
			con = DriverManager.getConnection(url, user, passwd);
			cleaning_database();
			System.out.println("Saving Titles metrics");
			save_titles_duplication_metrics();
			con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble with the database");
			System.exit(0);
		}
	}

	private static void cleaning_database() throws SQLException{
		PreparedStatement drop_nodes_table_st = con.prepareStatement(drop_CURRENT_CONTINUOUS_DUPLICATES_table);
		drop_nodes_table_st.executeUpdate();
		System.out.println("Dropping the old CURRENT_CONTINUOUS_DUPLICATES table");

		PreparedStatement create_nodes_table_st = con.prepareStatement(create_CURRENT_CONTINUOUS_DUPLICATESs_table);
		create_nodes_table_st.executeUpdate();
		System.out.println("Creating the new CURRENT_CONTINUOUS_DUPLICATES table");
	}

	public static void save_titles_duplication_metrics() throws SQLException{
		con.setAutoCommit(false);
		PreparedStatement pst = con.prepareStatement(insert_statement);
		int batch_current_size=0;
		Iterator<Map.Entry<String,List<URLTitleInfo>>> title_datas_it = titles_data.entrySet().iterator();
		// we loop over each entry
		while (title_datas_it.hasNext()) {
			Map.Entry<String,List<URLTitleInfo>> pairs = (Map.Entry<String,List<URLTitleInfo>>)title_datas_it.next();
			// we are here just interested by our argument naming
			String title =pairs.getKey();
			List<URLTitleInfo> duplicated_urls =pairs.getValue();
			int nb_urls = duplicated_urls.size();
			// we have at least two urls with the same title
			if (nb_urls>=2 && (!"".equals(title))){
				pst.setString(1,title);
				pst.setInt(2,nb_urls);
				pst.setString(3,translate_to_url(duplicated_urls));
				Date current_date = new Date();
				java.sql.Date sqlDate = new java.sql.Date(current_date.getTime());
				pst.setDate(4,sqlDate);
				pst.setString(5,duplicated_urls.get(0).getMagasin());
				pst.setString(6,duplicated_urls.get(0).getRayon());
				pst.addBatch();
				batch_current_size++;
				if (batch_current_size == batch_size){
					System.out.println("Inserting a batch");
					pst.executeBatch();		 
					con.commit();
					batch_current_size=0;
				}
			}					
		}
		System.out.println("Inserting the last batch");
		pst.executeBatch();		 
		con.commit();
	}

	private static String translate_to_url(List<URLTitleInfo> to_translate){
		StringBuilder to_append = new StringBuilder();
		for (URLTitleInfo info : to_translate){
			to_append.append(info.getUrl() +";");
		}
		return to_append.toString();
	}

	public static void fetch_titles_from_continuous_crawl(){
		// getting the URLs infos for each rayon
		PreparedStatement field_pst;
		try {
			field_pst  = con.prepareStatement(select_statement);
			System.out.println("I am requesting the database, please wait a few seconds");
			ResultSet field_rs = field_pst.executeQuery();
			System.out.println("Processing all titles");
			while (field_rs.next()) {
				URLTitleInfo url_info = new URLTitleInfo();
				String title = field_rs.getString(1);
				//System.out.println("Adding title : "+title);
				String url = field_rs.getString(2);
				String magasin = field_rs.getString(3);
				String rayon = field_rs.getString(4);
				Boolean isVendor = field_rs.getBoolean(5);
				String page_type = field_rs.getString(6);
				url_info.setUrl(url);
				url_info.setTitle(title);
				url_info.setMagasin(magasin);
				url_info.setRayon(rayon);
				url_info.setVendor(isVendor ? "Cdiscount" : "Market Place");
				url_info.setPage_type(page_type);
				List<URLTitleInfo> loc_infos = titles_data.get(title);
				if (loc_infos == null){
					loc_infos = new ArrayList<URLTitleInfo>();
					titles_data.put(title, loc_infos);
				}
				loc_infos.add(url_info);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Database trouble with the continuous crawl database");
			e.printStackTrace();
		}
	}
}
