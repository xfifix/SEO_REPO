package com.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgresql.util.PSQLException;

public class PopulatingDatabase {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/keywords_enrichment.properties";

	public static void main(String[] args){
		// Reading the property of our database
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
		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");

		// Instantiating the database
		Connection con = null;
		PreparedStatement pst = null;
		// the csv file variables
		ResultSet rs = null;
		try {
			con = DriverManager.getConnection(url, user, passwd);

			// we here fetch the base of key words to fetch from Amazon
			pst = con.prepareStatement("SELECT distinct keyword FROM KEYWORDS");
			rs = pst.executeQuery();
			while (rs.next()) {
				String my_string=rs.getString(1);
				StringTokenizer token= new StringTokenizer(my_string);
				while (token.hasMoreElements()) {    
					String my_word=(String)token.nextElement();

					String stm = "INSERT INTO UNIQUE_KEYWORDS(KEYWORD)"
							+ " VALUES(?)";

					pst = con.prepareStatement(stm);
					pst.setString(1,my_word);
					try {
						System.out.println("Inserting word"+my_word);
						pst.executeUpdate();
					} catch (PSQLException e){
						// the primary key constraint has been violated
						e.printStackTrace();
					}
				}
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(EnrichingDatabase.class.getName());
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
				//                if (br != null) {
				//                    br.close();
				//                }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(EnrichingDatabase.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

	}
}
