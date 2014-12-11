package com.merging;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MergingReferential {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/keywords_enrichment.properties";
	private static int counter = 0;
	public static void main(String args[]){
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
		try{
			con = DriverManager.getConnection(url, user, passwd);
			update_referential(con,"AMAZON");
			update_referential(con,"DARTY");
			update_referential(con,"GOOGLE");
			update_referential(con,"PM");
			update_referential(con,"RDC");
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(MergingReferential.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);
		} finally {
			try {
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(MergingReferential.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
	}

	public static void update_referential(Connection con, String source) throws SQLException{
		// inserting the referential
		String select_from_referential = "SELECT keyword FROM "+source+"_KEYWORDS";
		PreparedStatement pst = con.prepareStatement(select_from_referential);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			String keyword = rs.getString(1);
			System.out.println("Inserting : "+counter +"  "+keyword);
			counter++;
			// Inserting the keyword in the referential
			String ref_stm = "INSERT INTO REFERENTIAL_KEYWORDS(KEYWORD,SOURCE,SEARCH_VOLUME,CDS_TREND)"
					+ " VALUES(?,?,?,?)";
			pst = con.prepareStatement(ref_stm);
			pst.setString(1,keyword);
			pst.setString(2,source);
			pst.setInt(3,-1);
			pst.setInt(4,-1);	
			try{
				pst.executeUpdate();
			}catch (Exception e){
				System.out.println(keyword + " already there");
				e.printStackTrace();
			}
		}
	}
}
