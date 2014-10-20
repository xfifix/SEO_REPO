package com.processing;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgresql.util.PSQLException;

public class LetterDartyEnrichingDatabase {
	public static int my_threshold=0;
	public static void main(String[] args){
		// Reading the property of our database
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream("database.properties");
			props.load(in);
		} catch (IOException ex) {
			Logger lgr = Logger.getLogger(LetterDartyEnrichingDatabase.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				Logger lgr = Logger.getLogger(LetterDartyEnrichingDatabase.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
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
		// the characters we are looping over
		char[] ch = {'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

		try {
			con = DriverManager.getConnection(url, user, passwd);
			//			//DetectorFactory.loadProfile("profiles");
			//			// we here fetch the base of key words to fetch from Amazon
			//			pst = con.prepareStatement("SELECT distinct keyword FROM KEYWORDS");
			//			rs = pst.executeQuery();
			int counter=0;

			counter++;

			for (int i=0;i<ch.length;i++){
				for (int j=-1;j<ch.length;j++){
					String beginning_string = Character.toString(ch[i]);
					if (j>=0){
						beginning_string=beginning_string+Character.toString(ch[j]);
					}
					if (counter>=my_threshold){
						System.out.println("Enriching database for beginning of word : "+beginning_string);
						// scraping the suggestions
						List<String> my_suggestions=DartyRequestingClass.fetch(beginning_string);
						System.out.println("Suggestion : "+beginning_string+" results "+my_suggestions);
						// inserting each suggestions
						for (String suggestion : my_suggestions){
							/// inserting in the amazon database
							String am_stm = "INSERT INTO DARTY_KEYWORDS(KEYWORD)"
									+ " VALUES(?)";
							pst = con.prepareStatement(am_stm);
							pst.setString(1,suggestion);
							try {
								System.out.println("Inserting in DARTY_KEYWORDS keyword the suggested word : "+suggestion);
								System.out.println("Counter : "+counter);
								pst.executeUpdate();
								counter++;
							} catch (PSQLException e){
								// the primary key constraint has been violated
								System.out.println("Failed inserting "  +suggestion + " : already in database");
								e.printStackTrace();
								System.out.println("Counter : "+counter);
							}
						}
						
					}
				}
			}
			// updating the enriched database the keyword and all the suggestions from amazon
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(LetterDartyEnrichingDatabase.class.getName());
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
				Logger lgr = Logger.getLogger(LetterDartyEnrichingDatabase.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

	}
}
