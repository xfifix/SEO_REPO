package com.populating;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.urlutilities.URL_Utilities;

public class CurrentTitlePopulatingClass {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/title_duplication.properties";
	private static int counter = 0;
	private static String insert_statement = "INSERT INTO CURRENT_DUPLICATES(TITLE,NB_URLS,URLS,DUPLICATE_TIME,MAGASIN,RAYON,PRODUCT)"
			+ " VALUES(?,?,?,?,?,?,?)";
	public static void main(String[] args) {
		// Reading the property of our database
		Properties props = new Properties();
		FileInputStream in = null;      
		try {
			in = new FileInputStream(database_con_path);
			props.load(in);

		} catch (IOException ex) {

			Logger lgr = Logger.getLogger(CurrentTitlePopulatingClass.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {

			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				Logger lgr = Logger.getLogger(CurrentTitlePopulatingClass.class.getName());
				lgr.log(Level.SEVERE, ex.getMessage(), ex);
			}
		}

		// the following properties have been identified
		String url = props.getProperty("db.url");
		String user = props.getProperty("db.user");
		String passwd = props.getProperty("db.passwd");
		// the following properties have been identified for our files to parse 
		// and insert into a database
		String csvFile = "/home/sduprey/My_Data/My_GWT_Extracts/15012015.csv";
//		String csvFile = "/home/sduprey/My_Data/My_GWT_Extracts/extract_dupli_20_oct.csv";
		// Instantiating the database
		Connection con = null;
		PreparedStatement pst = null;
		// the csv file variables
		ResultSet rs = null;
		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = ",|;";
		int nb_line=1;
		int batch_current_size=1;
		// last error
		try {
			con = DriverManager.getConnection(url, user, passwd);
			br = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), "UTF-8"));	
			// we skip the first line : the headers
			header = br.readLine();
			column_names= header.split(cvsSplitBy);
			while ((line = br.readLine()) != null) {
				// System.out.println(line);
				// use comma as separator

				if (nb_line >= counter){
					try{
						System.out.println("Inserting line number :"+nb_line);
						String title = line.substring(0,line.lastIndexOf(","));
						String remaining = line.substring(line.lastIndexOf(",")+1,line.indexOf(";",line.lastIndexOf(",")+1));
						System.out.println("Inserting line number :"+nb_line);
						String[] splitted_line = remaining.split("\\|");
						String protocol = "";
						String magasin = "";
						String rayon = "";
						String produit = "";
						String domain = "";
						String current_url="";
						// we have at least two couples
						if (splitted_line.length>=2){
							current_url = splitted_line[0];
							magasin = URL_Utilities.checkMagasin(current_url);
							rayon = URL_Utilities.checkRayon(current_url);
							produit = URL_Utilities.checkProduit(current_url);
							//							System.out.println("Title : "+title);
							//							System.out.println("counter : "+counter);
							//							System.out.println("URL : "+remaining);
							//							System.out.println("Magasin : "+magasin);
							//							System.out.println("Rayon : "+rayon);
							//							System.out.println("Produit : "+produit);
							pst = con.prepareStatement(insert_statement);
							pst.setString(1,title);
							pst.setInt(2,counter);
							pst.setString(3,remaining);
							Date current_date = new Date();
							java.sql.Date sqlDate = new java.sql.Date(current_date.getTime());
							pst.setDate(4,sqlDate);
							pst.setString(5,magasin);
							pst.setString(6,rayon);
							pst.setString(7,produit);
							pst.executeUpdate();
						}					
					}catch (Exception e){
						e.printStackTrace();
						System.out.println("Trouble with line : " + line);
					}
					nb_line++;
				}
			}
		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(CurrentTitlePopulatingClass.class.getName());
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
				if (br != null) {
					br.close();
				}

			} catch (SQLException | IOException ex) {
				Logger lgr = Logger.getLogger(CurrentTitlePopulatingClass.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
	}
}