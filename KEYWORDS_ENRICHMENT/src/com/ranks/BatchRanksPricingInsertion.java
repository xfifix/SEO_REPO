package com.ranks;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.magasin.attributing.URL_Utilities;

public class BatchRanksPricingInsertion {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/keywords_enrichment.properties";
	private static String request_url = "http://www.cdiscount.com/sa-10/";
	private static String insert_statement = "INSERT INTO PRICING_KEYWORDS(KEYWORD,LANGUAGE,SEARCH_ENGINE,"
			+ "CHECK_DATE,URL,SEARCH_POSITION,ABSOLUTE_POSITION,PAGE_NUMBER"
			+ ",RESULT_NUMBER,STAR_NUMBER,CPC_MAX,SEARCH_VOLUME,COMPETITION,"
			+ "PARAMETER,DOMAIN,SUBDOMAIN,MAGASIN,RAYON,PRODUIT)"
			+ " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	private static int already_inserted =10849062;
	private static int batch_size = 100000;

	public static void main(String[] args) {
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

		// the following properties have been identified for our files to parse 
		// and insert into a database
		String csvFile = "/home/sduprey/My_Data/My_Ranks_Pricing/spider_2014-09-29-780084051.csv";

		// Instantiating the database
		Connection con = null;
		PreparedStatement pst = null;
		// the csv file variables
		ResultSet rs = null;
		BufferedReader br = null;
		String line = "";
		String header = null;
		String[] column_names = null;
		String cvsSplitBy = "\t";
		int nb_line=1;
		// last error
		try {
			con = DriverManager.getConnection(url, user, passwd);
			br = new BufferedReader(new FileReader(csvFile));
			// we skip the first line : the headers
			header = br.readLine();
			column_names= header.split(cvsSplitBy);
			con.setAutoCommit(false);
			pst = con.prepareStatement(insert_statement);
			int my_batch_size=0;
			while ((line = br.readLine()) != null) {
				// System.out.println(line);
				// use comma as separator


				if (nb_line > already_inserted){
					System.out.println("Inserting line number :"+nb_line);
					String[] splitted_line = line.split(cvsSplitBy);
					// INSERT INTO 
					String toinsertkeyword = splitted_line[0];
					CdiscountInformation my_info = new CdiscountInformation();

					// we fetch this information in a separate process
					//				try {
					//					my_info=getKeywordInfo(toinsertkeyword);
					//				}catch (IOException e){
					//					e.printStackTrace();
					//					System.out.println("Trouble fetching information "+ toinsertkeyword);
					//				}


					pst.setString(1,toinsertkeyword);
					pst.setString(2,splitted_line[1]);
					pst.setString(3,splitted_line[2]);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd"); 
					Date  date = sdf.parse(splitted_line[3]);
					java.sql.Date sqlDate = new java.sql.Date(date.getTime());

					pst.setDate(4,sqlDate);
					pst.setString(5,splitted_line[4]);
					pst.setInt(6,Integer.valueOf(splitted_line[5]));
					pst.setInt(7,"".equals(splitted_line[6]) ? 0 : Integer.valueOf(splitted_line[6]));
					pst.setInt(8,Integer.valueOf(splitted_line[7]));
					pst.setLong(9,Long.valueOf(splitted_line[8]));
					pst.setInt(10,Integer.valueOf(splitted_line[9]));
					pst.setFloat(11,Float.valueOf(splitted_line[10]));
					pst.setInt(12,Integer.valueOf(splitted_line[11]));
					pst.setInt(13,Integer.valueOf(splitted_line[12]));
					pst.setString(14,splitted_line[13]);
					pst.setString(15,splitted_line[14]);
					pst.setString(16,splitted_line[15]);
					pst.setString(17,my_info.getMagasin());
					pst.setString(18,my_info.getRayon());
					pst.setString(19,my_info.getProduit());
					pst.addBatch();
					my_batch_size++;
				}
				if (my_batch_size == batch_size){
					pst.executeBatch();		 
					con.commit();
					System.out.println("Inserting a big batch here of 100000 lines");
					my_batch_size=0;
				}
				nb_line++;
			}

		} catch (Exception ex) {
			Logger lgr = Logger.getLogger(BatchRanksPricingInsertion.class.getName());
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
				Logger lgr = Logger.getLogger(BatchRanksPricingInsertion.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
	}


	public static CdiscountInformation getKeywordInfo(String keyword) throws IOException{
		keyword=keyword.replace(" ", "+");
		String my_url = request_url+keyword+".html";
		URL url = new URL(my_url);
		HttpURLConnection connection = (HttpURLConnection)url.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; en-GB;     rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)");
		// we here want to be redirected to the proper magasin
		connection.setInstanceFollowRedirects(true);
		connection.connect();	
		System.out.println(connection.getResponseCode());	
		String redirected_url =connection.getURL().toString(); 
		System.out.println(redirected_url);
		CdiscountInformation info =new CdiscountInformation();
		String magasin =URL_Utilities.checkMagasin(redirected_url);
		info.setMagasin(magasin);
		String rayon =URL_Utilities.checkRayon(redirected_url);
		info.setRayon(rayon);
		String produit =URL_Utilities.checkProduit(redirected_url);
		info.setProduit(produit);
		return info;
	}
}