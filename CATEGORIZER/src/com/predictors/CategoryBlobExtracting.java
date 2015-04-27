package com.predictors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class CategoryBlobExtracting {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/categorizer.properties";
	private static Connection con; 

	private static String fetching_blobsoid_by_level_request= "SELECT CATEGORY, BLOBOID FROM CATEGORY_BAG_OF_WORDS";

	public static void main(String[] args){
		try {
			instantiate_connection();
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}	

		try{
			con.setAutoCommit(false);
			// fetching data from the Postgresql data base and looping over
			looping_over_blobs();
		} catch (SQLException | ClassNotFoundException | IOException e){
			e.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}

	}

	@SuppressWarnings({ "deprecation", "unchecked" })
	public static void looping_over_blobs() throws SQLException, IOException, ClassNotFoundException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all BLOBS ");
		PreparedStatement pst = con.prepareStatement(fetching_blobsoid_by_level_request);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			String url = rs.getString(1);
			Integer blobOID = rs.getInt(2);
			System.out.println("Opening BLOB for oid : "+blobOID+" and URL : "+url);
			LargeObjectManager lobj = ((org.postgresql.PGConnection)con).getLargeObjectAPI();
			LargeObject obj = lobj.open(blobOID, LargeObjectManager.READ);
			// Read the data
			byte buf[] = new byte[obj.size()];
			obj.read(buf, 0, obj.size());
			// Do something with the data read here

			Map<String,Double> myList =null;
			ByteArrayInputStream bis = null;
			ObjectInputStream ois = null;
			try {
				bis = new ByteArrayInputStream(buf);
				ois = new ObjectInputStream(bis);
				myList = (Map<String,Double>)ois.readObject();
			} finally {
				if (bis != null) {
					bis.close();
				}
				if (ois != null) {
					ois.close();
				}
			}

			//	byte[] uncompressed_bytes = uncompress_byte_stream(buf);
			System.out.println("Categor bag of words size : "+myList.size());
			// Close the object
			obj.close();

		}
		rs.close();
		pst.close();
	}

	public static byte[] uncompress_byte_stream(byte[] dataToCompress) {
		ByteArrayInputStream bytein = new java.io.ByteArrayInputStream(dataToCompress);
		GZIPInputStream gzin = null;
		ByteArrayOutputStream byteout =null;
		try {
			gzin = new java.util.zip.GZIPInputStream(bytein);
			byteout = new java.io.ByteArrayOutputStream();
			try
			{
				int res = 0;
				byte buf[] = new byte[2048];
				while (res >= 0) {
					res = gzin.read(buf, 0, buf.length);
					if (res > 0) {
						byteout.write(buf, 0, res);
					}
				}
			}
			finally
			{
				byteout.close();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if (gzin != null){
				try {
					gzin.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		byte uncompressed[] = new byte[0];
		if (byteout != null){
			uncompressed = byteout.toByteArray();
		}
		return uncompressed;
	}

	private static void instantiate_connection() throws SQLException{
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
		con = DriverManager.getConnection(url, user, passwd);
	}
}
