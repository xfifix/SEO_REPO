package crawl4j.daemon.blobdecoding;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class CompressedBlobTesting {
	private static String string_to_store = "je suis le code source de la page a stocker et je veux être stockée compressé et être décompressé pour être lu";
	public static void main(String[] args) throws SQLException{
		//creating_table();
		//compressing_inserting_blob();
		decompressing_retrieving_blob();
		//updating_blob();

	}

	public static void compressing_inserting_blob() throws SQLException, IOException{
		Connection conn = null;
		String url = "jdbc:postgresql://localhost/testdb";
		String user = "postgres";
		String password = "root";
		conn = DriverManager.getConnection(url, user, password);
		// inserting a blob
		// All LargeObject API calls must be within a transaction block
		conn.setAutoCommit(false);
		// Get the Large Object Manager to perform operations with
		LargeObjectManager lobj = ((org.postgresql.PGConnection)conn).getLargeObjectAPI();

		// Create a new large object
		int oid = lobj.create(LargeObjectManager.READ | LargeObjectManager.WRITE);

		// Open the large object for writing
		LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);

		// Now open the file
		byte[] bytes_to_compress = string_to_store.getBytes(Charset.forName("UTF-8"));
		String my_page_code_source_uncompressed = new String(bytes_to_compress, "UTF-8");
		System.out.println(my_page_code_source_uncompressed);
		byte[] bytes_compressed = compress_byte_stream(bytes_to_compress);
		String my_page_code_source_compressed = new String(bytes_compressed, "UTF-8");
		System.out.println(my_page_code_source_compressed);
		InputStream fis = new ByteArrayInputStream(bytes_compressed);
		// Copy the data from the file to the large object
		// 2048 is the buffer size, does not really matter
		byte buf[] = new byte[2048];
		int s, tl = 0;
		while ((s = fis.read(buf, 0, 2048)) > 0) {
			obj.write(buf, 0, s);
			tl += s;
		}
		System.out.println("BLOB byte size : "+tl);
		// Close the large object
		obj.close();

		// Now insert the row into imageslo
		PreparedStatement ps = conn.prepareStatement("INSERT INTO imageslo VALUES (?, ?)");
		ps.setString(1, "machainecompressee");
		ps.setInt(2, oid);
		ps.executeUpdate();
		ps.close();
		fis.close();

		// Finally, commit the transaction.
		conn.commit();
	}

	public static void creating_table(){
		Connection con = null;
		Statement st = null;

		String url = "jdbc:postgresql://localhost/testdb";
		String user = "postgres";
		String password = "root";
		try {
			con = DriverManager.getConnection(url, user, password);
			st = con.createStatement();
			con.setAutoCommit(false);
			st.addBatch("CREATE TABLE imageslo (imgname text, imgoid oid)");
			int counts[] = st.executeBatch();
			con.commit();
			System.out.println("Committed " + counts.length + " updates");
		} catch (SQLException ex) {
			System.out.println(ex.getNextException());
			if (con != null) {
				try {
					con.rollback();
				} catch (SQLException ex1) {
					ex1.printStackTrace();
				}
			}
			ex.printStackTrace();
		} finally {
			try {

				if (st != null) {
					st.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static void decompressing_retrieving_blob() throws SQLException{
		Connection conn = null;

		String url = "jdbc:postgresql://localhost/testdb";
		String user = "postgres";
		String password = "root";

		conn = DriverManager.getConnection(url, user, password);
		// All LargeObject API calls must be within a transaction block
		conn.setAutoCommit(false);

		// Get the Large Object Manager to perform operations with
		LargeObjectManager lobj = ((org.postgresql.PGConnection)conn).getLargeObjectAPI();
		PreparedStatement ps = conn.prepareStatement("SELECT imgoid FROM imageslo WHERE imgname = ?");
		//ps.setString(1, "example.html");
		ps.setString(1, "machainecompressee");
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			// Open the large object for reading
			int oid = rs.getInt(1);
			LargeObject obj = lobj.open(oid, LargeObjectManager.READ);

			// Read the data
			byte buf[] = new byte[obj.size()];
			obj.read(buf, 0, obj.size());
			// Do something with the data read here
			byte[] uncompressed_bytes = uncompress_byte_stream(buf);
			String my_page_code_source_recovered ="";
			try {
				my_page_code_source_recovered = new String(uncompressed_bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(my_page_code_source_recovered);
			// Close the object
			obj.close();
		}
		rs.close();
		ps.close();

		// Finally, commit the transaction.
		conn.commit();
	}

	public static void updating_blob() throws SQLException, IOException{
		Connection conn = null;
		String url = "jdbc:postgresql://localhost/testdb";
		String user = "postgres";
		String password = "root";
		conn = DriverManager.getConnection(url, user, password);
		// inserting a blob
		// All LargeObject API calls must be within a transaction block
		conn.setAutoCommit(false);
		// Get the Large Object Manager to perform operations with
		LargeObjectManager lobj = ((org.postgresql.PGConnection)conn).getLargeObjectAPI();


		// we here don't create the object (already created, we just get the oid
		// let's assume we know the oid the oid from the bordeaux png
		int oid = 285976;
		//		// Create a new large object
		//		int oid = lobj.create(LargeObjectManager.READ | LargeObjectManager.WRITE);

		// Open the large object for writing
		LargeObject obj = lobj.open(oid, LargeObjectManager.WRITE);
		// Now open the file
		File file = new File("C:\\Users\\Stefan.Duprey\\Documents\\Mes fichiers reçus\\example.html");
		FileInputStream fis = new FileInputStream(file);

		// Copy the data from the file to the large object
		// 2048 is the buffer size, does not really matter
		byte buf[] = new byte[2048];
		int s, tl = 0;
		while ((s = fis.read(buf, 0, 2048)) > 0) {
			obj.write(buf, 0, s);
			tl += s;
		}
		System.out.println("BLOB byte size : "+tl);
		obj.truncate(tl);
		// Close the large object
		obj.close();

		// we don't have to insert the row as the 
		//		// Now insert the row into imageslo
		//		PreparedStatement ps = conn.prepareStatement("INSERT INTO imageslo VALUES (?, ?)");
		//		ps.setString(1, file.getName());
		//		ps.setInt(2, oid);
		//		ps.executeUpdate();
		//		ps.close();
		//		fis.close();
		// Finally, commit the transaction.
		conn.commit();
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


	public static byte[] compress_byte_stream(byte[] dataToCompress ){
		ByteArrayOutputStream byteStream = null;
		try{
			byteStream =
					new ByteArrayOutputStream(dataToCompress.length);

			GZIPOutputStream zipStream =
					zipStream = new GZIPOutputStream(byteStream);
			try
			{
				zipStream.write(dataToCompress);
			}
			finally
			{
				zipStream.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			if (byteStream != null){
				try {
					byteStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		byte[] compressedData = new byte[0];
		if (byteStream != null){
			compressedData = byteStream.toByteArray();
		}	
		return compressedData;
	}
}