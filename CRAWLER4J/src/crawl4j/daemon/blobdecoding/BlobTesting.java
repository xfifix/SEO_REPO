package crawl4j.daemon.blobdecoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

public class BlobTesting {
	public static void main(String[] args) throws SQLException, IOException{
		//creating_table();
		//inserting_blob();
		retrieving_blob();
		//updating_blob();
		
	}

	public static void inserting_blob() throws SQLException, IOException{
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
		// Close the large object
		obj.close();

		// Now insert the row into imageslo
		PreparedStatement ps = conn.prepareStatement("INSERT INTO imageslo VALUES (?, ?)");
		ps.setString(1, file.getName());
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

	public static void retrieving_blob() throws SQLException, UnsupportedEncodingException{
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
		ps.setString(1, "bordeaux.PNG");
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			// Open the large object for reading
			int oid = rs.getInt(1);
			LargeObject obj = lobj.open(oid, LargeObjectManager.READ);

			// Read the data
			byte buf[] = new byte[obj.size()];
			obj.read(buf, 0, obj.size());
			// Do something with the data read here
			String my_page_code_source = new String(buf, "UTF-8");
			System.out.println(my_page_code_source);
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
}