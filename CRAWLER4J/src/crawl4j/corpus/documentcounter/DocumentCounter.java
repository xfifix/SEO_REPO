package crawl4j.corpus.documentcounter;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class DocumentCounter {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";

	private static String select_statement="select DOC_LIST from CORPUS_WORDS";
	private static String update_statement ="UPDATE CORPUS_WORDS_METADATA SET NB_TOTAL_DOCUMENTS=? WHERE THEMA='TOTAL_NUMBER_DOCUMENTS'";

	private static Set<String> documents_set = new HashSet<String>();
	private static Connection con; 

	public static void main(String[] args){
		try {
			instantiate_connection();
			counting_documents();
			System.out.println("We found  : " + documents_set.size() + " documents ");
			update_database();
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}
	}

	private static void instantiate_connection() throws SQLException{
		// instantiating database connection
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

	private static void update_database() throws SQLException{
		PreparedStatement st = con.prepareStatement(update_statement);
		st.setInt(1,documents_set.size());
		st.executeUpdate();
		st.close();
	}

	private static void counting_documents() throws SQLException{
		System.out.println("Getting all URLs and outside links from the crawl results database");
		PreparedStatement pst = con.prepareStatement(select_statement);
		ResultSet rs = pst.executeQuery();
		int counter=0;
		while (rs.next()) {
			counter++;
			String docs_list = rs.getString(1);
			Set<String> docs = parse_nodes_out_links(docs_list);
			documents_set.addAll(docs);
			System.out.println("Appending documents for number :"+counter + " with size : " +docs.size());
		}		
	}

	private static Set<String> parse_nodes_out_links(String output_links){
		output_links = output_links.replace("[", "");
		output_links = output_links.replace("]", "");
		String[] url_outs = output_links.split(",");
		Set<String> outputSet = new HashSet<String>();
		for (String url_out : url_outs){
			outputSet.add(url_out);
		}
		return outputSet;
	}
}
