package crawl4j.corpus.documentcounter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class DocumentCounter {
	private static String select_statement="select DOC_LIST from CORPUS_WORDS";
	private static Set<String> documents_set = new HashSet<String>();
	private static Connection con; 
	
	public static void main(String[] args){
		try {
			instantiate_connection();
			counting_documents();
			System.out.println("We found  : " + documents_set.size() + " documents ");
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}
	}
	
	private static void instantiate_connection() throws SQLException{
		// instantiating database connection
		String url="jdbc:postgresql://localhost/CRAWL4J";
		String user="postgres";
		String passwd="mogette";
		con = DriverManager.getConnection(url, user, passwd);
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