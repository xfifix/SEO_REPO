package crawl4j.corpus.wikipedia.fr;

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

public class WikipediaFRCorpusCrawlDataManagement {
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private Connection con;
	private static String find_statement="select DOC_LIST from CORPUS_WORDS where WORD=?";
	private static String insert_statement="INSERT INTO CORPUS_WORDS(WORD,NB_DOCUMENTS,DOC_LIST) values(?,?,?)";
	private static String update_statement="UPDATE CORPUS_WORDS SET NB_DOCUMENTS=?,DOC_LIST=? WHERE WORD=?";
	private int totalProcessedPages;
	private long totalTextSize;

	public WikipediaFRCorpusCrawlDataManagement() {
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
		try{
			con = DriverManager.getConnection(url, user, passwd);
		} catch (Exception e){
			System.out.println("Error instantiating either database or solr server");
			e.printStackTrace();
		}
	}

	public Set<String> parse_nodes_out_links(String output_links) {
		output_links = output_links.replace("[", "");
		output_links = output_links.replace("]", "");
		String[] url_outs = output_links.split(",");
		Set<String> outputSet = new HashSet<String>();
		for (String url_out : url_outs){
			url_out=url_out.trim();
			outputSet.add(url_out);
		}
		return outputSet;
	}

	public void updateWord(String word, String currentUrl){
		try{
			// finding if the world is already present in our database dictionary
			PreparedStatement select_st = con.prepareStatement(find_statement);
			select_st.setString(1,word);
			ResultSet rs = select_st.executeQuery();
			boolean found = false;
			String document_list="";
			if (rs.next()) {
				document_list=rs.getString(1);
				found = true;
			}
			select_st.close();
			if (found){	
				// we found it
				// we check if the current url is already in the list
				Set<String> links_set = parse_nodes_out_links(document_list);
				boolean isAlreadyPresent=links_set.contains(currentUrl);

				// if the document is already listed, the frequency is up to date, we do nothing
				if (!isAlreadyPresent){
					// else if the document is not present, we must update the database row by adding the document
					links_set.add(currentUrl);
					PreparedStatement update_st = con.prepareStatement(update_statement);
					//NB_DOCUMENTS=?,DOCUMENTS=? WHERE WORD=?
					update_st.setInt(1,links_set.size());
					update_st.setString(2,links_set.toString());
					update_st.setString(3,word);
					update_st.executeUpdate();
				}
			}else{
				// we did not find it	
				// we create the new document set
				Set<String> documents_set = new HashSet<String>();
				documents_set.add(currentUrl);
				// we have to add it 
				PreparedStatement insert_st = con.prepareStatement(insert_statement);
				//(WORD,NB_DOCUMENTS,DOCUMENTS) values(?,?,?)
				insert_st.setString(1,word);
				insert_st.setInt(2,documents_set.size());
				insert_st.setString(3,documents_set.toString());
				insert_st.executeUpdate();
			}
			System.out.println(Thread.currentThread()+"Committed " +  " updates");

		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble inserting "+word + " for URL : "+currentUrl);
//			if (con != null) {
//				try {
//					con.rollback();
//				} catch (SQLException ex1) {
//					ex1.printStackTrace();
//					System.out.println("Trouble inserting "+word + " for URL : "+currentUrl);
//				}
//			}
		}	
	}

	public int getTotalProcessedPages() {
		return totalProcessedPages;
	}

	public void setTotalProcessedPages(int totalProcessedPages) {
		this.totalProcessedPages = totalProcessedPages;
	}

	public long getTotalTextSize() {
		return totalTextSize;
	}

	public void setTotalTextSize(long totalTextSize) {
		this.totalTextSize = totalTextSize;
	}
}