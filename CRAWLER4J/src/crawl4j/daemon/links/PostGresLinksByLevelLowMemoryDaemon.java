package crawl4j.daemon.links;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.url.WebURL;

public class PostGresLinksByLevelLowMemoryDaemon {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static Map<String, Integer> node_locator = new HashMap<String, Integer>(); 

	private static int counter = 0;
	// the depth up to which we want to go
	private static int depth_threshold = 5;

	private static String fetching_by_level_request= "SELECT URL, LINKS FROM CRAWL_RESULTS WHERE DEPTH BETWEEN ";
	private static String order_by_depth = " ORDER BY DEPTH ";

	private static Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" + "|ico|png|tiff?|mid|mp2|mp3|mp4"
			+ "|wav|avi|mov|mpeg|ram|m4v|pdf" + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	private static String find_node_statement ="SELECT ID FROM NODES WHERE LABEL=?";
	private static String insert_node_statement ="INSERT INTO NODES (LABEL)"
			+ " VALUES(?)";
	private static String insert_relation_statement ="INSERT INTO EDGES (SOURCE, TARGET)"
			+ " VALUES(?,?)";
	private static String beginning_string = "^\\s*http://([a-z0-9]*\\.)*www.cdiscount.com.*";

	private static Connection con; 

	public static void main(String[] args){
		try {
			instantiate_connection();
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}
	
		for (int depth=1;depth<depth_threshold;depth ++){
			try{
				// fetching data from the Postgresql data base and looping over
				looping_over_urls(depth);
			} catch (SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with the POSTGRESQL database");
				System.exit(0);
			}
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


	public static void looping_over_urls(int depth) throws SQLException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs between depth "+depth +" and "+(depth +1));
		String level_statement = fetching_by_level_request + depth + " and "+(depth +1)+order_by_depth;
		PreparedStatement pst = con.prepareStatement(level_statement);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			counter++;
			String url_node = rs.getString(1);
			String output_links = rs.getString(2);
			System.out.println("Depth :"+depth+"Dealing with URL number : "+counter + " : " +url_node);
			manage_input(url_node,output_links);
		}
	}

	public static void manage_input(String url_node, String output_links){
		// creating the nodes in neo4j
		try {
			create_node(url_node);
			Set<String> parsed_output = parse_nodes_out_links_and_create(output_links);
			relations_insertion(url_node,parsed_output);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Trouble creating node : "+url_node);
			e.printStackTrace();
		}
	}

	private static Set<String> parse_nodes_out_links_and_create(String output_links) throws SQLException{
		output_links = output_links.replace("[", "");
		output_links = output_links.replace("]", "");
		String[] url_outs = output_links.split(",");
		Set<String> outputSet = new HashSet<String>();
		for (String url_out : url_outs){
			WebURL web_url = new WebURL();
			web_url.setURL(url_out);
			if ((shouldVisit(url_out))){
				url_out=url_out.trim();
				create_node(url_out);
				outputSet.add(url_out);
			}
		}
		return outputSet;
	}

	private static boolean shouldVisit(String url_out) {
		String href = url_out.toLowerCase();
		return !filters.matcher(href).matches() && href.matches(beginning_string);
	}


	private static void relations_insertion(String url,Set<String> outgoing_links) throws SQLException{
		int total_size = outgoing_links.size();
		int local_counter = 0;
		Integer beginningNode = node_locator.get(url);
		for (String ending_Node_URL : outgoing_links){
			Integer endingNode = node_locator.get(ending_Node_URL);
			if (endingNode != null && !(beginningNode.equals(endingNode))){			
				System.out.println(" Beginning node : " + beginningNode);
				System.out.println(" Ending node : "+endingNode);
				//URI relationshipUri = addRelationship( beginningNode, endingNode, "link","{}");
				//System.out.println("First relationship URI : "+relationshipUri);
				createRelationShip(beginningNode, endingNode);
				local_counter++;
			} else {
				System.out.println("Trouble with url : "+url);
				System.out.println("One node has not been found : "+url+total_size);
			}
		}
		System.out.println("Having inserted "+local_counter+" over "+total_size);
	}

	private static void createRelationShip(Integer beginningNode, Integer endingNode) throws SQLException{
		PreparedStatement insert_st = con.prepareStatement(insert_relation_statement);
		insert_st.setInt(1, beginningNode);
		insert_st.setInt(2,endingNode);
		insert_st.executeUpdate();
	}

	private static Integer find_node(String url_to_search) throws SQLException{
		Integer found_id = null;
		PreparedStatement pst = con.prepareStatement(find_node_statement);
		pst.setString(1, url_to_search);
		ResultSet rs = pst.executeQuery();
		if (rs.next()) {
			found_id = rs.getInt(1);
		}
		return found_id;
	}

	private static void create_node(String url_node) throws SQLException{
		Integer potential_id = find_node(url_node);
		// the node is not present in the database, we create it
		if (potential_id == null){
			PreparedStatement insert_st = con.prepareStatement(insert_node_statement,Statement.RETURN_GENERATED_KEYS);
			//insert_st.setString(1,URL_Utilities.checkMagasin(url_node));
			insert_st.setString(1,url_node);
			insert_st.executeUpdate();
			ResultSet rs = insert_st.getGeneratedKeys();
			int inserted_keys=0;
			if (rs != null && rs.next()) {
				inserted_keys = rs.getInt(1);
			}
			node_locator.put(url_node, inserted_keys);
		} else {
			// the node is already in the database, we just refresh the cache
			node_locator.put(url_node,potential_id);
		}
	}
}
