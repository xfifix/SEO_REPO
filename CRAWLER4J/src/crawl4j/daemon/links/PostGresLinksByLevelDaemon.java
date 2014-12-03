package crawl4j.daemon.links;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PostGresLinksByLevelDaemon {

	private static Map<String, Integer> node_locator = new HashMap<String, Integer>(); 
	private static int counter = 0;
	private static String fetching_by_level_request= "SELECT URL, LINKS FROM CRAWL_RESULTS WHERE DEPTH BETWEEN ";
	private static String order_by_default = " ORDER BY DEPTH ";

	private static String find_node_statement ="SELECT ID FROM NODES WHERE LABEL=?";
	private static String insert_node_statement ="INSERT INTO NODES (LABEL)"
			+ " VALUES(?)";
	private static String insert_relation_statement ="INSERT INTO EDGES (SOURCE, TARGET)"
			+ " VALUES(?,?)";

	private static Connection con; 

	public static void main(String[] args){
		try {
			instantiate_connection();
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}
		for (int depth=1;depth<10;depth++){
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
		String url="jdbc:postgresql://localhost/CRAWL4J";
		String user="postgres";
		String passwd="mogette";
		con = DriverManager.getConnection(url, user, passwd);
	}




	public static void looping_over_urls(int depth) throws SQLException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs between depth "+depth +" and "+(depth +1));
		String level_statement = fetching_by_level_request + depth + " and "+(depth +1)+order_by_default;
		PreparedStatement pst = con.prepareStatement(level_statement);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			counter++;
			String url_node = rs.getString(1);
			String output_links = rs.getString(2);
			System.out.println("Dealing with URL number :"+counter + " : " +url_node);
			manage_input(url_node,output_links);
		}
	}

	public static void manage_input(String url_node, String output_links){
		// creating the nodes in neo4j

		try {
			create_node(url_node);
			Set<String> parsed_output = parse_nodes_out_links(output_links);
			for (String tocreate : parsed_output){
				create_node(tocreate);
			}
			relations_insertion(url_node,parsed_output);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Trouble creating node : "+url_node);
			e.printStackTrace();
		}
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
