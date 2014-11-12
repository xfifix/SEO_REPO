package crawl4j.daemon.links;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class FastBatchPostGresLinksDaemon {

	private static Map<NodeInfos,Set<String>> nodes_infos = new HashMap<NodeInfos,Set<String>>();
    private static Map<String, Integer> node_locator = new HashMap<String, Integer>(); 

	private static int counter = 0;
	private static String fetching_request = "SELECT URL, STATUS_CODE, MAGASIN, PAGE_TYPE, LINKS FROM CRAWL_RESULTS WHERE DEPTH >0 ORDER BY DEPTH LIMIT 1000000";

	private static String insert_node_statement ="INSERT INTO NODES (LABEL, MAGASIN, PAGE_TYPE, STATUS_CODE, URL)"
			+ " VALUES(?,?,?,?,?)";
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
		try{
			// fetching data from the Postgresql data base and looping over
			looping_over_urls();
		} catch (SQLException e){
			e.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}

		building_database();
	}

	private static void building_database(){
		// we create all nodes at once
		all_nodes_creation();
		// we create all relations at once
		all_relations_creation();
	}

	private static void all_nodes_creation(){
		Iterator it = nodes_infos.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();
			NodeInfos url_infos =(NodeInfos)pairs.getKey();
			//Set<String> outgoing_links = (Set<String>)pairs.getValue();
			System.out.println("Creating node : "+url_infos.getUrl());
			// we create the node and we put its id into the cache
			try {
				create_node_without_finding(url_infos);
				System.out.println("Node created : "+url_infos.getUrl());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Trouble creating node : "+url_infos.getUrl());
				e.printStackTrace();
			}
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
	
	private static void all_relations_creation(){
		Iterator it = nodes_infos.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry)it.next();
			NodeInfos url_infos =(NodeInfos)pairs.getKey();
			Set<String> outgoing_links = (Set<String>)pairs.getValue();
			System.out.println("Creating node : "+url_infos.getUrl());
			// we create the node and we put its id into the cache
			try {
				relations_insertion(url_infos.getUrl(),outgoing_links);	
				System.out.println("Relations created for node : "+url_infos.getUrl());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Trouble creating relations for node : "+url_infos.getUrl());
				e.printStackTrace();
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

	private static void create_node_without_finding(NodeInfos infos) throws SQLException{
		PreparedStatement insert_st = con.prepareStatement(insert_node_statement,Statement.RETURN_GENERATED_KEYS);
		//(LABEL, MAGASIN, PAGE_TYPE, STATUS_CODE, URL)
		insert_st.setString(1,infos.getMagasin());
		insert_st.setString(2,infos.getMagasin());
		insert_st.setString(3,infos.getType());
		insert_st.setInt(4,infos.getStatus());
		insert_st.setString(5,infos.getUrl());
		insert_st.executeUpdate();
		ResultSet rs = insert_st.getGeneratedKeys();
		int inserted_keys=0;
		if (rs != null && rs.next()) {
			inserted_keys = rs.getInt(1);
		}
		node_locator.put(infos.getUrl(), inserted_keys);
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

	public static void looping_over_urls() throws SQLException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs and outside links from the crawl results database");
		PreparedStatement pst = con.prepareStatement(fetching_request);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			counter++;
			String url_node = rs.getString(1);
			Integer status = rs.getInt(2);
			String magasin = rs.getString(3);
			String page_type = rs.getString(4);
			String output_links = rs.getString(5);
			NodeInfos result = new NodeInfos();
			result.setUrl(url_node);
			result.setStatus(status);
			result.setMagasin(magasin);
			result.setType(page_type);
			Set<String> outSet = parse_nodes_out_links(output_links);
			nodes_infos.put(result, outSet);
			System.out.println("Getting URL number :"+counter + " : " +url_node);
		}
	}

	private static void createRelationShip(Integer beginningNode, Integer endingNode) throws SQLException{
		PreparedStatement insert_st = con.prepareStatement(insert_relation_statement);
		insert_st.setInt(1, beginningNode);
		insert_st.setInt(2,endingNode);
		insert_st.executeUpdate();
	}

	private static class NodeInfos{	
		private String url;
		private String magasin;
		private Integer status;
		private String type;
	
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public String getMagasin() {
			return magasin;
		}
		public void setMagasin(String magasin) {
			this.magasin = magasin;
		}
		public Integer getStatus() {
			return status;
		}
		public void setStatus(Integer status) {
			this.status = status;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
	}
}
