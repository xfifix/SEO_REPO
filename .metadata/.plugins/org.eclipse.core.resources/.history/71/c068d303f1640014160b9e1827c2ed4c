package crawl4j.daemon.links;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class LinksDaemon {


	private static Map<String, Set<String>> output_links_map = new HashMap<String, Set<String>>();
	private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";
    private static List<URLNode> neo4j_nodes = new ArrayList<URLNode>();
	
	
	public static void main(String[] args){
		//we here check the Neo4j database is up
		if (!(checkDatabaseIsRunning() == 200)){
			System.out.println("Trouble with the NEO4J graph database");
			System.exit(0);
		}
		//we here empty the Neo4j database that we'll be filling up with brand new data
		cleanUpDatabase();

		try{
			// fetching data from the Postgresql data base and looping over
			looping_over_urls();
		} catch (SQLException e){
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}
		
		// creating the relationships in neo4j		
		building_relationships();
	}

	
	
	public static void looping_over_urls() throws SQLException{
		// here is the links daemon starting point
		String url="jdbc:postgresql://localhost/CRAWL4J";
		String user="postgres";
		String passwd="mogette";

		Connection con = DriverManager.getConnection(url, user, passwd);
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs and outside links from the crawl results database");
		PreparedStatement pst = con.prepareStatement("SELECT URL, LINKS FROM CRAWL_RESULTS LIMIT 1000");
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			String url_node = rs.getString(1);
			String output_links = rs.getString(2);
			manage_input(url_node,output_links);
		}
	}


	private static int checkDatabaseIsRunning()
	{
		// START SNIPPET: checkServer
		WebResource resource = Client.create()
				.resource( SERVER_ROOT_URI );
		ClientResponse response = resource.get( ClientResponse.class );

		System.out.println( String.format( "GET on [%s], status code [%d]",
				SERVER_ROOT_URI, response.getStatus() ) );
		response.close();
		return response.getStatus();
	}

	public static void manage_input(String url_node, String output_links){
		// creating the nodes in neo4j
		create_node(url_node);
        // populating the incoming map
		populate_out_links(url_node, output_links);
	}

	private static void building_relationships(){

	}

	private static void populate_out_links(String url_node, String output_links){
		// we here compute the output links
		output_links = output_links.replace("[", "");
		output_links = output_links.replace("]", "");
		String[] url_outs = output_links.split(",");
		for (String url_out : url_outs){
			Set<String> outputSet = output_links_map.get(url_out);
			if ( outputSet == null){
				// we don't have any entries yes
				outputSet  = new HashSet<String>();
				output_links_map.put(url_out, outputSet);
			}
			// we add the currently parsed URL
			outputSet.add(url_node);
		}
	}

	private static void cleanUpDatabase(){
		sendTransactionalCypherQuery("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r");
	}

	private static void sendTransactionalCypherQuery(String query) {
		final String txUri = SERVER_ROOT_URI + "transaction/commit";
		WebResource resource = Client.create().resource( txUri );

		String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
		ClientResponse response = resource
				.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( payload )
				.post( ClientResponse.class );

		System.out.println( String.format(
				"POST [%s] to [%s], status code [%d], returned data: "
						+ System.getProperty( "line.separator" ) + "%s",
						payload, txUri, response.getStatus(),
						response.getEntity( String.class ) ) );

		response.close();
	}

	private static URI createNode()
	{
		final String nodeEntryPointUri = SERVER_ROOT_URI + "node";
		// http://localhost:7474/db/data/node

		WebResource resource = Client.create()
				.resource( nodeEntryPointUri );
		// POST {} to the node entry point URI
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "{}" )
				.post( ClientResponse.class );

		final URI location = response.getLocation();
		System.out.println( String.format(
				"POST to [%s], status code [%d], location header [%s]",
				nodeEntryPointUri, response.getStatus(), location.toString() ) );
		response.close();

		return location;
	}

	private static void create_node(String url_node){
		URI node_location = createNode();
		addProperty(node_location,"name",url_node);		
		URLNode node = new URLNode();
		node.setNode_url(url_node);
		node.setNeo4jURI(node_location);
		neo4j_nodes.add(node);
	}

	private static void addProperty( URI nodeUri, String propertyName,
			String propertyValue )
	{
		String propertyUri = nodeUri.toString() + "/properties/" + propertyName;
		// http://localhost:7474/db/data/node/{node_id}/properties/{property_name}

		WebResource resource = Client.create()
				.resource( propertyUri );
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "\"" + propertyValue + "\"" )
				.put( ClientResponse.class );

		System.out.println( String.format( "PUT to [%s], status code [%d]",
				propertyUri, response.getStatus() ) );
		response.close();
	}

}
