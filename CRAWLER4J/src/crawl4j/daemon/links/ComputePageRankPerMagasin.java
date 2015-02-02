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
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.gephi.data.attributes.api.AttributeColumn;
import org.gephi.data.attributes.api.AttributeController;
import org.gephi.data.attributes.api.AttributeModel;
import org.gephi.graph.api.DirectedGraph;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.Node;
import org.gephi.io.database.drivers.PostgreSQLDriver;
import org.gephi.io.importer.api.Container;
import org.gephi.io.importer.api.EdgeDefault;
import org.gephi.io.importer.api.ImportController;
import org.gephi.io.importer.plugin.database.EdgeListDatabaseImpl;
import org.gephi.io.importer.plugin.database.ImporterEdgeList;
import org.gephi.io.processor.plugin.DefaultProcessor;
import org.gephi.project.api.ProjectController;
import org.gephi.project.api.Workspace;
import org.gephi.statistics.plugin.PageRank;
import org.openide.util.Lookup;

import crawl4j.urlutilities.URL_Utilities;

public class ComputePageRankPerMagasin {
	
	private static String drop_nodes_table = "DROP TABLE IF EXISTS NODES";
	private static String create_nodes_table = "CREATE TABLE IF NOT EXISTS NODES (ID SERIAL PRIMARY KEY NOT NULL, LABEL TEXT, MAGASIN VARCHAR(100), STATUS_CODE INT, PAGE_TYPE VARCHAR(50)) TABLESPACE mydbspace";
	private static String create_nodes_index = "CREATE INDEX ON NODES (label)";
	private static String drop_edges_table = "DROP TABLE IF EXISTS EDGES";
	private static String create_edges_table = "CREATE TABLE IF NOT EXISTS EDGES (SOURCE INT,TARGET INT) TABLESPACE mydbspace";
	
	private static Map<NodeInfos,Set<String>> nodes_infos = new HashMap<NodeInfos,Set<String>>();
	private static Map<String, Integer> node_locator = new HashMap<String, Integer>(); 
	private static int counter = 0;
	private static String beginning_whole_fetching_request = "SELECT URL, STATUS_CODE, MAGASIN, PAGE_TYPE, LINKS FROM CRAWL_RESULTS WHERE MAGASIN = '";
	private static String end_request = "' AND DEPTH >0 ORDER BY DEPTH";
	private static String insert_node_statement ="INSERT INTO NODES (LABEL, MAGASIN, PAGE_TYPE, STATUS_CODE)"
			+ " VALUES(?,?,?,?)";
	private static String insert_relation_statement ="INSERT INTO EDGES (SOURCE, TARGET)"
			+ " VALUES(?,?)";
	
	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static String update_statement ="UPDATE CRAWL_RESULTS SET PAGE_RANK=? WHERE URL=?";
	private static Connection con;
	private static String[] magasins ={
		"animalerie",
		"bijouterie",
		"musique-instruments",
		"vetements-bebe",
		"bricolage-chauffage",
		"lingerie-feminine",
		"musique-cd-dvd",
		"jardin-animalerie",
		"le-sport",
		"maison",
		"telephonie",
		"vin-alimentaire",
		"boutique-erotique",
		//	"cadeaux-noel",
		"juniors",
		//"tout-a-moins-de-10-euros",
		"arts-loisirs",
		"jeux-educatifs",
		"vetements-homme",
		"cosmetique",
		"sante-mieux-vivre",
		//shippinginformation
		//carte-cdiscount
		//"edito",
		//"clemarche",
		//"CmesApps",
		//"cmesapps",
		//abonnements
		//"boutique-cadeaux",
		//"soldes-promotions",
		//"destockage",
		"vetements-femme",
		"pret-a-porter",
		"informatique",
		"jeux-pc-video-console",
		"livres-bd",
		"dvd",
		"electromenager",
		"bagages",
		"vetements-enfant",
		"au-quotidien",
		"vin-champagne",
		"jardin",
		"bebe-puericulture",
		"photo-numerique",
		"culture-multimedia",
		//"personnalisation-3d",
		"chaussures",
		"auto",
	    "high-tech"};

	public static void main(String[] args){
		try {
			instantiate_connection();
			// making the database clean for the new magasin if it already exists
			cleaning_database();
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}

		
		// looping over all distinct magasins to compute page rank
		for (int i=0;i<magasins.length;i++){
			String magasin = magasins[i];
			// we here loop over depths
			try{
				// fetching data from the Postgresql data base and looping over
				looping_over_urls(magasin);
			} catch (SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with the POSTGRESQL database");
				System.exit(0);
			}

			building_database();		

			compute_page_rank_and_update_database();
		}
	}

	
	private static void cleaning_database() throws SQLException{
		PreparedStatement drop_nodes_table_st = con.prepareStatement(drop_nodes_table);
		drop_nodes_table_st.executeUpdate();
		System.out.println("Dropping the old NODES table");
		
		PreparedStatement create_nodes_table_st = con.prepareStatement(create_nodes_table);
		create_nodes_table_st.executeUpdate();
		System.out.println("Creating the new NODES table");
		
		PreparedStatement create_nodes_index_st = con.prepareStatement(create_nodes_index);
		create_nodes_index_st.executeUpdate();
		System.out.println("Creating the new INDEX for NODES table");
		
		PreparedStatement drop_edges_table_st = con.prepareStatement(drop_edges_table);
		drop_edges_table_st.executeUpdate();
		System.out.println("Dropping the old EDGES table");
		
		PreparedStatement create_edges_table_st = con.prepareStatement(create_edges_table);
		create_edges_table_st.executeUpdate();
		System.out.println("Creating the new EDGES table");
	}
	
	private static void building_database(){
		// we create all nodes at once
		all_nodes_creation();
		// we create all relations at once
		all_relations_creation();
	}

	private static void all_nodes_creation(){
		Iterator<Map.Entry<NodeInfos,Set<String>>> it = nodes_infos.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<NodeInfos,Set<String>> pairs = (Map.Entry<NodeInfos,Set<String>>)it.next();
			NodeInfos url_infos =pairs.getKey();
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
		Iterator<Map.Entry<NodeInfos, Set<String>>> it = nodes_infos.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<NodeInfos, Set<String>> pairs = (Map.Entry<NodeInfos, Set<String>>)it.next();
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

	private static void compute_page_rank_and_update_database() {
		//Init a project - and therefore a workspace
		ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
		pc.newProject();
		Workspace workspace = pc.getCurrentWorkspace();

		//Get controllers and models
		ImportController importController = Lookup.getDefault().lookup(ImportController.class);
		GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getModel();
		AttributeModel attributeModel = Lookup.getDefault().lookup(AttributeController.class).getModel();

		//Import database
		EdgeListDatabaseImpl db = new EdgeListDatabaseImpl();
		db.setDBName("CRAWL4J");
		db.setHost("localhost");
		db.setUsername("postgres");
		db.setPasswd("mogette");
		//        db.setSQLDriver(new MySQLDriver());
		db.setSQLDriver(new PostgreSQLDriver());
		//db.setSQLDriver(new SQLServerDriver());
		db.setPort(5432);
		db.setNodeQuery("SELECT nodes.id AS id, nodes.label AS label FROM nodes");
		//       db.setEdgeQuery("SELECT edges.source AS source, edges.target AS target, edges.name AS label, edges.weight AS weight FROM edges");
		db.setEdgeQuery("SELECT edges.source AS source, edges.target AS target FROM edges");
		ImporterEdgeList edgeListImporter = new ImporterEdgeList();
		System.out.println("Importing all graph data from the PostgreSQL database");

		Container container = importController.importDatabase(db, edgeListImporter);
		container.setAllowAutoNode(false);      //Don't create missing nodes
		container.getLoader().setEdgeDefault(EdgeDefault.DIRECTED);   //Force UNDIRECTED

		//Append imported data to GraphAPI
		importController.process(container, new DefaultProcessor(), workspace);

		System.out.println("Building a directed graph from the imported PostgreSQL data");		
		//See if graph is well imported
		DirectedGraph graph = graphModel.getDirectedGraph();
		System.out.println("Nodes: " + graph.getNodeCount());
		System.out.println("Edges: " + graph.getEdgeCount());

		System.out.println("Computing the page rank for the directed graph");	
		// Computing the page rank
		PageRank pageRank = new PageRank();
		pageRank.setDirected(true);
		pageRank.execute(graphModel, attributeModel);

		System.out.println("Page rank computed !!! ! !");
		// updating the database to store the pageRank
		AttributeColumn col = attributeModel.getNodeTable().getColumn(PageRank.PAGERANK);
		//Iterate over values
		for (Node n : graph.getNodes()) {
			String url_string=n.getNodeData().getLabel();
			System.out.println(url_string);
			Double nodePageRank = (Double)n.getNodeData().getAttributes().getValue(col.getIndex());
			System.out.println("Page rank for node : "+nodePageRank);
			try {
				PreparedStatement st = con.prepareStatement(update_statement);
				// preparing the statement
				st.setDouble(1,nodePageRank);
				st.setString(2,url_string);
				st.executeUpdate();
				System.out.println("URL page rank inserted in database : "+url_string);
				st.close();
			} catch (SQLException e){
				e.printStackTrace();
			}
		}
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

	private static void create_node_without_finding(NodeInfos infos) throws SQLException{
		PreparedStatement insert_st = con.prepareStatement(insert_node_statement,Statement.RETURN_GENERATED_KEYS);
		//(LABEL, MAGASIN, PAGE_TYPE, STATUS_CODE, URL)
		insert_st.setString(1,infos.getUrl());
		insert_st.setString(2,infos.getMagasin());
		insert_st.setString(3,infos.getType());
		insert_st.setInt(4,infos.getStatus());
		insert_st.executeUpdate();
		ResultSet rs = insert_st.getGeneratedKeys();
		int inserted_keys=0;
		if (rs != null && rs.next()) {
			inserted_keys = rs.getInt(1);
		}
		node_locator.put(infos.getUrl(), inserted_keys);
	}

	public static void looping_over_urls(String magasin_to_fetch) throws SQLException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs and outside links from the crawl results database");
		String request = beginning_whole_fetching_request+magasin_to_fetch+end_request;
		PreparedStatement pst = con.prepareStatement(request);
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
			Set<String> outSet = URL_Utilities.parse_nodes_out_links(output_links);
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
}
