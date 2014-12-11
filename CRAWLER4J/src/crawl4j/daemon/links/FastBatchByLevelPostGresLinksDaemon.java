package crawl4j.daemon.links;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.gephi.data.attributes.api.AttributeColumn;
import org.gephi.data.attributes.api.AttributeController;
import org.gephi.data.attributes.api.AttributeModel;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.graph.api.Node;
import org.gephi.graph.api.UndirectedGraph;
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

public class FastBatchByLevelPostGresLinksDaemon {

	private static String database_con_path = "/home/sduprey/My_Data/My_Postgre_Conf/crawler4j.properties";
	private static int depth_threshold = 6;

	// global cache which is never flushed until insertion of all relations for all depths
	private static Map<String,Integer> url_id_mapping = new HashMap<String,Integer>();
	// global cache which is never flushed listing all nodes not found
	private static List<String> url_not_found = new ArrayList<String>();
	

	// local cache for each depth which is flushed after each depth completion
	private static List<NodeInfos> nodes_infos = new ArrayList<NodeInfos>();

	private static int counter = 0;
	private static int node_id_counter = 0;
	private static String fetching_nodes_by_level_request= "SELECT URL, STATUS_CODE, MAGASIN, PAGE_TYPE FROM CRAWL_RESULTS WHERE DEPTH BETWEEN ";
	private static String order_by_depth = " ORDER BY DEPTH ";
	private static String insert_node_statement ="INSERT INTO NODES (ID,LABEL, MAGASIN, PAGE_TYPE, STATUS_CODE)"
			+ " VALUES(?,?,?,?,?)";	

	// local cache for each depth which is flushed after each depth completion
	private static List<EdgeInfos> edges_infos = new ArrayList<EdgeInfos>();

	private static String fetching_relations_by_level_request= "SELECT URL, LINKS FROM CRAWL_RESULTS WHERE DEPTH BETWEEN ";
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
		// we create all nodes by levels looping over the depth
		for (int depth=1;depth<depth_threshold;depth ++){
			try{
				// fetching data from the Postgresql data base and looping over
				looping_over_urls_for_node_creation(depth);
				// creating all nodes for the specified depth
				all_nodes_batch_creation(depth);
				clearing_nodes();
			} catch (SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with the POSTGRESQL database");
				System.exit(0);
			}
		}

		// we reset our counter and start again
		counter =0;

		// to do to be done to do to be done to do
		// we create all nodes by levels looping over the depth
		for (int depth=1;depth<depth_threshold;depth ++){
			try{
				// fetching data from the Postgresql data base and looping over
				looping_over_urls_for_relations_creation(depth);
				all_edges_batch_creation(depth);
				clearing_edges();
			} catch (SQLException e){
				e.printStackTrace();
				System.out.println("Trouble with the POSTGRESQL database");
				System.exit(0);
			}
		}
		
		// we here list all nodes never found 
		listingNofFoundNodes();
		
		// we don't do it here as the computation might be heavy
		// we delegate to another subcrawler
		//		// computing page rank
		//		compute_page_rank();
	}

	private static void listingNofFoundNodes(){
		for (String urlNotFound : url_not_found){
			System.out.println("URL node not found : "+urlNotFound);
		}
	}

	private static void clearing_nodes(){
		nodes_infos.clear();
	}

	private static void clearing_edges(){
		edges_infos.clear();
	}

	private static void all_nodes_batch_creation(int depth){
		// batch insertion
		try {
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_node_statement);
			for (NodeInfos info : nodes_infos){
				node_id_counter++;
				//INSERT INTO NODES (ID,LABEL, MAGASIN, PAGE_TYPE, STATUS_CODE)"
				st.setInt(1,node_id_counter);
				st.setString(2,info.getUrl());
				st.setString(3,info.getMagasin());
				st.setString(4,info.getType());
				st.setInt(5,info.getStatus());
				st.addBatch();
				url_id_mapping.put(info.getUrl(), node_id_counter);
			}
			st.executeBatch();		 
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble inserting a depht node creation batch");
			System.exit(0);
		}
		System.out.println(Thread.currentThread()+"Committing batch for depth : " + depth);
	}

	private static void all_edges_batch_creation(int depth){
		// batch insertion
		try {
			con.setAutoCommit(false);
			PreparedStatement st = con.prepareStatement(insert_relation_statement);
			for (EdgeInfos info : edges_infos){
				//INSERT INTO EDGES (SOURCE, TARGET)
				st.setInt(1,info.getBeginning());
				st.setInt(2,info.getEnding());
				st.addBatch();
			}
			st.executeBatch();		 
			con.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Trouble inserting a depth relation creation batch");
			System.exit(0);
		}
		System.out.println(Thread.currentThread()+"Committing batch for depth : "+depth);
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

	public static void looping_over_urls_for_node_creation(int depth) throws SQLException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs between depth "+depth +" and "+(depth +1));
		String level_statement = fetching_nodes_by_level_request + depth + " and "+(depth +1)+order_by_depth;
		PreparedStatement pst = con.prepareStatement(level_statement);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			counter++;
			String url_node = rs.getString(1);
			Integer status = rs.getInt(2);
			String magasin = rs.getString(3);
			String page_type = rs.getString(4);
			NodeInfos result = new NodeInfos();
			result.setUrl(url_node);
			result.setStatus(status);
			result.setMagasin(magasin);
			result.setType(page_type);
			nodes_infos.add(result);
			System.out.println("Putting into cache URL node number :"+counter + " : " +url_node +" for depth : "+depth);
		}
		pst.close();
	}

	public static void looping_over_urls_for_relations_creation(int depth) throws SQLException{
		// here is the links daemon starting point
		// getting all URLS and out.println links for each URL
		System.out.println("Getting all URLs between depth "+depth +" and "+(depth +1));
		String level_statement = fetching_relations_by_level_request + depth + " and "+(depth +1)+order_by_depth;
		PreparedStatement pst = con.prepareStatement(level_statement);
		ResultSet rs = pst.executeQuery();
		while (rs.next()) {
			counter++;
			String url_node = rs.getString(1);
			String output_links = rs.getString(2);
			Set<String> outSet = URL_Utilities.parse_nodes_out_links(output_links);
			build_all_edges(url_node,outSet);
			System.out.println("Getting into cache relations for URL number :"+counter + " : " +url_node);
		}
		pst.close();
	}

	private static void build_all_edges(String beginningNode, Set<String> endingNodes){
		Integer beginningId =url_id_mapping.get(beginningNode);
		if (beginningId != null){
			for (String endingNode : endingNodes){
				Integer endingId =url_id_mapping.get(endingNode);
				if (endingId != null){
					EdgeInfos edge = new EdgeInfos();
					edge.setBeginning(beginningId);
					edge.setEnding(endingId);
					edges_infos.add(edge);
				} else {
					// the beginning node is not found
					System.out.println("The ending node is not found : we don't create it");
					System.out.println("URL : " + endingNode);
					url_not_found.add(endingNode);
				}
			}
		} else {
			// the beginning node is not found
			System.out.println("The beginning node is not found");
			url_not_found.add(beginningNode);
		}
	}

	// we keep it even if we don't use it here ( if everything runs fine memory speaking, we'll add it
	private static void compute_page_rank() {
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
		db.setNodeQuery("SELECT nodes.id AS id, nodes.label AS label, nodes.url FROM nodes");
		//       db.setEdgeQuery("SELECT edges.source AS source, edges.target AS target, edges.name AS label, edges.weight AS weight FROM edges");
		db.setEdgeQuery("SELECT edges.source AS source, edges.target AS target FROM edges");
		ImporterEdgeList edgeListImporter = new ImporterEdgeList();
		Container container = importController.importDatabase(db, edgeListImporter);
		container.setAllowAutoNode(false);      //Don't create missing nodes
		container.getLoader().setEdgeDefault(EdgeDefault.DIRECTED);   //Force UNDIRECTED

		//Append imported data to GraphAPI
		importController.process(container, new DefaultProcessor(), workspace);

		//See if graph is well imported
		UndirectedGraph graph = graphModel.getUndirectedGraph();
		System.out.println("Nodes: " + graph.getNodeCount());
		System.out.println("Edges: " + graph.getEdgeCount());

		// Computing the page rank

		PageRank pageRank = new PageRank();
		pageRank.setDirected(true);
		pageRank.execute(graphModel, attributeModel);


		System.out.println("Page rank computed !!! ! !");

		//Get Centrality column created
		AttributeColumn col = attributeModel.getNodeTable().getColumn(PageRank.PAGERANK);

		//Iterate over values
		for (Node n : graph.getNodes()) {
			Double nodePageRank = (Double)n.getNodeData().getAttributes().getValue(col.getIndex());
			System.out.println("Page rank for node : "+nodePageRank);
		}
	}

	private static class EdgeInfos{
		private int beginning;
		private int ending;
		public int getBeginning() {
			return beginning;
		}
		public void setBeginning(int beginning) {
			this.beginning = beginning;
		}
		public int getEnding() {
			return ending;
		}
		public void setEnding(int ending) {
			this.ending = ending;
		}
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

