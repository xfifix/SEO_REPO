package crawl4j.daemon.links;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

public class ComputePageRank {

	private static String update_statement ="UPDATE CRAWL_RESULTS SET PAGE_RANK=? WHERE URL=?";
	private static Connection con;

	public static void main(String[] args){
		try {
			instantiate_connection();
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Trouble with the POSTGRESQL database");
			System.exit(0);
		}
		compute_page_rank_and_update_database();
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
		Container container = importController.importDatabase(db, edgeListImporter);
		container.setAllowAutoNode(false);      //Don't create missing nodes
		container.getLoader().setEdgeDefault(EdgeDefault.DIRECTED);   //Force UNDIRECTED

		//Append imported data to GraphAPI
		importController.process(container, new DefaultProcessor(), workspace);

		//See if graph is well imported
		DirectedGraph graph = graphModel.getDirectedGraph();
		System.out.println("Nodes: " + graph.getNodeCount());
		System.out.println("Edges: " + graph.getEdgeCount());

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
		// instantiating database connection
		String url="jdbc:postgresql://localhost/CRAWL4J";
		String user="postgres";
		String passwd="mogette";
		con = DriverManager.getConnection(url, user, passwd);
	}

}
