package vsm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public abstract class ClusteringMethod {
	protected List<Document> documents;
	public List<Cluster> clusters;
	
	public ClusteringMethod(){
		documents = Document.getAllDocuments();
		for (Iterator<Document> iterator = documents.iterator(); iterator.hasNext();){
			iterator.next().resetNode();
		}
		clusters = new ArrayList<Cluster>();
	}
	
	public abstract List<Cluster> getClusters();
	public abstract HashMap<String, Double> parameters();
}
