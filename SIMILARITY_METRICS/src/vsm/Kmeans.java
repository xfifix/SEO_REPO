package vsm;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Kmeans extends ClusteringMethod {
	
	private Integer k;
	
	public Kmeans(Integer k){
		super();
		this.k = k;
		for(int i=0; i<k; i++){
			clusters.add(new KmeansCluster(documents.get(i)));
		}
		System.out.println("Initiating Kmeans with k = " + k);
	}
	
	public List<Cluster> getClusters(){
		boolean clustersUpdated = true;
		Iterator<Document> docsIterator;
		Iterator<Cluster> clustersIterator;
		while(clustersUpdated){
			clustersIterator = clusters.iterator();
			while (clustersIterator.hasNext()){
				clustersIterator.next().empty();
			}
			
			docsIterator = documents.iterator();
			while(docsIterator.hasNext()){
				assignDocument(docsIterator.next());
			}
			
			clustersIterator = clusters.iterator();
			clustersUpdated = false;
			while(clustersIterator.hasNext()){
				clustersUpdated = ((KmeansCluster)clustersIterator.next()).updateCentroid() || clustersUpdated;
			}
			System.out.print("\n");
		}		
		return clusters;
	}

	private void assignDocument(Document document) {
		Iterator<Cluster> clustersIterator = clusters.iterator();
		Double max = 0.0;
		Cluster maxCluster = null;
		Double sim;
		Cluster cluster;
		while(clustersIterator.hasNext()){
			cluster = clustersIterator.next();
			sim = ((KmeansCluster)cluster).getSimilarity(document);
			if (sim > max){
				max = sim;
				maxCluster = cluster;
			}
		}
		if (maxCluster != null){
			maxCluster.add(document);
		}
	}
	
	public HashMap<String, Double> parameters() {
		HashMap<String, Double> params = new HashMap<String, Double>();
		params.put("K", new Double(k));
		return params;
	}
	
}
