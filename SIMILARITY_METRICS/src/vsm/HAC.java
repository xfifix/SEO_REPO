package vsm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public abstract class HAC extends ClusteringMethod implements ClusterSimilarity{

	private Integer resultSize;
	
	public HAC(Integer size) {
		super();
		resultSize = size;
		Iterator<Document> docsIterator = documents.iterator();
		while (docsIterator.hasNext()){
			clusters.add(new HACluster(docsIterator.next(), this));
		}
	}

	public List<Cluster> getClusters() {
		while (clusters.size() > resultSize){
			System.out.println(clusters.size());
			agglomerate();
		}
		return clusters;
	}

	private void agglomerate() {
		Cluster maxCluster1 = null;
		Cluster maxCluster2 = null;
		Double maxSim = -1.0;
		for (int i=0; i<clusters.size(); i++){
			for (int j=i+1; j<clusters.size(); j++){
				Double sim = clusters.get(i).getSimilarity(clusters.get(j));
				if (sim > maxSim){
					maxCluster1 = clusters.get(i);
					maxCluster2 = clusters.get(j);
					maxSim = sim;
				}
			}
		}
		merge(maxCluster1, maxCluster2);
	}

	private void merge(Cluster cluster1, Cluster cluster2) {
		List<Document> mergedDocs = new ArrayList<Document>();
		mergedDocs.addAll(cluster1.documents);
		mergedDocs.addAll(cluster2.documents);
		clusters.remove(cluster1);
		clusters.remove(cluster2);
		clusters.add(new HACluster(mergedDocs, this));
	}
	
	public HashMap<String, Double> parameters() {
		HashMap<String, Double> params = new HashMap<String, Double>();
		params.put("size", new Double(resultSize));
		return params;
	}

}
