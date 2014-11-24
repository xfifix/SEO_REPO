package crawl4j.vsm;

import java.util.List;


public class DBScanCluster extends Cluster {

	public DBScanCluster(Document initialDocument, List<Document> neighbors, DBScan owner) {
		super();
		add(initialDocument);
		int size = neighbors.size();
		for (int i=0; i<size; i++){
			Document neighbor = neighbors.get(i);
			if (!neighbor.isVisited()){
				neighbor.visit();
				List<Document> newNeighbors = owner.getNeighbors(neighbor);
				if (owner.largeEnough(newNeighbors)){
					neighbors.addAll(newNeighbors);
					size += newNeighbors.size();
				}
			}
			if (!neighbor.isClustered()){
				add(neighbor);
			}
		}
	}
	
	

	protected Double calculateSimilarity(Cluster cluster) {

		return null;
	}

}
