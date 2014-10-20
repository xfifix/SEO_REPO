package vsm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DBScan extends ClusteringMethod {

	private Double threshold;
	private Integer minPoints;
	
	public DBScan(Double threshold, Integer minPoints) {
		this.minPoints = minPoints;
		this.threshold = threshold;
	}

	public List<Cluster> getClusters() {
		Iterator<Document> docsIterator = documents.iterator();
		while (docsIterator.hasNext()){
			scan(docsIterator.next());
		}
		return clusters;
	}

	private void scan(Document document) {
		System.out.print("Scanning document: " + document.getFileName());
		if (document.isVisited()){
			System.out.println(": visited");
			return; 
		}
		System.out.println(": not visited");
		document.visit();
		List<Document> neighbors = getNeighbors(document);
		if (largeEnough(neighbors)){
			clusters.add(new DBScanCluster(document, neighbors, this));
		}else{
			document.markAsNoise();
		}
	}

	public List<Document> getNeighbors(Document document) {
		List<Document> result = new ArrayList<Document>();
		Iterator<Document> docsIterator = documents.iterator();
		while (docsIterator.hasNext()){
			Document candidate = docsIterator.next();
			if (document.getSimilarity(candidate) > threshold){
				result.add(candidate);
			}
		}
		return result;
	}
	
	public boolean largeEnough(List<Document> list){
		return list.size() >= minPoints;
	}

	public HashMap<String, Double> parameters() {
		HashMap<String, Double> params = new HashMap<String, Double>();
		params.put("threshold", threshold);
		params.put("minPoints", new Double(minPoints));
		return params;
	}

}
