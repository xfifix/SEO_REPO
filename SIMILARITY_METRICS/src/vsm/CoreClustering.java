package vsm;

import java.util.HashMap;
import java.util.List;

public class CoreClustering extends ClusteringMethod {
	
	private Double neighborhood;
	private Integer sizeThreshold;

	public CoreClustering(Double neighborhood, Integer sizeThreshold){
		this.neighborhood = neighborhood;
		this.sizeThreshold = sizeThreshold;
	}
	
	@Override
	public List<Cluster> getClusters() {
		System.out.println("Starting Coring");
		List<Core> cores = new Coring(neighborhood, sizeThreshold).run();
		for (Core core:cores){
			clusters.add(new CoreCluster(core));
		}
		System.out.println("Coring done, assigning documents");
		for (Document doc:documents){
			assignDocument(doc);
		}
		return clusters;
	}

	private void assignDocument(Document document) {
		Double max = 0.0;
		Cluster maxCluster = null;
		Double sim;
		for (Cluster cluster:clusters){
			sim = ((CoreCluster)cluster).getSimilarity(document);
			if (sim > max){
				max = sim;
				maxCluster = cluster;
			}
		}
		if (maxCluster != null){
			maxCluster.add(document);
		}
	}

	@Override
	public HashMap<String, Double> parameters() {
		HashMap<String, Double> params = new HashMap<String, Double>();
		params.put("neighborhood", neighborhood);
		params.put("sizeThreshold", new Double(sizeThreshold));
		return params;
	}

}
