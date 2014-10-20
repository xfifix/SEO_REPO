package vsm;

import java.util.List;

public class HACluster extends Cluster {
	
	private ClusterSimilarity similarityDelegate;
	
	public HACluster(Document initialDocument, ClusterSimilarity delegate){
		super();
		documents.add(initialDocument);
		similarityDelegate = delegate;
	}
	
	public HACluster(List<Document> documents, ClusterSimilarity delegate){
		this.documents = documents;
		similarityDelegate = delegate;
	}
	
	protected Double calculateSimilarity(Cluster cluster){
		return similarityDelegate.calculateSimilarity(this, cluster);
	}

}
