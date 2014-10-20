package vsm;

public class KmeansCluster extends Cluster {
	
	private Document centroid;
	
	public KmeansCluster(Document centroid){
		super();
		this.centroid = centroid;
	}
	
	public Double getSimilarity(Document document){
		return centroid.getSimilarity(document);
	}
	
	public boolean updateCentroid(){
		Document newCetroid = Document.mean(documents);
		System.out.print(newCetroid.magnitude() + ", ");
		if (centroid.isEquivalent(newCetroid)){
			return false;
		}
		centroid = newCetroid;
		return true;
	}

	protected Double calculateSimilarity(Cluster cluster) {
		return centroid.getSimilarity( ((KmeansCluster)cluster).centroid );
	}
}
