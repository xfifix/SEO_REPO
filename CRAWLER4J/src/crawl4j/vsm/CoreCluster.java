package crawl4j.vsm;

public class CoreCluster extends Cluster {
	
	private Core core;
	
	public CoreCluster(Core core){
		this.core = core;
	}

	@Override
	protected Double calculateSimilarity(Cluster cluster) {
		return null;
	}

	public Double getSimilarity(Document document) {
		return this.core.getDocument().getSimilarity(document);
	}

}
