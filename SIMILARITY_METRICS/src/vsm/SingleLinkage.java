package vsm;

import java.util.Iterator;

public class SingleLinkage extends HAC {
	
	public SingleLinkage(Integer size){
		super(size);
	}

	public Double calculateSimilarity(Cluster cluster1, Cluster cluster2) {
		Iterator<Document> c1Iterator = cluster1.iterator();
		Double min = 1.0;
		while (c1Iterator.hasNext()){
			Document thisDoc = c1Iterator.next();
			Iterator<Document> c2Iterator = cluster2.iterator();
			while (c2Iterator.hasNext()){
				Double sim = thisDoc.getSimilarity(c2Iterator.next());
				if (sim < min){
					min = sim;
				}
			}
		}
		return min;
	}

}
