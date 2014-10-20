package vsm;

import java.util.Iterator;

public class CompleteLinkage extends HAC {
	
	public CompleteLinkage(Integer size){
		super(size);
	}

	public Double calculateSimilarity(Cluster cluster1, Cluster cluster2) {
		Iterator<Document> c1Iterator = cluster1.iterator();
		Double max = 0.0;
		while (c1Iterator.hasNext()){
			Document thisDoc = c1Iterator.next();
			Iterator<Document> c2Iterator = cluster2.iterator();
			while (c2Iterator.hasNext()){
				Double sim = thisDoc.getSimilarity(c2Iterator.next());
				if (sim > max){
					max = sim;
				}
			}
		}
		return max;
	}

}
