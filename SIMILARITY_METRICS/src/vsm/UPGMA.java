package vsm;

import java.util.Iterator;

public class UPGMA extends HAC {
	
	public UPGMA(Integer size){
		super(size);
	}

	public Double calculateSimilarity(Cluster cluster1, Cluster cluster2) {
		Iterator<Document> c1Iterator = cluster1.iterator();
		Double total = 0.0;
		while (c1Iterator.hasNext()){
			Document thisDoc = c1Iterator.next();
			Iterator<Document> c2Iterator = cluster2.iterator();
			while (c2Iterator.hasNext()){
				total += thisDoc.getSimilarity(c2Iterator.next());
			}
		}
		return total / (cluster1.documents.size() * cluster2.documents.size());
	}

}
