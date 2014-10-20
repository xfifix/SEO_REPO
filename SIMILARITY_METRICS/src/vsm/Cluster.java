package vsm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public abstract class Cluster {

	protected List<Document> documents;
	protected HashMap<Cluster, Double> similarities;
	
	protected Cluster(){
		documents = new ArrayList<Document>();
		similarities = new HashMap<Cluster, Double>();
	}
	
	public void add(Document doc){
		documents.add(doc);
		doc.markAsClustered();
	}
	
	public Integer size(){
		return documents.size();
	}
	
	public void empty(){
		documents = new ArrayList<Document>();
	}
	
	public Iterator<Document> iterator(){
		return documents.iterator();
	}
	
	protected abstract Double calculateSimilarity(Cluster cluster);
	
	public Double getSimilarity(Cluster cluster){
		Double similarity = similarities.get(cluster);
		if (similarity == null){
			similarity = calculateSimilarity(cluster);
			this.similarities.put(cluster, similarity);
			cluster.similarities.put(this, similarity);
		}
		return similarity;
	}
	
}
