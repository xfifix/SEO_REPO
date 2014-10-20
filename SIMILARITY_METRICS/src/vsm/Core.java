package vsm;

public class Core implements Comparable<Core>{
	
	private Document document;
	private Integer neighborhoodSize;
	
	public Core(Document doc, Integer size){
		this.document = doc;
		this.neighborhoodSize = size;
	}

	public int compareTo(Core core) {
		return core.neighborhoodSize - this.neighborhoodSize;
	}

	public Document getDocument() {
		return document;
	}

	public Integer getNeighborhoodSize() {
		return neighborhoodSize;
	}
	
}
