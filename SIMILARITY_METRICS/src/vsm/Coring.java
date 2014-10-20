package vsm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Coring {
	
	private Double neighborhood;
	private Integer sizeThreshold;
	
	public Coring(Double neighborhood, Integer sizeThreshold){
		this.neighborhood = neighborhood;
		this.sizeThreshold = sizeThreshold;
	}
	
	public List<Core> run(){
		List<Core> cores = new ArrayList<Core>();
		for (Iterator<Document> docsIterator = Document.getAllDocuments().iterator(); docsIterator.hasNext();){
			Document doc = docsIterator.next();
			Integer size = doc.getNeighborHoodSize(neighborhood);
			System.out.println(doc.getFileName() + " - neighborhood size: " + size);
			if (size >= sizeThreshold)
				cores.add(new Core(doc, size));
		}
		return consolidateCores(cores);
	}

	private List<Core> consolidateCores(List<Core> cores) {
		System.out.println("Consolidating cores");
		List<Core> results = new ArrayList<Core>();
		Collections.sort(cores);
		for (Iterator<Core> coresIterator = cores.iterator(); coresIterator.hasNext();){
			Core core = coresIterator.next();
			boolean reduntantCore = false;
			for (Iterator<Core> resultsIterator = results.iterator(); resultsIterator.hasNext();){
				if (resultsIterator.next().getDocument().getSimilarity(core.getDocument()) > neighborhood){
					reduntantCore = true;
					break;
				}
			}
			System.out.println("Core " + core.getDocument().getFileName() + (reduntantCore ? " ignored" : " added"));
			if (!reduntantCore)
				results.add(core);
		}
		return results;
	}
	
}
