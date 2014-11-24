package crawl4j.vsm;

import java.util.ArrayList;
import java.util.List;

public class MagasinComparison {
	private  int counter = 0;
	private String magasini;
	private String magasinj;
	private List<Double> similarities = new ArrayList<Double>();
	private Double total = new Double(0);
	
	public void increment(){
		setCounter(getCounter() + 1);
	}
	public Double getTotal() {
		return total;
	}
	public void setTotal(Double total) {
		this.total = total;
	}
	public Double average;
	
	public Double getAverage() {
		return average;
	}
	public void setAverage(Double average) {
		this.average = average;
	}
	public String getMagasini() {
		return magasini;
	}
	public void setMagasini(String magasini) {
		this.magasini = magasini;
	}
	public String getMagasinj() {
		return magasinj;
	}
	public void setMagasinj(String magasinj) {
		this.magasinj = magasinj;
	}
	
	public List<Double> getSimilarities() {
		return similarities;
	}
	public void setSimilarities(List<Double> similarities) {
		this.similarities = similarities;
	}
	public int getCounter() {
		return counter;
	}
	public void setCounter(int counter) {
		this.counter = counter;
	}
	
}
