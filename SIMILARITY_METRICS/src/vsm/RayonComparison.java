package vsm;

import java.util.ArrayList;
import java.util.List;

public class RayonComparison {
	private int counter = 0;
	private String rayoni;
	private String rayonj;
	private Double total = new Double(0);
	
	public void increment(){
		counter++;
	}
	
	public Double getTotal() {
		return total;
	}
	public void setTotal(Double total) {
		this.total = total;
	}
	public String getRayoni() {
		return rayoni;
	}
	public void setRayoni(String rayoni) {
		this.rayoni = rayoni;
	}
	public String getRayonj() {
		return rayonj;
	}
	public void setRayonj(String rayonj) {
		this.rayonj = rayonj;
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
	public List<Double> similarities = new ArrayList<Double>();

}
