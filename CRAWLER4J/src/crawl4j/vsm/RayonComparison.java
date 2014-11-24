package crawl4j.vsm;

import java.util.ArrayList;
import java.util.List;

public class RayonComparison {
	private String rayoni;
	private String rayonj;
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

	private int rayon_i_size = 0;
	private int rayon_j_size = 0;
	private double percent_exactly_matching;
	private double average_similarity;

	private List<RayonLevelDoublon> doublons = new ArrayList<RayonLevelDoublon>();

	public int getRayon_i_size() {
		return rayon_i_size;
	}

	public void setRayon_i_size(int rayon_i_size) {
		this.rayon_i_size = rayon_i_size;
	}

	public int getRayon_j_size() {
		return rayon_j_size;
	}

	public void setRayon_j_size(int rayon_j_size) {
		this.rayon_j_size = rayon_j_size;
	}

	public double getPercent_exactly_matching() {
		return percent_exactly_matching;
	}

	public void setPercent_exactly_matching(double percent_exactly_matching) {
		this.percent_exactly_matching = percent_exactly_matching;
	}

	public double getAverage_similarity() {
		return average_similarity;
	}

	public void setAverage_similarity(double average_similarity) {
		this.average_similarity = average_similarity;
	}

	public List<RayonLevelDoublon> getDoublons() {
		return doublons;
	}

	public void setDoublons(List<RayonLevelDoublon> doublons) {
		this.doublons = doublons;
	}

	
}
