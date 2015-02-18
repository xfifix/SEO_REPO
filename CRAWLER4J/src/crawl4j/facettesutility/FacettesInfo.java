package crawl4j.facettesutility;

public class FacettesInfo{

	private String facetteName;
	private String facetteValue;
	private int facetteCount;

	public String getFacetteName() {
		return facetteName;
	}
	public void setFacetteName(String facetteName) {
		this.facetteName = facetteName;
	}
	public String getFacetteValue() {
		return facetteValue;
	}
	public void setFacetteValue(String facetteValue) {
		this.facetteValue = facetteValue;
	}
	public int getFacetteCount() {
		return facetteCount;
	}
	public void setFacetteCount(int facetteCount) {
		this.facetteCount = facetteCount;
	}
}