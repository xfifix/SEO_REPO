package crawl4j.facettesutility;

import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class FacettesUtility {
	@SuppressWarnings("unchecked")
	public static String getFacettesJSONStringToStore(List<FacettesInfo> facettes_info){
		JSONArray facettesArray = new JSONArray();
		for (FacettesInfo info : facettes_info){
			JSONObject facetteObject = new JSONObject();
			facetteObject.put("facette_name", info.getFacetteName());
			facetteObject.put("facette_value", info.getFacetteValue());
			facetteObject.put("facette_count", info.getFacetteCount());
			facettesArray.add(facetteObject);
		}
		return facettesArray.toJSONString();
	}
}
