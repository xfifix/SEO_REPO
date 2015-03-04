package crawl4j.arbo.semantic.utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import crawl4j.arbo.semantic.LinkInfo;

public class SemanticCrawlerUtility {

	public static Map<String, Integer> getIncomingLinkSemanticCount(Set<LinkInfo> infos){
		List<String> anchor_list = new ArrayList<String>();
		for (LinkInfo info : infos){
			String linkAnchor = info.getAnchor();
			if (linkAnchor != null){
				anchor_list.add(linkAnchor);
			}
		}
		Map<String, Integer> counting_map = new HashMap<String, Integer>();
		Set<String> unique = new HashSet<String>(anchor_list);
		for (String key : unique) {
			counting_map.put(key, Collections.frequency(anchor_list, key));
		}
		return counting_map;
	}

	public static String formatIncomingLinkSemantic(Set<String> entry_set){
		return StringUtils.join(entry_set,"@");
	}

	@SuppressWarnings("unchecked")
	public static String getIncomingLinkSemanticCountJSON(Set<LinkInfo> infos){
		List<String> anchor_list = new ArrayList<String>();
		for (LinkInfo info : infos){
			String linkAnchor = info.getAnchor();
			if (linkAnchor != null){
				anchor_list.add(linkAnchor);
			}
		}

		Set<String> unique = new HashSet<String>(anchor_list);
		JSONArray anchorsCountArray = new JSONArray();
		for (String key : unique) {
			JSONObject anchorCountObject = new JSONObject();
			anchorCountObject.put("anchor", key);
			anchorCountObject.put("count", Collections.frequency(anchor_list, key));
			anchorsCountArray.add(anchorCountObject);
		}
		return anchorsCountArray.toJSONString();
	}

	@SuppressWarnings("unchecked")
	public static String getIncomingLinkSemanticJSON(Set<LinkInfo> infos){
		JSONArray anchorsArray = new JSONArray();
		for (LinkInfo info : infos){
			JSONObject anchorObject = new JSONObject();
			anchorObject.put("anchor", info.getAnchor());
			anchorObject.put("url", info.getUrl());
			anchorsArray.add(anchorObject);
		}
		return anchorsArray.toJSONString();
	}
}




