package crawl4j.attributesutility;

import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class AttributesUtility {
	@SuppressWarnings("unchecked")
	public static String getAttributesJSONStringToStore(List<AttributesInfo> attributes_info){
		JSONArray attributesArray = new JSONArray();
		for (AttributesInfo info : attributes_info){
			JSONObject attributeObject = new JSONObject();
			attributeObject.put("attribute_name", info.getData_name());
			attributeObject.put("attribute_value", info.getData());
			attributesArray.add(attributeObject);
		}
		return attributesArray.toJSONString();
	}
}
